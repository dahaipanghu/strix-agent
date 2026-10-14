"""SDK contract doubles; these tests do NOT claim a live SDK/provider evaluation."""
from __future__ import annotations

import json
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from strix.collaboration import ExecutionContext, ExecutionResult, Request, Scope, Store, Task
from strix.collaboration.sdk import (
    ExecutorBinding, ModelCritic, ModelPlanner, SandboxExecutor, build_executor_agent,
)


def task(mode="review"):
    return Task(id="a", target="asset:local", objective="Inspect fixture", mode=mode,
                acceptance_criteria=("Find fixture",))


@pytest.fixture
def sdk(monkeypatch):
    calls = []

    async def run(agent, input, **kwargs):
        calls.append((agent, json.loads(input), kwargs))
        return SimpleNamespace(final_output={"tasks": [task().model_dump(mode="json")]})

    fake = SimpleNamespace(Agent=lambda **kw: SimpleNamespace(**kw),
                           Runner=SimpleNamespace(run=run), RunHooks=object,
                           RunConfig=lambda **kw: SimpleNamespace(**kw), calls=calls)
    monkeypatch.setitem(sys.modules, "agents", fake)
    return fake


@pytest.mark.asyncio
async def test_planner_is_tool_free_and_isolated(sdk):
    planner = ModelPlanner("planner-model")
    req = Request(session_id="s", request_id="r", goal="test")
    await planner.plan(req, Scope(targets=("asset:local",)))
    await planner.plan(req, Scope(targets=("asset:local",)))
    first, second = sdk.calls
    assert first[0] is not second[0]
    assert first[0].tools == [] and first[0].handoffs == []
    assert first[0].model == "planner-model"
    assert "session" not in first[2]
    assert first[2]["run_config"].tracing_disabled


@pytest.mark.asyncio
async def test_critic_has_own_model_and_no_executor_history(sdk):
    async def run(agent, input, **kwargs):
        sdk.calls.append((agent, json.loads(input), kwargs))
        return SimpleNamespace(final_output={"task_id": "a", "verdict": "reject",
                                             "reason": "No evidence", "evidence_ids": []})

    sdk.Runner.run = run
    output = await ModelCritic("critic-model").review(
        task(), ExecutionResult(task_id="a", summary="claim"), (),
    )
    agent, payload, kwargs = sdk.calls[0]
    assert output.verdict == "reject"
    assert agent.model == "critic-model" and agent.tools == [] and agent.handoffs == []
    assert set(payload) == {"task", "claim", "untrusted_evidence"}
    assert "session" not in kwargs


@pytest.mark.asyncio
async def test_global_model_override_is_rejected(sdk):
    planner = ModelPlanner("planner-model", run_config=SimpleNamespace(model="wrong"))
    with pytest.raises(ValueError):
        await planner.plan(Request(session_id="s", request_id="r", goal="test"),
                           Scope(targets=("asset:local",)))


@pytest.fixture
def factory(monkeypatch):
    tools = [SimpleNamespace(name=name) for name in ["view_request", "create_agent",
                                                    "create_vulnerability_report", "repeat_request",
                                                    "agent_finish", "call_mcp"]]

    def build(**kwargs):
        return SimpleNamespace(tools=tools, capabilities=["shell", "filesystem"],
                               handoffs=[], instructions=kwargs["instructions_override"])

    monkeypatch.setitem(sys.modules, "strix.agents.factory", SimpleNamespace(build_strix_agent=build))
    return tools


@pytest.mark.parametrize("mode,expected", [("review", {"view_request"}),
                                          ("active", {"view_request", "repeat_request"})])
def test_native_factory_role_permissions(factory, mode, expected):
    agent = build_executor_agent(task(mode), model="executor-model")
    assert {tool.name for tool in agent.tools} == expected
    assert all(tool not in factory or all(tool is not original for original in factory)
               for tool in agent.tools)
    assert agent.output_type is ExecutionResult
    assert agent.model == "executor-model" and agent.tool_use_behavior == "run_llm_again"
    assert bool(agent.capabilities) == (mode == "active")


def test_factory_cannot_override_role(factory):
    with pytest.raises(ValueError):
        build_executor_agent(task(), model="e", extra_tools=["create_agent"])


@pytest.mark.asyncio
async def test_executor_attaches_hook_evidence_not_model_ids(sdk):
    store = Store()
    req = Request(session_id="s", request_id="r", goal="test")
    job, _ = store.claim(req, Scope(targets=("asset:local",)))
    context = ExecutionContext(job, task(), 1, "test", (), None, store)
    cleaned = []

    @asynccontextmanager
    async def bind(ctx):
        assert ctx is context
        try:
            agent = SimpleNamespace(tools=[SimpleNamespace(name="view_request")], handoffs=[],
                                    capabilities=[])
            yield ExecutorBinding(agent, None, {}, frozenset({"view_request"}))
        finally:
            cleaned.append(True)

    async def run(agent, input, **kwargs):
        hooks = kwargs["hooks"]
        await hooks.on_tool_end(SimpleNamespace(tool_arguments='{"request_id":"1"}'), agent,
                                SimpleNamespace(name="view_request"), "actual observed fixture")
        await hooks.on_tool_end(SimpleNamespace(), agent,
                                SimpleNamespace(name="think"), "not evidence")
        return SimpleNamespace(final_output=ExecutionResult(task_id="a", summary="read",
                                                             evidence_ids=("forged",)))

    sdk.Runner.run = run
    try:
        output = await SandboxExecutor(bind).execute(context)
        assert len(output.evidence_ids) == 1 and output.evidence_ids != ("forged",)
        evidence = store.read_evidence(job, task(), output.evidence_ids)
        assert "actual observed fixture" in evidence[0].content
        assert cleaned == [True]
    finally:
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("violation", ["tool", "capability", "handoff", "evidence-tool"])
async def test_binding_cannot_escalate_review(sdk, violation):
    store = Store()
    context = ExecutionContext("job", task(), 1, "test", (), None, store)

    @asynccontextmanager
    async def bind(ctx):
        agent = SimpleNamespace(tools=[], capabilities=[], handoffs=[])
        evidence_tools = frozenset({"view_request"})
        if violation == "tool":
            agent.tools = [SimpleNamespace(name="create_agent")]
        elif violation == "capability":
            agent.capabilities = ["shell"]
        elif violation == "handoff":
            agent.handoffs = ["another-agent"]
        else:
            evidence_tools = frozenset({"exec_command"})
        yield ExecutorBinding(agent, None, {}, evidence_tools)

    try:
        with pytest.raises(ValueError):
            await SandboxExecutor(bind).execute(context)
        assert sdk.calls == []
    finally:
        store.close()
