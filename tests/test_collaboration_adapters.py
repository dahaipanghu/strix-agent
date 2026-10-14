"""SDK shape tests use doubles; native process tests use harmless local fixtures."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from strix.collaboration import ExecutionContext, ExecutionResult, Plan, Request, Review, Scope, Store, Task
from strix.collaboration.native import NativeStrixExecutor
from strix.collaboration.sdk import Artifact, SDKArtifactExecutor, SDKCritic, SDKPlanner


@pytest.fixture
def context() -> Iterator[ExecutionContext]:
    store = Store()
    req = Request(session_id="s", request_id="r", goal="Review")
    job_id, _ = store.claim(req, Scope(targets=("fixture",), allow_active=True))
    item = Task(id="t", target="fixture", objective="Inspect",
                acceptance_criteria=("Supply evidence",), mode="active")
    yield ExecutionContext(job_id, item, 1, "Inspect", (), None, store)
    store.close()


def fake_worker(tmp_path: Path, *, status: str = "completed", exit_code: int = 0, delay: float = 0) -> tuple[str, ...]:
    script = tmp_path / "fixture_worker.py"
    script.write_text(
        "import pathlib,json,sys,time,os\n"
        f"pathlib.Path({str(tmp_path / 'pid')!r}).write_text(str(os.getpid()))\n"
        f"time.sleep({delay!r})\n"
        "root=pathlib.Path('strix_runs/fake');root.mkdir(parents=True)\n"
        f"(root/'run.json').write_text(json.dumps({{'status': {status!r}}}))\n"
        "(root/'vulnerabilities.json').write_text('[]')\n"
        f"sys.exit({exit_code})\n", encoding="utf-8",
    )
    return (sys.executable, str(script))


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [0, 2])
async def test_native_adapter_uses_process_and_labels_reports(tmp_path: Path, context: ExecutionContext, code: int) -> None:
    executor = NativeStrixExecutor(model="test-model", work_root=tmp_path / "jobs",
                                   command=fake_worker(tmp_path, exit_code=code))
    result = await executor.execute(context)
    evidence = context.store.read_evidence(context.job_id, context.task, result.evidence_ids)
    assert evidence[0].kind == "tool_output" and evidence[1].kind == "model_report"
    assert json.loads(evidence[0].content)["exit_code"] == code
    assert list((tmp_path / "jobs").iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status,code", [("running", 0), ("completed", 1)])
async def test_native_never_accepts_incomplete_scan(tmp_path: Path, context: ExecutionContext, status: str, code: int) -> None:
    executor = NativeStrixExecutor(model="test-model", work_root=tmp_path / "jobs",
                                   command=fake_worker(tmp_path, status=status, exit_code=code))
    with pytest.raises((ValueError, RuntimeError)):
        await executor.execute(context)


@pytest.mark.asyncio
async def test_native_refuses_review_mode(tmp_path: Path, context: ExecutionContext) -> None:
    executor = NativeStrixExecutor(model="test-model", work_root=tmp_path / "jobs")
    with pytest.raises(ValueError, match="active"):
        await executor.execute(replace(context, task=context.task.model_copy(update={"mode": "review"})))


@pytest.mark.asyncio
async def test_native_cancellation_stops_process(tmp_path: Path, context: ExecutionContext) -> None:
    executor = NativeStrixExecutor(model="test-model", work_root=tmp_path / "jobs",
                                   command=fake_worker(tmp_path, delay=60))
    pending = asyncio.create_task(executor.execute(context))
    try:
        async with asyncio.timeout(10):
            while not (tmp_path / "pid").exists():
                await asyncio.sleep(0.01)
    finally:
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
    pid = int((tmp_path / "pid").read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert list((tmp_path / "jobs").iterdir()) == []


def test_native_argument_boundaries_and_artifact_escape(tmp_path: Path) -> None:
    executor = NativeStrixExecutor(model="test-model", work_root=tmp_path / "jobs")
    args = executor._argv("--evil;not-a-shell", tmp_path / "instructions", tmp_path / "mcp")
    assert "--target=--evil;not-a-shell" in args
    outside = tmp_path / "outside"
    outside.write_text("private")
    link = tmp_path / "jobs" / "link"
    link.symlink_to(outside)
    with pytest.raises(ValueError, match="escaped"):
        executor._read(link, tmp_path / "jobs", 100)


@pytest.mark.parametrize("budget", [0, -1, float("inf"), float("nan")])
def test_native_budget_validation(tmp_path: Path, budget: float) -> None:
    with pytest.raises(ValueError):
        NativeStrixExecutor(model="test", work_root=tmp_path, max_budget_usd=budget)


@pytest.mark.asyncio
async def test_sdk_roles_are_fresh_tool_free_and_use_separate_models(monkeypatch: pytest.MonkeyPatch, context: ExecutionContext) -> None:
    calls: list[tuple[Any, dict[str, Any]]] = []

    class FakeRunner:
        @staticmethod
        async def run(agent: Any, **kwargs: Any) -> SimpleNamespace:
            calls.append((agent, kwargs))
            output: BaseModel
            if agent.name == "Planner":
                output = Plan(tasks=(context.task,))
            elif agent.name == "Executor":
                data = json.loads(kwargs["input"])
                output = ExecutionResult(task_id="t", summary="Inspected artifact",
                                         evidence_ids=(data["evidence"][0]["id"],))
            else:
                data = json.loads(kwargs["input"])
                output = Review(task_id="t", verdict="accept", reason="Fixture",
                                evidence_ids=tuple(data["result"]["evidence_ids"]))
            return SimpleNamespace(final_output=output)

    monkeypatch.setitem(sys.modules, "agents", SimpleNamespace(
        Agent=lambda **kw: SimpleNamespace(**kw), ModelSettings=lambda **kw: kw,
        RunConfig=lambda **kw: kw, Runner=FakeRunner,
    ))
    planner, critic = SDKPlanner("planner-model"), SDKCritic("critic-model")
    executor = SDKArtifactExecutor("executor-model", {"fixture": (Artifact("config", "original"),)})
    req = Request(session_id="s", request_id="r", goal="Inspect")
    await planner.plan(req, Scope(targets=("fixture",), allow_active=True))
    review_context = replace(context, task=context.task.model_copy(update={"mode": "review"}))
    result = await executor.execute(review_context)
    evidence = context.store.read_evidence(context.job_id, context.task, result.evidence_ids)
    await critic.review(context.task, result, evidence)
    assert [agent.name for agent, _ in calls] == ["Planner", "Executor", "Critic"]
    assert [agent.model for agent, _ in calls] == ["planner-model", "executor-model", "critic-model"]
    assert all(agent.tools == [] and agent.handoffs == [] for agent, _ in calls)
    assert all("session" not in kwargs and kwargs["max_turns"] == 2 for _, kwargs in calls)
    assert all(kwargs["run_config"]["tracing_disabled"] for _, kwargs in calls)


@pytest.mark.asyncio
async def test_artifact_executor_refuses_active_context(context: ExecutionContext) -> None:
    executor = SDKArtifactExecutor("test-model", {})
    with pytest.raises(ValueError, match="active"):
        await executor.execute(context)


@pytest.mark.asyncio
async def test_cancellation_during_spawn_is_reaped(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, context: ExecutionContext) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    stopped: list[SimpleNamespace] = []
    process = SimpleNamespace(pid=1)

    async def delayed_spawn(*args: Any, **kwargs: Any) -> SimpleNamespace:  # noqa: ARG001
        entered.set()
        await release.wait()
        return process

    async def stop(child: SimpleNamespace) -> None:
        stopped.append(child)

    executor = NativeStrixExecutor(model="test", work_root=tmp_path / "jobs")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_spawn)
    monkeypatch.setattr(executor, "_stop", stop)
    pending = asyncio.create_task(executor.execute(context))
    await entered.wait()
    pending.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert stopped == [process]
