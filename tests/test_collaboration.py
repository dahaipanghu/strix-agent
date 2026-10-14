"""Offline contract/orchestration tests. No live model or target traffic."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from strix.collaboration import PentestGPTBridge, Plan, Request, Scope, Store, Task, Workflow
from strix.collaboration.engine import ExecutionContext
from strix.collaboration.models import ExecutionResult, Review
from strix.collaboration.roles import JsonModel, ModelCritic, ModelPlanner
from strix.collaboration.store import JobInProgress, RequestConflict

TARGET = "https://lab.example.invalid"


def task(key="a", **kwargs):
    return Task(id=key, target=kwargs.pop("target", TARGET), objective="Inspect approved evidence",
                acceptance_criteria=("Evidence supports the conclusion",), **kwargs)


def request(*tasks, **kwargs):
    return Request(session_id="session-a", request_id=kwargs.pop("request_id", "req-a"),
                   goal="Review a lab assessment", plan=Plan(tasks=tasks) if tasks else None, **kwargs)


class FakePlanner:
    def __init__(self, *tasks):
        self.tasks, self.calls = tasks or (task(),), 0

    async def plan(self, req, scope):
        self.calls += 1
        return Plan(tasks=self.tasks)


class FakeExecutor:
    def __init__(self, *, fail=False, delay=0, missing=False, foreign=False):
        self.fail, self.delay, self.missing, self.foreign = fail, delay, missing, foreign
        self.contexts, self.running, self.peak = [], 0, 0

    async def execute(self, context):
        self.contexts.append(context)
        self.running += 1
        self.peak = max(self.peak, self.running)
        try:
            await asyncio.sleep(self.delay)
            if self.fail:
                raise RuntimeError("secret provider credential")
            evidence = context.record("synthetic-test-fixture", "Synthetic expected observation")
            ids = ("forged",) if self.foreign else () if self.missing else (evidence.id,)
            return ExecutionResult(task_id=context.task.id, summary="Synthetic test output",
                                   evidence_ids=ids)
        finally:
            self.running -= 1


class FakeCritic:
    def __init__(self, *verdicts, forged=False):
        self.verdicts, self.calls, self.forged = verdicts or ("accept",), [], forged

    async def review(self, assigned, result, evidence):
        self.calls.append((assigned, result, evidence))
        verdict = self.verdicts[min(len(self.calls) - 1, len(self.verdicts) - 1)]
        return Review(task_id=assigned.id, verdict=verdict, reason="Synthetic criterion assessment",
                      evidence_ids=("forged",) if self.forged else result.evidence_ids)


@pytest.fixture
def store():
    instance = Store()
    yield instance
    instance.close()


def workflow(store, planner=None, executor=None, critic=None):
    return Workflow(planner or FakePlanner(), executor or FakeExecutor(), critic or FakeCritic(), store)


@pytest.mark.parametrize("tasks", [
    (task(), task()),
    (task(depends_on=("unknown",)),),
    (task("a", depends_on=("b",)), task("b", depends_on=("a",))),
    (task(depends_on=("a", "a")),),
])
def test_bad_graph(tasks):
    with pytest.raises(ValidationError):
        Plan(tasks=tasks)


def test_schema_scope_and_policy():
    with pytest.raises(ValidationError):
        Request.model_validate({**request(task()).model_dump(), "scope": {"allow_active": True}})
    with pytest.raises(ValidationError):
        Scope(targets=(TARGET,), phase_timeout_seconds=float("nan"))
    with pytest.raises(ValueError):
        Plan(tasks=(task(mode="active"),)).check_scope(Scope(targets=(TARGET,)))
    with pytest.raises(ValueError):
        Plan(tasks=(task(target="https://other.invalid"),)).check_scope(Scope(targets=(TARGET,)))
    with pytest.raises(ValueError):
        Plan(tasks=(task("a"), task("b"))).check_scope(Scope(targets=(TARGET,), max_tasks=1))


def test_complete_and_supplied_plan_skips_planner(store):
    runner = workflow(store)
    result = asyncio.run(runner.run(request(task()), Scope(targets=(TARGET,))))
    assert result.status == "completed"
    assert result.role_calls == 2
    assert runner.planner.calls == 0
    assert result.outcomes[0].review.evidence_ids
    events = store.events(result.job_id, session_id="session-a")
    assert events[0].kind == "job.started"
    assert events[-1].kind == "job.completed"
    assert store.events(result.job_id, session_id="session-a", after=events[-1].sequence) == ()
    with pytest.raises(KeyError):
        store.events(result.job_id, session_id="another-session")


def test_planner_path_and_dependencies(store):
    planner = FakePlanner(task("b", depends_on=("a",)), task("a"))
    runner = workflow(store, planner=planner)
    result = asyncio.run(runner.run(request(), Scope(targets=(TARGET,), max_parallel=2)))
    assert result.status == "completed" and result.role_calls == 5
    assert [ctx.task.id for ctx in runner.executor.contexts] == ["a", "b"]
    assert runner.executor.contexts[1].dependencies[0].task_id == "a"
    assert [out.task_id for out in result.outcomes] == ["b", "a"]


def test_invalid_generated_scope_never_executes(store):
    runner = workflow(store, planner=FakePlanner(task(target="https://evil.invalid")))
    result = asyncio.run(runner.run(request(), Scope(targets=(TARGET,))))
    assert result.status == "failed" and not runner.executor.contexts


def test_dependency_blocks_on_rejection(store):
    runner = workflow(store, critic=FakeCritic("reject"))
    result = asyncio.run(runner.run(request(task(), task("b", depends_on=("a",))), Scope(targets=(TARGET,))))
    assert [out.status for out in result.outcomes] == ["rejected", "blocked"]
    assert len(runner.executor.contexts) == 1


def test_revision_is_bounded_and_feedback_passed(store):
    runner = workflow(store, critic=FakeCritic("revise", "accept"))
    result = asyncio.run(runner.run(request(task()), Scope(targets=(TARGET,))))
    assert result.outcomes[0].attempts == 2 and result.status == "completed"
    assert runner.executor.contexts[1].feedback.verdict == "revise"


def test_revision_exhausted(store):
    runner = workflow(store, critic=FakeCritic("revise"))
    result = asyncio.run(runner.run(request(task()), Scope(targets=(TARGET,), max_attempts=2)))
    assert result.outcomes[0].status == "rejected"
    assert len(runner.executor.contexts) == 2


def test_active_task_never_automatically_retried(store):
    runner = workflow(store, critic=FakeCritic("revise", "accept"))
    result = asyncio.run(runner.run(request(task(mode="active")), Scope(targets=(TARGET,), allow_active=True)))
    assert result.outcomes[0].status == "rejected" and len(runner.executor.contexts) == 1


@pytest.mark.parametrize("executor,critic", [
    (FakeExecutor(missing=True), FakeCritic()),
    (FakeExecutor(foreign=True), FakeCritic()),
    (FakeExecutor(), FakeCritic(forged=True)),
])
def test_missing_or_forged_evidence_never_accepted(store, executor, critic):
    result = asyncio.run(workflow(store, executor=executor, critic=critic).run(
        request(task()), Scope(targets=(TARGET,))))
    assert result.status == "failed" and result.outcomes[0].status == "failed"


def test_errors_redacted_and_never_retried(store):
    runner = workflow(store, executor=FakeExecutor(fail=True))
    result = asyncio.run(runner.run(request(task()), Scope(targets=(TARGET,))))
    assert result.outcomes[0].error == "RuntimeError"
    assert "secret" not in result.model_dump_json()
    assert len(runner.executor.contexts) == 1


def test_evidence_task_binding_and_integrity(store):
    job_id, _ = store.claim(request(task()), Scope(targets=(TARGET,)))
    item = store.record_evidence(job_id, task(), "fixture", "observation")
    with pytest.raises(ValueError):
        store.read_evidence(job_id, task("other"), (item.id,))
    with pytest.raises(ValueError):
        store.read_evidence(job_id, task(), (item.id, item.id))
    payload = item.model_dump()
    payload["content"] = "tampered"
    store._db.execute("UPDATE pec_evidence SET payload=? WHERE id=?", (json.dumps(payload), item.id))
    store._db.commit()
    with pytest.raises(ValueError):
        store.read_evidence(job_id, task(), (item.id,))


def test_idempotency_scope_conflicts_and_restart(tmp_path):
    path = tmp_path / "state.sqlite"
    first = Store(path)
    req, scope = request(task()), Scope(targets=(TARGET,))
    result = asyncio.run(workflow(first).run(req, scope))
    first.close()
    second = Store(path)
    try:
        runner = workflow(second)
        assert asyncio.run(runner.run(req, scope)) == result
        assert not runner.executor.contexts
        with pytest.raises(RequestConflict):
            asyncio.run(runner.run(req, scope.model_copy(update={"max_parallel": 2})))
    finally:
        second.close()


def test_running_job_not_replayed_across_connections(tmp_path):
    path = tmp_path / "state.sqlite"
    first, second = Store(path), Store(path)
    try:
        req, scope = request(task()), Scope(targets=(TARGET,))
        job_id, _ = first.claim(req, scope)
        with pytest.raises(JobInProgress, match=job_id):
            second.claim(req, scope)
    finally:
        first.close()
        second.close()


def test_call_budget(store):
    runner = workflow(store)
    result = asyncio.run(runner.run(request(task()), Scope(targets=(TARGET,), max_role_calls=1)))
    assert result.role_calls == 1 and result.status == "failed"
    assert result.outcomes[0].error == "CallBudgetExceeded"
    assert not runner.critic.calls


def test_phase_timeout_cleans_up(store):
    runner = workflow(store, executor=FakeExecutor(delay=10))
    result = asyncio.run(runner.run(request(task()), Scope(targets=(TARGET,), phase_timeout_seconds=0.01)))
    assert result.outcomes[0].error == "TimeoutError"
    assert runner.executor.running == 0 and len(runner.executor.contexts) == 1


def test_total_timeout_cleans_up(store):
    runner = workflow(store, executor=FakeExecutor(delay=10))
    result = asyncio.run(runner.run(request(task()), Scope(targets=(TARGET,), total_timeout_seconds=0.01)))
    assert result.status == "failed" and result.error == "TimeoutError"
    assert runner.executor.running == 0


def test_cancellation_is_terminal_and_not_replayed(store):
    async def scenario():
        runner = workflow(store, executor=FakeExecutor(delay=10))
        req, scope = request(task()), Scope(targets=(TARGET,))
        pending = asyncio.create_task(runner.run(req, scope))
        while not runner.executor.contexts:
            await asyncio.sleep(0)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        cached = await runner.run(req, scope)
        assert cached.status == "cancelled" and runner.executor.running == 0
        assert cached.outcomes[0].status == "cancelled"
        assert len(runner.executor.contexts) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("same_target,expected_peak", [(True, 1), (False, 2)])
def test_parallelism_and_same_asset_serialization(store, same_target, expected_peak):
    other = TARGET if same_target else "https://second.example.invalid"
    runner = workflow(store, executor=FakeExecutor(delay=0.015))
    result = asyncio.run(runner.run(request(task(), task("b", target=other)),
                                    Scope(targets=(TARGET, other), max_parallel=2)))
    assert result.status == "completed" and runner.executor.peak == expected_peak


def test_bridge_session_and_payload_bounds(store):
    bridge = PentestGPTBridge(workflow(store))
    with pytest.raises(PermissionError):
        asyncio.run(bridge.submit(request(task()).model_dump_json(),
                                 authenticated_session_id="another", scope=Scope(targets=(TARGET,))))
    with pytest.raises(ValueError):
        asyncio.run(bridge.submit("x" * 256001, authenticated_session_id="session-a",
                                 scope=Scope(targets=(TARGET,))))


def completion_response(content, finish="stop"):
    return SimpleNamespace(choices=[SimpleNamespace(
        message=SimpleNamespace(content=content), finish_reason=finish)])


def test_json_roles_have_no_tools_and_no_shared_history():
    calls = []
    async def complete(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return completion_response(Plan(tasks=(task(),)).model_dump_json())
        return completion_response(Review(task_id="a", verdict="reject", reason="Missing evidence").model_dump_json())
    model = JsonModel(model="test/model", completion=complete)
    planned = asyncio.run(ModelPlanner(model).plan(request(), Scope(targets=(TARGET,))))
    review = asyncio.run(ModelCritic(model).review(task(), ExecutionResult(task_id="a", summary="claim"), ()))
    assert planned.tasks[0].id == "a" and review.verdict == "reject"
    assert all("tools" not in call and len(call["messages"]) == 2 for call in calls)
    assert "independent Critic" in calls[1]["messages"][0]["content"]
    assert calls[0]["num_retries"] == 0


@pytest.mark.parametrize("content,finish", [("not JSON", "stop"), ("{}", "length")])
def test_malformed_model_outputs_fail_closed(content, finish):
    async def complete(**kwargs):
        return completion_response(content, finish)
    with pytest.raises(ValueError):
        asyncio.run(JsonModel(model="test/model", completion=complete).generate("Test", {}, Plan))


def test_context_limit_and_secret_repr():
    model = JsonModel(model="test/model", api_key="fixture-sensitive-value", max_input_chars=1)
    assert "fixture-sensitive-value" not in repr(model)
    with pytest.raises(ValueError):
        asyncio.run(model.generate("Test", {}, Plan))


def test_session_bound_event_lookup(store):
    runner = workflow(store)
    asyncio.run(runner.run(request(task()), Scope(targets=(TARGET,))))
    bridge = PentestGPTBridge(runner)
    packet = bridge.events("req-a", authenticated_session_id="session-a")
    assert packet["status"] == "completed" and packet["events"]
    again = bridge.events("req-a", authenticated_session_id="session-a", after=packet["next_cursor"])
    assert again["events"] == []
    with pytest.raises(KeyError):
        bridge.events("req-a", authenticated_session_id="wrong-session")


def test_cli_validation_has_no_execution_side_effects(tmp_path):
    import subprocess
    import sys
    req_path, scope_path = tmp_path / "request.json", tmp_path / "scope.json"
    req_path.write_text(request(task()).model_dump_json())
    scope_path.write_text(Scope(targets=(TARGET,)).model_dump_json())
    process = subprocess.run(
        [sys.executable, "-m", "strix.collaboration", "validate", "--request", str(req_path),
         "--scope", str(scope_path)], capture_output=True, text=True, check=True,
    )
    assert json.loads(process.stdout) == {"valid": True, "request_id": "req-a", "executed": False}


@pytest.mark.parametrize("role", ["planner", "executor", "critic"])
def test_bad_adapter_return_types_are_terminal_failures(store, role):
    class Wrong:
        async def plan(self, req, scope):
            return {"invalid": "not a contract"}
        async def execute(self, context):
            return {"invalid": "not a contract"}
        async def review(self, assigned, result, evidence):
            return {"invalid": "not a contract"}
    runner = workflow(store)
    setattr(runner, role, Wrong())
    req = request() if role == "planner" else request(task())
    result = asyncio.run(runner.run(req, Scope(targets=(TARGET,))))
    assert result.status == "failed"
    assert asyncio.run(runner.run(req, Scope(targets=(TARGET,)))) == result
