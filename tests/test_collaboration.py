"""Offline contracts, orchestration, persistence, isolation and bridge tests."""
from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from strix.collaboration import (
    Evidence, ExecutionContext, ExecutionResult, JobInProgress, Plan, Request, RequestConflict,
    Review, Scope, Store, Task, Workflow,
)
from strix.collaboration.pentestgpt import PentestGPTBridge


def task(name: str = "a", depends: tuple[str, ...] = (), mode: Any = "review", target: str = "asset") -> Task:
    return Task(id=name, target=target, objective="Inspect fixture",
                acceptance_criteria=("Use fixture evidence",), depends_on=depends, mode=mode)


def request(plan: Plan | None = None, request_id: str = "r", session_id: str = "s") -> Request:
    return Request(session_id=session_id, request_id=request_id, goal="Review fixture", plan=plan)


class Planner:
    def __init__(self, tasks: tuple[Task, ...] | None = None) -> None:
        self.tasks = tasks or (task(),)
        self.calls = 0

    async def plan(self, request: Request, scope: Scope) -> Plan:  # noqa: ARG002
        self.calls += 1
        return Plan(tasks=self.tasks)


class Executor:
    def __init__(self, *, delay: float = 0, error: Exception | None = None, kind: Any = "tool_output", wrong_task: bool = False, no_evidence: bool = False) -> None:
        self.calls: list[ExecutionContext] = []
        self.delay, self.error, self.kind = delay, error, kind
        self.active = self.peak = 0
        self.wrong_task, self.no_evidence = wrong_task, no_evidence

    async def execute(self, context: ExecutionContext) -> ExecutionResult:
        self.calls.append(context)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(self.delay)
            if self.error:
                raise self.error
            item = context.record("fixture", f"evidence:{context.task.id}", kind=self.kind)
            return ExecutionResult(
                task_id="wrong" if self.wrong_task else context.task.id, summary=item.content,
                evidence_ids=() if self.no_evidence else (item.id,),
            )
        finally:
            self.active -= 1


class Critic:
    def __init__(self, verdicts: tuple[Any, ...] = ("accept",), *, wrong_task: bool = False, wrong_evidence: bool = False) -> None:
        self.verdicts = verdicts
        self.calls: list[tuple[Task, ExecutionResult, tuple[Evidence, ...]]] = []
        self.wrong_task, self.wrong_evidence = wrong_task, wrong_evidence

    async def review(self, task: Task, result: ExecutionResult, evidence: tuple[Evidence, ...]) -> Review:
        index = min(len(self.calls), len(self.verdicts) - 1)
        self.calls.append((task, result, evidence))
        return Review(
            task_id="wrong" if self.wrong_task else task.id, verdict=self.verdicts[index],
            reason="Fixture review", evidence_ids=("forged",) if self.wrong_evidence else result.evidence_ids,
        )


@pytest.fixture
def rig() -> Iterator[Workflow]:
    store, planner, executor, critic = Store(), Planner(), Executor(), Critic()
    yield Workflow(planner, executor, critic, store)
    store.close()


@pytest.mark.parametrize("tasks", [
    (task(), task()),
    (task(depends=("missing",)),),
    (task(depends=("a",)),),
    (task("a", ("b",)), task("b", ("a",))),
    (task("a"), task("b", ("a", "a"))),
])
def test_invalid_dag(tasks: tuple[Task, ...]) -> None:
    with pytest.raises(ValidationError):
        Plan(tasks=tasks)


@pytest.mark.parametrize("overrides", [
    {"max_parallel": 0}, {"max_tasks": 101}, {"max_role_calls": 0},
    {"total_timeout_seconds": float("nan")}, {"targets": ()},
    {"unexpected": True},
])
def test_policy_validation(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Scope(**({"targets": ("asset",)} | overrides))


@pytest.mark.asyncio
async def test_full_loop_and_cached_response(rig: Any) -> None:
    first = await rig.run(request(), Scope(targets=("asset",)))
    second = await rig.run(request(), Scope(targets=("asset",)))
    assert first == second
    assert first.status == "completed" and first.role_calls == 3
    assert rig.planner.calls == 1 and len(rig.executor.calls) == 1
    assert first.outcomes[0].review.evidence_ids == first.outcomes[0].result.evidence_ids


@pytest.mark.asyncio
async def test_imported_plan_does_not_call_planner(rig: Any) -> None:
    result = await rig.run(request(Plan(tasks=(task(),))), Scope(targets=("asset",)))
    assert result.status == "completed" and result.role_calls == 2
    assert rig.planner.calls == 0
    events = rig.store.events(result.job_id, session_id="s")
    assert next(e for e in events if e.kind == "plan.ready").payload["source"] == "pentestgpt"


@pytest.mark.asyncio
@pytest.mark.parametrize("which", ["target", "active", "count"])
async def test_scope_rejected_before_execution(rig: Any, which: str) -> None:
    tasks = (task(target="other"),) if which == "target" else (
        (task(mode="active"),) if which == "active" else (task(), task("b"))
    )
    with pytest.raises(ValueError):
        await rig.run(request(Plan(tasks=tasks)), Scope(targets=("asset",), max_tasks=1))
    assert not rig.executor.calls


@pytest.mark.asyncio
async def test_generated_plan_is_also_scope_checked(rig: Any) -> None:
    rig.planner.tasks = (task(target="other"),)
    result = await rig.run(request(), Scope(targets=("asset",)))
    assert result.status == "failed" and not rig.executor.calls


@pytest.mark.asyncio
async def test_dag_concurrency_and_minimal_dependencies(rig: Any) -> None:
    tasks = (task("a"), task("b"), task("c"), task("d", ("a", "b")))
    rig.executor.delay = 0.01
    result = await rig.run(request(Plan(tasks=tasks)), Scope(targets=("asset",), max_parallel=2))
    assert result.status == "completed" and rig.executor.peak == 2
    dependent = next(c for c in rig.executor.calls if c.task.id == "d")
    assert {r.task_id for r in dependent.dependencies} == {"a", "b"}
    assert [o.task_id for o in result.outcomes] == ["a", "b", "c", "d"]


@pytest.mark.asyncio
async def test_rejected_dependency_blocks_descendant(rig: Any) -> None:
    rig.critic = Critic(("reject",))
    result = await rig.run(request(Plan(tasks=(task(), task("b", ("a",))))), Scope(targets=("asset",)))
    assert [o.status for o in result.outcomes] == ["rejected", "blocked"]
    assert len(rig.executor.calls) == 1


@pytest.mark.asyncio
async def test_critic_feedback_and_bounded_review_retry(rig: Any) -> None:
    rig.critic = Critic(("revise", "accept"))
    result = await rig.run(request(), Scope(targets=("asset",)))
    assert result.status == "completed" and result.outcomes[0].attempts == 2
    assert rig.executor.calls[1].feedback.verdict == "revise"
    assert rig.executor.calls[0].feedback is None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,expected", [("review", 2), ("active", 1)])
async def test_retry_cap_and_no_automatic_active_retry(rig: Any, mode: str, expected: int) -> None:
    rig.critic = Critic(("revise",))
    result = await rig.run(request(Plan(tasks=(task(mode=mode),))), Scope(targets=("asset",), allow_active=True))
    assert result.outcomes[0].status == "rejected" and len(rig.executor.calls) == expected


@pytest.mark.asyncio
async def test_execution_error_is_redacted_and_never_retried(rig: Any) -> None:
    rig.executor.error = RuntimeError("secret-token-must-not-leak")
    result = await rig.run(request(), Scope(targets=("asset",)))
    assert result.outcomes[0].error == "RuntimeError" and len(rig.executor.calls) == 1
    assert "secret-token" not in result.model_dump_json()
    assert "secret-token" not in str(rig.store.events(result.job_id, session_id="s"))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["wrong-executor", "wrong-critic", "forged-critic", "empty", "model-only"])
async def test_invalid_evidence_or_identity_never_accepted(rig: Any, failure: str) -> None:
    if failure == "wrong-executor":
        rig.executor.wrong_task = True
    elif failure == "wrong-critic":
        rig.critic.wrong_task = True
    elif failure == "forged-critic":
        rig.critic.wrong_evidence = True
    elif failure == "empty":
        rig.executor.no_evidence = True
    else:
        rig.executor.kind = "model_report"
    result = await rig.run(request(), Scope(targets=("asset",)))
    assert result.outcomes[0].status == "failed"


@pytest.mark.asyncio
async def test_role_budget_is_never_oversubscribed(rig: Any) -> None:
    tasks = tuple(task(f"t{i}") for i in range(10))
    result = await rig.run(request(Plan(tasks=tasks)), Scope(targets=("asset",), max_parallel=8, max_role_calls=3))
    assert result.role_calls == 3 and result.status != "completed"


@pytest.mark.asyncio
async def test_phase_timeout(rig: Any) -> None:
    rig.executor.delay = 2
    result = await rig.run(request(), Scope(targets=("asset",), phase_timeout_seconds=0.01))
    assert result.outcomes[0].error == "TimeoutError" and rig.executor.active == 0


@pytest.mark.asyncio
async def test_total_timeout_cleans_up_workers(rig: Any) -> None:
    rig.executor.delay = 2
    result = await rig.run(request(), Scope(targets=("asset",), total_timeout_seconds=0.01))
    assert result.status == "failed" and result.error == "TimeoutError"
    assert rig.executor.active == 0


@pytest.mark.asyncio
async def test_cancellation_propagates_and_is_durable(rig: Any) -> None:
    rig.executor.delay = 2
    req, policy = request(), Scope(targets=("asset",))
    pending = asyncio.create_task(rig.run(req, policy))
    await asyncio.sleep(0.01)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    _, cached = rig.store.claim(req, policy)
    assert cached.status == "cancelled" and rig.executor.active == 0


@pytest.mark.asyncio
async def test_concurrent_jobs_have_distinct_state(rig: Any) -> None:
    rig.executor.delay = 0.01
    results = await asyncio.gather(*(
        rig.run(request(request_id=f"r{i}", session_id=f"s{i}"), Scope(targets=("asset",)))
        for i in range(4)
    ))
    assert len({r.job_id for r in results}) == 4
    assert all(r.status == "completed" and r.role_calls == 3 for r in results)
    assert len({r.outcomes[0].result.evidence_ids[0] for r in results}) == 4


@pytest.mark.asyncio
async def test_request_and_scope_conflicts(rig: Any) -> None:
    req, policy = request(), Scope(targets=("asset",))
    await rig.run(req, policy)
    with pytest.raises(RequestConflict):
        await rig.run(req.model_copy(update={"goal": "changed"}), policy)
    with pytest.raises(RequestConflict):
        await rig.run(req, policy.model_copy(update={"max_parallel": 2}))


def test_multiple_store_connections_and_restart(tmp_path: Path) -> None:
    path = tmp_path / "jobs.sqlite"
    a, b = Store(path), Store(path)
    try:
        job_id, _ = a.claim(request(), Scope(targets=("asset",)))
        with pytest.raises(JobInProgress, match=job_id):
            b.claim(request(), Scope(targets=("asset",)))
    finally:
        a.close()
        b.close()
    reopened = Store(path)
    try:
        with pytest.raises(JobInProgress):
            reopened.claim(request(), Scope(targets=("asset",)))
    finally:
        reopened.close()


def test_evidence_binding_and_integrity(rig: Any) -> None:
    policy = Scope(targets=("asset",))
    first, _ = rig.store.claim(request(), policy)
    second, _ = rig.store.claim(request(request_id="other"), policy)
    item = rig.store.record_evidence(first, task(), "fixture", "x" * 25000)
    assert len(item.content) <= 24000 and "TRUNCATED" in item.content
    assert rig.store.read_evidence(first, task(), (item.id,)) == (item,)
    for job_id, current, ids in [
        (second, task(), (item.id,)), (first, task("other"), (item.id,)),
        (first, task(target="other"), (item.id,)), (first, task(), ("forged",)),
        (first, task(), (item.id, item.id)),
    ]:
        with pytest.raises(ValueError):
            rig.store.read_evidence(job_id, current, ids)
    rig.store._db.execute("UPDATE evidence SET payload=? WHERE id=?", (
        item.model_copy(update={"content": "tampered"}).model_dump_json(), item.id,
    ))
    with pytest.raises(ValueError, match="integrity"):
        rig.store.read_evidence(first, task(), (item.id,))


@pytest.mark.asyncio
async def test_event_cursor_and_session_isolation(rig: Any) -> None:
    result = await rig.run(request(), Scope(targets=("asset",)))
    events = rig.store.events(result.job_id, session_id="s")
    assert events[0].kind == "job.started" and events[-1].kind == "job.completed"
    assert list(e.sequence for e in events) == sorted(e.sequence for e in events)
    assert rig.store.events(result.job_id, session_id="s", after=events[-1].sequence) == ()
    with pytest.raises(KeyError):
        rig.store.events(result.job_id, session_id="other")
    with pytest.raises(ValueError):
        rig.store.events(result.job_id, session_id="s", after=-1)


@pytest.mark.asyncio
async def test_bridge_rejects_untrusted_session_scope_and_oversized_input(rig: Any) -> None:
    bridge = PentestGPTBridge(rig)
    policy = Scope(targets=("asset",))
    with pytest.raises(PermissionError):
        await bridge.submit(request().model_dump_json(), authenticated_session_id="other", approved_scope=policy)
    payload = request().model_dump(mode="json") | {"scope": {"allow_active": True}}
    with pytest.raises(ValidationError):
        await bridge.submit(json.dumps(payload), authenticated_session_id="s", approved_scope=policy)
    with pytest.raises(ValueError):
        await bridge.submit(b"x" * (1024 * 1024 + 1), authenticated_session_id="s", approved_scope=policy)
    assert not rig.executor.calls


@pytest.mark.asyncio
async def test_bridge_round_trip(rig: Any) -> None:
    bridge = PentestGPTBridge(rig)
    result = await bridge.submit(request().model_dump_json(), authenticated_session_id="s", approved_scope=Scope(targets=("asset",)))
    assert result.status == "completed"
    assert bridge.events(result.job_id, authenticated_session_id="s")
