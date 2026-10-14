"""Offline collaboration tests: no model keys, Docker or target traffic."""
from __future__ import annotations

import asyncio
import json

import pytest
from pydantic import ValidationError

from strix.collaboration import (
    ExecutionResult, JobInProgress, Plan, Request, RequestConflict, Review,
    Scope, Store, Task, Workflow,
)
from strix.collaboration.bridge import PentestGPTBridge
from strix.collaboration.demo import DemoCritic, DemoExecutor, DemoPlanner


def task(key="a", **changes):
    return Task(id=key, target="asset:local", objective="Read fixture",
                acceptance_criteria=("Observe 42",), **changes)


def request(plan=None, **changes):
    return Request(session_id="owner", request_id="r1", goal="Review fixture", plan=plan, **changes)


def scope(**changes):
    return Scope(targets=("asset:local",), **changes)


@pytest.fixture
def store():
    value = Store()
    yield value
    value.close()


def flow(store, *, executor=None, critic=None, planner=None):
    return Workflow(planner or DemoPlanner(), executor or DemoExecutor(),
                    critic or DemoCritic(), store)


@pytest.mark.parametrize("tasks", [
    (task(), task()),
    (task(depends_on=("missing",)),),
    (task(depends_on=("a",)),),
    (task(depends_on=("b",)), task("b", depends_on=("a",))),
    (task(depends_on=("b", "b")), task("b")),
])
def test_invalid_graph(tasks):
    with pytest.raises(ValidationError):
        Plan(tasks=tasks)


@pytest.mark.parametrize("policy", [Scope(targets=("other",)), scope(max_tasks=1)])
def test_scope_limits(policy):
    with pytest.raises(ValueError):
        Plan(tasks=(task(), task("b"))).check_scope(policy)


def test_active_requires_approval():
    plan = Plan(tasks=(task(mode="active"),))
    with pytest.raises(ValueError):
        plan.check_scope(scope())
    plan.check_scope(scope(allow_active=True))


@pytest.mark.parametrize("field,value", [("max_parallel", 0), ("max_attempts", 4),
                                         ("phase_timeout_seconds", float("nan")),
                                         ("total_timeout_seconds", float("inf"))])
def test_policy_bounds(field, value):
    with pytest.raises(ValidationError):
        scope(**{field: value})


@pytest.mark.asyncio
async def test_default_planner_and_evidence(store):
    result = await flow(store).run(request(), scope())
    assert result.status == "completed"
    assert result.role_calls == 3
    assert result.outcomes[0].review.evidence_ids


@pytest.mark.asyncio
async def test_supplied_plan_skips_planner_and_dependencies(store):
    class UnusedPlanner:
        async def plan(self, *args):
            raise AssertionError("External plan must not be replanned")

    seen = []

    class Executor(DemoExecutor):
        async def execute(self, context):
            seen.append((context.task.id, tuple(x.task_id for x in context.dependencies)))
            return await super().execute(context)

    plan = Plan(tasks=(task("b", depends_on=("a",)), task()))
    result = await flow(store, planner=UnusedPlanner(), executor=Executor()).run(request(plan), scope())
    assert result.status == "completed"
    assert result.role_calls == 4
    assert seen == [("a", ()), ("b", ("a",))]
    assert [x.task_id for x in result.outcomes] == ["b", "a"]


class RevisionCritic:
    async def review(self, task, result, evidence):
        return Review(task_id=task.id, verdict="revise", reason="Need more evidence",
                      evidence_ids=result.evidence_ids)


@pytest.mark.asyncio
async def test_revision_is_bounded_with_feedback(store):
    seen = []

    class Executor(DemoExecutor):
        async def execute(self, context):
            seen.append(context.feedback)
            return await super().execute(context)

    result = await flow(store, executor=Executor(), critic=RevisionCritic()).run(
        request(Plan(tasks=(task(),))), scope(max_attempts=2),
    )
    assert result.outcomes[0].status == "rejected"
    assert result.outcomes[0].attempts == 2
    assert seen[0] is None and seen[1].verdict == "revise"


@pytest.mark.asyncio
async def test_active_not_automatically_repeated(store):
    result = await flow(store, critic=RevisionCritic()).run(
        request(Plan(tasks=(task(mode="active"),))), scope(allow_active=True),
    )
    assert result.outcomes[0].attempts == 1


@pytest.mark.asyncio
async def test_revision_then_success(store):
    class Critic(DemoCritic):
        calls = 0

        async def review(self, task, result, evidence):
            self.calls += 1
            if self.calls == 1:
                return await RevisionCritic().review(task, result, evidence)
            return await super().review(task, result, evidence)

    result = await flow(store, critic=Critic()).run(request(Plan(tasks=(task(),))), scope())
    assert result.status == "completed" and result.outcomes[0].attempts == 2


@pytest.mark.asyncio
async def test_failed_dependency_blocks_only_dependants(store):
    class Executor(DemoExecutor):
        async def execute(self, context):
            if context.task.id == "a":
                raise RuntimeError("secret value must not appear in output")
            return await super().execute(context)

    plan = Plan(tasks=(task(), task("b", depends_on=("a",)), task("c")))
    result = await flow(store, executor=Executor()).run(request(plan), scope(max_parallel=2))
    assert result.status == "partial"
    assert [x.status for x in result.outcomes] == ["failed", "blocked", "accepted"]
    assert "secret value" not in result.model_dump_json()


@pytest.mark.asyncio
async def test_parallelism_limit(store):
    active = peak = 0

    class Executor(DemoExecutor):
        async def execute(self, context):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.01)
                return await super().execute(context)
            finally:
                active -= 1

    plan = Plan(tasks=tuple(task(f"t{i}") for i in range(5)))
    result = await flow(store, executor=Executor()).run(request(plan), scope(max_parallel=2))
    assert result.status == "completed"
    assert peak == 2 and active == 0


@pytest.mark.asyncio
async def test_budget_enforced_before_role_call(store):
    result = await flow(store).run(request(Plan(tasks=(task(),))), scope(max_role_calls=1))
    assert result.role_calls == 1
    assert result.outcomes[0].error == "CallBudgetExceeded"


@pytest.mark.asyncio
async def test_phase_timeout_not_retried(store):
    class Executor:
        calls = 0
        cleaned = False

        async def execute(self, context):
            self.calls += 1
            try:
                await asyncio.sleep(1)
            finally:
                self.cleaned = True

    executor = Executor()
    result = await flow(store, executor=executor).run(
        request(Plan(tasks=(task(),))), scope(phase_timeout_seconds=0.01),
    )
    assert result.outcomes[0].error == "TimeoutError"
    assert executor.calls == 1 and executor.cleaned


@pytest.mark.asyncio
async def test_total_timeout(store):
    class Planner:
        async def plan(self, request, scope):
            await asyncio.sleep(1)

    result = await flow(store, planner=Planner()).run(request(), scope(total_timeout_seconds=0.01))
    assert result.status == "failed" and result.error == "TimeoutError"


@pytest.mark.asyncio
async def test_cancellation_stops_children_and_is_durable(store):
    entered = asyncio.Event()
    stopped = asyncio.Event()

    class Executor:
        async def execute(self, context):
            entered.set()
            try:
                await asyncio.sleep(10)
            finally:
                stopped.set()

    workflow = flow(store, executor=Executor())
    req = request(Plan(tasks=(task(),)))
    future = asyncio.create_task(workflow.run(req, scope()))
    await entered.wait()
    future.cancel()
    with pytest.raises(asyncio.CancelledError):
        await future
    assert stopped.is_set()
    result = await workflow.run(req, scope())
    assert result.status == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["wrong-task", "fake-evidence", "no-evidence", "wrong-critic"])
async def test_untrusted_claims_cannot_be_accepted(store, kind):
    class Executor(DemoExecutor):
        async def execute(self, context):
            if kind == "wrong-critic":
                return await super().execute(context)
            return ExecutionResult(task_id="wrong" if kind == "wrong-task" else context.task.id,
                                   summary="claimed success",
                                   evidence_ids=("fake",) if kind == "fake-evidence" else ())

    class Critic:
        async def review(self, task, result, evidence):
            return Review(task_id=task.id, verdict="accept", reason="claim",
                          evidence_ids=("fake",) if kind == "wrong-critic" else result.evidence_ids)

    result = await flow(store, executor=Executor(), critic=Critic()).run(
        request(Plan(tasks=(task(),))), scope(),
    )
    assert result.status == "failed"


@pytest.mark.asyncio
async def test_idempotency_and_conflicts(store):
    workflow = flow(store)
    req = request()
    first = await workflow.run(req, scope())
    second = await workflow.run(req, scope())
    assert first == second
    with pytest.raises(RequestConflict):
        await workflow.run(req, scope(max_tasks=3))
    with pytest.raises(RequestConflict):
        await workflow.run(req.model_copy(update={"goal": "changed"}), scope())


def test_running_job_not_replayed_and_evidence_is_task_bound(store):
    req = request()
    job, _ = store.claim(req, scope())
    with pytest.raises(JobInProgress):
        store.claim(req, scope())
    value = store.record_evidence(job, task(), "fixture", "42")
    assert store.read_evidence(job, task(), (value.id,)) == (value,)
    with pytest.raises(ValueError):
        store.read_evidence(job, task("b"), (value.id,))
    with pytest.raises(ValueError):
        store.read_evidence(job, task(), (value.id, value.id))
    other, _ = store.claim(req.model_copy(update={"request_id": "other"}), scope())
    with pytest.raises(ValueError):
        store.read_evidence(other, task(), (value.id,))


@pytest.mark.asyncio
async def test_persistent_cache(tmp_path):
    path = tmp_path / "jobs.db"
    first = Store(path)
    result = await flow(first).run(request(), scope())
    first.close()
    second = Store(path)
    try:
        assert await flow(second).run(request(), scope()) == result
    finally:
        second.close()


@pytest.mark.asyncio
async def test_bridge_ownership_events_and_size(store):
    bridge = PentestGPTBridge(flow(store))
    raw = request().model_dump_json()
    with pytest.raises(PermissionError):
        await bridge.run(raw, authenticated_session="someone-else", scope=scope())
    with pytest.raises(ValueError):
        await bridge.run("x" * 262145, authenticated_session="owner", scope=scope())
    result = await bridge.run(raw, authenticated_session="owner", scope=scope())
    events = bridge.events(result["job_id"], authenticated_session="owner")
    assert events[0]["kind"] == "job.started"
    assert events[-1]["kind"] == "job.completed"
    assert bridge.events(result["job_id"], authenticated_session="owner",
                         after=events[-1]["sequence"]) == []
    with pytest.raises(KeyError):
        bridge.events(result["job_id"], authenticated_session="someone-else")
    with pytest.raises(ValueError):
        bridge.events(result["job_id"], authenticated_session="owner", after=-1)
    assert bridge.schemas()["protocol_version"] == "strix-pentestgpt/1"


@pytest.mark.parametrize("change", [{"scope": {"allow_active": True}},
                                    {"protocol_version": "future-version"}])
def test_wire_rejects_extra_authority_and_unknown_version(change):
    payload = request().model_dump(mode="json") | change
    with pytest.raises(ValidationError):
        Request.model_validate_json(json.dumps(payload))
