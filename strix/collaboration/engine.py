"""Bounded DAG orchestration with independent, evidence-gated review."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol, TypeVar

from .models import (
    Evidence, ExecutionResult, JobResult, JobStatus, Plan, Request, Review,
    Scope, Task, TaskOutcome,
)
from .store import Store

T = TypeVar("T")


@dataclass(frozen=True)
class ExecutionContext:
    job_id: str
    task: Task
    attempt: int
    goal: str
    dependencies: tuple[ExecutionResult, ...]
    feedback: Review | None
    store: Store

    def record(self, source: str, content: str) -> Evidence:
        return self.store.record_evidence(self.job_id, self.task, source, content)


class Planner(Protocol):
    async def plan(self, request: Request, scope: Scope) -> Plan: ...


class Executor(Protocol):
    async def execute(self, context: ExecutionContext) -> ExecutionResult: ...


class Critic(Protocol):
    async def review(
        self, task: Task, result: ExecutionResult, evidence: tuple[Evidence, ...]
    ) -> Review: ...


class CallBudgetExceeded(RuntimeError):
    pass


class Workflow:
    """Per-job state; role-call limits are not token or monetary limits."""

    def __init__(self, planner: Planner, executor: Executor, critic: Critic, store: Store) -> None:
        self.planner, self.executor, self.critic, self.store = planner, executor, critic, store

    async def run(self, request: Request, scope: Scope) -> JobResult:
        request = Request.model_validate_json(request.model_dump_json())
        scope = Scope.model_validate_json(scope.model_dump_json())
        if request.plan is not None:
            request.plan.check_scope(scope)
        job_id, cached = self.store.claim(request, scope)
        if cached is not None:
            return cached
        job = _Job(self, request, scope, job_id)
        try:
            async with asyncio.timeout(scope.total_timeout_seconds):
                await job.run()
            accepted = sum(item.status == "accepted" for item in job.outcomes.values())
            status: JobStatus = (
                "completed" if accepted == len(job.outcomes) else "partial" if accepted else "failed"
            )
            result = job.result(status)
        except asyncio.CancelledError:
            self.store.finish(job.result("cancelled", "CancelledError"))
            raise
        except Exception as exc:
            # Do not leak exception messages containing credentials or provider URLs.
            result = job.result("failed", type(exc).__name__)
        self.store.finish(result)
        return result


class _Job:
    def __init__(self, workflow: Workflow, request: Request, scope: Scope, job_id: str) -> None:
        self.workflow, self.request, self.scope, self.job_id = workflow, request, scope, job_id
        self.calls = 0
        self.outcomes: dict[str, TaskOutcome] = {}
        self.attempts: dict[str, int] = {}
        self.plan: Plan | None = None

    def result(self, status: JobStatus, error: str | None = None) -> JobResult:
        order = [task.id for task in self.plan.tasks] if self.plan is not None else []
        outcomes = tuple(
            self.outcomes.get(key) or TaskOutcome(
                task_id=key, status="cancelled" if status == "cancelled" else "blocked",
                attempts=self.attempts.get(key, 0), error="JobStopped",
            ) for key in order
        )
        return JobResult(
            job_id=self.job_id, session_id=self.request.session_id,
            request_id=self.request.request_id, status=status, outcomes=outcomes,
            role_calls=self.calls, error=error,
        )

    async def call(
        self, role: str, function: Callable[[], Awaitable[T]], task_id: str | None = None
    ) -> T:
        # Reserve without awaiting, before this job's other asyncio workers can run.
        if self.calls >= self.scope.max_role_calls:
            raise CallBudgetExceeded("Role-call budget exhausted")
        self.calls += 1
        self.workflow.store.emit(self.job_id, "role.started", role=role, task_id=task_id)
        async with asyncio.timeout(self.scope.phase_timeout_seconds):
            value = await function()
        self.workflow.store.emit(self.job_id, "role.finished", role=role, task_id=task_id)
        return value

    async def run(self) -> None:
        candidate = self.request.plan
        if candidate is None:
            candidate = await self.call(
                "planner", lambda: self.workflow.planner.plan(self.request, self.scope)
            )
        if not isinstance(candidate, Plan):
            raise ValueError("Planner must return a Plan contract")
        self.plan = Plan.model_validate_json(candidate.model_dump_json())
        self.plan.check_scope(self.scope)
        self.workflow.store.emit(
            self.job_id, "plan.ready", source="pentestgpt" if self.request.plan else "planner",
            plan=self.plan.model_dump(mode="json"),
        )
        done = {task.id: asyncio.Event() for task in self.plan.tasks}
        semaphore = asyncio.Semaphore(self.scope.max_parallel)
        target_locks = {task.target: asyncio.Lock() for task in self.plan.tasks}

        async def worker(task: Task) -> None:
            try:
                for dependency in task.depends_on:
                    await done[dependency].wait()
                if any(self.outcomes[key].status != "accepted" for key in task.depends_on):
                    outcome = TaskOutcome(
                        task_id=task.id, status="blocked", attempts=0,
                        error="DependencyNotAccepted",
                    )
                else:
                    # Same-asset tasks serialize; they do not occupy a worker while waiting.
                    async with target_locks[task.target], semaphore:
                        outcome = await self.execute(task)
                self.outcomes[task.id] = outcome
                self.workflow.store.emit(
                    self.job_id, f"task.{outcome.status}", outcome=outcome.model_dump(mode="json")
                )
            finally:
                done[task.id].set()

        async with asyncio.TaskGroup() as group:
            for task in self.plan.tasks:
                group.create_task(worker(task), name=f"strix-task-{task.id}")

    async def execute(self, task: Task) -> TaskOutcome:
        feedback: Review | None = None
        result: ExecutionResult | None = None
        dependencies = tuple(self.outcomes[key].result for key in task.depends_on)
        accepted = tuple(value for value in dependencies if value is not None)
        attempt = 0
        try:
            for attempt in range(1, self.scope.max_attempts + 1):
                self.attempts[task.id] = attempt
                context = ExecutionContext(
                    self.job_id, task, attempt, self.request.goal,
                    accepted, feedback, self.workflow.store,
                )
                self.workflow.store.emit(
                    self.job_id, "task.executing", task_id=task.id, attempt=attempt
                )
                execution = await self.call(
                    "executor", lambda: self.workflow.executor.execute(context), task.id
                )
                if not isinstance(execution, ExecutionResult):
                    raise ValueError("Executor must return an ExecutionResult contract")
                result = ExecutionResult.model_validate_json(execution.model_dump_json())
                if result.task_id != task.id:
                    raise ValueError("Executor returned the wrong task ID")
                evidence = self.workflow.store.read_evidence(self.job_id, task, result.evidence_ids)
                review = await self.call(
                    "critic", lambda: self.workflow.critic.review(task, result, evidence), task.id
                )
                if not isinstance(review, Review):
                    raise ValueError("Critic must return a Review contract")
                feedback = Review.model_validate_json(review.model_dump_json())
                if feedback.task_id != task.id:
                    raise ValueError("Critic returned the wrong task ID")
                if not set(feedback.evidence_ids) <= set(result.evidence_ids):
                    raise ValueError("Critic returned unknown evidence references")
                self.workflow.store.read_evidence(self.job_id, task, feedback.evidence_ids)
                self.workflow.store.emit(
                    self.job_id, "task.reviewed", review=feedback.model_dump(mode="json")
                )
                if feedback.verdict == "accept":
                    if not evidence or not feedback.evidence_ids:
                        raise ValueError("Acceptance requires executor-recorded evidence")
                    return TaskOutcome(
                        task_id=task.id, status="accepted", attempts=attempt,
                        result=result, review=feedback,
                    )
                # No automatic repeat of active operations or ambiguous execution errors.
                if feedback.verdict == "reject" or task.mode == "active":
                    break
            return TaskOutcome(
                task_id=task.id, status="rejected", attempts=attempt,
                result=result, review=feedback,
            )
        except Exception as exc:
            return TaskOutcome(
                task_id=task.id, status="failed", attempts=attempt,
                result=result, review=feedback, error=type(exc).__name__,
            )
