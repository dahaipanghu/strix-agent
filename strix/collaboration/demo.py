"""Deterministic offline contract demo, not a scan and not an AI evaluation."""
from __future__ import annotations

import asyncio
import sys

from .engine import ExecutionContext, Workflow
from .models import Evidence, ExecutionResult, Plan, Request, Review, Scope, Task
from .store import Store


class DemoPlanner:
    async def plan(self, request: Request, scope: Scope) -> Plan:
        return Plan(tasks=(Task(id="review", target=scope.targets[0], objective=request.goal,
                               acceptance_criteria=("Read the demo fixture value",)),))


class DemoExecutor:
    async def execute(self, context: ExecutionContext) -> ExecutionResult:
        item = context.record("offline-fixture", "DEMO ONLY: fixture value = 42")
        return ExecutionResult(task_id=context.task.id, summary="Demo fixture was read.",
                               evidence_ids=(item.id,))


class DemoCritic:
    async def review(self, task: Task, result: ExecutionResult,
                     evidence: tuple[Evidence, ...]) -> Review:
        accepted = bool(evidence and "42" in evidence[0].content)
        return Review(task_id=task.id, verdict="accept" if accepted else "reject",
                      reason="Offline fixture comparison; no target was scanned.",
                      evidence_ids=result.evidence_ids)


async def main() -> None:
    store = Store()
    try:
        flow = Workflow(DemoPlanner(), DemoExecutor(), DemoCritic(), store)
        result = await flow.run(Request(session_id="demo", request_id="demo-1",
                                        goal="Read the offline fixture"),
                                Scope(targets=("fixture:demo",)))
        sys.stdout.write(result.model_dump_json(indent=2) + "\n")
    finally:
        store.close()


if __name__ == "__main__":
    asyncio.run(main())
