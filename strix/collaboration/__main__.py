"""Offline deterministic demonstration: python -m strix.collaboration."""
from __future__ import annotations

import asyncio
import sys

from .engine import ExecutionContext, Workflow
from .models import Evidence, ExecutionResult, Plan, Request, Review, Scope, Task
from .pentestgpt import PentestGPTBridge
from .store import Store


class DemoPlanner:
    async def plan(self, request: Request, scope: Scope) -> Plan:
        return Plan(tasks=(Task(
            id="review", target=scope.targets[0], objective=request.goal,
            acceptance_criteria=("Report exactly what the supplied fixture says",),
        ),))


class DemoExecutor:
    async def execute(self, context: ExecutionContext) -> ExecutionResult:
        item = context.record("offline-fixture", "demo=true; no network operation performed")
        return ExecutionResult(
            task_id=context.task.id, summary=item.content, evidence_ids=(item.id,),
        )


class DemoCritic:
    async def review(
        self, task: Task, result: ExecutionResult, evidence: tuple[Evidence, ...],
    ) -> Review:
        matches = bool(evidence and result.summary == evidence[0].content)
        return Review(
            task_id=task.id, verdict="accept" if matches else "reject",
            reason="Checked exact fixture content; this demo is not a security assessment",
            evidence_ids=result.evidence_ids if matches else (),
        )


async def demo() -> None:
    store = Store()
    try:
        bridge = PentestGPTBridge(Workflow(DemoPlanner(), DemoExecutor(), DemoCritic(), store))
        request = Request(session_id="offline-demo", request_id="request-1", goal="Review fixture")
        result = await bridge.submit(
            request.model_dump_json(), authenticated_session_id="offline-demo",
            approved_scope=Scope(targets=("fixture",)),
        )
        sys.stdout.write(result.model_dump_json(indent=2) + "\n")
    finally:
        store.close()


if __name__ == "__main__":
    asyncio.run(demo())
