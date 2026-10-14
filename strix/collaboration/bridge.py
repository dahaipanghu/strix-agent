"""Transport-neutral PentestGPT integration; authentication belongs to the host."""
from __future__ import annotations

from .engine import Workflow
from .models import JobResult, Request, Scope


class PentestGPTBridge:
    def __init__(self, workflow: Workflow) -> None:
        self.workflow = workflow

    async def submit(
        self, payload: str | bytes, *, authenticated_session_id: str, scope: Scope
    ) -> JobResult:
        if len(payload) > 256000:
            raise ValueError("Request exceeds the transport limit")
        request = Request.model_validate_json(payload)
        if request.session_id != authenticated_session_id:
            raise PermissionError("Session does not match authenticated server context")
        # Scope is injected by the server, not taken from the request payload.
        return await self.workflow.run(request, scope)

    def events(
        self, request_id: str, *, authenticated_session_id: str, after: int = 0
    ) -> dict[str, object]:
        job = self.workflow.store.lookup(
            session_id=authenticated_session_id, request_id=request_id
        )
        events = self.workflow.store.events(
            job["job_id"], session_id=authenticated_session_id, after=after
        )
        return {
            **job, "events": [event.model_dump(mode="json") for event in events],
            "next_cursor": events[-1].sequence if events else after,
        }
