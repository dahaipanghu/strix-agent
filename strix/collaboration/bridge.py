"""Server-side PentestGPT adapter; the host supplies authentication and transport."""
from __future__ import annotations

from typing import Any

from .engine import Workflow
from .models import Event, JobResult, Plan, Request, Scope

MAX_REQUEST_BYTES = 262144


class PentestGPTBridge:
    def __init__(self, workflow: Workflow) -> None:
        self.workflow = workflow

    async def run(self, raw: str | bytes, *, authenticated_session: str,
                  scope: Scope) -> dict[str, Any]:
        # Derive the session from the authenticated tenant/user on the server.
        # Neither the JSON body nor a browser-controlled header grants authority.
        size = len(raw.encode()) if isinstance(raw, str) else len(raw)
        if size > MAX_REQUEST_BYTES:
            raise ValueError("Request too large")
        request = Request.model_validate_json(raw)
        if request.session_id != authenticated_session:
            raise PermissionError("Session ownership mismatch")
        result = await self.workflow.run(request, scope)
        return result.model_dump(mode="json")

    def events(self, job_id: str, *, authenticated_session: str,
               after: int = 0) -> list[dict[str, Any]]:
        return [event.model_dump(mode="json") for event in self.workflow.store.events(
            job_id, session_id=authenticated_session, after=after,
        )]

    @staticmethod
    def schemas() -> dict[str, Any]:
        return {"protocol_version": "strix-pentestgpt/1",
                "request": Request.model_json_schema(), "plan": Plan.model_json_schema(),
                "result": JobResult.model_json_schema(), "event": Event.model_json_schema()}
