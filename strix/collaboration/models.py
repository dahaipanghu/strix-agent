"""Versioned, bounded contracts shared by Strix and PentestGPT adapters."""
from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

Identifier = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")]
Text = Annotated[str, Field(min_length=1, max_length=12000)]
Target = Annotated[str, Field(min_length=1, max_length=2048)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Scope(Contract):
    """Trusted application policy, never taken from model/browser scope claims.

    Exact opaque asset IDs are a scheduling gate, not network/OS isolation.
    The execution backend must enforce its own target and egress restrictions.
    """

    targets: tuple[Target, ...] = Field(min_length=1, max_length=100)
    allow_active: bool = False
    max_tasks: int = Field(default=24, ge=1, le=100)
    max_parallel: int = Field(default=1, ge=1, le=8)
    max_attempts: int = Field(default=2, ge=1, le=3)
    max_role_calls: int = Field(default=100, ge=1, le=1000)
    phase_timeout_seconds: float = Field(default=120, gt=0, le=3600)
    total_timeout_seconds: float = Field(default=1800, gt=0, le=86400)


class Task(Contract):
    id: Identifier
    target: Target
    objective: Text
    acceptance_criteria: tuple[Text, ...] = Field(min_length=1, max_length=12)
    depends_on: tuple[Identifier, ...] = Field(default=(), max_length=100)
    mode: Literal["review", "active"] = "review"


class Plan(Contract):
    tasks: tuple[Task, ...] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def check_graph(self) -> Self:
        by_id = {task.id: task for task in self.tasks}
        if len(by_id) != len(self.tasks):
            raise ValueError("Task IDs must be unique")
        for task in self.tasks:
            if len(set(task.depends_on)) != len(task.depends_on):
                raise ValueError("Duplicate dependencies")
            if set(task.depends_on) - by_id.keys():
                raise ValueError("Unknown dependency")
        remaining = set(by_id)
        while remaining:
            ready = {key for key in remaining if not set(by_id[key].depends_on) & remaining}
            if not ready:
                raise ValueError("Task dependency cycle")
            remaining -= ready
        return self

    def check_scope(self, scope: Scope) -> None:
        if len(self.tasks) > scope.max_tasks:
            raise ValueError("Plan exceeds task limit")
        for task in self.tasks:
            if task.target not in scope.targets:
                raise ValueError(f"Task {task.id} is outside the approved scope")
            if task.mode == "active" and not scope.allow_active:
                raise ValueError("Active execution is not approved")


class Request(Contract):
    protocol_version: Literal["strix-pentestgpt/1"] = "strix-pentestgpt/1"
    session_id: Identifier
    request_id: Identifier
    goal: Text
    plan: Plan | None = None


class Evidence(Contract):
    id: Identifier
    task_id: Identifier
    target: Target
    source: Annotated[str, Field(min_length=1, max_length=200)]
    kind: Literal["tool_output", "artifact", "model_report"] = "tool_output"
    content: Annotated[str, Field(min_length=1, max_length=24000)]
    sha256: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class ExecutionResult(Contract):
    task_id: Identifier
    summary: Text
    evidence_ids: tuple[Identifier, ...] = Field(default=(), max_length=32)


class Review(Contract):
    task_id: Identifier
    verdict: Literal["accept", "revise", "reject"]
    reason: Text
    evidence_ids: tuple[Identifier, ...] = Field(default=(), max_length=32)


class TaskOutcome(Contract):
    task_id: Identifier
    status: Literal["accepted", "rejected", "failed", "blocked"]
    attempts: int = Field(ge=0, le=3)
    result: ExecutionResult | None = None
    review: Review | None = None
    error: str | None = None


class JobResult(Contract):
    job_id: Identifier
    session_id: Identifier
    request_id: Identifier
    status: Literal["completed", "partial", "failed", "cancelled"]
    outcomes: tuple[TaskOutcome, ...] = ()
    role_calls: int = Field(default=0, ge=0)
    error: str | None = None


class Event(Contract):
    sequence: int
    job_id: Identifier
    kind: str
    payload: dict[str, object]


def fingerprint(*values: BaseModel) -> str:
    data = [value.model_dump(mode="json") for value in values]
    serialized = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()
