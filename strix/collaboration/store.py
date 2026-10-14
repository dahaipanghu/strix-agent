"""Durable idempotency, cursor events and immutable executor evidence.

Interrupted jobs are deliberately not automatically replayed: side effects may
already have occurred. The host must protect this database and authenticate users.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Any

from .models import Event, Evidence, JobResult, Request, Scope, Task, fingerprint


class RequestConflict(ValueError):
    """An idempotency key was reused with a different request or policy."""


class JobInProgress(RuntimeError):
    """The request already has an owner. Inspect its events instead of replaying."""


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(path), timeout=10, check_same_thread=False)
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS pec_jobs (
                id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                request_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
                status TEXT NOT NULL, result TEXT,
                UNIQUE(session_id, request_id)
            );
            CREATE TABLE IF NOT EXISTS pec_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL REFERENCES pec_jobs(id),
                kind TEXT NOT NULL, payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS pec_events_job_sequence
                ON pec_events(job_id, sequence);
            CREATE TABLE IF NOT EXISTS pec_evidence (
                id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES pec_jobs(id),
                payload TEXT NOT NULL
            );
        """)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def claim(self, request: Request, scope: Scope) -> tuple[str, JobResult | None]:
        digest = fingerprint(request, scope)
        with self._lock, self._db:
            self._db.execute("BEGIN IMMEDIATE")
            row = self._db.execute(
                "SELECT id, fingerprint, status, result FROM pec_jobs "
                "WHERE session_id=? AND request_id=?",
                (request.session_id, request.request_id),
            ).fetchone()
            if row:
                if row[1] != digest:
                    raise RequestConflict("Request or scope differs from the stored idempotency key")
                if row[2] == "running":
                    raise JobInProgress(row[0])
                return row[0], JobResult.model_validate_json(row[3])
            job_id = uuid.uuid4().hex
            self._db.execute(
                "INSERT INTO pec_jobs VALUES (?, ?, ?, ?, 'running', NULL)",
                (job_id, request.session_id, request.request_id, digest),
            )
            self._insert_event(job_id, "job.started", {})
            return job_id, None

    def _insert_event(self, job_id: str, kind: str, payload: dict[str, Any]) -> None:
        self._db.execute(
            "INSERT INTO pec_events(job_id, kind, payload) VALUES (?, ?, ?)",
            (job_id, kind, json.dumps(payload, allow_nan=False)),
        )

    def emit(self, job_id: str, kind: str, **payload: Any) -> None:
        with self._lock, self._db:
            self._insert_event(job_id, kind, payload)

    def finish(self, result: JobResult) -> None:
        with self._lock, self._db:
            cursor = self._db.execute(
                "UPDATE pec_jobs SET status=?, result=? WHERE id=? AND status='running'",
                (result.status, result.model_dump_json(), result.job_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("Job is not running")
            self._insert_event(
                result.job_id, f"job.{result.status}", result.model_dump(mode="json")
            )

    def lookup(self, *, session_id: str, request_id: str) -> dict[str, str]:
        """Resolve a job by the session-bound request ID known before execution."""
        with self._lock:
            row = self._db.execute(
                "SELECT id, status FROM pec_jobs WHERE session_id=? AND request_id=?",
                (session_id, request_id),
            ).fetchone()
        if not row:
            raise KeyError("Request not found in session")
        return {"job_id": row[0], "status": row[1]}

    def events(
        self, job_id: str, *, session_id: str, after: int = 0
    ) -> tuple[Event, ...]:
        if after < 0:
            raise ValueError("Event cursor must be nonnegative")
        with self._lock:
            owned = self._db.execute(
                "SELECT 1 FROM pec_jobs WHERE id=? AND session_id=?", (job_id, session_id)
            ).fetchone()
            if not owned:
                raise KeyError("Job not found in session")
            rows = self._db.execute(
                "SELECT sequence, kind, payload FROM pec_events "
                "WHERE job_id=? AND sequence>? ORDER BY sequence LIMIT 1000",
                (job_id, after),
            ).fetchall()
        return tuple(
            Event(sequence=row[0], job_id=job_id, kind=row[1], payload=json.loads(row[2]))
            for row in rows
        )

    def record_evidence(
        self, job_id: str, task: Task, source: str, content: str
    ) -> Evidence:
        # Trusted executor/tool hook only; never register this as an LLM tool.
        if not content:
            raise ValueError("Empty output is not evidence")
        bounded = content if len(content) <= 24000 else content[:23960] + "\n[TRUNCATED]"
        item = Evidence(
            id=uuid.uuid4().hex, task_id=task.id, target=task.target,
            source=source, content=bounded,
            sha256=hashlib.sha256(bounded.encode()).hexdigest(),
        )
        with self._lock, self._db:
            owned = self._db.execute(
                "SELECT 1 FROM pec_jobs WHERE id=? AND status='running'", (job_id,)
            ).fetchone()
            if not owned:
                raise RuntimeError("Evidence can only be recorded for a running job")
            self._db.execute(
                "INSERT INTO pec_evidence VALUES (?, ?, ?)",
                (item.id, job_id, item.model_dump_json()),
            )
        return item

    def read_evidence(
        self, job_id: str, task: Task, ids: tuple[str, ...]
    ) -> tuple[Evidence, ...]:
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate evidence IDs")
        output = []
        with self._lock:
            for evidence_id in ids:
                row = self._db.execute(
                    "SELECT payload FROM pec_evidence WHERE id=? AND job_id=?",
                    (evidence_id, job_id),
                ).fetchone()
                if not row:
                    raise ValueError("Evidence is missing or belongs to another job")
                item = Evidence.model_validate_json(row[0])
                if item.task_id != task.id or item.target != task.target:
                    raise ValueError("Evidence does not belong to this task and target")
                if hashlib.sha256(item.content.encode()).hexdigest() != item.sha256:
                    raise ValueError("Evidence integrity failure")
                output.append(item)
        return tuple(output)
