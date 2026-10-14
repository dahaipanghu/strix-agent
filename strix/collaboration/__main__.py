"""JSON-only CLI: schema, validate and run. No target executes during validation."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

from .models import Request, Scope
from .engine import Workflow
from .native import NativeConfig, NativeExecutor
from .roles import JsonModel, ModelCritic, ModelPlanner
from .store import JobInProgress, Store


def _read(path: str) -> str:
    if path == "-":
        value = sys.stdin.read(256001)
    else:
        with Path(path).open(encoding="utf-8") as handle:
            value = handle.read(256001)
    if len(value) > 256000:
        raise ValueError("Input exceeds size limit")
    return value


def _model(role: str) -> JsonModel:
    prefix = f"STRIX_{role.upper()}_"
    return JsonModel(
        model=os.getenv(prefix + "MODEL"),
        api_key=os.getenv(prefix + "API_KEY"), api_base=os.getenv(prefix + "API_BASE"),
        json_mode=os.getenv(prefix + "JSON_MODE", "1") != "0",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("schema", "validate", "run", "events"))
    parser.add_argument("--request", default="-", help="Request JSON file or stdin")
    parser.add_argument("--scope", help="Trusted server-owned policy JSON file")
    parser.add_argument("--db", default=str(Path.home() / ".strix/collaboration/state.sqlite"))
    parser.add_argument("--work-root", type=Path, default=Path.home() / ".strix/collaboration/runs")
    parser.add_argument("--session-id")
    parser.add_argument("--request-id")
    parser.add_argument("--after", type=int, default=0)
    parser.add_argument("--executor-budget", type=float, default=3.0)
    parser.add_argument("--executor-turns", type=int, default=24)
    args = parser.parse_args()
    store: Store | None = None
    output: dict[str, Any]
    try:
        if args.action == "schema":
            output = {"request": Request.model_json_schema(), "scope": Scope.model_json_schema()}
        elif args.action == "events":
            if not args.session_id or not args.request_id:
                parser.error("events requires --session-id and --request-id")
            if not Path(args.db).is_file():
                raise KeyError("No collaboration database")
            store = Store(args.db)
            job = store.lookup(session_id=args.session_id, request_id=args.request_id)
            events = store.events(job["job_id"], session_id=args.session_id, after=args.after)
            output = {**job, "events": [event.model_dump(mode="json") for event in events],
                      "next_cursor": events[-1].sequence if events else args.after}
        else:
            if not args.scope:
                parser.error("--scope is required")
            request = Request.model_validate_json(_read(args.request))
            scope = Scope.model_validate_json(_read(args.scope))
            if request.plan:
                request.plan.check_scope(scope)
            if args.action == "validate":
                output = {"valid": True, "request_id": request.request_id, "executed": False}
            else:
                # Private storage and artifacts contain potentially sensitive target data.
                os.umask(0o077)
                Path(args.db).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                store = Store(args.db)
                native = NativeExecutor(NativeConfig(
                    work_root=args.work_root, model=os.getenv("STRIX_EXECUTOR_MODEL"),
                    max_budget_usd=args.executor_budget, max_turns=args.executor_turns,
                ))
                workflow = Workflow(
                    ModelPlanner(_model("planner"), native_execution=True),
                    native, ModelCritic(_model("critic")), store,
                )
                output = asyncio.run(workflow.run(request, scope)).model_dump(mode="json")
        sys.stdout.write(json.dumps(output, ensure_ascii=False) + "\n")
    except JobInProgress as exc:
        sys.stdout.write(json.dumps({"error": "JobInProgress", "job_id": str(exc)}) + "\n")
        raise SystemExit(3) from None
    except (ValueError, KeyError, OSError) as exc:
        sys.stdout.write(json.dumps({"error": type(exc).__name__}) + "\n")
        raise SystemExit(2) from None
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    main()
