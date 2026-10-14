"""Existing Strix CLI as an isolated active Executor backend.

Each task gets its own process/workdir, avoiding the legacy report/config
singletons crossing jobs. Legacy internal subagent delegation is unchanged.
Its generated findings are explicitly NOT promoted to verified evidence.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
import signal
import sys
import tempfile
from pathlib import Path

from .engine import ExecutionContext
from .models import ExecutionResult


class NativeStrixExecutor:
    def __init__(
        self, *, model: str, work_root: str | Path,
        max_turns: int = 30, max_budget_usd: float = 5.0,
        command: tuple[str, ...] | None = None,
    ) -> None:
        if os.name != "posix":
            raise RuntimeError("Native executor currently requires a POSIX process supervisor")
        if not model or not 1 <= max_turns <= 1000:
            raise ValueError("Provide an executor model and bounded max_turns")
        if not math.isfinite(max_budget_usd) or max_budget_usd <= 0:
            raise ValueError("max_budget_usd must be finite and positive")
        self.model, self.max_turns, self.max_budget_usd = model, max_turns, max_budget_usd
        self.work_root = Path(work_root).expanduser().resolve()
        self.work_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Only trusted server configuration may override command, never request/model output.
        self.command = command or (
            sys.executable, "-c", "from strix.interface.main import main; main()",
        )

    def _argv(self, target: str, instruction_path: Path, mcp_path: Path) -> tuple[str, ...]:
        return (
            *self.command, "-n", f"--target={target}", "--scan-mode=quick",
            f"--max-turns={self.max_turns}", f"--max-budget={self.max_budget_usd}",
            f"--instruction-file={instruction_path}", f"--mcp-config={mcp_path}",
        )

    @staticmethod
    async def _stop(process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            async with asyncio.timeout(5):
                await process.wait()
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            await process.wait()

    @staticmethod
    def _read(path: Path, root: Path, limit: int) -> str:
        if not path.resolve().is_relative_to(root.resolve()) or path.is_symlink():
            raise ValueError("Artifact escaped the task directory")
        with path.open("rb") as stream:
            content = stream.read(limit + 1)
        if len(content) > limit:
            raise ValueError("Native artifact exceeds limit; review it outside this adapter")
        return content.decode("utf-8")

    async def execute(self, context: ExecutionContext) -> ExecutionResult:
        if context.task.mode != "active":
            raise ValueError("Native Strix is an active backend, not a passive review sandbox")
        target = context.task.target
        candidate = Path(target).expanduser()
        if candidate.exists():
            target = str(candidate.resolve())
        with tempfile.TemporaryDirectory(prefix="strix-executor-", dir=self.work_root) as directory:
            root = Path(directory)
            instructions, mcp = root / "task.json", root / "mcp.json"
            instructions.write_text(json.dumps({
                "role": "Executor", "objective": context.task.objective,
                "acceptance_criteria": context.task.acceptance_criteria,
                "constraints": [
                    "Only perform this assigned authorized task; do not widen its target.",
                    "All findings are provisional and will be reviewed by a separate Critic.",
                ],
                "dependencies": [item.model_dump(mode="json") for item in context.dependencies],
            }, ensure_ascii=False), encoding="utf-8")
            mcp.write_text('{"mcpServers":{}}', encoding="utf-8")
            env = {**os.environ, "STRIX_LLM": self.model}
            spawning = asyncio.create_task(asyncio.create_subprocess_exec(
                *self._argv(target, instructions, mcp), cwd=root, env=env,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            ))
            try:
                process = await asyncio.shield(spawning)
            except asyncio.CancelledError:
                # Cancellation can race OS process creation. Obtain the handle
                # before propagating cancellation, so the new process is reaped.
                process = await spawning
                await self._stop(process)
                raise
            try:
                code = await process.wait()
            finally:
                await self._stop(process)
            if code not in (0, 2):
                raise RuntimeError("Native Strix did not finish successfully")
            records = list(root.glob("strix_runs/*/run.json"))
            if len(records) != 1:
                raise ValueError("Expected exactly one native run record")
            record = json.loads(self._read(records[0], root, 1024 * 1024))
            if not isinstance(record, dict) or record.get("status") != "completed":
                raise ValueError("Native scan is incomplete; exit code alone is not proof")
            observed = context.record("strix.runtime", json.dumps({
                "status": record["status"], "exit_code": code,
                "note": "Runtime metadata only; not proof of vulnerability or target safety",
            }))
            evidence_ids = [observed.id]
            for filename in ("vulnerabilities.json", "penetration_test_report.md"):
                path = records[0].parent / filename
                if path.is_file():
                    text = self._read(path, root, 24000)
                    if text:
                        item = context.record(f"strix.{filename}", text, kind="model_report")
                        evidence_ids.append(item.id)
            return ExecutionResult(
                task_id=context.task.id,
                summary="Native task completed. Generated findings remain provisional; "
                        "the Critic must not use runtime metadata as vulnerability proof.",
                evidence_ids=tuple(evidence_ids),
            )
