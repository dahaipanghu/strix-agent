"""Test the native adapter with subprocess doubles, never a real security scan."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from strix.collaboration.engine import ExecutionContext
from strix.collaboration.models import Request, Scope, Task
from strix.collaboration.native import NativeConfig, NativeExecutor, _read_private
from strix.collaboration.store import Store

TARGET = "https://lab.example.invalid/?literal=;not-a-command"


@pytest.fixture
def context():
    store = Store()
    assigned = Task(id="a", target=TARGET, objective="Inspect approved lab",
                    acceptance_criteria=("Record observed evidence",), mode="active")
    job, _ = store.claim(Request(session_id="s", request_id="r", goal="Lab test"),
                         Scope(targets=(TARGET,), allow_active=True))
    yield ExecutionContext(job, assigned, 1, "Lab test", (), None, store)
    store.close()


@pytest.mark.parametrize("exit_code", [0, 2])
def test_subprocess_arguments_and_artifact_collection(tmp_path, monkeypatch, context, exit_code):
    async def fake_spawn(*args, **kwargs):
        assert args[3] == "-n" and f"--target={TARGET}" in args
        assert "shell" not in kwargs and kwargs["start_new_session"] is True
        assert kwargs["env"]["STRIX_LLM"] == "test/executor"
        root = Path(kwargs["cwd"]) / "strix_runs" / "one-run"
        root.mkdir(parents=True)
        (root / "run.json").write_text(json.dumps({"status": "completed"}))
        (root / "vulnerabilities.json").write_text("[]")
        class Process:
            returncode = exit_code
            async def wait(self):
                return exit_code
        return Process()
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    native = NativeExecutor(NativeConfig(work_root=tmp_path, model="test/executor"))
    result = asyncio.run(native.execute(context))
    assert len(result.evidence_ids) == 2
    evidence = context.store.read_evidence(context.job_id, context.task, result.evidence_ids)
    assert evidence[0].source == "strix-artifact:run.json"


def test_native_refuses_review_without_launching(tmp_path, context):
    assigned = context.task.model_copy(update={"mode": "review"})
    denied = ExecutionContext(context.job_id, assigned, 1, "goal", (), None, context.store)
    with pytest.raises(ValueError, match="active approval"):
        asyncio.run(NativeExecutor(NativeConfig(work_root=tmp_path)).execute(denied))
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("status,code", [("running", 0), ("completed", 1)])
def test_native_incomplete_or_failed_run_is_not_success(tmp_path, monkeypatch, context, status, code):
    async def fake_spawn(*args, **kwargs):
        root = Path(kwargs["cwd"]) / "strix_runs" / "one-run"
        root.mkdir(parents=True)
        (root / "run.json").write_text(json.dumps({"status": status}))
        class Process:
            returncode = code
            async def wait(self):
                return code
        return Process()
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    with pytest.raises(RuntimeError):
        asyncio.run(NativeExecutor(NativeConfig(work_root=tmp_path)).execute(context))


def test_artifact_path_and_size_limits(tmp_path):
    private = tmp_path / "private"
    private.mkdir()
    external = tmp_path / "external.txt"
    external.write_text("Do not read outside the task directory")
    link = private / "link.txt"
    link.symlink_to(external)
    with pytest.raises(ValueError, match="escapes"):
        _read_private(link, private, limit=100)
    file = private / "large.txt"
    file.write_text("a" * 100)
    with pytest.raises(ValueError, match="size"):
        _read_private(file, private, limit=30)
    assert "[TRUNCATED]" in _read_private(file, private, limit=30, truncate=True)


def test_native_config_rejects_unbounded_limits(tmp_path):
    with pytest.raises(ValueError):
        NativeConfig(work_root=tmp_path, max_budget_usd=float("nan"))
    with pytest.raises(ValueError):
        NativeConfig(work_root=tmp_path, timeout_seconds=0)
