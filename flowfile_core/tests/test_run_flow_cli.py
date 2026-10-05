"""A headless run child must not sweep the cache and temp directories a live core still references."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import flowfile_core
from shared.storage_config import storage

MAIN_PY = Path(flowfile_core.__file__).resolve().parent / "main.py"
RUN_ID = "987654"


def _frozen_verb(flow_path: str) -> list[str]:
    """main.py's ``--run-flow`` verb, run as ``__main__`` the way the frozen binary enters it."""
    runner = (
        "import runpy, sys\n"
        f"sys.argv = [{str(MAIN_PY)!r}, '--run-flow', {flow_path!r}, '--run-id', {RUN_ID!r}]\n"
        f"runpy.run_path({str(MAIN_PY)!r}, run_name='__main__')\n"
    )
    return [sys.executable, "-c", runner]


def _dev_verb(flow_path: str) -> list[str]:
    return [sys.executable, "-m", "flowfile", "run", "flow", flow_path, "--run-id", RUN_ID]


def _plant_stale(path: Path, age_hours: float) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("still referenced by the running core")
    stale = time.time() - age_hours * 3600
    os.utime(path, (stale, stale))
    return path


@pytest.mark.parametrize("command", [_frozen_verb, _dev_verb], ids=["frozen-verb", "dev-cli"])
def test_run_flow_child_leaves_cache_and_temp_alone(tmp_path, monkeypatch, command):
    storage_dir = tmp_path / "storage"
    cached = _plant_stale(storage_dir / "cache" / "stale.arrow", age_hours=2)
    scratch = _plant_stale(storage_dir / "temp" / "stale.txt", age_hours=48)
    env = {
        **os.environ,
        "FLOWFILE_DB_PATH": str(tmp_path / "catalog.db"),
        "FLOWFILE_STORAGE_DIR": str(storage_dir),
        "FLOWFILE_USER_DATA_DIR": str(tmp_path / "user_data"),
        "FLOWFILE_TELEMETRY": "0",
    }

    result = subprocess.run(
        command(str(tmp_path / "missing.yaml")), env=env, capture_output=True, text=True, timeout=300
    )

    output = result.stdout + result.stderr
    assert result.returncode == 1, output
    assert f"Run {RUN_ID} not found" in output, output
    assert cached.exists() and scratch.exists()

    # Control: a core start's sweep does delete these entries, so their survival above is meaningful.
    monkeypatch.setattr(storage, "_base_dir", storage_dir)
    storage.cleanup_directories()
    assert not cached.exists() and not scratch.exists()
