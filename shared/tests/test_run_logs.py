"""Tests for shared.run_logs — run-log path resolution and age-based retention.

The storage singleton is redirected by monkeypatching its memoised ``_base_dir``;
``logs_directory`` recomputes per call, so this needs no change to the singleton
contract. ``TESTING`` is deleted per test because the flowfile_core conftest sets
it process-wide when the suites are run together.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import pytest

from shared.run_logs import DEFAULT_LOG_RETENTION_DAYS, cleanup_old_logs, run_log_path
from shared.storage_config import storage

DAY = 86400


@pytest.fixture
def logs_dir(tmp_path, monkeypatch) -> Path:
    """Redirect the storage singleton at tmp_path and return its logs dir."""
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.delenv("FLOWFILE_RUN_LOG_RETENTION_DAYS", raising=False)
    monkeypatch.setattr(storage, "_base_dir", tmp_path)
    directory = storage.logs_directory
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _write(directory: Path, name: str, age_days: float = 0.0) -> Path:
    path = directory / name
    path.write_text(f"contents of {name}")
    if age_days:
        stamp = time.time() - age_days * DAY
        os.utime(path, (stamp, stamp))
    return path


# Path resolution


def test_run_log_path_follows_storage_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.setattr(storage, "_base_dir", tmp_path)
    assert run_log_path(7) == tmp_path / "logs" / "scheduled_run_7.log"


def test_run_log_path_is_not_home_when_storage_dir_set(tmp_path, monkeypatch):
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.setattr(storage, "_base_dir", tmp_path)
    resolved = run_log_path(7)
    assert Path.home() / ".flowfile" not in resolved.parents
    assert Path.home() / ".flowfile" / "logs" != resolved.parent


# Retention


def test_cleanup_removes_only_expired(logs_dir):
    _write(logs_dir, "scheduled_run_1.log", age_days=40)
    _write(logs_dir, "flow_1.log", age_days=40)
    _write(logs_dir, "scheduled_run_2.log")
    _write(logs_dir, "flow_2.log")

    assert cleanup_old_logs() == 2

    assert sorted(p.name for p in logs_dir.iterdir()) == ["flow_2.log", "scheduled_run_2.log"]


def test_cleanup_ignores_unrelated_files(logs_dir):
    _write(logs_dir, "notes.txt", age_days=40)
    _write(logs_dir, "something.log", age_days=40)
    _write(logs_dir, "flow_1.log", age_days=40)

    assert cleanup_old_logs(1) == 1

    assert sorted(p.name for p in logs_dir.iterdir()) == ["notes.txt", "something.log"]


@pytest.mark.parametrize(
    ("raw", "expect_deleted"),
    [
        ("7", True),  # 10-day-old file is past a 7-day window
        ("40", False),  # ...but inside a 40-day one
        ("0", False),  # retention disabled
        ("-1", False),  # negative also disables
        ("banana", False),  # unparseable → default 30
        (None, False),  # unset → default 30
    ],
)
def test_retention_env_var(logs_dir, monkeypatch, raw, expect_deleted):
    if raw is None:
        monkeypatch.delenv("FLOWFILE_RUN_LOG_RETENTION_DAYS", raising=False)
    else:
        monkeypatch.setenv("FLOWFILE_RUN_LOG_RETENTION_DAYS", raw)
    aged = _write(logs_dir, "scheduled_run_1.log", age_days=10)

    deleted = cleanup_old_logs()

    assert deleted == (1 if expect_deleted else 0)
    assert aged.exists() is not expect_deleted


def test_retention_env_var_is_read_per_call(logs_dir, monkeypatch):
    """A value set after import must be honoured (no import-time caching)."""
    aged = _write(logs_dir, "scheduled_run_1.log", age_days=10)
    monkeypatch.setenv("FLOWFILE_RUN_LOG_RETENTION_DAYS", "0")
    assert cleanup_old_logs() == 0
    assert aged.exists()

    monkeypatch.setenv("FLOWFILE_RUN_LOG_RETENTION_DAYS", "7")
    assert cleanup_old_logs() == 1
    assert not aged.exists()


def test_invalid_retention_warns_and_uses_default(logs_dir, monkeypatch, caplog):
    monkeypatch.setenv("FLOWFILE_RUN_LOG_RETENTION_DAYS", "banana")
    beyond_default = _write(
        logs_dir, "scheduled_run_1.log", age_days=DEFAULT_LOG_RETENTION_DAYS + 10
    )
    within_default = _write(logs_dir, "scheduled_run_2.log", age_days=1)

    with caplog.at_level(logging.WARNING, logger="flowfile.run_logs"):
        assert cleanup_old_logs() == 1

    assert not beyond_default.exists()
    assert within_default.exists()
    assert any("FLOWFILE_RUN_LOG_RETENTION_DAYS" in rec.message for rec in caplog.records)


def test_cleanup_survives_unreadable_entry(logs_dir, monkeypatch):
    """A file that vanishes mid-sweep must not abort the rest of the sweep."""
    _write(logs_dir, "flow_1.log", age_days=40)
    real_stat = Path.stat

    def flaky_stat(self, *args, **kwargs):
        if self.name == "scheduled_run_1.log":
            raise OSError("boom")
        return real_stat(self, *args, **kwargs)

    _write(logs_dir, "scheduled_run_1.log", age_days=40)
    monkeypatch.setattr(Path, "stat", flaky_stat)

    assert cleanup_old_logs() == 1


# Test isolation


def test_logs_directory_is_isolated_under_testing(tmp_path, monkeypatch):
    monkeypatch.setenv("TESTING", "True")
    monkeypatch.setattr(storage, "_base_dir", tmp_path)

    assert storage.logs_directory == storage.temp_directory / "test_logs"
    assert storage.logs_directory != Path.home() / ".flowfile" / "logs"
    assert run_log_path(1).parent == storage.temp_directory / "test_logs"


# The removed hardcoded 168h sweep (storage_config.cleanup_directories)


def test_cleanup_directories_does_not_expire_logs(tmp_path, monkeypatch):
    """``cleanup_directories`` must leave the logs dir alone.

    It used to sweep it at a hardcoded 168h, which silently overrode
    FLOWFILE_RUN_LOG_RETENTION_DAYS. The system-logs assertion proves the sweep
    machinery still runs, so log survival is not vacuous.
    """
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.setattr(storage, "_base_dir", tmp_path)
    for directory in (storage.logs_directory, storage.system_logs_directory, storage.temp_directory):
        directory.mkdir(parents=True, exist_ok=True)

    run_log = _write(storage.logs_directory, "scheduled_run_1.log", age_days=8)
    flow_log = _write(storage.logs_directory, "flow_1.log", age_days=8)
    system_log = _write(storage.system_logs_directory, "core.log", age_days=8)

    storage.cleanup_directories()

    assert run_log.exists()
    assert flow_log.exists()
    assert not system_log.exists()


def test_subprocess_run_types_mirror_reapable_run_types():
    """run_logs duplicates the tuple to stay free of run_completion's sqlalchemy import.

    Pin the mirror so the two cannot drift: a run type that is reaped but whose
    logs are never swept (or vice versa) is silent breakage.
    """
    from shared.run_completion import REAPABLE_RUN_TYPES
    from shared.run_logs import SUBPROCESS_RUN_TYPES

    assert SUBPROCESS_RUN_TYPES == REAPABLE_RUN_TYPES
