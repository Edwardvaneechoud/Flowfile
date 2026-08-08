"""SecureStorage store-file integrity and electron path resolution.

``set_password`` is a read-modify-write over one encrypted blob and
``_read_store`` returns ``{}`` on *any* error, so a half-written store file
turns the next write into a wipe of every other secret — including the master
key that every ``$ffsec$`` user secret depends on.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

import flowfile_core.auth.secrets as secrets_module
from flowfile_core.auth.secrets import SecureStorage
from tests.secure_storage_isolation import (
    DEFAULT_STORE_DIRNAME,
    resolve_test_secure_storage_path,
    worker_is_listening,
)


@pytest.fixture
def storage(tmp_path, monkeypatch) -> SecureStorage:
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(tmp_path / "store"))
    return SecureStorage()


def _temp_files(storage: SecureStorage) -> list[Path]:
    return list(storage.storage_path.glob(".flowfile.*.tmp"))


class _BadEncrypt:
    """Fernet stand-in whose encrypt() output blows up on file.write()."""

    def __init__(self, key):
        self._real = Fernet(key)

    def encrypt(self, data):
        return object()

    def decrypt(self, data):
        return self._real.decrypt(data)


def test_set_password_preserves_other_keys(storage):
    storage.set_password("flowfile", "master_key", "mk")
    storage.set_password("flowfile", "internal_token", "it")
    storage.set_password("flowfile", "jwt_secret", "js")

    assert storage.get_password("flowfile", "master_key") == "mk"
    assert storage.get_password("flowfile", "internal_token") == "it"
    assert storage.get_password("flowfile", "jwt_secret") == "js"


def test_failed_write_does_not_destroy_existing_secrets(storage, monkeypatch):
    """A write that dies mid-stream must not truncate the live store file."""
    storage.set_password("flowfile", "master_key", "mk")
    monkeypatch.setattr(secrets_module, "Fernet", _BadEncrypt)

    storage.set_password("flowfile", "internal_token", "it")

    monkeypatch.undo()
    assert storage.get_password("flowfile", "master_key") == "mk"
    assert _temp_files(storage) == []


def test_replace_failure_leaves_store_intact_and_cleans_up(storage, monkeypatch):
    storage.set_password("flowfile", "master_key", "mk")

    def boom(src, dst):
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", boom)
    storage.set_password("flowfile", "internal_token", "it")
    monkeypatch.undo()

    assert storage.get_password("flowfile", "master_key") == "mk"
    assert storage.get_password("flowfile", "internal_token") is None
    assert _temp_files(storage) == []


def test_electron_store_path_honors_override(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    override = tmp_path / "override"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(override))

    assert SecureStorage().storage_path == override


def test_electron_store_path_defaults_to_app_data(tmp_path, monkeypatch):
    """Without the override, real desktop users keep the app-data location."""
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.delenv("FLOWFILE_SECURE_STORAGE_PATH", raising=False)
    monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))

    assert SecureStorage().storage_path == tmp_path / "appdata" / "flowfile"


def _worker_store_path(env_overrides: dict[str, str]) -> str:
    """Resolve flowfile_worker's store path in its own process.

    Importing flowfile_worker here would call multiprocessing.set_start_method(
    "spawn", force=True) on the test process.
    """
    env = {**os.environ, **env_overrides}
    for key, value in env_overrides.items():
        if value is None:
            env.pop(key, None)
    out = subprocess.run(
        [
            sys.executable,
            "-c",
            "from flowfile_worker.secrets import SecureStorage; print(SecureStorage().storage_path)",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=True,
    )
    return out.stdout.strip().splitlines()[-1]


@pytest.mark.parametrize("with_override", [True, False])
def test_worker_resolves_the_same_store_as_core(tmp_path, monkeypatch, with_override):
    """Core encrypts $ffsec$ secrets with the master key in this dir; the worker
    re-derives it from its own resolver, so a divergence breaks every
    worker-offloaded run that touches a secret (database readers, cloud writers).
    """
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))
    if with_override:
        monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(tmp_path / "store"))
        env_overrides = {"FLOWFILE_SECURE_STORAGE_PATH": str(tmp_path / "store")}
    else:
        monkeypatch.delenv("FLOWFILE_SECURE_STORAGE_PATH", raising=False)
        env_overrides = {"FLOWFILE_SECURE_STORAGE_PATH": None}
    env_overrides["FLOWFILE_MODE"] = "electron"
    env_overrides["APPDATA"] = str(tmp_path / "appdata")

    assert _worker_store_path(env_overrides) == str(SecureStorage().storage_path)


def test_suite_store_matches_the_isolation_decision():
    """The conftest decision must be applied before flowfile_core is imported.

    When no worker was running the suite must be isolated; when one was, core
    deliberately stays on the real store so it shares a master key with it.
    """
    app_data = os.environ.get("APPDATA") or os.path.expanduser("~/.config")
    real_store = Path(app_data) / "flowfile"
    effective = secrets_module._storage.storage_path

    pinned = os.environ.get("FLOWFILE_SECURE_STORAGE_PATH")
    if pinned:
        assert effective == Path(pinned)
        assert effective != real_store
    else:
        # Only legitimate because an external worker owns the real store.
        assert effective == real_store
        assert worker_is_listening()


@pytest.mark.parametrize(
    ("worker_running", "env", "expects_path"),
    [
        (False, {}, True),
        (True, {}, False),
        (True, {"FLOWFILE_SECURE_STORAGE_PATH": "/explicit"}, False),
        (False, {"FLOWFILE_SECURE_STORAGE_PATH": "/explicit"}, False),
    ],
)
def test_isolation_decision(worker_running, env, expects_path):
    """Isolate only when nothing else already owns the store."""
    resolved = resolve_test_secure_storage_path(worker_running, env)
    assert (resolved is not None) is expects_path
    if expects_path:
        assert resolved.endswith(DEFAULT_STORE_DIRNAME)
