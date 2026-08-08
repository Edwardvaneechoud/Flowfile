"""Cross-process agreement for the kernel to core internal service token.

The token is minted lazily per process. Before persistence, a CLI subprocess
spawned by a manual or scheduled run minted a *different* token than the
long-lived server, handed it to its kernel container, and every kernel callback
to core came back 401.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

import pytest

from flowfile_core.auth.secrets import get_password, set_password


@pytest.fixture
def isolated_token(tmp_path, monkeypatch):
    """Point SecureStorage at a scratch dir and reset the per-process token cache."""
    import flowfile_core.auth.jwt as jwt_module
    import flowfile_core.auth.secrets as secrets_module

    original_env_token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(tmp_path / "store"))
    os.environ.pop("FLOWFILE_INTERNAL_TOKEN", None)
    # _storage is built at import time, so it must be rebuilt against the new path.
    monkeypatch.setattr(secrets_module, "_storage", secrets_module.SecureStorage())
    monkeypatch.setattr(jwt_module, "_internal_token", None)

    yield jwt_module

    if original_env_token is not None:
        os.environ["FLOWFILE_INTERNAL_TOKEN"] = original_env_token
    else:
        os.environ.pop("FLOWFILE_INTERNAL_TOKEN", None)


def _store_entries(tmp_path) -> set[str]:
    store_dir = tmp_path / "store"
    return {p.name for p in store_dir.iterdir()} if store_dir.exists() else set()


# --- A. cross-process agreement -------------------------------------------------


def test_token_is_persisted_and_reused_by_a_fresh_process(isolated_token, tmp_path):
    jwt_module = isolated_token

    first = jwt_module.get_internal_token()

    # Simulate a second process: no inherited env var, cold module cache.
    os.environ.pop("FLOWFILE_INTERNAL_TOKEN", None)
    jwt_module._internal_token = None
    second = jwt_module.get_internal_token()

    assert first == second
    assert get_password("flowfile", "internal_token") == first


def test_env_var_wins_over_persisted_token(isolated_token, monkeypatch):
    """Docker, CI and the kernel fixture all configure the token through the env."""
    jwt_module = isolated_token
    set_password("flowfile", "internal_token", "persisted-token")
    monkeypatch.setenv("FLOWFILE_INTERNAL_TOKEN", "abc123")

    assert jwt_module.get_internal_token() == "abc123"
    # The operator-configured token is not written over the store.
    assert get_password("flowfile", "internal_token") == "persisted-token"


def test_env_var_is_not_persisted(isolated_token, monkeypatch):
    jwt_module = isolated_token
    monkeypatch.setenv("FLOWFILE_INTERNAL_TOKEN", "abc123")

    assert jwt_module.get_internal_token() == "abc123"
    assert get_password("flowfile", "internal_token") is None


def test_docker_mode_still_raises_without_env(isolated_token, monkeypatch, tmp_path):
    jwt_module = isolated_token
    monkeypatch.setenv("FLOWFILE_MODE", "docker")

    with pytest.raises(ValueError):
        jwt_module.get_internal_token()

    assert _store_entries(tmp_path) == {".secret_key"}


def test_non_electron_non_docker_mode_still_raises(isolated_token, monkeypatch, tmp_path):
    """A service token must never land in the world-writable /tmp/.flowfile default."""
    jwt_module = isolated_token
    monkeypatch.setenv("FLOWFILE_MODE", "package")

    with pytest.raises(ValueError):
        jwt_module.get_internal_token()

    assert _store_entries(tmp_path) == {".secret_key"}


def test_loser_waits_for_the_winners_token_instead_of_minting(isolated_token):
    """The unfavorable interleaving: the other process writes *after* we would re-read.

    A re-read alone cannot fix this — it happens before the loser's write. The
    lock has to keep the loser out of the mint entirely.
    """
    jwt_module = isolated_token
    winner_token = "a" * 64
    lock_path = jwt_module._token_lock_path()

    # Stand in for a process that holds the lock and is still mid-mint.
    fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)

    def finish_winner():
        time.sleep(0.15)
        set_password("flowfile", "internal_token", winner_token)
        os.unlink(lock_path)

    helper = threading.Thread(target=finish_winner)
    helper.start()
    try:
        token = jwt_module._load_or_create_persisted_token()
    finally:
        helper.join()

    assert token == winner_token
    assert token == get_password("flowfile", "internal_token")


def test_concurrent_minters_agree(isolated_token):
    """Two first-ever minters racing for real must not end up on different tokens."""
    jwt_module = isolated_token
    results: dict[str, str] = {}
    barrier = threading.Barrier(2)

    def mint(name: str) -> None:
        barrier.wait()
        results[name] = jwt_module._load_or_create_persisted_token()

    threads = [threading.Thread(target=mint, args=(name,)) for name in ("p1", "p2")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results["p1"] == results["p2"] == get_password("flowfile", "internal_token")


def test_stale_lock_is_broken(isolated_token, monkeypatch):
    """A holder that died mid-mint must not wedge every later boot."""
    jwt_module = isolated_token
    lock_path = jwt_module._token_lock_path()
    fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    os.utime(lock_path, (0, 0))
    monkeypatch.setattr(jwt_module, "_LOCK_WAIT_SECONDS", 1.0)

    token = jwt_module._load_or_create_persisted_token()

    assert token == get_password("flowfile", "internal_token")
    assert not lock_path.exists()


# --- B. eager mint in the lifespan ----------------------------------------------


def test_lifespan_mints_token_before_children_can_spawn(isolated_token):
    """Any Popen child spawned after startup inherits the server's own token."""
    from fastapi.testclient import TestClient

    import flowfile_core.main as core_main

    jwt_module = isolated_token

    with TestClient(core_main.app):
        assert os.environ["FLOWFILE_INTERNAL_TOKEN"] == jwt_module.get_internal_token()


def test_lifespan_mint_does_not_break_docker_startup(isolated_token, monkeypatch):
    from fastapi.testclient import TestClient

    import flowfile_core.main as core_main

    monkeypatch.setenv("FLOWFILE_MODE", "docker")

    with TestClient(core_main.app):
        assert "FLOWFILE_INTERNAL_TOKEN" not in os.environ


# --- C. one true subprocess crossing -------------------------------------------


@pytest.mark.core
def test_cli_subprocess_agrees_with_parent_token(isolated_token, tmp_path):
    """A real child process with no inherited token reaches the same value."""
    jwt_module = isolated_token
    parent_token = jwt_module.get_internal_token()

    env = {
        **os.environ,
        "FLOWFILE_MODE": "electron",
        "FLOWFILE_SECURE_STORAGE_PATH": str(tmp_path / "store"),
        "TESTING": "True",
        "FLOWFILE_SKIP_INIT_DB": "1",
        "FLOWFILE_SKIP_STARTUP_MIGRATION": "1",
    }
    env.pop("FLOWFILE_INTERNAL_TOKEN", None)

    out = subprocess.run(
        [
            sys.executable,
            "-c",
            "from flowfile_core.auth.jwt import get_internal_token; print(get_internal_token())",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=True,
    )

    assert out.stdout.strip().splitlines()[-1] == parent_token
