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


def test_concurrent_mint_converges_via_reread(isolated_token, monkeypatch):
    """Two first-ever minters converge on whichever token actually landed."""
    jwt_module = isolated_token
    import flowfile_core.auth.secrets as secrets_module

    racing_token = "f" * 64

    def set_password_then_race(service, username, password):
        secrets_module.set_password(service, username, password)
        # A second process persists its own token last.
        secrets_module.set_password(service, username, racing_token)

    monkeypatch.setattr(jwt_module, "set_password", set_password_then_race)

    token = jwt_module.get_internal_token()

    assert token == get_password("flowfile", "internal_token")
    assert token == racing_token


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
