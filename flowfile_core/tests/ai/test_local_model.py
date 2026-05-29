"""Tests for the on-demand local LLM runtime (manager + LocalProvider + routes).

Pure-unit: no network, no real ``llama-server``. Download / extract / model
write are monkeypatched so the install orchestration (skip logic, progress
events, GGUF verify) is exercised without touching the network or spawning a
subprocess.
"""

from __future__ import annotations

import pytest

from flowfile_core.ai.local_model import manager
from flowfile_core.ai.providers.base import Provider
from flowfile_core.ai.providers.local import LocalProvider


@pytest.fixture
def _tmp_storage(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    """Point ``storage.base_directory`` at tmp_path and ensure no server lingers."""
    from shared.storage_config import storage

    original = storage._base_dir
    storage._base_dir = tmp_path
    manager.stop()
    try:
        yield tmp_path
    finally:
        manager.stop()
        storage._base_dir = original


# --------------------------------------------------------------------------- #
# Platform / asset mapping                                                    #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("plat", "machine", "suffix"),
    [
        ("darwin", "arm64", "bin-macos-arm64.tar.gz"),
        ("darwin", "x86_64", "bin-macos-x64.tar.gz"),
        ("linux", "x86_64", "bin-ubuntu-x64.tar.gz"),
        ("linux", "aarch64", "bin-ubuntu-arm64.tar.gz"),
        ("win32", "AMD64", "bin-win-cpu-x64.zip"),
        ("win32", "ARM64", "bin-win-cpu-arm64.zip"),
    ],
)
def test_asset_for_maps_platform(monkeypatch, plat, machine, suffix):  # type: ignore[no-untyped-def]
    monkeypatch.setattr(manager.sys, "platform", plat)
    monkeypatch.setattr(manager.platform, "machine", lambda: machine)
    asset = manager.asset_for()
    assert asset is not None
    assert asset.endswith(suffix)
    assert manager.LLAMACPP_BUILD in asset


def test_asset_for_unsupported_platform(monkeypatch):  # type: ignore[no-untyped-def]
    monkeypatch.setattr(manager.sys, "platform", "sunos")
    monkeypatch.setattr(manager.platform, "machine", lambda: "sparc")
    assert manager.asset_for() is None
    assert manager.is_available() is False


def test_status_not_installed(_tmp_storage):  # type: ignore[no-untyped-def]
    st = manager.status()
    assert st["model_name"] == "qwen2.5-coder-1.5b"
    assert st["installed"] is False
    assert st["running"] is False
    assert st["install_dir"].endswith("local_model")


# --------------------------------------------------------------------------- #
# Install orchestration (monkeypatched I/O)                                   #
# --------------------------------------------------------------------------- #


def _fake_install_io(monkeypatch) -> dict[str, int]:  # type: ignore[no-untyped-def]
    """Replace the three network/IO seams; track how often each ran."""
    calls = {"download_bytes": 0, "extract": 0, "download_model": 0}

    def fake_download_bytes(url, on_progress, phase):  # type: ignore[no-untyped-def]
        calls["download_bytes"] += 1
        on_progress({"phase": phase, "received": 10, "total": 10})
        return b"fake-archive"

    def fake_extract(asset, data, dest_dir):  # type: ignore[no-untyped-def]
        calls["extract"] += 1
        manager._binary_path().write_bytes(b"#!/bin/sh\n")

    def fake_download_model(url, dest, on_progress, phase):  # type: ignore[no-untyped-def]
        calls["download_model"] += 1
        on_progress({"phase": phase, "received": 1, "total": 1})
        dest.write_bytes(b"GGUF" + b"\x00" * 1_200_000)

    monkeypatch.setattr(manager, "_download_bytes", fake_download_bytes)
    monkeypatch.setattr(manager, "_extract_archive", fake_extract)
    monkeypatch.setattr(manager, "_download_to_file", fake_download_model)
    monkeypatch.setattr(manager, "asset_for", lambda: f"llama-{manager.LLAMACPP_BUILD}-bin-macos-arm64.tar.gz")
    return calls


def test_install_downloads_and_verifies(_tmp_storage, monkeypatch):  # type: ignore[no-untyped-def]
    calls = _fake_install_io(monkeypatch)
    events: list[dict] = []
    path = manager.install(on_progress=events.append)
    assert path == str(manager._binary_path())
    assert manager.is_installed() is True
    assert manager.status()["installed"] is True
    phases = [e["phase"] for e in events]
    assert "downloading_binary" in phases
    assert "downloading_model" in phases
    assert phases[-1] == "done"
    assert calls == {"download_bytes": 1, "extract": 1, "download_model": 1}


def test_install_is_idempotent(_tmp_storage, monkeypatch):  # type: ignore[no-untyped-def]
    calls = _fake_install_io(monkeypatch)
    manager.install()
    manager.install()  # both components present → no re-download
    assert calls == {"download_bytes": 1, "extract": 1, "download_model": 1}


def test_install_unsupported_platform_raises(_tmp_storage, monkeypatch):  # type: ignore[no-untyped-def]
    monkeypatch.setattr(manager, "asset_for", lambda: None)
    with pytest.raises(manager.UnsupportedPlatform):
        manager.install()


def test_install_rejects_bad_gguf_header(_tmp_storage, monkeypatch):  # type: ignore[no-untyped-def]
    _fake_install_io(monkeypatch)

    def bad_model(url, dest, on_progress, phase):  # type: ignore[no-untyped-def]
        dest.write_bytes(b"NOTG" + b"\x00" * 1_200_000)

    monkeypatch.setattr(manager, "_download_to_file", bad_model)
    with pytest.raises(manager.LocalModelError, match="GGUF"):
        manager.install()
    assert manager._model_path().exists() is False


# --------------------------------------------------------------------------- #
# Provider                                                                    #
# --------------------------------------------------------------------------- #


def test_local_provider_config():
    p = LocalProvider(api_base="http://127.0.0.1:1234/v1", api_key="sk-local")
    assert p.name == "local"
    assert p.model == "openai/qwen2.5-coder-1.5b"
    assert p.api_base == "http://127.0.0.1:1234/v1"
    assert p.supports_tools is False
    assert isinstance(p, Provider)


# --------------------------------------------------------------------------- #
# Route registration                                                          #
# --------------------------------------------------------------------------- #


def test_local_model_routes_registered():
    import flowfile_core.ai.routes as ai_routes

    paths = {getattr(r, "path", "") for r in ai_routes.router.routes}
    for expected in (
        "/local-model/status",
        "/local-model/install",
        "/local-model/start",
        "/local-model/stop",
        "/local-model/chat",
        "/local-model/generate",
        "/local-model",
    ):
        assert expected in paths, f"{expected} not registered ({sorted(paths)})"
