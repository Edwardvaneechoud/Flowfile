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
    assert st["selected_model_id"] == manager.DEFAULT_MODEL_ID
    assert st["installed"] is False
    assert st["running"] is False
    assert st["any_model_installed"] is False
    assert st["install_dir"].endswith("local_model")
    # The catalog is surfaced for the UI picker; nothing installed yet.
    ids = {m["id"] for m in st["models"]}
    assert ids == set(manager.MODELS)
    assert all(m["installed"] is False for m in st["models"])


def test_extract_archive_preserves_symlinks(tmp_path):  # type: ignore[no-untyped-def]
    """Regression: llama.cpp tarballs ship version-alias symlinks the binary
    dlopens via @rpath. The extractor must recreate them (flattened to basename
    targets), not drop them — dropping caused 'llama-server exited during
    startup' (dyld: Library not loaded)."""
    import io
    import tarfile

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        real = b"\x00" * 32
        info = tarfile.TarInfo(name="build/bin/libllama.0.0.9305.dylib")
        info.size = len(real)
        tf.addfile(info, io.BytesIO(real))
        # Version-alias symlink (target is a sibling, possibly path-qualified).
        link = tarfile.TarInfo(name="build/bin/libllama.0.dylib")
        link.type = tarfile.SYMTYPE
        link.linkname = "libllama.0.0.9305.dylib"
        tf.addfile(link)

    manager._extract_archive("llama-bxxxx-bin-macos-arm64.tar.gz", buf.getvalue(), tmp_path)

    alias = tmp_path / "libllama.0.dylib"
    assert alias.is_symlink(), "version-alias symlink was not recreated"
    assert (tmp_path / "libllama.0.0.9305.dylib").exists()
    # Flattened to basename so it resolves as a sibling.
    import os

    assert os.readlink(alias) == "libllama.0.0.9305.dylib"
    assert alias.resolve().exists()


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
    assert manager._model_path(manager.DEFAULT_MODEL_ID).exists() is False


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
# Provider unification — local resolvable on read-only surfaces               #
# --------------------------------------------------------------------------- #


def test_is_resolvable_provider():
    from flowfile_core.ai.providers import is_resolvable_provider

    assert is_resolvable_provider("local") is True
    assert is_resolvable_provider("anthropic") is True
    assert is_resolvable_provider("nope") is False
    assert is_resolvable_provider(None) is False


def test_local_not_in_byok_providers():
    # Local must stay OUT of the BYOK registry so it never pollutes the
    # credential list / upsert / test routes.
    from flowfile_core.ai.providers import PROVIDERS

    assert "local" not in PROVIDERS


def test_get_configured_provider_returns_local_without_db():
    # The local branch returns before any DB / credential access, so a None db
    # is fine — proving local needs no BYOK row, key, or env var.
    from flowfile_core.ai.byok import get_configured_provider

    p = get_configured_provider(None, 0, "local", surface="explain")  # type: ignore[arg-type]
    assert isinstance(p, LocalProvider)
    assert p.supports_tools is False


# --------------------------------------------------------------------------- #
# Route registration                                                          #
# --------------------------------------------------------------------------- #


def test_local_model_routes_registered():
    import flowfile_core.ai.routes as ai_routes

    paths = {getattr(r, "path", "") for r in ai_routes.router.routes}
    for expected in (
        "/local-model/status",
        "/local-model/install",
        "/local-model/select",
        "/local-model/start",
        "/local-model/stop",
        "/local-model",
        # Flow generation is now a provider-agnostic surface, not local-only.
        "/generate",
    ):
        assert expected in paths, f"{expected} not registered ({sorted(paths)})"
    # Local chat + local-only generate are gone — local rides /ai/chat/stream
    # and /ai/generate like any other provider.
    assert "/local-model/chat" not in paths
    assert "/local-model/generate" not in paths
