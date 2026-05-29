"""On-demand local LLM runtime (llama.cpp ``llama-server`` + a small GGUF).

Python port of Duckle's ``engine_manager.rs`` + ``llama_chat.rs``: download a
pre-built ``llama-server`` and a ~1.1 GB Qwen2.5-Coder-1.5B GGUF into the
Flowfile storage dir, run it as a subprocess exposing an OpenAI-compatible API
on ``127.0.0.1``, and let the litellm layer (:class:`~flowfile_core.ai.providers.local.LocalProvider`)
drive it.

Nothing is downloaded or spawned until the user opts in via the
``/ai/local-model/*`` routes — if they never want it, nothing installs. The
binary + model are fetched at install time (never bundled in the wheel) so
PyPI / Electron stay small. Targets desktop / server modes; not WASM.

Module-level singleton: at most one server runs at a time, guarded by
``_lock``; installs are serialised by ``_install_lock``.
"""

from __future__ import annotations

import io
import logging
import os
import platform
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from shared.storage_config import storage

logger = logging.getLogger(__name__)

# Pinned artifacts — mirror duckle/apps/desktop/src/engine_manager.rs. The GGUF
# wire format is stable, so a newer server build keeps working with this model.
LLAMACPP_REPO = "ggml-org/llama.cpp"
LLAMACPP_BUILD = "b9305"
LLAMA_MODEL_REPO = "Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF"
LLAMA_MODEL_FILE = "qwen2.5-coder-1.5b-instruct-q4_k_m.gguf"
LLAMA_MODEL_NAME = "qwen2.5-coder-1.5b"
APPROX_DOWNLOAD_MB = 1150

_USER_AGENT = "flowfile"
_CHUNK = 256 * 1024
_CONNECT_TIMEOUT = 60.0
_HEALTH_TIMEOUT = 90.0
# Small context fits the system prompt + a few turns / a flow JSON on a small
# machine; override via env for power users.
_CTX_SIZE = int(os.environ.get("FLOWFILE_LOCAL_MODEL_CTX", "4096"))

ProgressFn = Callable[[dict], None]

_lock = threading.RLock()
_install_lock = threading.Lock()
_server: _RunningServer | None = None


class LocalModelError(RuntimeError):
    """Base error for local-model install / runtime failures."""


class LocalModelNotInstalled(LocalModelError):
    def __init__(self) -> None:
        super().__init__("Local model is not installed. Install it first from AI settings.")


class UnsupportedPlatform(LocalModelError):
    """Raised when no llama.cpp build exists for this OS/arch."""


# --------------------------------------------------------------------------- #
# Platform + paths                                                            #
# --------------------------------------------------------------------------- #


def _os_arch() -> tuple[str, str]:
    plat = sys.platform
    if plat == "darwin":
        os_name = "macos"
    elif plat.startswith("win"):
        os_name = "windows"
    elif plat.startswith("linux"):
        os_name = "linux"
    else:
        os_name = plat
    machine = platform.machine().lower()
    if machine in ("x86_64", "amd64"):
        arch = "x64"
    elif machine in ("arm64", "aarch64"):
        arch = "arm64"
    else:
        arch = machine
    return os_name, arch


def asset_for() -> str | None:
    """llama.cpp release asset (CPU build) for this OS/arch, or ``None`` if unsupported."""
    table = {
        ("windows", "x64"): f"llama-{LLAMACPP_BUILD}-bin-win-cpu-x64.zip",
        ("windows", "arm64"): f"llama-{LLAMACPP_BUILD}-bin-win-cpu-arm64.zip",
        ("linux", "x64"): f"llama-{LLAMACPP_BUILD}-bin-ubuntu-x64.tar.gz",
        ("linux", "arm64"): f"llama-{LLAMACPP_BUILD}-bin-ubuntu-arm64.tar.gz",
        ("macos", "arm64"): f"llama-{LLAMACPP_BUILD}-bin-macos-arm64.tar.gz",
        ("macos", "x64"): f"llama-{LLAMACPP_BUILD}-bin-macos-x64.tar.gz",
    }
    return table.get(_os_arch())


def is_available() -> bool:
    return asset_for() is not None


def unsupported_platform_detail() -> str:
    os_name, arch = _os_arch()
    return f"No local-model build available for {os_name}-{arch}."


def _engine_dir() -> Path:
    return storage.local_model_directory


def _binary_name() -> str:
    return "llama-server.exe" if sys.platform.startswith("win") else "llama-server"


def _binary_path() -> Path:
    return _engine_dir() / _binary_name()


def _model_path() -> Path:
    return _engine_dir() / LLAMA_MODEL_FILE


def _binary_installed() -> bool:
    p = _binary_path()
    return p.exists() and p.stat().st_size > 0


def _model_installed() -> bool:
    p = _model_path()
    return p.exists() and p.stat().st_size > 1_000_000


def is_installed() -> bool:
    return _binary_installed() and _model_installed()


# --------------------------------------------------------------------------- #
# Download + extract                                                          #
# --------------------------------------------------------------------------- #


def _open(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    return urllib.request.urlopen(req, timeout=_CONNECT_TIMEOUT)  # noqa: S310


def _content_length(resp) -> int | None:
    raw = resp.headers.get("Content-Length")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _download_bytes(url: str, on_progress: ProgressFn, phase: str) -> bytes:
    try:
        with _open(url) as resp:
            total = _content_length(resp)
            on_progress({"phase": phase, "received": 0, "total": total})
            buf = bytearray()
            while True:
                chunk = resp.read(_CHUNK)
                if not chunk:
                    break
                buf += chunk
                on_progress({"phase": phase, "received": len(buf), "total": total})
            return bytes(buf)
    except Exception as exc:  # noqa: BLE001 — wrap to a friendly install error
        raise LocalModelError(f"Download failed ({url}): {exc}") from exc


def _download_to_file(url: str, dest: Path, on_progress: ProgressFn, phase: str) -> None:
    """Stream a (large) file to disk via a ``.part`` temp, then atomic-rename."""
    tmp = dest.with_name(dest.name + ".part")
    try:
        with _open(url) as resp, open(tmp, "wb") as fh:
            total = _content_length(resp)
            on_progress({"phase": phase, "received": 0, "total": total})
            received = 0
            while True:
                chunk = resp.read(_CHUNK)
                if not chunk:
                    break
                fh.write(chunk)
                received += len(chunk)
                on_progress({"phase": phase, "received": received, "total": total})
        tmp.replace(dest)
    except Exception as exc:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise LocalModelError(f"Download failed ({url}): {exc}") from exc


def _extract_archive(asset: str, data: bytes, dest_dir: Path) -> None:
    """Extract every file's leaf into ``dest_dir`` (flattened).

    llama.cpp ships the server binary alongside shared libs it dlopens at run
    time; they must co-locate. Using only the basename also defends against
    path-traversal entries in the archive.
    """
    lower = asset.lower()
    if lower.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                leaf = Path(info.filename).name
                if not leaf:
                    continue
                with zf.open(info) as src, open(dest_dir / leaf, "wb") as out:
                    shutil.copyfileobj(src, out)
    elif lower.endswith(".tar.gz") or lower.endswith(".tgz"):
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue
                leaf = Path(member.name).name
                if not leaf:
                    continue
                src = tf.extractfile(member)
                if src is None:
                    continue
                with src, open(dest_dir / leaf, "wb") as out:
                    shutil.copyfileobj(src, out)
    else:
        raise LocalModelError(f"Unknown archive type for asset {asset!r}")


def _make_executable(path: Path) -> None:
    if sys.platform.startswith("win"):
        return
    try:
        path.chmod(0o755)
    except OSError as exc:
        logger.warning("could not chmod %s: %s", path, exc)


def _verify_gguf(path: Path) -> None:
    with open(path, "rb") as fh:
        magic = fh.read(4)
    if magic != b"GGUF":
        path.unlink(missing_ok=True)
        raise LocalModelError("Downloaded model is not a valid GGUF file (bad header).")


def install(on_progress: ProgressFn | None = None) -> str:
    """Download + install the llama-server binary and the GGUF model.

    Idempotent: skips a component that's already present. Emits progress dicts
    (``{"phase", "received", "total"}``; terminal ``{"phase": "done"}``) via
    ``on_progress``. Raises :class:`LocalModelError` on failure. Returns the
    binary path. Blocking — call from a worker thread.
    """
    cb: ProgressFn = on_progress or (lambda ev: None)
    asset = asset_for()
    if asset is None:
        raise UnsupportedPlatform(unsupported_platform_detail())

    if not _install_lock.acquire(blocking=False):
        raise LocalModelError("An install is already in progress.")
    try:
        engine_dir = _engine_dir()
        engine_dir.mkdir(parents=True, exist_ok=True)

        binary = _binary_path()
        if not _binary_installed():
            url = f"https://github.com/{LLAMACPP_REPO}/releases/download/{LLAMACPP_BUILD}/{asset}"
            data = _download_bytes(url, cb, "downloading_binary")
            cb({"phase": "extracting"})
            _extract_archive(asset, data, engine_dir)
            if not binary.exists():
                raise LocalModelError("llama-server binary not found inside the downloaded archive.")
        _make_executable(binary)

        if not _model_installed():
            model_url = f"https://huggingface.co/{LLAMA_MODEL_REPO}/resolve/main/{LLAMA_MODEL_FILE}"
            _download_to_file(model_url, _model_path(), cb, "downloading_model")
            cb({"phase": "verifying"})
            _verify_gguf(_model_path())

        cb({"phase": "done", "path": str(binary)})
        return str(binary)
    finally:
        _install_lock.release()


# --------------------------------------------------------------------------- #
# Server lifecycle                                                            #
# --------------------------------------------------------------------------- #


@dataclass
class _RunningServer:
    proc: subprocess.Popen
    port: int


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _num_threads() -> int:
    # Half the cores so the rest of the machine stays responsive.
    cpu = os.cpu_count() or 4
    return max(2, cpu // 2)


def _health_ok(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1.0) as resp:  # noqa: S310
            return resp.status < 400
    except Exception:  # noqa: BLE001 — any failure means "not ready yet"
        return False


def _spawn(binary: Path, model: Path) -> _RunningServer:
    port = _free_port()
    cmd = [
        str(binary),
        "--host", "127.0.0.1",
        "--port", str(port),
        "--model", str(model),
        "--ctx-size", str(_CTX_SIZE),
        "--threads", str(_num_threads()),
        "--log-disable",
    ]
    creationflags = 0x08000000 if sys.platform.startswith("win") else 0  # CREATE_NO_WINDOW
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    except OSError as exc:
        raise LocalModelError(f"Failed to start llama-server: {exc}") from exc

    deadline = time.monotonic() + _HEALTH_TIMEOUT
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise LocalModelError("llama-server exited during startup.")
        if _health_ok(port):
            return _RunningServer(proc=proc, port=port)
        time.sleep(0.25)
    _kill_proc(proc)
    raise LocalModelError(f"llama-server did not become ready within {int(_HEALTH_TIMEOUT)}s.")


def _kill_proc(proc: subprocess.Popen) -> None:
    try:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    except Exception as exc:  # noqa: BLE001
        logger.warning("error stopping llama-server: %s", exc)


def is_running() -> bool:
    global _server
    with _lock:
        if _server is None:
            return False
        if _server.proc.poll() is not None:
            _server = None
            return False
        return True


def current_base_url() -> str | None:
    """OpenAI base URL of the running server, or ``None`` if not running (no spawn)."""
    with _lock:
        if is_running() and _server is not None:
            return f"http://127.0.0.1:{_server.port}/v1"
        return None


def ensure_running() -> str:
    """Start the server if needed (lazy boot) and return its OpenAI base URL.

    Blocks until the server passes ``/health`` (cold boot takes a few seconds).
    Call from a worker thread (``asyncio.to_thread``) so the event loop stays
    free. Raises :class:`LocalModelNotInstalled` if files are missing.
    """
    global _server
    with _lock:
        if is_running() and _server is not None:
            return f"http://127.0.0.1:{_server.port}/v1"
        if not is_installed():
            raise LocalModelNotInstalled()
        _server = _spawn(_binary_path(), _model_path())
        return f"http://127.0.0.1:{_server.port}/v1"


def stop() -> None:
    global _server
    with _lock:
        if _server is not None:
            _kill_proc(_server.proc)
            _server = None


def uninstall() -> None:
    """Stop the server and remove the binary + model from disk."""
    stop()
    shutil.rmtree(_engine_dir(), ignore_errors=True)


def status() -> dict:
    """Snapshot for the UI: availability, install state, run state, sizing."""
    return {
        "available": is_available(),
        "installed": is_installed(),
        "binary_installed": _binary_installed(),
        "model_installed": _model_installed(),
        "running": is_running(),
        "model_name": LLAMA_MODEL_NAME,
        "model_file": LLAMA_MODEL_FILE,
        "approx_download_mb": APPROX_DOWNLOAD_MB,
        "install_dir": str(_engine_dir()),
    }
