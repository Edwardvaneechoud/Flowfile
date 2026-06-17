"""On-demand local LLM runtime (llama.cpp ``llama-server`` + a small GGUF).

Python port of Duckle's ``engine_manager.rs`` + ``llama_chat.rs``: download a
pre-built ``llama-server`` and a small Qwen2.5-Coder GGUF (default ~2 GB
Qwen2.5-Coder-3B) into the Flowfile storage dir, run it as a subprocess exposing
an OpenAI-compatible API on ``127.0.0.1``, and let the litellm layer
(:class:`~flowfile_core.ai.providers.local.LocalProvider`) drive it.

Nothing is downloaded or spawned until the user opts in via the
``/ai/local-model/*`` routes — if they never want it, nothing installs. The
binary + model are fetched at install time (never bundled in the wheel) so
PyPI / Electron stay small. Targets desktop / server modes; not WASM.

Module-level singleton: at most one server runs at a time. ``_lock`` guards the
``_server`` global for fast reads/writes, ``_boot_lock`` serialises the blocking
boot (so ``status()`` stays responsive during a cold start), and installs are
serialised by ``_install_lock``.
"""

from __future__ import annotations

import io
import logging
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from shared.storage_config import storage

logger = logging.getLogger(__name__)

# Pinned llama.cpp server build — mirror duckle/apps/desktop/src/engine_manager.rs.
# The GGUF wire format is stable, so this one server binary serves every model
# in the catalog below.
LLAMACPP_REPO = "ggml-org/llama.cpp"
LLAMACPP_BUILD = "b9305"


@dataclass(frozen=True)
class ModelSpec:
    """One installable GGUF model. All q4_k_m single-file GGUFs (the manager's
    streaming downloader can't reassemble split files), verified to resolve on
    HuggingFace. ``id`` is the stable wire key; the GGUF lands at
    ``<dir>/<id>.gguf`` so several models can coexist on disk."""

    id: str
    name: str
    repo: str
    file: str
    approx_mb: int
    description: str


# Curated catalog. ``qwen2.5-coder-3b`` is the default — the best balance of
# quality and speed for flow building, and a clear step up from the 1.5B without
# the RAM/latency cost of the 7B. All entries are Qwen2.5-Coder/Instruct: code-
# and structured-JSON-tuned, which is what one-shot flow generation needs.
# Ordered smallest→largest.
MODELS: dict[str, ModelSpec] = {
    "qwen2.5-coder-1.5b": ModelSpec(
        id="qwen2.5-coder-1.5b",
        name="Qwen2.5-Coder 1.5B",
        repo="Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF",
        file="qwen2.5-coder-1.5b-instruct-q4_k_m.gguf",
        approx_mb=1100,
        description="Fastest and lightest. Good for low-RAM machines, but lower quality on bigger flows.",
    ),
    "qwen2.5-coder-3b": ModelSpec(
        id="qwen2.5-coder-3b",
        name="Qwen2.5-Coder 3B",
        repo="Qwen/Qwen2.5-Coder-3B-Instruct-GGUF",
        file="qwen2.5-coder-3b-instruct-q4_k_m.gguf",
        approx_mb=1960,
        description="Recommended. Best balance of quality and speed for flow building; still snappy on CPU.",
    ),
    "qwen2.5-7b": ModelSpec(
        id="qwen2.5-7b",
        name="Qwen2.5 7B Instruct",
        repo="bartowski/Qwen2.5-7B-Instruct-GGUF",
        file="Qwen2.5-7B-Instruct-Q4_K_M.gguf",
        approx_mb=4360,
        description="Strongest reasoning, but slow on CPU (a few tok/s) and needs ~6 GB free RAM.",
    ),
}

DEFAULT_MODEL_ID = "qwen2.5-coder-3b"

_USER_AGENT = "flowfile"
_CHUNK = 256 * 1024
_CONNECT_TIMEOUT = 60.0
_HEALTH_TIMEOUT = 90.0
# Context window default. 16384 gives headroom for the (compacted) flow
# context + several chat turns; Qwen2.5 supports 32k natively and q4 KV-cache at
# 16k is still modest RAM for a 1.5-3B model. The effective value is
# user-settable from the UI (persisted in a sidecar, see ``get_ctx_size``); this
# env var only seeds the first-run default.
_DEFAULT_CTX_SIZE = int(os.environ.get("FLOWFILE_LOCAL_MODEL_CTX", "16384"))
# Guardrails for the user-set value: below ~2k the prompt won't fit; above 32k
# exceeds Qwen2.5's native window and burns RAM for no gain on these models.
_MIN_CTX_SIZE = 2048
_MAX_CTX_SIZE = 32768

ProgressFn = Callable[[dict], None]

# ``_lock`` guards the ``_server`` global for FAST reads/writes only — it must
# never be held across the blocking ``_spawn`` health-poll, or ``status()`` /
# ``is_running()`` (which also take it) would hang for the whole cold boot.
# ``_boot_lock`` serialises the slow boot path instead, so at most one server
# spawns at a time while status stays responsive.
_lock = threading.RLock()
_boot_lock = threading.Lock()
_install_lock = threading.Lock()
_server: _RunningServer | None = None


class LocalModelError(RuntimeError):
    """Base error for local-model install / runtime failures."""


class LocalModelNotInstalled(LocalModelError):
    def __init__(self) -> None:
        super().__init__("Local model is not installed. Install it first from AI settings.")


class UnsupportedPlatform(LocalModelError):
    """Raised when no llama.cpp build exists for this OS/arch."""


# Platform + paths


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


def resolve_model(model_id: str | None) -> ModelSpec:
    """Map a model id to its spec, falling back to the default. Unknown ids
    raise so a typo in an API call fails loud rather than silently swapping."""
    if model_id is None:
        return MODELS[DEFAULT_MODEL_ID]
    spec = MODELS.get(model_id)
    if spec is None:
        raise LocalModelError(f"Unknown local model id {model_id!r}. Known: {sorted(MODELS)}.")
    return spec


def _model_path(model_id: str) -> Path:
    # Store under the stable id (not the upstream filename) so several models
    # coexist and the selected-model lookup is filename-agnostic.
    return _engine_dir() / f"{model_id}.gguf"


_SELECTED_FILE = "selected_model.txt"


def _selected_path() -> Path:
    return _engine_dir() / _SELECTED_FILE


def get_selected_model_id() -> str:
    """The model the server will run. Reads the sidecar file; falls back to the
    default, or to any single installed model if the default isn't present."""
    try:
        raw = _selected_path().read_text(encoding="utf-8").strip()
        if raw in MODELS:
            return raw
    except OSError:
        pass
    if _model_installed(DEFAULT_MODEL_ID):
        return DEFAULT_MODEL_ID
    for mid in MODELS:
        if _model_installed(mid):
            return mid
    return DEFAULT_MODEL_ID


def set_selected_model_id(model_id: str) -> None:
    resolve_model(model_id)  # validate
    _engine_dir().mkdir(parents=True, exist_ok=True)
    _selected_path().write_text(model_id, encoding="utf-8")


_CTX_FILE = "ctx_size.txt"


def _ctx_path() -> Path:
    return _engine_dir() / _CTX_FILE


def get_ctx_size() -> int:
    """The llama-server context window the next boot will use. Reads the sidecar
    (set from the UI); falls back to the env-seeded default. Always clamped to
    ``[_MIN_CTX_SIZE, _MAX_CTX_SIZE]`` so a stale/garbage file can't break boot."""
    try:
        raw = int(_ctx_path().read_text(encoding="utf-8").strip())
        return max(_MIN_CTX_SIZE, min(_MAX_CTX_SIZE, raw))
    except (OSError, ValueError):
        return _DEFAULT_CTX_SIZE


def set_ctx_size(ctx_size: int) -> int:
    """Persist the context window (clamped). Returns the stored value. The caller
    recycles the server so the new size takes effect on the next boot."""
    clamped = max(_MIN_CTX_SIZE, min(_MAX_CTX_SIZE, int(ctx_size)))
    _engine_dir().mkdir(parents=True, exist_ok=True)
    _ctx_path().write_text(str(clamped), encoding="utf-8")
    return clamped


def _binary_installed() -> bool:
    p = _binary_path()
    return p.exists() and p.stat().st_size > 0


def _model_installed(model_id: str) -> bool:
    p = _model_path(model_id)
    return p.exists() and p.stat().st_size > 1_000_000


def installed_model_ids() -> list[str]:
    return [mid for mid in MODELS if _model_installed(mid)]


def is_installed(model_id: str | None = None) -> bool:
    """True when the binary + the given model (default: selected) are present."""
    mid = model_id or get_selected_model_id()
    return _binary_installed() and _model_installed(mid)


# Download + extract


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


def _write_symlink(dest_dir: Path, leaf: str, target: str) -> None:
    """Recreate a (flattened) symlink at ``dest_dir/leaf`` -> basename(target).

    llama.cpp's macOS / Linux tarballs ship version-alias symlinks
    (``libllama.0.dylib`` -> ``libllama.0.0.9305.dylib``) that the server
    binary loads via ``@rpath`` / ``DT_NEEDED``. Because we flatten the archive
    to basenames, the link target is reduced to its basename too so it resolves
    as a sibling. Idempotent (replaces an existing entry).
    """
    if not target:
        return
    link_path = dest_dir / leaf
    if link_path.exists() or link_path.is_symlink():
        link_path.unlink()
    os.symlink(Path(target).name, link_path)


def _extract_archive(asset: str, data: bytes, dest_dir: Path) -> None:
    """Extract every entry's leaf into ``dest_dir`` (flattened).

    llama.cpp ships the server binary alongside shared libs it dlopens at run
    time AND version-alias **symlinks** pointing at them; all must co-locate
    or the binary fails to start with ``dyld: Library not loaded``. Symlinks
    are preserved (not dereferenced) so the alias names exist on disk. Using
    only the basename also defends against path-traversal entries.
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
                # Zip stores a symlink as a regular entry whose body is the
                # link target, flagged S_IFLNK (0o120000) in the high mode bits.
                mode = (info.external_attr >> 16) & 0o170000
                if mode == 0o120000:
                    target = zf.read(info).decode("utf-8", errors="replace").strip()
                    _write_symlink(dest_dir, leaf, target)
                    continue
                with zf.open(info) as src, open(dest_dir / leaf, "wb") as out:
                    shutil.copyfileobj(src, out)
    elif lower.endswith(".tar.gz") or lower.endswith(".tgz"):
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            for member in tf.getmembers():
                leaf = Path(member.name).name
                if not leaf:
                    continue
                if member.issym() or member.islnk():
                    _write_symlink(dest_dir, leaf, member.linkname)
                    continue
                if not member.isfile():
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


def install(model_id: str | None = None, on_progress: ProgressFn | None = None) -> str:
    """Download + install the shared llama-server binary and one model's GGUF.

    ``model_id`` selects the catalog entry (default: the catalog default). The
    downloaded model is marked selected so the next ``start`` runs it. Idempotent
    per component: an already-present binary / model is skipped. Emits progress
    dicts (``{"phase", "received", "total", "model_id"}``; terminal
    ``{"phase": "done"}``). Raises :class:`LocalModelError` on failure. Blocking —
    call from a worker thread.
    """
    cb: ProgressFn = on_progress or (lambda ev: None)
    asset = asset_for()
    if asset is None:
        raise UnsupportedPlatform(unsupported_platform_detail())
    spec = resolve_model(model_id)

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

        target = _model_path(spec.id)
        if not _model_installed(spec.id):
            model_url = f"https://huggingface.co/{spec.repo}/resolve/main/{spec.file}"

            def _model_progress(ev: dict) -> None:
                cb({**ev, "model_id": spec.id})

            _download_to_file(model_url, target, _model_progress, "downloading_model")
            cb({"phase": "verifying", "model_id": spec.id})
            _verify_gguf(target)

        # Newly-installed model becomes the active one.
        set_selected_model_id(spec.id)
        cb({"phase": "done", "path": str(binary), "model_id": spec.id})
        return str(binary)
    finally:
        _install_lock.release()


# Server lifecycle


@dataclass
class _RunningServer:
    proc: subprocess.Popen
    port: int
    model_id: str


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


def _describe_exit(rc: int) -> str:
    """Cause for a llama-server exit. Negative ``rc`` is a POSIX signal: SIGKILL
    is almost always the OOM killer, SIGILL the binary using CPU instructions
    this host/arch lacks (incl. cross-arch qemu emulation in Docker)."""
    if rc >= 0:
        if rc == 127:
            return "exit code 127 (a shared library is missing, e.g. libgomp.so.1)"
        return f"exit code {rc}"
    try:
        sig = signal.Signals(-rc)
    except ValueError:
        return f"killed by signal {-rc}"
    if sig == getattr(signal, "SIGKILL", None):
        return f"killed by {sig.name} (likely out of memory)"
    if sig == getattr(signal, "SIGILL", None):
        return f"killed by {sig.name} (binary needs CPU instructions this host/arch lacks)"
    return f"killed by {sig.name}"


def _spawn(binary: Path, model: Path, model_id: str) -> _RunningServer:
    port = _free_port()
    cmd = [
        str(binary),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--model",
        str(model),
        "--ctx-size",
        str(get_ctx_size()),
        "--threads",
        str(_num_threads()),
    ]
    creationflags = 0x08000000 if sys.platform.startswith("win") else 0  # CREATE_NO_WINDOW
    # Capture stderr to a temp file so a startup failure carries the real ggml /
    # dyld error (no --log-disable, which would suppress those lines). A read-only
    # temp dir disables capture; that fact is surfaced so it isn't a blind spot.
    try:
        err_fh = tempfile.TemporaryFile()
    except OSError:
        err_fh = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=err_fh if err_fh is not None else subprocess.DEVNULL,
            creationflags=creationflags,
        )
    except OSError as exc:
        if err_fh is not None:
            err_fh.close()
        raise LocalModelError(f"Failed to start llama-server: {exc}") from exc

    def _stderr_suffix() -> str:
        if err_fh is None:
            return " (stderr capture unavailable — temp dir not writable)"
        try:
            err_fh.seek(0)
            raw = err_fh.read().decode("utf-8", errors="replace").strip()
        except OSError:
            return ""
        if not raw:
            return ""
        tail = " | ".join(line.strip() for line in raw.splitlines()[-3:] if line.strip())
        return f": {tail}" if tail else ""

    try:
        deadline = time.monotonic() + _HEALTH_TIMEOUT
        while time.monotonic() < deadline:
            rc = proc.poll()
            if rc is not None:
                raise LocalModelError(f"llama-server exited during startup ({_describe_exit(rc)}){_stderr_suffix()}")
            if _health_ok(port):
                return _RunningServer(proc=proc, port=port, model_id=model_id)
            time.sleep(0.25)
        _kill_proc(proc)
        raise LocalModelError(f"llama-server did not become ready within {int(_HEALTH_TIMEOUT)}s{_stderr_suffix()}.")
    finally:
        if err_fh is not None:
            err_fh.close()


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


def running_model_id() -> str | None:
    with _lock:
        return _server.model_id if (is_running() and _server is not None) else None


def ensure_running() -> str:
    """Start the selected model's server if needed and return its base URL.

    If a server is already running a *different* model than the current
    selection, it's stopped and respawned with the selected one (so switching
    models in the UI takes effect). Blocks until ``/health`` passes. Call from a
    worker thread. Raises :class:`LocalModelNotInstalled` if files are missing.

    The blocking ``_spawn`` runs OUTSIDE ``_lock`` (serialised by ``_boot_lock``)
    so a concurrent ``status()`` / ``is_running()`` poll stays responsive during
    a cold boot instead of hanging for the whole health-check window.
    """
    global _server
    # Fast path: already running exactly what we want — no boot needed.
    with _lock:
        wanted = get_selected_model_id()
        if is_running() and _server is not None and _server.model_id == wanted:
            return f"http://127.0.0.1:{_server.port}/v1"

    # Slow path: serialise boots so two callers can't spawn at once. ``_lock``
    # is taken only for the quick re-check + the final ``_server`` swap.
    with _boot_lock:
        with _lock:
            wanted = get_selected_model_id()
            if is_running() and _server is not None and _server.model_id == wanted:
                return f"http://127.0.0.1:{_server.port}/v1"
            # A running wrong-model server (or None). Capture, drop the lock,
            # then recycle it before booting the wanted model.
            stale = _server
        if stale is not None:
            _kill_proc(stale.proc)
            with _lock:
                if _server is stale:
                    _server = None
        if not is_installed(wanted):
            raise LocalModelNotInstalled()
        # Blocks up to ``_HEALTH_TIMEOUT`` polling /health — deliberately not
        # under ``_lock``.
        server = _spawn(_binary_path(), _model_path(wanted), wanted)
        with _lock:
            _server = server
        return f"http://127.0.0.1:{server.port}/v1"


def set_active_model(model_id: str) -> None:
    """Mark ``model_id`` selected and, if a server is running a different model,
    stop it so the next ``ensure_running`` boots the new pick. Raises if the
    model isn't installed yet."""
    global _server
    resolve_model(model_id)
    if not _model_installed(model_id):
        raise LocalModelNotInstalled()
    set_selected_model_id(model_id)
    with _lock:
        if _server is not None and _server.model_id != model_id:
            _kill_proc(_server.proc)
            _server = None


def stop() -> None:
    global _server
    with _lock:
        if _server is not None:
            _kill_proc(_server.proc)
            _server = None


def uninstall(model_id: str | None = None) -> None:
    """Remove a single model's GGUF, or (when ``model_id`` is None) the entire
    runtime — binary, every model, and the selection sidecar."""
    global _server
    if model_id is None:
        stop()
        shutil.rmtree(_engine_dir(), ignore_errors=True)
        return
    resolve_model(model_id)
    with _lock:
        if _server is not None and _server.model_id == model_id:
            _kill_proc(_server.proc)
            _server = None
    _model_path(model_id).unlink(missing_ok=True)


def status() -> dict:
    """Snapshot for the UI: availability, per-model install state, run state."""
    selected = get_selected_model_id()
    selected_spec = MODELS[selected]
    models = [
        {
            "id": spec.id,
            "name": spec.name,
            "approx_download_mb": spec.approx_mb,
            "description": spec.description,
            "installed": _model_installed(spec.id),
        }
        for spec in MODELS.values()
    ]
    return {
        "available": is_available(),
        "binary_installed": _binary_installed(),
        "installed": is_installed(selected),
        "model_installed": _model_installed(selected),
        "running": is_running(),
        "running_model_id": running_model_id(),
        "selected_model_id": selected,
        "model_name": selected_spec.name,
        "approx_download_mb": selected_spec.approx_mb,
        "any_model_installed": len(installed_model_ids()) > 0,
        "models": models,
        "install_dir": str(_engine_dir()),
        # Context window: the persisted value + the allowed range, so the UI can
        # render a bounded input. ``running`` vs this lets the UI hint that a
        # restart is needed when the user changed it mid-session.
        "ctx_size": get_ctx_size(),
        "ctx_size_min": _MIN_CTX_SIZE,
        "ctx_size_max": _MAX_CTX_SIZE,
    }
