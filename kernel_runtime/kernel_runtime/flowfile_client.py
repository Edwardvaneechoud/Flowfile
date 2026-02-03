from __future__ import annotations

import contextvars
import os
from pathlib import Path
from typing import Any, Literal

import httpx
import polars as pl

from kernel_runtime.artifact_store import ArtifactStore

_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("flowfile_context")

# Reusable HTTP client for log callbacks (created per execution context)
_log_client: contextvars.ContextVar[httpx.Client | None] = contextvars.ContextVar(
    "flowfile_log_client", default=None
)


def _set_context(
    node_id: int,
    input_paths: dict[str, list[str]],
    output_dir: str,
    artifact_store: ArtifactStore,
    flow_id: int = 0,
    log_callback_url: str = "",
) -> None:
    _context.set({
        "node_id": node_id,
        "input_paths": input_paths,
        "output_dir": output_dir,
        "artifact_store": artifact_store,
        "flow_id": flow_id,
        "log_callback_url": log_callback_url,
    })
    # Create a reusable HTTP client for log callbacks
    if log_callback_url:
        _log_client.set(httpx.Client(timeout=httpx.Timeout(5.0)))
    else:
        _log_client.set(None)


def _clear_context() -> None:
    client = _log_client.get(None)
    if client is not None:
        try:
            client.close()
        except Exception:
            pass
        _log_client.set(None)
    _context.set({})


def _get_context_value(key: str) -> Any:
    ctx = _context.get({})
    if key not in ctx:
        raise RuntimeError(f"flowfile context not initialized (missing '{key}'). This API is only available during /execute.")
    return ctx[key]


def read_input(name: str = "main") -> pl.LazyFrame:
    """Read all input files for *name* and return them as a single LazyFrame.

    When multiple paths are registered under the same name (e.g. a union
    of several upstream nodes), all files are scanned and concatenated
    automatically by Polars.
    """
    input_paths: dict[str, list[str]] = _get_context_value("input_paths")
    if name not in input_paths:
        available = list(input_paths.keys())
        raise KeyError(f"Input '{name}' not found. Available inputs: {available}")
    paths = input_paths[name]
    if len(paths) == 1:
        return pl.scan_parquet(paths[0])
    return pl.scan_parquet(paths)


def read_first(name: str = "main") -> pl.LazyFrame:
    """Read only the first input file for *name*.

    This is a convenience shortcut equivalent to scanning
    ``input_paths[name][0]``.
    """
    input_paths: dict[str, list[str]] = _get_context_value("input_paths")
    if name not in input_paths:
        available = list(input_paths.keys())
        raise KeyError(f"Input '{name}' not found. Available inputs: {available}")
    return pl.scan_parquet(input_paths[name][0])


def read_inputs() -> dict[str, pl.LazyFrame]:
    """Read all named inputs, returning a dict of LazyFrames.

    Each entry concatenates all paths registered under that name.
    """
    input_paths: dict[str, list[str]] = _get_context_value("input_paths")
    result: dict[str, pl.LazyFrame] = {}
    for name, paths in input_paths.items():
        if len(paths) == 1:
            result[name] = pl.scan_parquet(paths[0])
        else:
            result[name] = pl.scan_parquet(paths)
    return result


def publish_output(df: pl.LazyFrame | pl.DataFrame, name: str = "main") -> None:
    output_dir = _get_context_value("output_dir")
    os.makedirs(output_dir, exist_ok=True)
    output_path = Path(output_dir) / f"{name}.parquet"
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    df.write_parquet(str(output_path))
    # Ensure the file is fully flushed to disk before the host reads it
    # This prevents "File must end with PAR1" errors from race conditions
    with open(output_path, "rb") as f:
        os.fsync(f.fileno())


def publish_artifact(name: str, obj: Any) -> None:
    store: ArtifactStore = _get_context_value("artifact_store")
    node_id: int = _get_context_value("node_id")
    flow_id: int = _get_context_value("flow_id")
    store.publish(name, obj, node_id, flow_id=flow_id)


def read_artifact(name: str) -> Any:
    store: ArtifactStore = _get_context_value("artifact_store")
    flow_id: int = _get_context_value("flow_id")
    return store.get(name, flow_id=flow_id)


def delete_artifact(name: str) -> None:
    store: ArtifactStore = _get_context_value("artifact_store")
    flow_id: int = _get_context_value("flow_id")
    store.delete(name, flow_id=flow_id)


def list_artifacts() -> dict:
    store: ArtifactStore = _get_context_value("artifact_store")
    flow_id: int = _get_context_value("flow_id")
    return store.list_all(flow_id=flow_id)


# ===== Logging APIs =====

def log(message: str, level: Literal["INFO", "WARNING", "ERROR"] = "INFO") -> None:
    """Send a log message to the FlowFile log viewer.

    The message appears in the frontend log stream in real time.

    Args:
        message: The log message text.
        level: Log severity — ``"INFO"`` (default), ``"WARNING"``, or ``"ERROR"``.
    """
    flow_id: int = _get_context_value("flow_id")
    node_id: int = _get_context_value("node_id")
    callback_url: str = _get_context_value("log_callback_url")
    if not callback_url:
        # No callback configured — fall back to printing so the message
        # still shows up in captured stdout.
        print(f"[{level}] {message}")  # noqa: T201
        return

    client = _log_client.get(None)
    if client is None:
        print(f"[{level}] {message}")  # noqa: T201
        return

    payload = {
        "flowfile_flow_id": flow_id,
        "node_id": node_id,
        "log_message": message,
        "log_type": level,
    }
    try:
        client.post(callback_url, json=payload)
    except Exception:
        # Best-effort — don't let logging failures break user code.
        pass


def log_info(message: str) -> None:
    """Convenience wrapper: ``flowfile.log(message, level="INFO")``."""
    log(message, level="INFO")


def log_warning(message: str) -> None:
    """Convenience wrapper: ``flowfile.log(message, level="WARNING")``."""
    log(message, level="WARNING")


def log_error(message: str) -> None:
    """Convenience wrapper: ``flowfile.log(message, level="ERROR")``."""
    log(message, level="ERROR")
