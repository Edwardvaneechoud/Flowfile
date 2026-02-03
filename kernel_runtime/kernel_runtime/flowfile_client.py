from __future__ import annotations

import base64
import contextvars
import io
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

# Display outputs collector (reset at start of each execution)
_displays: contextvars.ContextVar[list[dict[str, str]]] = contextvars.ContextVar(
    "flowfile_displays", default=[]
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


# ===== Display APIs =====

def _is_matplotlib_figure(obj: Any) -> bool:
    """Check if obj is a matplotlib Figure (without requiring matplotlib)."""
    try:
        import matplotlib.figure
        return isinstance(obj, matplotlib.figure.Figure)
    except ImportError:
        return False


def _is_plotly_figure(obj: Any) -> bool:
    """Check if obj is a plotly Figure (without requiring plotly)."""
    try:
        import plotly.graph_objects as go
        return isinstance(obj, go.Figure)
    except ImportError:
        return False


def _is_pil_image(obj: Any) -> bool:
    """Check if obj is a PIL Image (without requiring PIL)."""
    try:
        from PIL import Image
        return isinstance(obj, Image.Image)
    except ImportError:
        return False


def _is_html_string(obj: Any) -> bool:
    """Check if obj is a string that looks like HTML."""
    if not isinstance(obj, str):
        return False
    return "<" in obj and ">" in obj


def _reset_displays() -> None:
    """Clear the display outputs list. Called at start of each execution."""
    _displays.set([])


def _get_displays() -> list[dict[str, str]]:
    """Return the current list of display outputs."""
    return _displays.get([])


def display(obj: Any, title: str = "") -> None:
    """Display a rich object in the output panel.

    Supported object types:
    - matplotlib.figure.Figure: Rendered as PNG image
    - plotly.graph_objects.Figure: Rendered as interactive HTML
    - PIL.Image.Image: Rendered as PNG image
    - str containing HTML tags: Rendered as HTML
    - Anything else: Converted to string and displayed as plain text

    Args:
        obj: The object to display.
        title: Optional title for the display output.
    """
    displays = _displays.get([])

    if _is_matplotlib_figure(obj):
        # Render matplotlib figure to PNG
        buf = io.BytesIO()
        obj.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        buf.seek(0)
        data = base64.b64encode(buf.read()).decode("utf-8")
        displays.append({
            "mime_type": "image/png",
            "data": data,
            "title": title,
        })
    elif _is_plotly_figure(obj):
        # Render plotly figure to HTML
        html = obj.to_html(include_plotlyjs="cdn", full_html=False)
        displays.append({
            "mime_type": "text/html",
            "data": html,
            "title": title,
        })
    elif _is_pil_image(obj):
        # Render PIL image to PNG
        buf = io.BytesIO()
        obj.save(buf, format="PNG")
        buf.seek(0)
        data = base64.b64encode(buf.read()).decode("utf-8")
        displays.append({
            "mime_type": "image/png",
            "data": data,
            "title": title,
        })
    elif _is_html_string(obj):
        # Store HTML string directly
        displays.append({
            "mime_type": "text/html",
            "data": obj,
            "title": title,
        })
    else:
        # Fall back to plain text
        displays.append({
            "mime_type": "text/plain",
            "data": str(obj),
            "title": title,
        })

    _displays.set(displays)
