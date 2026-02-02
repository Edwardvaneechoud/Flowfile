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


# ===== Global Artifact APIs =====

# Core API URL for global artifact operations
CORE_URL = os.environ.get("FLOWFILE_CORE_URL", "http://host.docker.internal:63578")


def publish_global(
    name: str,
    obj: Any,
    description: str | None = None,
    tags: list[str] | None = None,
    namespace_id: int | None = None,
    format: str | None = None,
) -> int:
    """Persist a Python object to the global artifact store.

    The object is serialized and stored so it can be retrieved later in any
    flow or session via ``get_global()``.

    Args:
        name: Name for the artifact (used for lookup).
        obj: Any serializable Python object.
        description: Optional human-readable description.
        tags: Optional list of tags for searching/filtering.
        namespace_id: Optional catalog namespace ID. Defaults to None (global).
        format: Serialization format override ("parquet", "joblib", "pickle").
            Auto-detected from the object type if not specified.

    Returns:
        The artifact ID.

    Example::

        artifact_id = flowfile.publish_global(
            "my_model",
            trained_model,
            description="Random Forest v2",
            tags=["ml", "production"],
        )
    """
    from kernel_runtime.serialization import detect_format, serialize_to_file, serialize_to_bytes

    fmt = format or detect_format(obj)
    python_type = f"{type(obj).__module__}.{type(obj).__name__}"
    python_module = type(obj).__module__

    # Get lineage context if available
    ctx = _context.get({})
    flow_id = ctx.get("flow_id")
    node_id = ctx.get("node_id")

    # 1. Request upload target from Core
    resp = httpx.post(
        f"{CORE_URL}/artifacts/prepare-upload",
        json={
            "name": name,
            "serialization_format": fmt,
            "description": description,
            "tags": tags or [],
            "namespace_id": namespace_id,
            "source_flow_id": flow_id,
            "source_node_id": node_id,
            "python_type": python_type,
            "python_module": python_module,
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    target = resp.json()

    # 2. Serialize and write directly to storage
    if target["method"] == "file":
        sha256 = serialize_to_file(obj, target["path"], fmt)
        size_bytes = os.path.getsize(target["path"])
    else:
        blob, sha256 = serialize_to_bytes(obj, fmt)
        size_bytes = len(blob)
        upload_resp = httpx.put(
            target["path"],
            content=blob,
            headers={"Content-Type": "application/octet-stream"},
            timeout=600.0,
        )
        upload_resp.raise_for_status()

    # 3. Finalize with Core
    resp = httpx.post(
        f"{CORE_URL}/artifacts/finalize",
        json={
            "artifact_id": target["artifact_id"],
            "storage_key": target["storage_key"],
            "sha256": sha256,
            "size_bytes": size_bytes,
        },
        timeout=30.0,
    )
    resp.raise_for_status()

    return target["artifact_id"]


def get_global(
    name: str,
    version: int | None = None,
    namespace_id: int | None = None,
) -> Any:
    """Retrieve a Python object from the global artifact store.

    Args:
        name: Name of the artifact.
        version: Specific version number. Latest version if not specified.
        namespace_id: Optional catalog namespace ID.

    Returns:
        The deserialized Python object.

    Raises:
        KeyError: If the artifact is not found.

    Example::

        model = flowfile.get_global("my_model")
        model_v1 = flowfile.get_global("my_model", version=1)
    """
    from kernel_runtime.serialization import deserialize_from_file, deserialize_from_bytes

    params: dict[str, Any] = {}
    if version is not None:
        params["version"] = version
    if namespace_id is not None:
        params["namespace_id"] = namespace_id

    resp = httpx.get(
        f"{CORE_URL}/artifacts/by-name/{name}",
        params=params,
        timeout=30.0,
    )
    if resp.status_code == 404:
        raise KeyError(f"Artifact '{name}' not found")
    resp.raise_for_status()

    meta = resp.json()
    download = meta["download_source"]
    fmt = meta["serialization_format"]

    if download["method"] == "file":
        obj = deserialize_from_file(download["path"], fmt)
    else:
        download_resp = httpx.get(download["path"], timeout=600.0)
        download_resp.raise_for_status()
        obj = deserialize_from_bytes(download_resp.content, fmt)

    return obj


def list_global_artifacts(
    namespace_id: int | None = None,
    tags: list[str] | None = None,
) -> list[dict]:
    """List available global artifacts.

    Args:
        namespace_id: Filter by namespace.
        tags: Filter by tags.

    Returns:
        List of artifact metadata dicts.
    """
    params: dict[str, Any] = {}
    if namespace_id is not None:
        params["namespace_id"] = namespace_id
    if tags:
        params["tags"] = tags

    resp = httpx.get(f"{CORE_URL}/artifacts/", params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def delete_global_artifact(
    name: str,
    version: int | None = None,
    namespace_id: int | None = None,
) -> None:
    """Delete a global artifact.

    Args:
        name: Name of the artifact.
        version: Specific version to delete. If not specified, deletes the latest.
        namespace_id: Optional catalog namespace ID.

    Raises:
        KeyError: If the artifact is not found.
    """
    params: dict[str, Any] = {}
    if version is not None:
        params["version"] = version
    if namespace_id is not None:
        params["namespace_id"] = namespace_id

    resp = httpx.get(
        f"{CORE_URL}/artifacts/by-name/{name}",
        params=params,
        timeout=30.0,
    )
    if resp.status_code == 404:
        raise KeyError(f"Artifact '{name}' not found")
    resp.raise_for_status()

    artifact_id = resp.json()["id"]

    resp = httpx.delete(f"{CORE_URL}/artifacts/{artifact_id}", timeout=30.0)
    resp.raise_for_status()


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
