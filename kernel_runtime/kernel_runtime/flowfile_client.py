from __future__ import annotations

import contextvars
import io
import json
import os
from pathlib import Path
from typing import Any, Literal

import httpx
import polars as pl

from kernel_runtime.artifact_store import ArtifactStore
from kernel_runtime.serialization import detect_format, get_serializer

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


# ===== Global Artifact Registry APIs =====

# The kernel container reaches the Core via this URL.
_CORE_URL = os.environ.get(
    "FLOWFILE_CORE_URL",
    f"http://host.docker.internal:{os.environ.get('FLOWFILE_CORE_PORT', '63578')}",
)


def publish_global(
    name: str,
    obj: Any = None,
    *,
    artifact_name: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    namespace_id: int | None = None,
    serialization_format: str | None = None,
) -> int:
    """Persist a Python object to the FlowFile Global Artifact Catalog.

    The object is serialized inside the kernel, uploaded to the Core API,
    and stored with full metadata so it can be discovered and loaded by
    any other flow or user.

    Args:
        name: The catalog name for this artifact (e.g. ``"my_model"``).
        obj: The Python object to persist.  If ``None``, the transient
            artifact named *artifact_name* (or *name*) is used instead.
        artifact_name: Name of a transient artifact already in the local
            store.  Defaults to *name*.  Only used when *obj* is ``None``.
        description: Optional human-readable description.
        tags: Optional list of string tags for filtering.
        namespace_id: Catalog namespace to file this artifact under.
        serialization_format: Force a serialization format (``"pickle"``,
            ``"joblib"``, ``"parquet"``).  Auto-detected when omitted.

    Returns:
        The integer ``artifact_id`` assigned by the catalog.

    Raises:
        RuntimeError: If the upload fails.

    Example::

        import flowfile

        model = train_my_model(data)
        flowfile.publish_artifact("model", model)

        # Promote the transient artifact to the global catalog:
        artifact_id = flowfile.publish_global("my_model", model)

        # Or promote an existing transient artifact by name:
        flowfile.publish_artifact("model", model)
        artifact_id = flowfile.publish_global("my_model", artifact_name="model")
    """
    store: ArtifactStore = _get_context_value("artifact_store")
    flow_id: int = _get_context_value("flow_id")
    node_id: int = _get_context_value("node_id")

    # Resolve the object
    if obj is None:
        src_name = artifact_name or name
        obj = store.get(src_name, flow_id=flow_id)

    # Serialize
    fmt = serialization_format or detect_format(obj)
    serializer = get_serializer(fmt)
    blob = serializer.dumps(obj)

    type_name = type(obj).__name__
    module_name = type(obj).__module__ or ""
    filename = f"{name}{serializer.file_extension}"

    ctx = _context.get({})
    auth_token = ctx.get("auth_token", "")

    headers: dict[str, str] = {}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
        resp = client.post(
            f"{_CORE_URL}/artifacts/publish",
            files={"file": (filename, io.BytesIO(blob), "application/octet-stream")},
            data={
                "name": name,
                "python_type": type_name,
                "python_module": module_name,
                "serialization_format": fmt,
                "description": description or "",
                "tags": json.dumps(tags or []),
                "namespace_id": str(namespace_id) if namespace_id is not None else "",
                "source_flow_id": str(flow_id) if flow_id else "",
                "source_node_id": str(node_id),
                "source_kernel_id": ctx.get("kernel_id", ""),
            },
            headers=headers,
        )

    if resp.status_code == 201:
        body = resp.json()
        log(f"Published global artifact '{name}' (id={body['id']}, format={fmt}, "
            f"size={len(blob)} bytes)")
        return body["id"]

    raise RuntimeError(
        f"publish_global failed (HTTP {resp.status_code}): {resp.text[:500]}"
    )


def get_global(
    name: str,
    *,
    namespace_id: int | None = None,
    version: int | None = None,
) -> Any:
    """Load a Python object from the FlowFile Global Artifact Catalog.

    Downloads the blob from the Core, deserializes it, and places it in
    the local transient artifact store so subsequent
    ``flowfile.read_artifact(name)`` calls can access it without
    re-downloading.

    Args:
        name: Catalog name of the artifact.
        namespace_id: Limit lookup to a specific namespace.
        version: Load a specific version.  Latest if omitted.

    Returns:
        The deserialized Python object.

    Raises:
        RuntimeError: If the artifact is not found or download fails.

    Example::

        import flowfile

        model = flowfile.get_global("my_model")
        predictions = model.predict(new_data)
    """
    store: ArtifactStore = _get_context_value("artifact_store")
    flow_id: int = _get_context_value("flow_id")
    node_id: int = _get_context_value("node_id")

    ctx = _context.get({})
    auth_token = ctx.get("auth_token", "")

    headers: dict[str, str] = {}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
        # 1. Resolve metadata
        params: dict[str, Any] = {}
        if namespace_id is not None:
            params["namespace_id"] = namespace_id
        if version is not None:
            params["version"] = version

        meta_resp = client.get(
            f"{_CORE_URL}/artifacts/by-name/{name}",
            params=params,
            headers=headers,
        )
        if meta_resp.status_code != 200:
            raise RuntimeError(
                f"Global artifact '{name}' not found (HTTP {meta_resp.status_code})"
            )

        meta = meta_resp.json()
        artifact_id = meta["id"]
        fmt = meta["serialization_format"]

        # 2. Download blob
        dl_resp = client.get(
            f"{_CORE_URL}/artifacts/{artifact_id}/download",
            headers=headers,
        )
        if dl_resp.status_code != 200:
            raise RuntimeError(
                f"Failed to download artifact blob (HTTP {dl_resp.status_code})"
            )

    # 3. Deserialize
    serializer = get_serializer(fmt)
    obj = serializer.loads(dl_resp.content)

    # 4. Place in local store for read_artifact() access
    try:
        store.delete(name, flow_id=flow_id)
    except KeyError:
        pass
    store.publish(name, obj, node_id=node_id, flow_id=flow_id)

    log(f"Loaded global artifact '{name}' (id={artifact_id}, format={fmt}, "
        f"size={len(dl_resp.content)} bytes)")
    return obj
