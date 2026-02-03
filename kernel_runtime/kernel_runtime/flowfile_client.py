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


# ===== Global Artifacts APIs =====

# Core URL for global artifact API calls
_CORE_URL = os.environ.get("FLOWFILE_CORE_URL", "http://host.docker.internal:63578")

# Shared path inside container for file-based storage
_SHARED_PATH = os.environ.get("FLOWFILE_SHARED_PATH", "/shared")


def publish_global(
    name: str,
    obj: Any,
    description: str | None = None,
    tags: list[str] | None = None,
    namespace_id: int | None = None,
    format: str | None = None,
) -> int:
    """Persist a Python object to the global artifact store.

    Global artifacts are persisted beyond the current flow execution and can be
    retrieved later in the same flow, a different flow, or a different session.

    Args:
        name: Artifact name (required). Used to retrieve the artifact later.
        obj: Python object to persist (required). Supported types include:
             - Polars/Pandas DataFrames (serialized as parquet)
             - scikit-learn models (serialized with joblib)
             - Any picklable Python object (serialized with pickle)
        description: Human-readable description of the artifact.
        tags: List of tags for categorization and search.
        namespace_id: Namespace (schema) ID. Defaults to user's default namespace.
        format: Serialization format override ("parquet", "joblib", or "pickle").
                Auto-detected from object type if not specified.

    Returns:
        The artifact ID (database ID).

    Raises:
        RuntimeError: If flowfile context is not initialized.
        httpx.HTTPStatusError: If API calls fail.

    Example:
        >>> import flowfile
        >>> from sklearn.ensemble import RandomForestClassifier
        >>> model = RandomForestClassifier().fit(X, y)
        >>> artifact_id = flowfile.publish_global(
        ...     "my_model",
        ...     model,
        ...     description="Random Forest trained on Q4 data",
        ...     tags=["ml", "classification"],
        ... )
    """
    from kernel_runtime.serialization import detect_format, serialize_to_file, serialize_to_bytes

    format = format or detect_format(obj)
    python_type = f"{type(obj).__module__}.{type(obj).__name__}"
    python_module = type(obj).__module__

    # Get context for lineage tracking
    try:
        flow_id = _get_context_value("flow_id")
        node_id = _get_context_value("node_id")
    except RuntimeError:
        # Context not available - allow publish without lineage
        flow_id = None
        node_id = None

    # Get kernel ID from environment
    kernel_id = os.environ.get("FLOWFILE_KERNEL_ID")

    # 1. Request upload target from Core
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            f"{_CORE_URL}/artifacts/prepare-upload",
            json={
                "name": name,
                "serialization_format": format,
                "description": description,
                "tags": tags or [],
                "namespace_id": namespace_id,
                "source_flow_id": flow_id,
                "source_node_id": node_id,
                "source_kernel_id": kernel_id,
                "python_type": python_type,
                "python_module": python_module,
            },
        )
        resp.raise_for_status()
        target = resp.json()

        # 2. Serialize and write directly to storage
        if target["method"] == "file":
            # Shared filesystem - write to staging path
            sha256 = serialize_to_file(obj, target["path"], format)
            size_bytes = os.path.getsize(target["path"])
        else:
            # S3 presigned URL - upload directly
            blob, sha256 = serialize_to_bytes(obj, format)
            size_bytes = len(blob)
            upload_resp = client.put(
                target["path"],
                content=blob,
                headers={"Content-Type": "application/octet-stream"},
                timeout=600.0,  # 10 min for large uploads
            )
            upload_resp.raise_for_status()

        # 3. Finalize with Core
        resp = client.post(
            f"{_CORE_URL}/artifacts/finalize",
            json={
                "artifact_id": target["artifact_id"],
                "storage_key": target["storage_key"],
                "sha256": sha256,
                "size_bytes": size_bytes,
            },
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
        name: Artifact name to retrieve.
        version: Specific version to retrieve. Returns latest version if not specified.
        namespace_id: Namespace (schema) filter.

    Returns:
        The deserialized Python object.

    Raises:
        KeyError: If artifact is not found.
        httpx.HTTPStatusError: If API calls fail.

    Example:
        >>> import flowfile
        >>> model = flowfile.get_global("my_model")
        >>> model_v1 = flowfile.get_global("my_model", version=1)
    """
    from kernel_runtime.serialization import deserialize_from_file, deserialize_from_bytes

    # 1. Get metadata and download source from Core
    params = {}
    if version is not None:
        params["version"] = version
    if namespace_id is not None:
        params["namespace_id"] = namespace_id

    with httpx.Client(timeout=30.0) as client:
        resp = client.get(
            f"{_CORE_URL}/artifacts/by-name/{name}",
            params=params,
        )
        if resp.status_code == 404:
            raise KeyError(f"Artifact '{name}' not found")
        resp.raise_for_status()

        meta = resp.json()
        download = meta["download_source"]
        format = meta["serialization_format"]

        # 2. Read directly from storage
        if download["method"] == "file":
            # Shared filesystem
            obj = deserialize_from_file(download["path"], format)
        else:
            # S3 presigned URL
            download_resp = client.get(download["path"], timeout=600.0)
            download_resp.raise_for_status()
            obj = deserialize_from_bytes(download_resp.content, format)

    return obj


def list_global_artifacts(
    namespace_id: int | None = None,
    tags: list[str] | None = None,
) -> list[dict]:
    """List available global artifacts.

    Args:
        namespace_id: Filter by namespace.
        tags: Filter by tags (AND logic - all tags must match).

    Returns:
        List of artifact metadata dictionaries.

    Example:
        >>> import flowfile
        >>> artifacts = flowfile.list_global_artifacts(tags=["ml"])
        >>> for a in artifacts:
        ...     print(f"{a['name']} v{a['version']} - {a['python_type']}")
    """
    params = {}
    if namespace_id is not None:
        params["namespace_id"] = namespace_id
    if tags:
        params["tags"] = tags

    with httpx.Client(timeout=30.0) as client:
        resp = client.get(f"{_CORE_URL}/artifacts/", params=params)
        resp.raise_for_status()
        return resp.json()


def delete_global_artifact(
    name: str,
    version: int | None = None,
    namespace_id: int | None = None,
) -> None:
    """Delete a global artifact.

    Args:
        name: Artifact name to delete.
        version: Specific version to delete. Deletes all versions if not specified.
        namespace_id: Namespace (schema) filter.

    Raises:
        KeyError: If artifact is not found.
        httpx.HTTPStatusError: If API calls fail.

    Example:
        >>> import flowfile
        >>> flowfile.delete_global_artifact("my_model")  # delete all versions
        >>> flowfile.delete_global_artifact("my_model", version=1)  # delete v1 only
    """
    with httpx.Client(timeout=30.0) as client:
        if version is not None:
            # Delete specific version - need to get artifact ID first
            params = {"version": version}
            if namespace_id is not None:
                params["namespace_id"] = namespace_id

            resp = client.get(
                f"{_CORE_URL}/artifacts/by-name/{name}",
                params=params,
            )
            if resp.status_code == 404:
                raise KeyError(f"Artifact '{name}' version {version} not found")
            resp.raise_for_status()

            artifact_id = resp.json()["id"]
            resp = client.delete(f"{_CORE_URL}/artifacts/{artifact_id}")
            resp.raise_for_status()
        else:
            # Delete all versions by name
            params = {}
            if namespace_id is not None:
                params["namespace_id"] = namespace_id

            resp = client.delete(
                f"{_CORE_URL}/artifacts/by-name/{name}",
                params=params,
            )
            if resp.status_code == 404:
                raise KeyError(f"Artifact '{name}' not found")
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
