from __future__ import annotations

import base64
import contextvars
import io
import os
import re
from pathlib import Path
from typing import Any, Literal

import httpx
import polars as pl

from kernel_runtime.artifact_store import ArtifactStore


def _translate_host_path_to_container(host_path: str) -> str:
    """Translate a host filesystem path to the container's /shared mount.

    When running in local mode, the host's shared directory is mounted at
    /shared inside the kernel container.  Core API returns paths using the
    host's perspective, so we swap the prefix.

    In Docker-in-Docker mode ``FLOWFILE_HOST_SHARED_DIR`` is not set and
    the path is returned unchanged (same volume, same mount path).
    """
    host_shared_dir = os.environ.get("FLOWFILE_HOST_SHARED_DIR")
    if not host_shared_dir:
        return host_path

    normalized_host_path = os.path.normpath(host_path)
    normalized_shared_dir = os.path.normpath(host_shared_dir)

    if normalized_host_path.startswith(normalized_shared_dir + os.sep):
        relative_path = normalized_host_path[len(normalized_shared_dir) + 1 :]
        return f"/shared/{relative_path}"
    elif normalized_host_path == normalized_shared_dir:
        return "/shared"

    return host_path


_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("flowfile_context")

# Reusable HTTP client for log callbacks (created per execution context)
_log_client: contextvars.ContextVar[httpx.Client | None] = contextvars.ContextVar("flowfile_log_client", default=None)

# Display outputs collector (reset at start of each execution)
_displays: contextvars.ContextVar[list[dict[str, str]]] = contextvars.ContextVar("flowfile_displays", default=[])


def _set_context(
    node_id: int,
    input_paths: dict[str, list[str]],
    output_dir: str,
    artifact_store: ArtifactStore,
    flow_id: int = 0,
    source_registration_id: int | None = None,
    log_callback_url: str = "",
    internal_token: str | None = None,
) -> None:
    _context.set(
        {
            "node_id": node_id,
            "input_paths": input_paths,
            "output_dir": output_dir,
            "artifact_store": artifact_store,
            "flow_id": flow_id,
            "source_registration_id": source_registration_id,
            "log_callback_url": log_callback_url,
            "internal_token": internal_token,
        }
    )
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
    _displays.set([])


def _get_context_value(key: str) -> Any:
    ctx = _context.get({})
    if key not in ctx:
        raise RuntimeError(
            f"flowfile context not initialized (missing '{key}'). This API is only available during /execute."
        )
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


def read_inputs() -> dict[str, list[pl.LazyFrame]]:
    """Read all named inputs, returning a dict of LazyFrame lists.

    Each entry contains a list of LazyFrames, one for each connected input.
    This allows distinguishing between multiple upstream nodes.
    """
    input_paths: dict[str, list[str]] = _get_context_value("input_paths")
    result: dict[str, list[pl.LazyFrame]] = {}
    for name, paths in input_paths.items():
        result[name] = [pl.scan_parquet(path) for path in paths]
    return result


def publish_output(df: pl.LazyFrame | pl.DataFrame, name: str = "main") -> None:
    output_dir = _get_context_value("output_dir")
    os.makedirs(output_dir, exist_ok=True)
    output_path = Path(output_dir) / f"{name}.parquet"
    if isinstance(df, pl.LazyFrame):
        df.sink_parquet(str(output_path))
    else:
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


# ===== Global Artifacts APIs =====

# Core URL for global artifact API calls
_CORE_URL = os.environ.get("FLOWFILE_CORE_URL", "http://host.docker.internal:63578")

# Shared path inside container for file-based storage
_SHARED_PATH = os.environ.get("FLOWFILE_SHARED_PATH", "/shared")


def _get_internal_auth_headers() -> dict[str, str]:
    """Get authentication headers for Core API calls.

    Prefers the token passed via ExecuteRequest context (always fresh),
    falls back to FLOWFILE_INTERNAL_TOKEN env var for backwards compatibility.
    """
    # Prefer token from execution context (set per-request by Core)
    try:
        ctx = _context.get({})
        token = ctx.get("internal_token")
        if token:
            return {"X-Internal-Token": token}
    except LookupError:
        pass
    # Fall back to env var (set at container creation time)
    token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")
    if token:
        return {"X-Internal-Token": token}
    return {}


def publish_global(
    name: str,
    obj: Any,
    description: str | None = None,
    tags: list[str] | None = None,
    namespace_id: int | None = None,
    fmt: str | None = None,
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
        fmt: Serialization format override ("parquet", "joblib", or "pickle").
             Auto-detected from object type if not specified.

    Returns:
        The artifact ID (database ID).

    Raises:
        UnpickleableObjectError: If the object cannot be serialized. Common causes:
            - Lambda functions or nested functions
            - Classes defined inside functions (local classes)
            - Objects with open file handles or network connections
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
    from kernel_runtime.serialization import (
        check_pickleable,
        detect_format,
        serialize_to_file,
        serialize_to_bytes,
    )

    serialization_format = fmt or detect_format(obj)
    python_type = f"{type(obj).__module__}.{type(obj).__name__}"
    python_module = type(obj).__module__

    # Validate that the object can be serialized before making API calls
    # This provides a clear error message upfront rather than failing during serialization
    if serialization_format in ("pickle", "joblib"):
        check_pickleable(obj)

    # Get context for lineage tracking
    try:
        flow_id = _get_context_value("flow_id")
        node_id = _get_context_value("node_id")
        source_registration_id = _get_context_value("source_registration_id")
    except RuntimeError:
        # Context not available - allow publish without lineage
        flow_id = None
        node_id = None
        source_registration_id = None

    if source_registration_id is None:
        raise RuntimeError(
            "source_registration_id is required for publish_global. "
            "This artifact must be produced by a registered catalog flow."
        )

    # Get kernel ID from environment
    kernel_id = os.environ.get("FLOWFILE_KERNEL_ID")

    # 1. Request upload target from Core
    auth_headers = _get_internal_auth_headers()
    with httpx.Client(timeout=30.0, headers=auth_headers) as client:
        resp = client.post(
            f"{_CORE_URL}/artifacts/prepare-upload",
            json={
                "name": name,
                "source_registration_id": source_registration_id,
                "serialization_format": serialization_format,
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
            # Shared filesystem - translate host path for local Docker mode
            staging_path = _translate_host_path_to_container(target["path"])
            Path(staging_path).parent.mkdir(parents=True, exist_ok=True)
            sha256 = serialize_to_file(obj, staging_path, serialization_format)
            size_bytes = os.path.getsize(staging_path)
        else:
            # S3 presigned URL - upload directly
            blob, sha256 = serialize_to_bytes(obj, serialization_format)
            size_bytes = len(blob)
            upload_resp = client.put(
                target["path"],
                content=blob,
                headers={"Content-Type": "application/octet-stream"},
                timeout=600.0,  # 10 min for large uploads
            )
            upload_resp.raise_for_status()

        # 3. Finalize with Core
        finalize_body = {
            "artifact_id": target["artifact_id"],
            "storage_key": target["storage_key"],
            "sha256": sha256,
            "size_bytes": size_bytes,
        }
        resp = client.post(
            f"{_CORE_URL}/artifacts/finalize",
            json=finalize_body,
        )
        if resp.status_code >= 400:
            detail = resp.text
            raise RuntimeError(
                f"Artifact finalize failed ({resp.status_code}): {detail}. " f"Request body: {finalize_body}"
            )

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

    auth_headers = _get_internal_auth_headers()
    with httpx.Client(timeout=30.0, headers=auth_headers) as client:
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
            # Shared filesystem - translate host path to container path if in Docker
            file_path = _translate_host_path_to_container(download["path"])
            obj = deserialize_from_file(file_path, format)
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

    auth_headers = _get_internal_auth_headers()
    with httpx.Client(timeout=30.0, headers=auth_headers) as client:
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
    auth_headers = _get_internal_auth_headers()
    with httpx.Client(timeout=30.0, headers=auth_headers) as client:
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


# Regex to detect HTML tags: <tag>, </tag>, <tag attr="val">, <br/>, etc.
_HTML_TAG_RE = re.compile(r"<[a-zA-Z/][^>]*>")


def _is_html_string(obj: Any) -> bool:
    """Check if obj is a string that looks like HTML.

    Uses a regex to detect actual HTML tags like <b>, </div>, <br/>, etc.
    This avoids false positives from strings like "x < 10 and y > 5".
    """
    if not isinstance(obj, str):
        return False
    return bool(_HTML_TAG_RE.search(obj))


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
        data = base64.b64encode(buf.read()).decode("ascii")
        displays.append(
            {
                "mime_type": "image/png",
                "data": data,
                "title": title,
            }
        )
    elif _is_plotly_figure(obj):
        # Render plotly figure to HTML
        html = obj.to_html(include_plotlyjs="cdn", full_html=False)
        displays.append(
            {
                "mime_type": "text/html",
                "data": html,
                "title": title,
            }
        )
    elif _is_pil_image(obj):
        # Render PIL image to PNG
        buf = io.BytesIO()
        obj.save(buf, format="PNG")
        buf.seek(0)
        data = base64.b64encode(buf.read()).decode("ascii")
        displays.append(
            {
                "mime_type": "image/png",
                "data": data,
                "title": title,
            }
        )
    elif _is_html_string(obj):
        # Store HTML string directly
        displays.append(
            {
                "mime_type": "text/html",
                "data": obj,
                "title": title,
            }
        )
    else:
        # Fall back to plain text
        displays.append(
            {
                "mime_type": "text/plain",
                "data": str(obj),
                "title": title,
            }
        )

    _displays.set(displays)
