from __future__ import annotations

import base64
import contextvars
import io
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

import httpx
import polars as pl

from kernel_runtime.artifact_store import ArtifactStore
from kernel_runtime.schemas import ArtifactInfo, GlobalArtifactInfo


def _translate_host_path_to_container(host_path: str) -> str:
    """Translate a host filesystem path to the kernel container's mount.

    Two prefixes are recognized (in priority order):
      * ``FLOWFILE_HOST_CATALOG_TABLES_DIR`` → ``/catalog_tables`` (catalog Delta dirs)
      * ``FLOWFILE_HOST_SHARED_DIR`` → ``/shared`` (parquet exchange, artifacts)

    When running locally the kernel container bind-mounts both of these host
    directories. Core API returns paths using the host's perspective, so we
    swap the longest matching prefix. In Docker-in-Docker mode neither env
    var is set (the same volume is mounted at the same path everywhere) so
    the input path is returned unchanged.
    """
    normalized_host_path = os.path.normpath(host_path)

    host_catalog_dir = os.environ.get("FLOWFILE_HOST_CATALOG_TABLES_DIR")
    if host_catalog_dir:
        normalized_catalog_dir = os.path.normpath(host_catalog_dir)
        if normalized_host_path == normalized_catalog_dir:
            return "/catalog_tables"
        if normalized_host_path.startswith(normalized_catalog_dir + os.sep):
            relative_path = normalized_host_path[len(normalized_catalog_dir) + 1 :]
            return f"/catalog_tables/{relative_path}"

    host_shared_dir = os.environ.get("FLOWFILE_HOST_SHARED_DIR")
    if host_shared_dir:
        normalized_shared_dir = os.path.normpath(host_shared_dir)
        if normalized_host_path.startswith(normalized_shared_dir + os.sep):
            relative_path = normalized_host_path[len(normalized_shared_dir) + 1 :]
            return f"/shared/{relative_path}"
        if normalized_host_path == normalized_shared_dir:
            return "/shared"

    return host_path


_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("flowfile_context")

# Reusable HTTP client for log callbacks (created per execution context)
_log_client: contextvars.ContextVar[httpx.Client | None] = contextvars.ContextVar("flowfile_log_client", default=None)

# Display outputs collector (reset at start of each execution)
_displays: contextvars.ContextVar[list[dict[str, str]]] = contextvars.ContextVar("flowfile_displays", default=None)


def _set_context(
    node_id: int,
    input_paths: dict[str, list[str]],
    output_dir: str,
    artifact_store: ArtifactStore,
    flow_id: int = 0,
    source_registration_id: int | None = None,
    log_callback_url: str = "",
    internal_token: str | None = None,
    interactive: bool = False,
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
            "interactive": interactive,
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


def _check_input_available(input_paths: dict[str, list[str]], name: str) -> list[str]:
    if name not in input_paths or not input_paths[name]:
        available = [k for k, v in input_paths.items() if v]
        if not available:
            raise RuntimeError(
                "Upstream nodes did not run yet. Make sure you run the flow before calling read_input()."
            )
        raise KeyError(f"Input '{name}' not found. Available inputs: {available}")
    return input_paths[name]


def read_input(name: str = "main") -> pl.LazyFrame:
    """Read all input files for *name* and return them as a single LazyFrame.

    When multiple paths are registered under the same name (e.g. a union
    of several upstream nodes), all files are scanned and concatenated
    automatically by Polars.
    """
    input_paths: dict[str, list[str]] = _get_context_value("input_paths")
    paths = _check_input_available(input_paths, name)
    if len(paths) == 1:
        return pl.scan_parquet(paths[0])
    return pl.scan_parquet(paths)


def read_first(name: str = "main") -> pl.LazyFrame:
    """Read only the first input file for *name*.

    This is a convenience shortcut equivalent to scanning
    ``input_paths[name][0]``.
    """
    input_paths: dict[str, list[str]] = _get_context_value("input_paths")
    paths = _check_input_available(input_paths, name)
    return pl.scan_parquet(paths[0])


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


def list_artifacts() -> list[ArtifactInfo]:
    store: ArtifactStore = _get_context_value("artifact_store")
    flow_id: int = _get_context_value("flow_id")
    raw = store.list_all(flow_id=flow_id)
    return [ArtifactInfo.model_validate(v) for v in raw.values()]


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
        >>> from sklearn.ensemble import RandomForestClassifier
        >>> model = RandomForestClassifier().fit(X, y)
        >>> artifact_id = flowfile_ctx.publish_global(
        ...     "my_model",
        ...     model,
        ...     description="Random Forest trained on Q4 data",
        ...     tags=["ml", "classification"],
        ... )
    """
    from kernel_runtime.serialization import (
        check_pickleable,
        detect_format,
        serialize_to_bytes,
        serialize_to_file,
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
        # Core's KernelManager.execute now injects a per-kernel scratch
        # FlowRegistration id whenever the caller didn't supply one, so this
        # branch should be unreachable in normal operation. We keep it as a
        # safety net for older Core versions or kernels that started before
        # the scratch-flow feature shipped.
        print(  # noqa: T201
            "[flowfile_ctx] publish_global ran without a source_registration_id. "
            "This usually means the kernel is talking to an older Core that does "
            "not auto-provision a scratch FlowRegistration. The artifact will not "
            "be persisted; returning -1."
        )
        return -1

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
        >>> model = flowfile_ctx.get_global("my_model")
        >>> model_v1 = flowfile_ctx.get_global("my_model", version=1)
    """
    from kernel_runtime.serialization import deserialize_from_bytes, deserialize_from_file

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
) -> list[GlobalArtifactInfo]:
    """List available global artifacts.

    Args:
        namespace_id: Filter by namespace.
        tags: Filter by tags (AND logic - all tags must match).

    Returns:
        List of :class:`GlobalArtifactInfo` objects.

    Example:
        >>> artifacts = flowfile_ctx.list_global_artifacts(tags=["ml"])
        >>> for a in artifacts:
        ...     print(f"{a.name} v{a.version} - {a.python_type}")
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
        return [GlobalArtifactInfo.model_validate(item) for item in resp.json()]


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
        >>> flowfile_ctx.delete_global_artifact("my_model")  # delete all versions
        >>> flowfile_ctx.delete_global_artifact("my_model", version=1)  # delete v1 only
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


# ===== File Utilities =====


def get_shared_location(filename: str) -> str:
    """Return the absolute path for a file in the shared directory.

    The shared directory is accessible from all FlowFile services (core,
    worker, kernel) and persists across kernel executions.  Use this to
    write files that should be readable by other services or that should
    survive container restarts.

    Parent directories are created automatically.

    Args:
        filename: Relative filename or path, e.g. ``"test_file.csv"`` or
                  ``"other_dir/test_file.csv"``.

    Returns:
        Absolute path as a string, ready to pass to file-writing functions.

    Examples::

        df.write_csv(flowfile_ctx.get_shared_location("test_file.csv"))
        df.write_csv(flowfile_ctx.get_shared_location("reports/monthly.csv"))
    """
    base = os.environ.get("FLOWFILE_KERNEL_SHARED_DIR", "/shared")
    full_path = os.path.join(base, "user_files", filename)
    parent = os.path.dirname(full_path)
    os.makedirs(parent, exist_ok=True)
    return full_path


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


# ===== Catalog Tables APIs =====
#
# These wrap Core's ``/catalog/tables/*`` HTTP endpoints. The kernel writes
# Delta directories locally (under the mounted ``/catalog_tables`` directory
# or its DinD equivalent) and reports metadata to Core; Core never materializes
# the dataset, matching the project rule that Core ships paths/JSON only.

_CATALOG_VALID_WRITE_MODES = frozenset(
    {"overwrite", "append", "upsert", "update", "delete", "error"}
)


def _kernel_catalog_dir() -> str:
    """Return the absolute path of ``/catalog_tables`` as the kernel sees it.

    In Docker-in-Docker mode the env var holds the same path the host uses;
    in local mode it holds the bind-mount target inside the container
    (``/catalog_tables``). Falls back to ``/catalog_tables`` if unset.
    """
    return os.environ.get("FLOWFILE_KERNEL_CATALOG_TABLES_DIR", "/catalog_tables")


def _host_catalog_dir() -> str | None:
    """Return the host path of the catalog_tables directory (local mode only).

    Used to translate kernel-visible paths back to host paths when registering
    a new table with Core (Core stores host paths). Returns ``None`` in DinD
    mode where no translation is needed.
    """
    return os.environ.get("FLOWFILE_HOST_CATALOG_TABLES_DIR")


def _translate_container_path_to_host(container_path: str) -> str:
    """Inverse of :func:`_translate_host_path_to_container` for catalog paths.

    The kernel writes Delta directories using its in-container path
    (``/catalog_tables/...``) but Core stores the host path. In local mode
    we swap the prefix; in DinD mode the path passes through unchanged.
    """
    host_catalog_dir = _host_catalog_dir()
    if not host_catalog_dir:
        return container_path
    kernel_catalog_dir = _kernel_catalog_dir()
    normalized = os.path.normpath(container_path)
    normalized_kernel = os.path.normpath(kernel_catalog_dir)
    if normalized == normalized_kernel:
        return host_catalog_dir
    if normalized.startswith(normalized_kernel + os.sep):
        rel = normalized[len(normalized_kernel) + 1 :]
        return os.path.join(host_catalog_dir, rel)
    return container_path


def _new_catalog_table_dir(name: str) -> str:
    """Generate a fresh kernel-visible directory path for a new catalog table.

    Mirrors flowfile_frame's convention (``<name>_<8-char-uuid>``) so directory
    names don't collide when the same table name is reused under different
    namespaces.
    """
    import uuid

    safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_") or "table"
    suffix = uuid.uuid4().hex[:8]
    return os.path.join(_kernel_catalog_dir(), f"{safe_name}_{suffix}")


def _friendly_dtype(t: Any) -> str:
    # deltalake's PrimitiveType carries the short name on `.type`
    # ("long" / "string" / "double" / "boolean" / …); complex types
    # (StructType, ArrayType, MapType) fall back to str().
    inner = getattr(t, "type", None)
    if isinstance(inner, str):
        return inner
    return str(t)


def _read_delta_metadata(table_path: str) -> dict[str, Any]:
    """Read schema / row_count / size_bytes from a Delta directory.

    Lightweight enough to run after every write; the kernel sends these
    values to Core via the from-data / refresh endpoints so the catalog
    record matches the on-disk state.
    """
    from deltalake import DeltaTable

    dt = DeltaTable(table_path)
    schema_fields = dt.schema().fields
    schema_columns = [{"name": f.name, "dtype": _friendly_dtype(f.type)} for f in schema_fields]
    row_count: int | None = None
    try:
        row_count = pl.scan_delta(table_path).select(pl.len()).collect().item()
    except Exception:
        row_count = None
    size_bytes = 0
    try:
        add_actions = dt.get_add_actions(flatten=True)
        size_col = add_actions.column("size_bytes")
        size_bytes = sum(v for v in size_col.to_pylist() if v is not None)
    except Exception:
        for f in Path(table_path).rglob("*.parquet"):
            try:
                size_bytes += f.stat().st_size
            except OSError:
                pass
    return {
        "schema_columns": schema_columns,
        "row_count": row_count,
        "column_count": len(schema_columns),
        "size_bytes": size_bytes,
    }


def _catalog_get(client: httpx.Client, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
    # httpx serializes None as ``?key=`` which FastAPI's ``int | None`` parser
    # rejects with 422. Strip None entries so the param is simply absent.
    cleaned = {k: v for k, v in (params or {}).items() if v is not None}
    resp = client.get(f"{_CORE_URL}{path}", params=cleaned or None)
    resp.raise_for_status()
    return resp


def _catalog_post(client: httpx.Client, path: str, json: dict[str, Any]) -> httpx.Response:
    resp = client.post(f"{_CORE_URL}{path}", json=json)
    resp.raise_for_status()
    return resp


def _resolve_schema_namespace_id(client: httpx.Client, schema_name: str) -> int:
    """Resolve a schema name to its namespace_id.

    Searches across all level-1 namespaces (schemas live one level below
    top-level catalog namespaces) and returns the first exact-name match.
    Raises :class:`LookupError` if no schema with that name exists.
    """
    # Get the list of top-level (catalog) namespaces, then for each list its
    # children and look for a schema with the requested name.
    catalogs = _catalog_get(client, "/catalog/namespaces", params={"parent_id": None}).json()
    candidates: list[dict[str, Any]] = []
    for cat in catalogs:
        schemas = _catalog_get(client, "/catalog/namespaces", params={"parent_id": cat["id"]}).json()
        for sch in schemas:
            if sch["name"] == schema_name:
                candidates.append(sch)
    if not candidates:
        raise LookupError(f"Schema '{schema_name}' not found in any catalog")
    if len(candidates) > 1:
        names = [f"{c.get('parent_name') or '?'}.{c['name']} (id={c['id']})" for c in candidates]
        raise LookupError(
            f"Schema '{schema_name}' is ambiguous — found in multiple catalogs: {names}. "
            "Pass namespace_id= directly to disambiguate."
        )
    return candidates[0]["id"]


def _resolve_target_namespace_id(
    client: httpx.Client,
    schema: str | None,
    namespace_id: int | None,
) -> int:
    """Pick the namespace_id for a read/write operation.

    Precedence: explicit ``namespace_id`` > ``schema`` (name lookup) > default.
    Raises :class:`ValueError` if both ``schema`` and ``namespace_id`` are passed.
    """
    if schema is not None and namespace_id is not None:
        raise ValueError("Pass either 'schema' or 'namespace_id', not both")
    if namespace_id is not None:
        return namespace_id
    if schema is not None:
        return _resolve_schema_namespace_id(client, schema)
    default = _catalog_get(client, "/catalog/default-namespace-id").json()
    if isinstance(default, dict):
        # Endpoint may return a wrapped result; tolerate either shape
        return int(default.get("id") or default.get("namespace_id") or default["default"])
    return int(default)


def _resolve_catalog_table(
    client: httpx.Client,
    table_name: str,
    namespace_id: int | None,
    strict: bool = False,
) -> dict[str, Any] | None:
    """Resolve a catalog table by name; return ``None`` if not found.

    Wraps Core's ``GET /catalog/tables/resolve``. Translates 404 (table missing)
    into ``None`` so callers can distinguish "doesn't exist" from real errors.
    """
    params: dict[str, Any] = {"q": table_name, "strict": str(strict).lower()}
    if namespace_id is not None:
        params["namespace_id"] = namespace_id
    resp = client.get(f"{_CORE_URL}/catalog/tables/resolve", params=params)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json().get("table")


# ----- Typed reference objects ------------------------------------------------
#
# Mirror the ergonomics of ``flowfile_frame.catalog_reference.CatalogReference``
# / ``SchemaReference`` on the kernel side. The kernel can't import
# flowfile_core (no DB access), so these wrap the same Core HTTP endpoints
# the procedural API already calls. Frozen + slots makes them immutable,
# hashable, and cheap; ``__repr__`` comes for free via ``@dataclass``.


@dataclass(frozen=True, slots=True)
class CatalogRef:
    """Validated handle to a top-level catalog namespace.

    Use :func:`get_catalog` or :func:`list_catalogs` to obtain one; direct
    construction is permitted but won't validate the id against Core.

    Equivalent to ``flowfile_frame.CatalogReference`` on the editor side.
    """

    id: int
    name: str

    def get_schema(self, name: str) -> SchemaRef:
        """Return the named child schema. Raises ``LookupError`` if missing."""
        for schema in self.list_schemas():
            if schema.name == name:
                return schema
        raise LookupError(f"Schema '{name}' not found in catalog '{self.name}'")

    def list_schemas(self) -> list[SchemaRef]:
        """Return every schema (level-1 namespace) under this catalog."""
        auth_headers = _get_internal_auth_headers()
        with httpx.Client(timeout=30.0, headers=auth_headers) as client:
            rows = _catalog_get(
                client, "/catalog/namespaces", params={"parent_id": self.id}
            ).json()
        return [SchemaRef(id=row["id"], name=row["name"], catalog=self) for row in rows]

    def list_tables(self) -> list[TableRef]:
        """Return every table across every schema in this catalog (flat list)."""
        result: list[TableRef] = []
        for schema in self.list_schemas():
            result.extend(schema.list_tables())
        return result

    def get_table_ref(self, *, schema_name: str, table_name: str) -> TableRef:
        """Convenience shortcut for ``self.get_schema(schema_name).get_table_ref(table_name)``."""
        return self.get_schema(schema_name).get_table_ref(table_name)


@dataclass(frozen=True, slots=True)
class SchemaRef:
    """Validated handle to a level-1 schema namespace.

    Equivalent to ``flowfile_frame.SchemaReference`` on the editor side.
    """

    id: int
    name: str
    catalog: CatalogRef

    def list_tables(self) -> list[TableRef]:
        """Return every table registered in this schema."""
        auth_headers = _get_internal_auth_headers()
        with httpx.Client(timeout=30.0, headers=auth_headers) as client:
            rows = _catalog_get(
                client, "/catalog/tables", params={"namespace_id": self.id}
            ).json()
        return [_table_ref_from_row(row, self) for row in rows]

    def get_table_ref(self, name: str) -> TableRef:
        """Return a :class:`TableRef` for *name*.

        Returns a ref with ``id=None`` if the table does not exist yet —
        use :meth:`TableRef.exists` to check, or call :meth:`TableRef.write`
        to create it.
        """
        auth_headers = _get_internal_auth_headers()
        with httpx.Client(timeout=30.0, headers=auth_headers) as client:
            row = _resolve_catalog_table(client, name, self.id, strict=False)
        if row is None:
            return TableRef(name=name, schema=self)
        return _table_ref_from_row(row, self)

    def read_table(
        self,
        name: str,
        *,
        delta_version: int | None = None,
    ) -> pl.LazyFrame:
        """Read a table from this schema as a Polars ``LazyFrame``.

        Equivalent to ``flowfile_ctx.read_catalog_table(name, namespace_id=self.id, ...)``.
        """
        return read_catalog_table(name, namespace_id=self.id, delta_version=delta_version)

    def write_table(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        name: str,
        *,
        write_mode: str = "overwrite",
        merge_keys: list[str] | None = None,
        description: str | None = None,
    ) -> TableRef:
        """Write *df* into this schema as ``name``."""
        return write_catalog_table(
            df,
            name,
            namespace_id=self.id,
            write_mode=write_mode,
            merge_keys=merge_keys,
            description=description,
        )

    # Convenience aliases whose names match the top-level
    # ``flowfile_ctx.read_catalog_table`` / ``write_catalog_table`` functions.
    # The ``read_table`` / ``write_table`` names above match flowfile_frame's
    # ``SchemaReference`` API; these aliases match the kernel's procedural API.
    # Both call the same code paths.

    def read_catalog_table(
        self,
        name: str,
        *,
        delta_version: int | None = None,
    ) -> pl.LazyFrame:
        """Alias for :meth:`read_table` — same name as ``flowfile_ctx.read_catalog_table``."""
        return self.read_table(name, delta_version=delta_version)

    def write_catalog_table(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        name: str,
        *,
        write_mode: str = "overwrite",
        merge_keys: list[str] | None = None,
        description: str | None = None,
    ) -> TableRef:
        """Alias for :meth:`write_table` — same name as ``flowfile_ctx.write_catalog_table``."""
        return self.write_table(
            df,
            name,
            write_mode=write_mode,
            merge_keys=merge_keys,
            description=description,
        )

    # ----- Global artifacts scoped to this schema -----------------------------
    #
    # These delegate to the top-level ``publish_global`` / ``get_global`` /
    # ``list_global_artifacts`` / ``delete_global_artifact`` functions with
    # ``namespace_id=self.id`` baked in. The shorter ``publish_artifact`` /
    # ``read_artifact`` names are reused here even though they collide with the
    # kernel-local in-memory store at the module level — the receiver
    # (``schema.foo`` vs ``flowfile_ctx.foo``) disambiguates which store is
    # involved.
    #
    # Limitation inherited from ``publish_global``: requires a registered
    # catalog flow (``source_registration_id``). In interactive cell mode the
    # underlying ``publish_global`` no-ops with a printed warning and returns
    # ``-1``.

    def publish_artifact(
        self,
        name: str,
        obj: Any,
        *,
        description: str | None = None,
        tags: list[str] | None = None,
        fmt: str | None = None,
    ) -> int:
        """Persist *obj* to the global artifact store under this schema's namespace.

        Convenience wrapper around :func:`publish_global` with
        ``namespace_id=self.id``.
        """
        return publish_global(
            name,
            obj,
            description=description,
            tags=tags,
            namespace_id=self.id,
            fmt=fmt,
        )

    def read_artifact(
        self,
        name: str,
        *,
        version: int | None = None,
    ) -> Any:
        """Retrieve an artifact from this schema's namespace.

        Convenience wrapper around :func:`get_global`.
        """
        return get_global(name, version=version, namespace_id=self.id)

    def list_artifacts(
        self,
        *,
        tags: list[str] | None = None,
    ) -> list[GlobalArtifactInfo]:
        """List artifacts in this schema's namespace.

        Convenience wrapper around :func:`list_global_artifacts`.
        """
        return list_global_artifacts(namespace_id=self.id, tags=tags)

    def delete_artifact(
        self,
        name: str,
        *,
        version: int | None = None,
    ) -> None:
        """Delete an artifact from this schema's namespace.

        Convenience wrapper around :func:`delete_global_artifact`.
        """
        delete_global_artifact(name, version=version, namespace_id=self.id)


@dataclass(frozen=True, slots=True)
class TableRef:
    """Handle to a catalog table (existing or potential).

    ``id`` is ``None`` for refs that point at a name within a schema where
    no table exists yet — :meth:`write` will create it; :meth:`read` will
    raise ``KeyError``.
    """

    name: str
    schema: SchemaRef
    id: int | None = None
    file_path: str | None = None
    row_count: int | None = None
    column_count: int | None = None
    size_bytes: int | None = None
    schema_columns: list[dict[str, str]] = field(default_factory=list)

    def exists(self) -> bool:
        """``True`` if this ref points at a table that exists in the catalog."""
        return self.id is not None

    def read(self, *, delta_version: int | None = None) -> pl.LazyFrame:
        """Read this table as a Polars ``LazyFrame``."""
        return read_catalog_table(self, delta_version=delta_version)

    def write(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        *,
        write_mode: str = "overwrite",
        merge_keys: list[str] | None = None,
        description: str | None = None,
    ) -> TableRef:
        """Write *df* into this table; creates it if it doesn't exist yet.

        Returns a fresh ``TableRef`` with updated metadata (row_count,
        size_bytes, etc.) — the original ref is unchanged.
        """
        return write_catalog_table(
            df,
            self,
            write_mode=write_mode,
            merge_keys=merge_keys,
            description=description,
        )

    def refresh(self) -> TableRef:
        """Re-fetch the table's metadata from Core.

        Useful when another process may have written to the same table and
        you want this ref's ``row_count`` / ``size_bytes`` / ``file_path``
        to reflect the latest state.
        """
        auth_headers = _get_internal_auth_headers()
        with httpx.Client(timeout=30.0, headers=auth_headers) as client:
            row = _resolve_catalog_table(client, self.name, self.schema.id, strict=False)
        if row is None:
            return replace(
                self,
                id=None,
                file_path=None,
                row_count=None,
                column_count=None,
                size_bytes=None,
                schema_columns=[],
            )
        return _table_ref_from_row(row, self.schema)


def _table_ref_from_row(row: dict[str, Any], schema: SchemaRef) -> TableRef:
    """Build a :class:`TableRef` from a ``CatalogTableOut`` JSON row.

    Tolerant of the shape variations between Core endpoints — ``file_path``
    is sometimes named ``table_path`` / ``path``, ``schema_columns`` may
    be absent on list responses.
    """
    return TableRef(
        name=row["name"],
        schema=schema,
        id=row.get("id"),
        file_path=_table_file_path(row),
        row_count=row.get("row_count"),
        column_count=row.get("column_count"),
        size_bytes=row.get("size_bytes"),
        schema_columns=list(row.get("schema_columns") or []),
    )


# ----- Top-level entry points -------------------------------------------------


def get_catalog(name: str) -> CatalogRef:
    """Return a :class:`CatalogRef` for the named top-level catalog.

    Raises ``LookupError`` if no catalog with that name exists.
    """
    for cat in list_catalogs():
        if cat.name == name:
            return cat
    raise LookupError(f"Catalog '{name}' not found")


def default_schema() -> SchemaRef:
    """Return the seeded ``General/default`` schema (or whichever Core has marked as default).

    Raises ``LookupError`` if the catalog hasn't been initialised.
    """
    auth_headers = _get_internal_auth_headers()
    with httpx.Client(timeout=30.0, headers=auth_headers) as client:
        default_id_raw = _catalog_get(client, "/catalog/default-namespace-id").json()
    # Endpoint may return a bare int OR a wrapper dict — tolerate both.
    if isinstance(default_id_raw, dict):
        default_id = int(
            default_id_raw.get("id")
            or default_id_raw.get("namespace_id")
            or default_id_raw["default"]
        )
    elif default_id_raw is None:
        raise LookupError(
            "Default schema has not been initialised. Create one from the visual "
            "editor or call flowfile_ctx.list_catalogs() to see what exists."
        )
    else:
        default_id = int(default_id_raw)
    # Walk catalogs to find the parent of the default schema so we can
    # return a fully-populated SchemaRef.
    for catalog in list_catalogs():
        for schema in catalog.list_schemas():
            if schema.id == default_id:
                return schema
    raise LookupError(
        f"Default schema id={default_id} returned by Core does not exist in any catalog."
    )


def list_catalogs() -> list[CatalogRef]:
    """List all top-level catalogs.

    Returns:
        Typed refs — navigate further via :meth:`CatalogRef.list_schemas` /
        :meth:`CatalogRef.get_schema`.
    """
    auth_headers = _get_internal_auth_headers()
    with httpx.Client(timeout=30.0, headers=auth_headers) as client:
        rows = _catalog_get(client, "/catalog/namespaces").json()
    return [CatalogRef(id=row["id"], name=row["name"]) for row in rows]


def list_schemas(
    catalog: str | None = None,
    *,
    catalog_id: int | None = None,
) -> list[SchemaRef]:
    """List schemas, optionally restricted to one catalog.

    Args:
        catalog: Catalog name; resolved client-side via :func:`list_catalogs`.
            Mutually exclusive with ``catalog_id``.
        catalog_id: Explicit catalog id.

    Returns:
        Typed refs. When neither argument is given, returns schemas from
        every catalog in a single flat list.

    Raises:
        ValueError: Both ``catalog`` and ``catalog_id`` were passed.
        LookupError: ``catalog`` name didn't match any known catalog.
    """
    if catalog is not None and catalog_id is not None:
        raise ValueError("Pass either 'catalog' or 'catalog_id', not both")
    if catalog_id is not None:
        # We don't have the catalog name here without an extra round-trip;
        # fetch from list_catalogs to get a fully-populated CatalogRef.
        for cat in list_catalogs():
            if cat.id == catalog_id:
                return cat.list_schemas()
        raise LookupError(f"Catalog id={catalog_id} not found")
    if catalog is not None:
        return get_catalog(catalog).list_schemas()
    result: list[SchemaRef] = []
    for cat in list_catalogs():
        result.extend(cat.list_schemas())
    return result


def list_catalog_tables(
    *,
    schema: str | None = None,
    namespace_id: int | None = None,
) -> list[TableRef]:
    """List catalog tables.

    Args:
        schema: Schema name; resolved to ``namespace_id`` via Core.
            Mutually exclusive with ``namespace_id``.
        namespace_id: Explicit namespace id to filter by.

    Returns:
        Typed refs. When neither argument is given, returns every table
        visible to this kernel (across all schemas in all catalogs).

    Raises:
        ValueError: Both ``schema`` and ``namespace_id`` were passed.
    """
    if schema is not None and namespace_id is not None:
        raise ValueError("Pass either 'schema' or 'namespace_id', not both")
    if namespace_id is not None:
        return _list_tables_under_namespace(namespace_id)
    if schema is not None:
        # Resolve schema name → fully populated SchemaRef so the refs carry
        # the schema name (not just the id).
        for cat in list_catalogs():
            for sch in cat.list_schemas():
                if sch.name == schema:
                    return sch.list_tables()
        raise LookupError(f"Schema '{schema}' not found in any catalog")
    # Unfiltered: flatten across every schema in every catalog.
    result: list[TableRef] = []
    for cat in list_catalogs():
        for sch in cat.list_schemas():
            result.extend(sch.list_tables())
    return result


def _list_tables_under_namespace(namespace_id: int) -> list[TableRef]:
    """Helper for the ``namespace_id=...`` path of :func:`list_catalog_tables`.

    Resolves the schema/catalog metadata so the returned refs are fully
    populated (one extra GET per distinct namespace — cheap; this function
    is called interactively, not on a hot path).
    """
    for cat in list_catalogs():
        for sch in cat.list_schemas():
            if sch.id == namespace_id:
                return sch.list_tables()
    raise LookupError(f"Namespace id={namespace_id} not found")


def read_catalog_table(
    table: TableRef | str,
    *,
    schema: str | None = None,
    namespace_id: int | None = None,
    delta_version: int | None = None,
) -> pl.LazyFrame:
    """Read a catalog table as a Polars ``LazyFrame``.

    Calls Core to resolve the table's path, then opens the Delta directory
    locally via ``pl.scan_delta`` — Core never materialises the dataset.

    Args:
        table: Either a :class:`TableRef` (returned from :func:`list_catalog_tables`
            or :meth:`SchemaRef.get_table_ref`) **or** a string table name.
            When a string, ``schema``/``namespace_id`` disambiguate.
        schema: Only used when ``table`` is a string. Schema name; resolved
            via Core. Mutually exclusive with ``namespace_id``.
        namespace_id: Only used when ``table`` is a string. Explicit namespace ID.
        delta_version: If provided, opens that specific Delta commit (time travel).

    Returns:
        A lazy ``pl.LazyFrame`` over the Delta directory.

    Raises:
        KeyError: Table not found.
        ValueError: Both ``schema`` and ``namespace_id`` were passed alongside a string.
    """
    if isinstance(table, TableRef):
        if schema is not None or namespace_id is not None:
            raise ValueError(
                "When passing a TableRef, 'schema' and 'namespace_id' must be omitted "
                "(the ref carries that information itself)."
            )
        target_ns = table.schema.id
        table_name = table.name
    else:
        table_name = table
    auth_headers = _get_internal_auth_headers()
    with httpx.Client(timeout=30.0, headers=auth_headers) as client:
        if not isinstance(table, TableRef):
            target_ns = _resolve_target_namespace_id(client, schema, namespace_id)
        row = _resolve_catalog_table(client, table_name, target_ns, strict=False)
        if row is None:
            raise KeyError(f"Catalog table '{table_name}' not found")
        host_path = _table_file_path(row)
        if host_path is None:
            raise RuntimeError(f"Catalog table '{table_name}' has no file_path")
        kernel_path = _translate_host_path_to_container(host_path)
    if delta_version is not None:
        return pl.scan_delta(kernel_path, version=delta_version)
    return pl.scan_delta(kernel_path)


def write_catalog_table(
    df: pl.DataFrame | pl.LazyFrame,
    table: TableRef | str,
    *,
    schema: str | None = None,
    namespace_id: int | None = None,
    write_mode: str = "overwrite",
    merge_keys: list[str] | None = None,
    description: str | None = None,
) -> TableRef:
    """Write a Polars DataFrame / LazyFrame to a catalog table.

    The kernel performs the Delta write locally and reports metadata to Core;
    Core never materialises the dataset.

    Args:
        df: Polars ``DataFrame`` or ``LazyFrame`` to write.
        table: Either a :class:`TableRef` (skips the resolution dance) **or**
            a string table name (combined with ``schema``/``namespace_id``).
        schema: Only used when ``table`` is a string. Schema name; resolved via Core.
            Mutually exclusive with ``namespace_id``.
        namespace_id: Only used when ``table`` is a string. Explicit namespace ID.
        write_mode: One of ``"overwrite"``, ``"append"``, ``"upsert"``,
            ``"update"``, ``"delete"``, ``"error"``.
        merge_keys: Column names used as merge predicates for
            ``"upsert"``/``"update"``/``"delete"`` modes.
        description: Optional description (only applied to newly-created
            table records).

    Returns:
        A :class:`TableRef` for the table that was written — with populated
        ``id``, ``row_count``, ``size_bytes``, ``schema_columns``, etc.

    Raises:
        ValueError: Unknown ``write_mode`` or missing ``merge_keys`` for merge modes,
            or ``schema``/``namespace_id`` were passed alongside a ``TableRef``.
        FileExistsError: ``write_mode="error"`` and the table already exists.
    """
    if write_mode not in _CATALOG_VALID_WRITE_MODES:
        raise ValueError(
            f"Unknown write_mode '{write_mode}'. Expected one of: "
            f"{sorted(_CATALOG_VALID_WRITE_MODES)}"
        )
    if write_mode in ("upsert", "update", "delete") and not merge_keys:
        raise ValueError(f"write_mode='{write_mode}' requires merge_keys")

    ref_input: TableRef | None
    if isinstance(table, TableRef):
        if schema is not None or namespace_id is not None:
            raise ValueError(
                "When passing a TableRef, 'schema' and 'namespace_id' must be omitted "
                "(the ref carries that information itself)."
            )
        ref_input = table
        table_name = table.name
    else:
        ref_input = None
        table_name = table

    eager_df = df.collect() if isinstance(df, pl.LazyFrame) else df

    auth_headers = _get_internal_auth_headers()
    with httpx.Client(timeout=60.0, headers=auth_headers) as client:
        if ref_input is not None:
            target_ns = ref_input.schema.id
            target_schema: SchemaRef = ref_input.schema
        else:
            target_ns = _resolve_target_namespace_id(client, schema, namespace_id)
            target_schema = _schema_ref_for_namespace_id(client, target_ns)
        existing = _resolve_catalog_table(client, table_name, target_ns, strict=False)

        if write_mode == "error" and existing is not None:
            raise FileExistsError(
                f"Catalog table '{table_name}' already exists in namespace {target_ns} "
                f"and write_mode='error'."
            )

        if existing is None:
            # Fresh write: generate a new Delta directory, write, then register.
            kernel_path = _new_catalog_table_dir(table_name)
            _perform_delta_write(eager_df, kernel_path, write_mode, merge_keys, table_exists=False)
            meta = _read_delta_metadata(kernel_path)
            host_path = _translate_container_path_to_host(kernel_path)
            body = {
                "name": table_name,
                "table_path": host_path,
                "namespace_id": target_ns,
                "description": description,
                "storage_format": "delta",
                "schema_columns": meta["schema_columns"],
                "row_count": meta["row_count"],
                "column_count": meta["column_count"],
                "size_bytes": meta["size_bytes"],
            }
            row = _catalog_post(client, "/catalog/tables/from-data", body).json()
            return _table_ref_from_row(row, target_schema)

        # Existing table: write in-place to its current path, then refresh.
        existing_host_path = _table_file_path(existing)
        if existing_host_path is None:
            raise RuntimeError(
                f"Existing catalog table '{table_name}' has no file_path; cannot write."
            )
        kernel_path = _translate_host_path_to_container(existing_host_path)
        _perform_delta_write(eager_df, kernel_path, write_mode, merge_keys, table_exists=True)
        meta = _read_delta_metadata(kernel_path)
        body = {
            "table_path": existing_host_path,
            "storage_format": "delta",
            "schema_columns": meta["schema_columns"],
            "row_count": meta["row_count"],
            "column_count": meta["column_count"],
            "size_bytes": meta["size_bytes"],
        }
        if description is not None:
            body["description"] = description
        row = _catalog_post(
            client, f"/catalog/tables/{existing['id']}/refresh", body
        ).json()
        return _table_ref_from_row(row, target_schema)


def _schema_ref_for_namespace_id(client: httpx.Client, namespace_id: int) -> SchemaRef:
    """Construct a fully-populated :class:`SchemaRef` from a namespace_id.

    Walks the catalog tree once — only used by :func:`write_catalog_table`
    when the caller passed a bare ``namespace_id`` / ``schema`` and we need
    a typed schema ref to return inside the resulting ``TableRef``.
    """
    cats = _catalog_get(client, "/catalog/namespaces").json()
    for cat in cats:
        cat_ref = CatalogRef(id=cat["id"], name=cat["name"])
        schemas = _catalog_get(
            client, "/catalog/namespaces", params={"parent_id": cat["id"]}
        ).json()
        for sch in schemas:
            if sch["id"] == namespace_id:
                return SchemaRef(id=sch["id"], name=sch["name"], catalog=cat_ref)
    raise LookupError(f"Namespace id={namespace_id} not found in any catalog")


def _table_file_path(table: dict[str, Any]) -> str | None:
    """Pull the on-disk path out of a ``CatalogTableOut`` response.

    Tolerant of multiple key names (``file_path``, ``table_path``,
    ``path``) since different Core versions have used different names.
    """
    for key in ("file_path", "table_path", "path"):
        value = table.get(key)
        if value:
            return value
    return None


def _perform_delta_write(
    df: pl.DataFrame,
    kernel_path: str,
    write_mode: str,
    merge_keys: list[str] | None,
    table_exists: bool,
) -> None:
    """Run the Delta write locally inside the kernel.

    Mirrors the logic in ``shared/delta_utils.py``: simple overwrite/append
    uses Polars' built-in ``write_delta``; merge modes use
    ``deltalake.DeltaTable.merge`` directly so the kernel doesn't depend on
    ``flowfile_core`` or ``flowfile_worker``.
    """
    os.makedirs(kernel_path, exist_ok=True)

    if write_mode in ("overwrite", "append"):
        delta_write_options: dict[str, str] = {}
        if write_mode == "overwrite":
            delta_write_options["schema_mode"] = "overwrite"
        elif write_mode == "append":
            delta_write_options["schema_mode"] = "merge"
        df.write_delta(kernel_path, mode=write_mode, delta_write_options=delta_write_options)
        return

    if write_mode == "error":
        # Caller already verified the table didn't exist. Plain create.
        df.write_delta(kernel_path, mode="error")
        return

    # Merge modes: upsert / update / delete.
    if not table_exists:
        # Need to create the Delta structure first so subsequent operations
        # have something to target.
        if write_mode in ("update", "delete"):
            # No-op operation against an empty table — establish the schema only.
            df.clear().write_delta(kernel_path, mode="error")
        else:
            df.write_delta(kernel_path, mode="error")
        return

    from deltalake import DeltaTable

    dt = DeltaTable(kernel_path)
    if write_mode in ("upsert", "update"):
        target_col_names = {field.name for field in dt.schema().fields}
        new_cols = [c for c in df.columns if c not in target_col_names]
        if new_cols:
            df.clear().write_delta(
                kernel_path, mode="append", delta_write_options={"schema_mode": "merge"}
            )
            dt = DeltaTable(kernel_path)

    if not merge_keys:
        raise ValueError(f"merge_keys is required for write_mode='{write_mode}'")
    predicate = " AND ".join(f'target."{k}" = source."{k}"' for k in merge_keys)
    merger = dt.merge(
        source=df.to_arrow(),
        predicate=predicate,
        source_alias="source",
        target_alias="target",
    )
    if write_mode == "upsert":
        merger.when_matched_update_all().when_not_matched_insert_all().execute()
    elif write_mode == "update":
        merger.when_matched_update_all().execute()
    elif write_mode == "delete":
        merger.when_matched_delete().execute()
