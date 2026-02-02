import contextlib
import io
import json
import os
import time
from pathlib import Path

import httpx
from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

from kernel_runtime import __version__, flowfile_client
from kernel_runtime.artifact_store import ArtifactStore
from kernel_runtime.serialization import detect_format, get_serializer

app = FastAPI(title="FlowFile Kernel Runtime", version=__version__)
artifact_store = ArtifactStore()


class ExecuteRequest(BaseModel):
    node_id: int
    code: str
    input_paths: dict[str, list[str]] = {}
    output_dir: str = ""
    flow_id: int = 0
    log_callback_url: str = ""


class ClearNodeArtifactsRequest(BaseModel):
    node_ids: list[int]
    flow_id: int | None = None


class ExecuteResponse(BaseModel):
    success: bool
    output_paths: list[str] = []
    artifacts_published: list[str] = []
    artifacts_deleted: list[str] = []
    stdout: str = ""
    stderr: str = ""
    error: str | None = None
    execution_time_ms: float = 0.0


@app.post("/execute", response_model=ExecuteResponse)
async def execute(request: ExecuteRequest):
    start = time.perf_counter()
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    output_dir = request.output_dir
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Clear any artifacts this node previously published so re-execution
    # doesn't fail with "already exists".
    artifact_store.clear_by_node_ids({request.node_id}, flow_id=request.flow_id)

    artifacts_before = set(artifact_store.list_all(flow_id=request.flow_id).keys())

    try:
        flowfile_client._set_context(
            node_id=request.node_id,
            input_paths=request.input_paths,
            output_dir=output_dir,
            artifact_store=artifact_store,
            flow_id=request.flow_id,
            log_callback_url=request.log_callback_url,
        )

        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            exec(request.code, {"flowfile": flowfile_client})  # noqa: S102

        # Collect output parquet files
        output_paths: list[str] = []
        if output_dir and Path(output_dir).exists():
            output_paths = [
                str(p) for p in sorted(Path(output_dir).glob("*.parquet"))
            ]

        artifacts_after = set(artifact_store.list_all(flow_id=request.flow_id).keys())
        new_artifacts = sorted(artifacts_after - artifacts_before)
        deleted_artifacts = sorted(artifacts_before - artifacts_after)

        elapsed = (time.perf_counter() - start) * 1000
        return ExecuteResponse(
            success=True,
            output_paths=output_paths,
            artifacts_published=new_artifacts,
            artifacts_deleted=deleted_artifacts,
            stdout=stdout_buf.getvalue(),
            stderr=stderr_buf.getvalue(),
            execution_time_ms=elapsed,
        )
    except Exception as exc:
        elapsed = (time.perf_counter() - start) * 1000
        return ExecuteResponse(
            success=False,
            stdout=stdout_buf.getvalue(),
            stderr=stderr_buf.getvalue(),
            error=f"{type(exc).__name__}: {exc}",
            execution_time_ms=elapsed,
        )
    finally:
        flowfile_client._clear_context()


@app.post("/clear")
async def clear_artifacts(flow_id: int | None = Query(default=None)):
    """Clear all artifacts, or only those belonging to a specific flow."""
    artifact_store.clear(flow_id=flow_id)
    return {"status": "cleared"}


@app.post("/clear_node_artifacts")
async def clear_node_artifacts(request: ClearNodeArtifactsRequest):
    """Clear only artifacts published by the specified node IDs."""
    removed = artifact_store.clear_by_node_ids(
        set(request.node_ids), flow_id=request.flow_id,
    )
    return {"status": "cleared", "removed": removed}


@app.get("/artifacts")
async def list_artifacts(flow_id: int | None = Query(default=None)):
    """List all artifacts, optionally filtered by flow_id."""
    return artifact_store.list_all(flow_id=flow_id)


@app.get("/artifacts/node/{node_id}")
async def list_node_artifacts(
    node_id: int, flow_id: int | None = Query(default=None),
):
    """List artifacts published by a specific node."""
    return artifact_store.list_by_node_id(node_id, flow_id=flow_id)


@app.get("/health")
async def health():
    return {"status": "healthy", "version": __version__, "artifact_count": len(artifact_store.list_all())}


# ---------------------------------------------------------------------------
# Global Artifact Registry bridge
# ---------------------------------------------------------------------------
# These endpoints let user code running inside the kernel call
# ``flowfile.publish_global()`` and ``flowfile.get_global()``.
# The kernel serializes/deserializes the Python object locally, then
# streams the blob to/from the Core API over HTTP.
# ---------------------------------------------------------------------------

# Core API base URL — the Docker container reaches the host via this address.
_CORE_URL = os.environ.get(
    "FLOWFILE_CORE_URL",
    f"http://host.docker.internal:{os.environ.get('FLOWFILE_CORE_PORT', '63578')}",
)


class PublishGlobalRequest(BaseModel):
    """Request body for the kernel-side ``/publish_global`` endpoint."""
    artifact_name: str
    name: str
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    namespace_id: int | None = None
    serialization_format: str | None = None  # auto-detect if omitted
    flow_id: int = 0


class PublishGlobalResponse(BaseModel):
    success: bool
    artifact_id: int | None = None
    error: str | None = None


class GetGlobalRequest(BaseModel):
    """Request body for the kernel-side ``/get_global`` endpoint."""
    name: str
    namespace_id: int | None = None
    version: int | None = None


class GetGlobalResponse(BaseModel):
    success: bool
    artifact_name: str | None = None
    error: str | None = None


@app.post("/publish_global", response_model=PublishGlobalResponse)
async def publish_global(request: PublishGlobalRequest):
    """Serialize a transient artifact and upload it to the Core catalog.

    Steps:
    1. Retrieve the Python object from the local ArtifactStore.
    2. Serialize with the chosen (or auto-detected) format.
    3. POST the blob + metadata to ``Core /artifacts/publish``.
    """
    try:
        obj = artifact_store.get(request.artifact_name, flow_id=request.flow_id)
    except KeyError:
        return PublishGlobalResponse(
            success=False,
            error=f"Transient artifact '{request.artifact_name}' not found in this kernel",
        )

    # Serialization
    fmt = request.serialization_format or detect_format(obj)
    serializer = get_serializer(fmt)
    try:
        blob = serializer.dumps(obj)
    except Exception as exc:
        return PublishGlobalResponse(
            success=False,
            error=f"Serialization failed ({fmt}): {exc}",
        )

    type_name = type(obj).__name__
    module_name = type(obj).__module__ or ""
    filename = f"{request.name}{serializer.file_extension}"

    # Retrieve auth token from execution context (if available)
    ctx = flowfile_client._context.get({})
    auth_token = ctx.get("auth_token", "")

    headers = {}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    # Upload to Core
    try:
        with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
            resp = client.post(
                f"{_CORE_URL}/artifacts/publish",
                files={"file": (filename, io.BytesIO(blob), "application/octet-stream")},
                data={
                    "name": request.name,
                    "python_type": type_name,
                    "python_module": module_name,
                    "serialization_format": fmt,
                    "description": request.description or "",
                    "tags": json.dumps(request.tags),
                    "namespace_id": str(request.namespace_id) if request.namespace_id is not None else "",
                    "source_flow_id": str(request.flow_id) if request.flow_id else "",
                    "source_node_id": str(ctx.get("node_id", "")),
                    "source_kernel_id": ctx.get("kernel_id", ""),
                },
                headers=headers,
            )
        if resp.status_code == 201:
            body = resp.json()
            return PublishGlobalResponse(success=True, artifact_id=body.get("id"))
        return PublishGlobalResponse(
            success=False,
            error=f"Core returned {resp.status_code}: {resp.text[:500]}",
        )
    except Exception as exc:
        return PublishGlobalResponse(success=False, error=f"Upload to Core failed: {exc}")


@app.post("/get_global", response_model=GetGlobalResponse)
async def get_global(request: GetGlobalRequest):
    """Download a global artifact from the Core catalog, deserialize it, and
    store it in the local transient ArtifactStore so user code can access it
    via ``flowfile.read_artifact()``.

    Steps:
    1. Resolve the artifact metadata via ``Core GET /artifacts/by-name/{name}``.
    2. Download the blob from ``Core GET /artifacts/{id}/download``.
    3. Deserialize using the recorded format.
    4. Store in the local ArtifactStore.
    """
    ctx = flowfile_client._context.get({})
    auth_token = ctx.get("auth_token", "")
    flow_id = ctx.get("flow_id", 0)
    node_id = ctx.get("node_id", 0)

    headers = {}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    try:
        with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
            # 1. Resolve metadata
            params = {}
            if request.namespace_id is not None:
                params["namespace_id"] = request.namespace_id
            if request.version is not None:
                params["version"] = request.version

            meta_resp = client.get(
                f"{_CORE_URL}/artifacts/by-name/{request.name}",
                params=params,
                headers=headers,
            )
            if meta_resp.status_code != 200:
                return GetGlobalResponse(
                    success=False,
                    error=f"Artifact '{request.name}' not found in catalog (HTTP {meta_resp.status_code})",
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
                return GetGlobalResponse(
                    success=False,
                    error=f"Failed to download artifact blob (HTTP {dl_resp.status_code})",
                )

            # 3. Deserialize
            serializer = get_serializer(fmt)
            obj = serializer.loads(dl_resp.content)

            # 4. Store in local artifact store (overwrite if exists)
            try:
                artifact_store.delete(request.name, flow_id=flow_id)
            except KeyError:
                pass
            artifact_store.publish(request.name, obj, node_id=node_id, flow_id=flow_id)

            return GetGlobalResponse(success=True, artifact_name=request.name)

    except Exception as exc:
        return GetGlobalResponse(success=False, error=f"get_global failed: {exc}")
