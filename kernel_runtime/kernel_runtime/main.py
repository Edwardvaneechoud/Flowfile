import contextlib
import io
import os
import time
from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel

from kernel_runtime import __version__, flowfile_client
from kernel_runtime.artifact_store import ArtifactStore

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

    artifacts_before = set(artifact_store.list_all().keys())

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

        artifacts_after = set(artifact_store.list_all().keys())
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
async def clear_artifacts():
    artifact_store.clear()
    return {"status": "cleared"}


@app.post("/clear_node_artifacts")
async def clear_node_artifacts(request: ClearNodeArtifactsRequest):
    """Clear only artifacts published by the specified node IDs."""
    removed = artifact_store.clear_by_node_ids(set(request.node_ids))
    return {"status": "cleared", "removed": removed}


@app.get("/artifacts")
async def list_artifacts():
    return artifact_store.list_all()


@app.get("/artifacts/node/{node_id}")
async def list_node_artifacts(node_id: int):
    """List artifacts published by a specific node."""
    return artifact_store.list_by_node_id(node_id)


@app.get("/health")
async def health():
    return {"status": "healthy", "version": __version__, "artifact_count": len(artifact_store.list_all())}
