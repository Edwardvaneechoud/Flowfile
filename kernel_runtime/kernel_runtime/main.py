import ast
import contextlib
import io
import os
import time
from pathlib import Path

from fastapi import FastAPI, Query
from pydantic import BaseModel

from kernel_runtime import __version__, flowfile_client
from kernel_runtime.artifact_store import ArtifactStore

app = FastAPI(title="FlowFile Kernel Runtime", version=__version__)
artifact_store = ArtifactStore()


# Matplotlib setup code to auto-capture plt.show() calls
_MATPLOTLIB_SETUP = """\
try:
    import matplotlib as _mpl
    _mpl.use('Agg')
    import matplotlib.pyplot as _plt
    _original_show = _plt.show
    def _flowfile_show(*args, **kwargs):
        import matplotlib.pyplot as __plt
        for _fig_num in __plt.get_fignums():
            flowfile.display(__plt.figure(_fig_num))
        __plt.close('all')
    _plt.show = _flowfile_show
except ImportError:
    pass
"""


def _maybe_wrap_last_expression(code: str) -> str:
    """If the last statement is a bare expression, wrap it in flowfile.display().

    This provides Jupyter-like behavior where the result of the last expression
    is automatically displayed.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return code
    if not tree.body:
        return code
    last = tree.body[-1]
    if not isinstance(last, ast.Expr):
        return code

    # Don't wrap if the expression is None, a string literal, or already a call to display/print
    if isinstance(last.value, ast.Constant) and last.value.value is None:
        return code
    if isinstance(last.value, ast.Call):
        # Check if it's already a print or display call
        func = last.value.func
        if isinstance(func, ast.Name) and func.id in ("print", "display"):
            return code
        if isinstance(func, ast.Attribute) and func.attr in ("print", "display"):
            return code

    # Use ast.get_source_segment for robust source extraction (Python 3.8+)
    last_expr_text = ast.get_source_segment(code, last)
    if last_expr_text is None:
        # Fallback if get_source_segment fails
        return code

    # Build the new code with the last expression wrapped
    lines = code.split('\n')
    prefix = '\n'.join(lines[:last.lineno - 1])
    if prefix:
        prefix += '\n'
    return prefix + f'flowfile.display({last_expr_text})\n'


class ExecuteRequest(BaseModel):
    node_id: int
    code: str
    input_paths: dict[str, list[str]] = {}
    output_dir: str = ""
    flow_id: int = 0
    log_callback_url: str = ""
    interactive: bool = False  # When True, auto-display last expression


class ClearNodeArtifactsRequest(BaseModel):
    node_ids: list[int]
    flow_id: int | None = None


class DisplayOutput(BaseModel):
    """A single display output from code execution."""
    mime_type: str  # "image/png", "text/html", "text/plain"
    data: str       # base64 for images, raw HTML for text/html, plain text otherwise
    title: str = ""


class ExecuteResponse(BaseModel):
    success: bool
    output_paths: list[str] = []
    artifacts_published: list[str] = []
    artifacts_deleted: list[str] = []
    display_outputs: list[DisplayOutput] = []
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

        # Reset display outputs for this execution
        flowfile_client._reset_displays()

        # Prepare execution namespace with flowfile module
        exec_globals = {"flowfile": flowfile_client}

        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            # Execute matplotlib setup to patch plt.show()
            exec(_MATPLOTLIB_SETUP, exec_globals)  # noqa: S102

            # Prepare user code - optionally wrap last expression for interactive mode
            user_code = request.code
            if request.interactive:
                user_code = _maybe_wrap_last_expression(user_code)

            # Execute user code
            exec(user_code, exec_globals)  # noqa: S102

        # Collect display outputs
        display_outputs = [
            DisplayOutput(**d) for d in flowfile_client._get_displays()
        ]

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
            display_outputs=display_outputs,
            stdout=stdout_buf.getvalue(),
            stderr=stderr_buf.getvalue(),
            execution_time_ms=elapsed,
        )
    except Exception as exc:
        # Still collect any display outputs that were generated before the error
        display_outputs = [
            DisplayOutput(**d) for d in flowfile_client._get_displays()
        ]
        elapsed = (time.perf_counter() - start) * 1000
        return ExecuteResponse(
            success=False,
            display_outputs=display_outputs,
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
