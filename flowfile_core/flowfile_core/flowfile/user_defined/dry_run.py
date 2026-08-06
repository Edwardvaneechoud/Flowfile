"""Dry-run backend for the Node Designer test panel.

Executes a candidate custom node against bounded sample inputs in the worker
(never in core), returning a per-output preview, buffered logs, and a typed
error kind. Core ast-parses first so syntax errors never reach the worker;
sample inputs come from the request or the node's ``example_inputs``; secrets
resolve for real via the author's own/shared secrets.
"""

import ast
import json
import os
import threading
import time
import uuid
from typing import Annotated, Any, Literal

import polars as pl
import requests
from pydantic import BaseModel, Field, model_validator

from flowfile_core.configs import logger
from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import CustomNodeExecuteInput
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_custom_node_operation,
)
from flowfile_core.flowfile.node_designer.codegen import CodegenError, generate_source
from flowfile_core.flowfile.node_designer.parsing import extract_example_inputs, extract_manifest
from flowfile_core.flowfile.node_designer.state import DesignerState, SecretSelectorState
from flowfile_core.schemas.input_schema import RawData

ErrorKind = Literal["syntax", "load", "settings", "execution", "timeout", "no_sample_data"]

_DEFAULT_ROW_LIMIT = 100
_MAX_ROW_LIMIT = 1000
_DEFAULT_TIMEOUT = 30
_MAX_TIMEOUT = 120
_POLL_INTERVAL = 0.25
_DRY_RUN_FLOW_ID = -1
# Sample inputs are materialized in core (schema-typed FlowDataEngine construction) before shipping
# to the worker/kernel, so bound them: core never materializes full datasets, and dry-run test data
# is small.
_MAX_SAMPLE_ROWS = 10_000
_MAX_SAMPLE_CELLS = 200_000


class DryRunColumn(BaseModel):
    name: str
    data_type: str


class DryRunOutput(BaseModel):
    name: str
    columns: list[DryRunColumn] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False


class DryRunRequest(BaseModel):
    designer_state: DesignerState | None = None
    code: str | None = None
    settings_values: dict[str, dict[str, Any]] = Field(default_factory=dict)
    # Typed RawData (schema + columnar data) preferred; bare {col: values} dicts still
    # accepted, with dtypes inferred (mixed int/float columns promote to Float64).
    # left_to_right: smart union would match a RawData payload as the bare dict.
    sample_inputs: list[Annotated[RawData | dict[str, list], Field(union_mode="left_to_right")]] | None = None
    row_limit: int = _DEFAULT_ROW_LIMIT
    timeout_seconds: int = _DEFAULT_TIMEOUT
    kernel_id: str | None = None  # run on this Docker kernel instead of the worker (kernel-env nodes)

    @model_validator(mode="after")
    def _validate(self) -> "DryRunRequest":
        if (self.designer_state is None) == (self.code is None):
            raise ValueError("exactly one of designer_state or code must be provided")
        if not 1 <= self.row_limit <= _MAX_ROW_LIMIT:
            self.row_limit = max(1, min(self.row_limit, _MAX_ROW_LIMIT))
        if not 1 <= self.timeout_seconds <= _MAX_TIMEOUT:
            self.timeout_seconds = max(1, min(self.timeout_seconds, _MAX_TIMEOUT))
        return self


class DryRunResponse(BaseModel):
    success: bool
    outputs: list[DryRunOutput] = Field(default_factory=list)
    logs: list[str] = Field(default_factory=list)
    error: str | None = None
    error_kind: ErrorKind | None = None
    traceback: str | None = None
    duration_ms: float | None = None
    executed_in: Literal["worker", "in_core", "kernel"] = "worker"


def _fail(
    kind: ErrorKind, error: str, *, traceback: str | None = None, logs: list[str] | None = None
) -> DryRunResponse:
    return DryRunResponse(
        success=False, error=error, error_kind=kind, traceback=traceback, logs=logs or [], executed_in="worker"
    )


def _resolve_source(request: DryRunRequest) -> tuple[str | None, DryRunResponse | None]:
    """Return (source, None) or (None, error_response)."""
    if request.designer_state is not None:
        try:
            return generate_source(request.designer_state), None
        except CodegenError as e:
            return None, _fail("load", f"Cannot generate node source: {e}")
    return request.code, None


def _resolve_output_names(request: DryRunRequest, source: str) -> list[str]:
    if request.designer_state is not None:
        return request.designer_state.output_names or ["main"]
    # Code-only: lift declared output_names, else default to a single "main" output.
    try:
        module = ast.parse(source)
    except SyntaxError:
        return ["main"]
    for stmt in module.body:
        if not isinstance(stmt, ast.ClassDef):
            continue
        for item in stmt.body:
            target = None
            value = None
            if isinstance(item, ast.Assign) and len(item.targets) == 1 and isinstance(item.targets[0], ast.Name):
                target, value = item.targets[0].id, item.value
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name) and item.value is not None:
                target, value = item.target.id, item.value
            if target == "output_names":
                try:
                    names = ast.literal_eval(value)
                except (ValueError, SyntaxError):
                    return ["main"]
                if isinstance(names, list) and names and all(isinstance(n, str) for n in names):
                    return names
    return ["main"]


def _coerce_samples(
    samples: list[RawData | dict[str, list]],
) -> tuple[list[RawData] | None, DryRunResponse | None]:
    coerced: list[RawData] = []
    for i, sample in enumerate(samples):
        try:
            coerced.append(sample if isinstance(sample, RawData) else RawData.from_pydict(sample))
        except Exception as e:
            return None, _fail("no_sample_data", f"Sample input {i} is not a valid table: {e}")
    return coerced, None


def _resolve_sample_inputs(request: DryRunRequest, source: str) -> tuple[list[RawData] | None, DryRunResponse | None]:
    if request.sample_inputs is not None:
        return _coerce_samples(request.sample_inputs)
    if request.designer_state is not None and request.designer_state.example_inputs is not None:
        return _coerce_samples([ex.data for ex in request.designer_state.example_inputs])
    lifted = extract_example_inputs(source)
    if lifted:
        return _coerce_samples(lifted)
    # No sample data at all: run against one empty frame per declared input (none for
    # a source node) so nodes that don't need input data are still testable, rather
    # than hard-blocking with "add sample data".
    n = (
        request.designer_state.number_of_inputs
        if request.designer_state is not None
        else extract_manifest(source).number_of_inputs
    )
    return [RawData(columns=[], data=[]) for _ in range(max(0, n))], None


def _check_sample_size(samples: list[RawData]) -> DryRunResponse | None:
    """Reject oversized sample inputs before any in-core materialization.

    ``sample_inputs`` comes straight from the request payload; an unbounded one would be
    serialized in core's own process, breaking the "core never materializes full datasets"
    contract. Guards both the worker and kernel dry-run paths.
    """
    total_cells = 0
    for i, sample in enumerate(samples):
        rows = max((len(col) for col in sample.data), default=0)
        if rows > _MAX_SAMPLE_ROWS:
            return _fail(
                "no_sample_data",
                f"Sample input {i} has {rows} rows; the dry-run limit is {_MAX_SAMPLE_ROWS}.",
            )
        total_cells += sum(len(col) for col in sample.data)
    if total_cells > _MAX_SAMPLE_CELLS:
        return _fail(
            "no_sample_data",
            f"Sample inputs total {total_cells} cells; the dry-run limit is {_MAX_SAMPLE_CELLS}.",
        )
    return None


def _serialize_inputs(samples: list[RawData]) -> tuple[list[bytes] | None, DryRunResponse | None]:
    # FlowDataEngine applies the RawData schema — the same typed construction as manual input,
    # so declared dtypes (e.g. Float64 over mixed int/float cells) cast instead of erroring.
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

    serialized: list[bytes] = []
    for i, sample in enumerate(samples):
        try:
            serialized.append(FlowDataEngine(sample).data_frame.serialize())
        except Exception as e:
            return None, _fail("no_sample_data", f"Sample input {i} is not a valid table: {e}")
    return serialized, None


def _resolve_secrets(request: DryRunRequest, user_id: int | None) -> tuple[dict[str, str], DryRunResponse | None]:
    """Resolve SecretSelector values referenced in settings_values to ciphertext.

    Designer mode only (code-only nodes get no secrets in v1). A missing secret
    or access failure surfaces as an ``execution`` error.
    """
    if request.designer_state is None:
        return {}, None

    from flowfile_core.secret_manager.secret_manager import get_encrypted_secret

    secret_names: set[str] = set()
    for section in request.designer_state.sections:
        section_values = request.settings_values.get(section.name, {})
        for component in section.components:
            if isinstance(component, SecretSelectorState):
                value = section_values.get(component.name)
                if isinstance(value, str) and value:
                    secret_names.add(value)

    payload: dict[str, str] = {}
    for name in secret_names:
        if user_id is None:
            return {}, _fail("execution", f"Secret '{name}' requires a signed-in user to resolve")
        try:
            encrypted = get_encrypted_secret(current_user_id=user_id, secret_name=name)
        except Exception as e:
            return {}, _fail("execution", f"Could not resolve secret '{name}': {e}")
        if encrypted is None:
            return {}, _fail("execution", f"Secret '{name}' not found for this user")
        payload[name] = encrypted
    return payload, None


def _poll_worker(task_id: str, deadline: float) -> tuple[dict | None, DryRunResponse | None]:
    """Poll ``/status/{task}`` until completion or the monotonic *deadline*.

    On deadline the task is cancelled and a ``timeout`` error is returned. A
    connection error becomes a ``load`` error ("worker unavailable") — user code
    is never run in core for dry runs.
    """
    while True:
        if time.monotonic() >= deadline:
            _cancel(task_id)
            return None, _fail("timeout", "Dry run exceeded the time limit and was cancelled.")
        try:
            resp = requests.get(f"{WORKER_URL}/status/{task_id}", timeout=10)
        except requests.RequestException as e:
            return None, _fail("load", f"The compute worker is unavailable: {e}")
        if resp.status_code == 404:
            return None, _fail("execution", "The dry-run task disappeared from the worker before completing.")
        if resp.status_code != 200:
            return None, _fail("execution", f"Worker returned HTTP {resp.status_code}: {resp.text[:300]}")
        status = resp.json()
        state = status.get("status")
        if state == "Completed":
            return status, None
        if state in ("Error", "Unknown Error"):
            message = status.get("error_message") or "Unknown execution error"
            error, tb = _split_traceback(message)
            return None, _fail("execution", error, traceback=tb)
        if state == "Cancelled":
            return None, _fail("timeout", "Dry run was cancelled.")
        time.sleep(_POLL_INTERVAL)


def _split_traceback(message: str) -> tuple[str, str | None]:
    """The dry-run child packs ``"<error>\\n<traceback>"`` into the error message."""
    if "\nTraceback (most recent call last):" in message:
        head, _, tail = message.partition("\nTraceback (most recent call last):")
        return head.strip(), ("Traceback (most recent call last):" + tail).strip()
    return message.strip(), None


def _cancel(task_id: str) -> None:
    try:
        requests.post(f"{WORKER_URL}/cancel_task/{task_id}", timeout=10)
    except requests.RequestException:
        logger.debug("Dry-run cancel for task %s failed (worker unreachable)", task_id)


def _clear(task_id: str) -> None:
    try:
        requests.delete(f"{WORKER_URL}/clear_task/{task_id}", timeout=10)
    except requests.RequestException:
        logger.debug("Dry-run clear for task %s failed (worker unreachable)", task_id)


def _map_payload(payload: dict, output_names: list[str]) -> DryRunResponse:
    preview = payload.get("preview", {})
    outputs: list[DryRunOutput] = []
    for name in output_names:
        info = payload["outputs"].get(name, {})
        prev = preview.get(name, {})
        columns = [DryRunColumn(name=c["name"], data_type=c["data_type"]) for c in prev.get("columns", [])]
        outputs.append(
            DryRunOutput(
                name=name,
                columns=columns,
                rows=[list(r) for r in prev.get("rows", [])],
                row_count=info.get("row_count", 0),
                truncated=bool(prev.get("truncated", False)),
            )
        )
    return DryRunResponse(
        success=True,
        outputs=outputs,
        logs=list(payload.get("logs", [])),
        duration_ms=payload.get("duration_ms"),
        executed_in="worker",
    )


def run_dry_run(request: DryRunRequest, user_id: int | None) -> DryRunResponse:
    """Execute a candidate node against bounded sample inputs in the worker."""
    source, err = _resolve_source(request)
    if err is not None:
        return err
    if not source or not source.strip():
        return _fail("load", "No node source was provided.")

    try:
        ast.parse(source)
    except SyntaxError as e:
        return _fail("syntax", f"Syntax error at line {e.lineno}: {e.msg}")

    output_names = _resolve_output_names(request, source)

    samples, err = _resolve_sample_inputs(request, source)
    if err is not None:
        return err

    err = _check_sample_size(samples)
    if err is not None:
        return err

    # Kernel-environment nodes run in their Docker kernel (which has the node's pip
    # deps), not the worker. designer-state carries the environment kind directly;
    # code-only nodes surface it via the exec-free manifest.
    env_kind = (
        request.designer_state.environment.kind
        if request.designer_state is not None
        else extract_manifest(source).environment.kind
    )
    if env_kind == "kernel":
        if not request.kernel_id:
            return _fail("execution", "This node runs in an isolated kernel — select a kernel to run the test.")
        return _run_dry_run_on_kernel(request, source, output_names, samples)

    serialized, err = _serialize_inputs(samples)
    if err is not None:
        return err

    secrets, err = _resolve_secrets(request, user_id)
    if err is not None:
        return err

    class_name = request.designer_state.class_name if request.designer_state is not None else None

    task_id = str(uuid.uuid4())
    execute_input = CustomNodeExecuteInput(
        task_id=task_id,
        node_source=source,
        class_name=class_name,
        settings_values=request.settings_values,
        secrets=secrets,
        inputs=serialized,
        output_names=output_names,
        dry_run=True,
        row_limit=request.row_limit,
        user_id=user_id,
        flowfile_flow_id=_DRY_RUN_FLOW_ID,
        flowfile_node_id=task_id,
    )

    try:
        start_status = trigger_custom_node_operation(execute_input)
    except Exception as e:
        return _fail("load", f"The compute worker is unavailable: {e}")

    task_id = start_status.background_task_id
    deadline = time.monotonic() + request.timeout_seconds
    try:
        status, err = _poll_worker(task_id, deadline)
        if err is not None:
            return err
        try:
            raw = status.get("results")
            payload = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError) as e:
            return _fail("execution", f"Could not read the dry-run result payload: {e}")
        return _map_payload(payload, output_names)
    finally:
        _clear(task_id)


def _kernel_logs(result) -> list[str]:
    """Flatten a kernel ExecuteResult's captured stdout/stderr into log lines."""
    lines: list[str] = []
    if getattr(result, "stdout", None):
        lines.extend(f"[stdout] {ln}" for ln in result.stdout.strip().splitlines())
    if getattr(result, "stderr", None):
        lines.extend(f"[stderr] {ln}" for ln in result.stderr.strip().splitlines())
    return lines


def _contained_output_path(output_dir: str, name: str) -> str | None:
    """Resolve ``<output_dir>/<name>.parquet``, or None if ``name`` escapes ``output_dir``.

    ``output_names`` is client-supplied and unvalidated (plain ``list[str]``), and ``output_dir``
    lives on a volume shared across every user's dry runs — so a name with ``..`` segments could
    otherwise read another run's parquet. Reject anything resolving outside the run's own dir.
    """
    root = os.path.realpath(output_dir)
    path = os.path.realpath(os.path.join(output_dir, f"{name}.parquet"))
    if not path.startswith(root + os.sep):
        return None
    return path


def _read_kernel_output_previews(
    output_dir: str, output_names: list[str], result, row_limit: int
) -> tuple[list[DryRunOutput], DryRunResponse | None]:
    """Build a bounded preview per declared output from the kernel's parquet files.

    Mirrors read_kernel_outputs' ``<name>.parquet`` convention and its guard: if the
    kernel reported published files but none match the expected names, surface a clear
    error instead of silently returning empty outputs.
    """
    outputs: list[DryRunOutput] = []
    found = False
    for name in output_names:
        path = _contained_output_path(output_dir, name)
        if path is None or not os.path.exists(path):
            outputs.append(DryRunOutput(name=name))
            continue
        found = True
        lf = pl.scan_parquet(path)
        head = lf.head(row_limit).collect()
        row_count = int(lf.select(pl.len()).collect().item())
        outputs.append(
            DryRunOutput(
                name=name,
                columns=[
                    DryRunColumn(name=c, data_type=str(dt)) for c, dt in zip(head.columns, head.dtypes, strict=True)
                ],
                rows=[list(r) for r in head.iter_rows()],
                row_count=row_count,
                truncated=row_count > head.height,
            )
        )
    if getattr(result, "output_paths", None) and not found:
        published = [p.replace("\\", "/").rsplit("/", 1)[-1] for p in result.output_paths]
        expected = [f"{name}.parquet" for name in output_names]
        detail = (
            f"published {published} but the node expects {expected} — match the name passed to "
            "flowfile_ctx.publish_output(df, name=...) with the node's output names."
            if not set(published) & set(expected)
            else f"reported outputs {published} but none were found on the shared volume."
        )
        return [], _fail("execution", f"Kernel {detail}")
    return outputs, None


def _build_dry_run_execute_request(**kwargs):
    """Build the kernel ExecuteRequest for a dry run with log streaming disabled.

    Production runs stream ``flowfile_ctx.log()`` to core's ``/raw_logs`` flow
    logger, but the Test tab only surfaces captured stdout/stderr. Blanking the
    callback URL makes ``log()`` fall back to ``print()`` so its output lands in
    stdout the tab already renders.

    ``dry_run`` tells the kernel to report ``flowfile_ctx.is_dry_run() == True``
    and to keep ``publish_global`` writes in memory, so pressing Test never adds
    a versioned row to the user's artifact catalog.
    """
    from flowfile_core.kernel.execution import build_execute_request

    request = build_execute_request(**kwargs)
    request.log_callback_url = ""
    request.dry_run = True
    return request


def _run_dry_run_on_kernel(
    request: DryRunRequest, source: str, output_names: list[str], samples: list[RawData]
) -> DryRunResponse:
    """Execute a kernel-environment candidate node on its Docker kernel.

    Mirrors production kernel execution (generate_kernel_script + the kernel manager)
    against bounded sample inputs, reusing the FlowGraph-free helpers in
    kernel/execution.py. Secrets aren't available inside kernels, so none are resolved.
    """
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
    from flowfile_core.flowfile.user_defined.kernel_codegen import KernelCodegenError, generate_kernel_script
    from flowfile_core.kernel import get_kernel_manager
    from flowfile_core.kernel.execution import clear_stale_parquets, write_inputs_to_parquet

    if request.designer_state is not None:
        class_name = request.designer_state.class_name
        number_of_inputs = request.designer_state.number_of_inputs
    else:
        manifest = extract_manifest(source)
        class_name = manifest.class_name
        number_of_inputs = manifest.number_of_inputs
    if not class_name:
        return _fail("load", "Could not find a CustomNodeBase subclass in the node source.")

    try:
        code = generate_kernel_script(
            node_source=source,
            class_name=class_name,
            settings_values=request.settings_values,
            output_names=output_names,
            number_of_inputs=number_of_inputs,
            dry_run=True,
        )
    except KernelCodegenError as e:
        return _fail("load", f"Cannot generate kernel script: {e}")

    try:
        frames = tuple(FlowDataEngine(s) for s in samples)
    except Exception as e:
        return _fail("no_sample_data", f"Sample input is not a valid table: {e}")

    manager = get_kernel_manager()
    node_id = uuid.uuid4().int % 1_000_000_000  # unique per-run dir; avoids concurrent-dry-run collisions
    input_dir = os.path.join(manager.shared_volume_path, str(_DRY_RUN_FLOW_ID), str(node_id), "inputs")
    output_dir = os.path.join(manager.shared_volume_path, str(_DRY_RUN_FLOW_ID), str(node_id), "outputs")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    clear_stale_parquets(input_dir)
    clear_stale_parquets(output_dir)

    started = time.monotonic()
    try:
        input_paths = write_inputs_to_parquet(frames, manager, input_dir, _DRY_RUN_FLOW_ID, node_id)
        execute_request = _build_dry_run_execute_request(
            node_id=node_id,
            code=code,
            input_paths=input_paths,
            output_dir=output_dir,
            flow_id=_DRY_RUN_FLOW_ID,
            manager=manager,
            source_registration_id=None,
        )
    except Exception as e:
        return _fail("execution", f"Could not prepare kernel inputs: {e}")

    cancel_event = threading.Event()
    timer = threading.Timer(request.timeout_seconds, cancel_event.set)
    timer.start()
    try:
        result = manager.execute_sync(request.kernel_id, execute_request, cancel_event=cancel_event)
    except KeyError:
        return _fail("execution", "The selected kernel is no longer available — pick another kernel.")
    except Exception as e:
        return _fail("load", f"The kernel is unavailable: {e}")
    finally:
        timer.cancel()

    if not result.success:
        if cancel_event.is_set():
            return _fail("timeout", "Dry run exceeded the time limit and was cancelled.")
        error, tb = _split_traceback(result.error or "Kernel execution failed")
        return _fail("execution", error, traceback=tb, logs=_kernel_logs(result))

    outputs, err = _read_kernel_output_previews(output_dir, output_names, result, request.row_limit)
    if err is not None:
        return err
    return DryRunResponse(
        success=True,
        outputs=outputs,
        logs=_kernel_logs(result),
        duration_ms=(time.monotonic() - started) * 1000,
        executed_in="kernel",
    )
