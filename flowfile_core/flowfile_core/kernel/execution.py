"""Helpers for executing user code on a kernel container.

Extracted from FlowGraph.add_python_script to keep the closure small and
each piece independently testable.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from collections.abc import Iterable
from typing import TYPE_CHECKING

import polars as pl

from flowfile_core.flowfile.execution.context import current_context
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.execution.payloads import FileWritePayload, op_payload
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult
from flowfile_core.kernel.urls import core_base_url
from flowfile_core.schemas.input_schema import OutputParquetTable, OutputSettings

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph

logger = logging.getLogger(__name__)

_SAFE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _assert_safe_name(name: str) -> None:
    """Raise if *name* is not a safe filesystem identifier or is the reserved ``"main"`` alias."""
    if not _SAFE_NAME_RE.match(name):
        raise ValueError(f"Unsafe input/output name rejected: {name!r}")
    if name == "main":
        raise ValueError(
            "Input name 'main' is reserved: read_inputs()['main'] holds every input in wiring order. "
            "Set a different node_reference on the upstream node."
        )


def clear_stale_parquets(dir_path: str) -> None:
    """Remove leftover ``*.parquet`` files from a prior run in *dir_path*.

    Stale outputs would mask missing publishes in read_kernel_outputs; stale
    inputs would be picked up as ghost inputs by resolve_node_paths (the
    interactive /execute and /execute_cell routes scan the whole input dir).
    No-op when the directory does not exist.
    """
    if not os.path.isdir(dir_path):
        return
    for stale in os.listdir(dir_path):
        if stale.endswith(".parquet"):
            os.remove(os.path.join(dir_path, stale))


def write_parquet_for_kernel(
    lf: pl.LazyFrame | pl.DataFrame,
    path: str,
    *,
    flow_id: int,
    node_id: int,
    what: str,
    graph: FlowGraph | None = None,
) -> None:
    """Write *lf* as parquet at *path* for a kernel to read.

    The write is the ``file_write`` operation on ``current_context(graph)``'s backend (the running flow's, else
    *graph*'s, else the default), so core never collects the rows. The file is complete when this returns. A
    ``ComputeError`` becomes a ``RuntimeError`` naming *what*.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    directory, name = os.path.split(path)
    settings = OutputSettings(
        name=name, directory=directory, file_type="parquet", write_mode="overwrite", table_settings=OutputParquetTable()
    ).model_copy(update={"abs_file_path": path})
    write = FileWritePayload(plan=lf.lazy(), settings=settings, flow_id=flow_id, node_id=node_id)
    try:
        current_context(graph).backend.run_op(OperationKind.file_write, op_payload(write))
    except ComputeError as e:
        raise RuntimeError(f"Failed to write parquet for {what}: {e}") from e


def write_inputs_to_parquet(
    flowfile_tables: tuple[FlowDataEngine, ...],
    manager: KernelManager,
    input_dir: str,
    flow_id: int,
    node_id: int,
    input_names: list[str] | None = None,
    *,
    graph: FlowGraph | None = None,
) -> dict[str, list[str]]:
    """Serialize input tables to parquet on the shared volume.

    When *input_names* is provided, each table gets its own named key in the
    returned dict (e.g. ``{"orders": [...], "customers": [...]}``).  A
    ``"main"`` key is always included pointing to **all** input files in wiring
    order, so ``flowfile_ctx.read_inputs()["main"][i]`` is input *i*; no input
    may itself be named ``"main"``.

    When *input_names* is ``None``, falls back to the original behaviour
    where every input is grouped under ``"main"``.

    Each file is written by :func:`write_parquet_for_kernel` on the current
    execution context's backend, resolved through *graph* when no run set one.

    Returns the ``input_paths`` dict expected by :class:`ExecuteRequest`.
    """
    if input_names is None:
        main_paths: list[str] = []
        for idx, ft in enumerate(flowfile_tables):
            local_path = os.path.join(input_dir, f"main_{idx}.parquet")
            write_parquet_for_kernel(
                ft.data_frame, local_path, flow_id=flow_id, node_id=node_id, what=f"input {idx}", graph=graph
            )
            main_paths.append(manager.to_kernel_path(local_path))
        return {"main": main_paths}

    result: dict[str, list[str]] = {}
    all_paths: list[str] = []
    for idx, (ft, name) in enumerate(zip(flowfile_tables, input_names, strict=True)):
        _assert_safe_name(name)
        local_path = os.path.join(input_dir, f"{name}_{idx}.parquet")
        write_parquet_for_kernel(
            ft.data_frame, local_path, flow_id=flow_id, node_id=node_id, what=f"input {idx} ({name})", graph=graph
        )
        kernel_path = manager.to_kernel_path(local_path)
        result.setdefault(name, []).append(kernel_path)
        all_paths.append(kernel_path)

    result["main"] = all_paths
    return result


class KernelBusyError(RuntimeError):
    """A kernel node refused because the caller of its run holds the kernel's execution lock (:class:`KernelHold`)."""

    def __init__(self, node_id: int, kernel_id: str) -> None:
        super().__init__(
            f"Kernel '{kernel_id}' is busy with the notebook cell waiting on this run, so node {node_id} cannot run "
            "on it now"
        )


class KernelHold:
    """The kernels whose execution lock the caller of a run holds while it waits on that run.

    A notebook session's canvas fallback is such a caller: its kernel call holds the kernel while the kernel
    waits on the run, so a node of the run executing on that kernel would wait forever. The run
    (``FlowGraph.run_graph(kernel_hold=...)``, its subflows included) refuses it at once instead, through
    :meth:`check`, which records ``(flow_id, node_id)`` in ``refusals``.
    """

    def __init__(self, kernel_ids: Iterable[str]) -> None:
        self.kernel_ids = frozenset(kernel_ids)
        self.refusals: list[tuple[int, int]] = []

    def check(self, flow_id: int, node_id: int, kernel_id: str) -> None:
        """Raise :class:`KernelBusyError` when *kernel_id* is held, recording the node."""
        if kernel_id in self.kernel_ids:
            self.refusals.append((flow_id, node_id))
            raise KernelBusyError(node_id, kernel_id)


def build_execute_request(
    *,
    node_id: int,
    code: str,
    input_paths: dict[str, list[str]],
    output_dir: str,
    flow_id: int,
    manager: KernelManager,
    source_registration_id: int | None,
    available_artifacts: dict[str, int] | None = None,
) -> ExecuteRequest:
    """Assemble the kernel ExecuteRequest with log callback URL and auth token."""
    # Keyed on the network: the kernel resolves core by service name only once joined to core's network.
    log_callback_url = f"{core_base_url(bool(manager._docker_network))}/raw_logs"

    internal_token: str | None = None
    try:
        from flowfile_core.auth.jwt import get_internal_token

        internal_token = get_internal_token()
    except (ValueError, ImportError):
        pass

    return ExecuteRequest(
        node_id=node_id,
        code=code,
        input_paths=input_paths,
        output_dir=manager.to_kernel_path(output_dir),
        flow_id=flow_id,
        source_registration_id=source_registration_id,
        log_callback_url=log_callback_url,
        internal_token=internal_token,
        available_artifacts=available_artifacts,
        # Minted here so the node holds it as its cancel address before the call; a cancel hits only this cell.
        exec_token=uuid.uuid4().hex,
    )


def read_kernel_outputs(
    *,
    output_dir: str,
    output_names: list[str],
    result: ExecuteResult,
    node,
) -> FlowDataEngine | None:
    """Read the parquet outputs the kernel wrote to *output_dir*.

    Registers each found output on ``node._named_outputs`` and returns the
    primary (index-0) output, or None when the kernel published nothing
    (caller falls back to input passthrough — intentional).

    Raises when the kernel reported published outputs but none of the expected
    files exist locally — either the published names don't match the node's
    output_names, or a host/container path-translation / shared-volume mount
    mismatch that would otherwise silently feed ghost data downstream.
    """
    primary: FlowDataEngine | None = None
    found = False
    for i, name in enumerate(output_names):
        output_path = os.path.join(output_dir, f"{name}.parquet")
        if os.path.exists(output_path):
            found = True
            fde = FlowDataEngine(pl.scan_parquet(output_path))
            if node is not None:
                node._named_outputs[f"output-{i}"] = fde
            if i == 0:
                primary = fde
    if result.output_paths and not found:
        published = [p.replace("\\", "/").rsplit("/", 1)[-1] for p in result.output_paths]
        expected = [f"{name}.parquet" for name in output_names]
        if not set(published) & set(expected):
            raise RuntimeError(
                f"Kernel published {published} but the node expects outputs named {expected}. "
                "Match the name passed to flowfile_ctx.publish_output(df, name=...) with the "
                "node's output names."
            )
        raise RuntimeError(
            f"Kernel reported {len(result.output_paths)} published output file(s) but none were "
            f"found under {output_dir!r} — host/container path-translation or shared-volume "
            f"mount mismatch. Kernel paths: {result.output_paths}"
        )
    return primary


def forward_kernel_logs(result: ExecuteResult, node_logger) -> None:
    """Pipe captured stdout/stderr from the kernel into the node logger."""
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            node_logger.info(f"[stdout] {line}")
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            node_logger.warning(f"[stderr] {line}")
