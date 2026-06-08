"""Helpers for executing user code on a kernel container.

Extracted from FlowGraph.add_python_script to keep the closure small and
each piece independently testable.
"""

import logging
import os
import re

import polars as pl

from flowfile_core.configs.settings import OFFLOAD_TO_WORKER, SERVER_PORT
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import ExternalDfFetcher
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult

logger = logging.getLogger(__name__)

_SAFE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _assert_safe_name(name: str) -> None:
    """Raise if *name* is not a safe filesystem identifier."""
    if not _SAFE_NAME_RE.match(name):
        raise ValueError(f"Unsafe input/output name rejected: {name!r}")


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


def _write_parquet_locally(lf: pl.LazyFrame | pl.DataFrame, output_path: str) -> None:
    """Collect a LazyFrame and write it to a parquet file locally.

    This mirrors the worker's write_parquet function for use when
    OFFLOAD_TO_WORKER is False (e.g. CLI execution via ``flowfile run flow``).
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if isinstance(lf, pl.DataFrame):
        df = lf
    else:
        try:
            df = lf.collect(engine="streaming")
        except Exception:
            df = lf.collect()
    df.write_parquet(output_path)
    # "rb+" — os.fsync needs a writable fd on Windows (EBADF on read-only handles)
    with open(output_path, "rb+") as f:
        os.fsync(f.fileno())


def write_inputs_to_parquet(
    flowfile_tables: tuple[FlowDataEngine, ...],
    manager: KernelManager,
    input_dir: str,
    flow_id: int,
    node_id: int,
    input_names: list[str] | None = None,
) -> dict[str, list[str]]:
    """Serialize input tables to parquet on the shared volume.

    When *input_names* is provided, each table gets its own named key in the
    returned dict (e.g. ``{"orders": [...], "customers": [...]}``).  A
    ``"main"`` key is always included pointing to **all** input files so that
    ``flowfile_ctx.read_input("main")`` continues to work.

    When *input_names* is ``None``, falls back to the original behaviour
    where every input is grouped under ``"main"``.

    Returns the ``input_paths`` dict expected by :class:`ExecuteRequest`.
    """
    use_local = not OFFLOAD_TO_WORKER

    if input_names is None:
        main_paths: list[str] = []
        for idx, ft in enumerate(flowfile_tables):
            local_path = os.path.join(input_dir, f"main_{idx}.parquet")
            if use_local:
                _write_parquet_locally(ft.data_frame, local_path)
            else:
                fetcher = ExternalDfFetcher(
                    flow_id=flow_id,
                    node_id=node_id,
                    lf=ft.data_frame,
                    wait_on_completion=True,
                    operation_type="write_parquet",
                    kwargs={"output_path": local_path},
                )
                if fetcher.has_error:
                    raise RuntimeError(f"Failed to write parquet for input {idx}: {fetcher.error_description}")
            main_paths.append(manager.to_kernel_path(local_path))
        return {"main": main_paths}

    result: dict[str, list[str]] = {}
    all_paths: list[str] = []
    for idx, (ft, name) in enumerate(zip(flowfile_tables, input_names, strict=True)):
        _assert_safe_name(name)
        local_path = os.path.join(input_dir, f"{name}_{idx}.parquet")
        if use_local:
            _write_parquet_locally(ft.data_frame, local_path)
        else:
            fetcher = ExternalDfFetcher(
                flow_id=flow_id,
                node_id=node_id,
                lf=ft.data_frame,
                wait_on_completion=True,
                operation_type="write_parquet",
                kwargs={"output_path": local_path},
            )
            if fetcher.has_error:
                raise RuntimeError(f"Failed to write parquet for input {idx} ({name}): {fetcher.error_description}")
        kernel_path = manager.to_kernel_path(local_path)
        result.setdefault(name, []).append(kernel_path)
        all_paths.append(kernel_path)

    # Always include "main" as a backward-compatible alias for all inputs
    if "main" not in result:
        result["main"] = all_paths

    return result


def build_execute_request(
    *,
    node_id: int,
    code: str,
    input_paths: dict[str, list[str]],
    output_dir: str,
    flow_id: int,
    manager: KernelManager,
    source_registration_id: int | None,
) -> ExecuteRequest:
    """Assemble the kernel ExecuteRequest with log callback URL and auth token."""
    if manager._kernel_volume:
        log_callback_url = f"http://flowfile-core:{SERVER_PORT}/raw_logs"
    else:
        log_callback_url = f"http://host.docker.internal:{SERVER_PORT}/raw_logs"

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
