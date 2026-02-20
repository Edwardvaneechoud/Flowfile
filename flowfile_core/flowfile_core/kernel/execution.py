"""Helpers for executing user code on a kernel container.

Extracted from FlowGraph.add_python_script to keep the closure small and
each piece independently testable.
"""

import logging
import os

from flowfile_core.configs.settings import SERVER_PORT
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import ExternalDfFetcher
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult

logger = logging.getLogger(__name__)


def write_inputs_to_parquet(
    flowfile_tables: tuple[FlowDataEngine, ...],
    manager: KernelManager,
    input_dir: str,
    flow_id: int,
    node_id: int,
) -> dict[str, list[str]]:
    """Serialize input tables to parquet on the shared volume.

    Returns the ``input_paths`` dict expected by :class:`ExecuteRequest`.
    """
    main_paths: list[str] = []
    for idx, ft in enumerate(flowfile_tables):
        local_path = os.path.join(input_dir, f"main_{idx}.parquet")
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


def forward_kernel_logs(result: ExecuteResult, node_logger) -> None:
    """Pipe captured stdout/stderr from the kernel into the node logger."""
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            node_logger.info(f"[stdout] {line}")
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            node_logger.warning(f"[stderr] {line}")
