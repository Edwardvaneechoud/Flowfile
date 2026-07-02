"""Backend that ships compute to the flowfile_worker service."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from flowfile_core.configs.settings import OFFLOAD_TO_WORKER
from flowfile_core.flowfile.execution.backends.base import ExecutionBackend
from flowfile_core.flowfile.execution.exceptions import WorkerTaskError
from flowfile_core.flowfile.execution.handles import TaskHandle
from flowfile_core.flowfile.execution.transport import WorkerTransport, get_default_transport

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
    from flowfile_core.flowfile.flow_node.multi_output import NamedOutputs
    from flowfile_core.flowfile.sources.external_sources.sql_source.models import DatabaseExternalReadSettings
    from flowfile_core.schemas.input_schema import OutputSettings


class RemoteWorkerBackend(ExecutionBackend):
    """Runs operations on the worker; results live in the worker's cache."""

    location: ClassVar[str] = "remote"

    def __init__(self, transport: WorkerTransport | None = None):
        self.transport = transport or get_default_transport()

    def run_lazyframe(
        self,
        lf: pl.LazyFrame,
        *,
        flow_id: int,
        node_id: int | str,
        file_ref: str,
        wait_on_completion: bool = False,
        operation_type: str = "store",
    ) -> TaskHandle:
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations import ExternalDfFetcher

        return ExternalDfFetcher(
            lf=lf,
            file_ref=file_ref,
            wait_on_completion=wait_on_completion,
            flow_id=flow_id,
            node_id=node_id,
            operation_type=operation_type,
            transport=self.transport,
        )

    def sample(
        self,
        lf: pl.LazyFrame,
        *,
        file_ref: str,
        flow_id: int,
        node_id: int | str,
        sample_size: int = 100,
        wait_on_completion: bool = True,
    ) -> TaskHandle:
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations import ExternalSampler

        return ExternalSampler(
            lf=lf,
            file_ref=file_ref,
            wait_on_completion=wait_on_completion,
            node_id=node_id,
            flow_id=flow_id,
            sample_size=sample_size,
            transport=self.transport,
        )

    def count_records(self, lf: pl.LazyFrame, *, flow_id: int, node_id: int | str) -> int | None:
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations import ExternalDfFetcher

        return ExternalDfFetcher(
            lf=lf,
            operation_type="calculate_number_of_records",
            flow_id=flow_id,
            node_id=node_id,
            transport=self.transport,
        ).result

    def random_split(
        self,
        df: FlowDataEngine,
        splits: list[tuple[str, float]],
        seed: int | None,
        *,
        flow_id: int,
        node_id: int | str,
    ) -> NamedOutputs:
        return df.random_split_external(splits, seed, flow_id=flow_id, node_id=node_id)

    def read_database(
        self,
        settings: DatabaseExternalReadSettings,
        *,
        cancel_check: Callable[[], bool] | None = None,
    ) -> TaskHandle:
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations import ExternalDatabaseFetcher

        return ExternalDatabaseFetcher(settings, wait_on_completion=False, transport=self.transport)

    def write_output(
        self,
        df: FlowDataEngine,
        settings: OutputSettings,
        *,
        flow_id: int,
        node_id: int | str,
    ) -> TaskHandle:
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations import ExternalOutputWriter

        return ExternalOutputWriter(
            lf=df.data_frame,
            data_type=settings.file_type,
            path=settings.abs_file_path,
            write_mode=settings.write_mode,
            sheet_name=settings.sheet_name,
            delimiter=settings.delimiter,
            compression=settings.compression,
            flow_id=flow_id,
            node_id=node_id,
            wait_on_completion=False,
            transport=self.transport,
        )

    def results_exist(self, file_ref: str) -> bool:
        if not OFFLOAD_TO_WORKER:
            return False
        return self.transport.results_exist(file_ref)

    def get_cached_lazyframe(self, file_ref: str) -> pl.LazyFrame:
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations import get_df_result

        status = self.transport.get_status(file_ref)
        if status.status != "Completed":
            raise WorkerTaskError(f"Status is not completed, {status.status}")
        if status.result_type != "polars":
            raise WorkerTaskError(f"Result type is not polars, {status.result_type}")
        return get_df_result(status.results)

    def clear_result(self, file_ref: str) -> bool:
        if not OFFLOAD_TO_WORKER:
            return False
        return self.transport.clear_task(file_ref)
