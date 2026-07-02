"""Backend that runs compute in the core process.

Only selected when ``execution_location == "local"`` (flowfile_frame, the
scheduler/CLI path, WASM-style single-process runs). Bounded preview collects
are allowed here; the core-never-collects contract applies to the remote path.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from flowfile_core.flowfile.execution.backends.base import ExecutionBackend
from flowfile_core.flowfile.execution.exceptions import WorkerTaskError
from flowfile_core.flowfile.execution.handles import LocalResultHandle, TaskHandle
from shared.storage_config import storage

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
    from flowfile_core.flowfile.flow_node.multi_output import NamedOutputs
    from flowfile_core.flowfile.sources.external_sources.sql_source.models import DatabaseExternalReadSettings
    from flowfile_core.schemas.input_schema import OutputSettings


class LocalBackend(ExecutionBackend):
    """Runs operations in-process; there is no shared result cache."""

    location: ClassVar[str] = "local"

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
        return LocalResultHandle(result=lf, file_ref=file_ref)

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
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import Status

        df = lf.head(sample_size).collect()
        path = Path(storage.cache_directory) / str(flow_id) / f"{file_ref}.arrow"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_ipc(path)
        status = Status(
            background_task_id=file_ref,
            status="Completed",
            file_ref=str(path),
            progress=100,
            results=None,
            result_type="other",
        )
        return LocalResultHandle(result=df.lazy(), file_ref=str(path), status=status)

    def count_records(self, lf: pl.LazyFrame, *, flow_id: int, node_id: int | str) -> int | None:
        return int(lf.select(pl.len()).collect().item())

    def random_split(
        self,
        df: FlowDataEngine,
        splits: list[tuple[str, float]],
        seed: int | None,
        *,
        flow_id: int,
        node_id: int | str,
    ) -> NamedOutputs:
        return df.random_split(splits, seed)

    def read_database(
        self,
        settings: DatabaseExternalReadSettings,
        *,
        cancel_check: Callable[[], bool] | None = None,
    ) -> TaskHandle:
        from flowfile_core.flowfile.sources.external_sources.sql_source import utils as sql_utils
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource
        from flowfile_core.secret_manager.secret_manager import decrypt_secret

        connection = settings.connection
        source = SqlSource(
            connection_string=sql_utils.construct_sql_uri(
                database_type=connection.database_type,
                host=connection.host,
                port=connection.port,
                database=connection.database,
                username=connection.username,
                password=decrypt_secret(connection.password) if connection.password else None,
                ssl_enabled=bool(getattr(connection, "ssl_enabled", False)),
                connect_timeout=10,
            ),
            query=settings.query,
            cancel_check=cancel_check,
        )
        return LocalResultHandle(result=source.get_pl_df().lazy(), file_ref=str(settings.flowfile_node_id))

    def write_output(
        self,
        df: FlowDataEngine,
        settings: OutputSettings,
        *,
        flow_id: int,
        node_id: int | str,
    ) -> TaskHandle:
        df.output(output_fs=settings, flow_id=flow_id, node_id=node_id, execute_remote=False)
        return LocalResultHandle(result=None, file_ref=str(node_id))

    def results_exist(self, file_ref: str) -> bool:
        return False

    def get_cached_lazyframe(self, file_ref: str) -> pl.LazyFrame:
        raise WorkerTaskError("Local execution keeps no shared result cache")

    def clear_result(self, file_ref: str) -> bool:
        return False
