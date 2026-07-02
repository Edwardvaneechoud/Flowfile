"""Backend that runs compute in the core process.

Only selected when ``execution_location == "local"`` (flowfile_frame, the
scheduler/CLI path, WASM-style single-process runs). Bounded preview collects
are allowed here; the core-never-collects contract applies to the remote path.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import polars as pl

from flowfile_core.flowfile.execution.backends.base import ExecutionBackend
from flowfile_core.flowfile.execution.exceptions import WorkerTaskError
from flowfile_core.flowfile.execution.handles import LocalResultHandle, TaskHandle
from shared.storage_config import storage


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

    def results_exist(self, file_ref: str) -> bool:
        return False

    def get_cached_lazyframe(self, file_ref: str) -> pl.LazyFrame:
        raise WorkerTaskError("Local execution keeps no shared result cache")

    def clear_result(self, file_ref: str) -> bool:
        return False
