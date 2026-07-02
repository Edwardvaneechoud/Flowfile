"""ExecutionBackend: the seam deciding where node compute runs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from flowfile_core.flowfile.execution.handles import TaskHandle

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
    from flowfile_core.flowfile.flow_node.multi_output import NamedOutputs
    from flowfile_core.flowfile.sources.external_sources.sql_source.models import DatabaseExternalReadSettings
    from flowfile_core.schemas.input_schema import OutputSettings


class ExecutionBackend(ABC):
    """Typed compute operations, implemented per execution location.

    ``LocalBackend`` runs in-process; ``RemoteWorkerBackend`` ships work to the
    flowfile_worker service. Node code calls these methods instead of branching
    on ``execution_location`` and constructing fetchers inline, so new backends
    (worker pools, remote runners) can be added without touching node logic.
    """

    location: ClassVar[str]

    # -- frame operations ------------------------------------------------------

    @abstractmethod
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
        """Materialise a LazyFrame plan; the result stays owned by the backend."""

    @abstractmethod
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
        """Produce preview rows; ``handle.status.file_ref`` points at an Arrow IPC file."""

    @abstractmethod
    def count_records(self, lf: pl.LazyFrame, *, flow_id: int, node_id: int | str) -> int | None:
        """Count the rows a LazyFrame plan produces."""

    @abstractmethod
    def random_split(
        self,
        df: FlowDataEngine,
        splits: list[tuple[str, float]],
        seed: int | None,
        *,
        flow_id: int,
        node_id: int | str,
    ) -> NamedOutputs:
        """Partition rows into labeled outputs by the given percentages."""

    # -- sources and sinks -------------------------------------------------------

    @abstractmethod
    def read_database(
        self,
        settings: DatabaseExternalReadSettings,
        *,
        cancel_check: Callable[[], bool] | None = None,
    ) -> TaskHandle:
        """Run the rendered SQL read; the result is a LazyFrame."""

    @abstractmethod
    def write_output(
        self,
        df: FlowDataEngine,
        settings: OutputSettings,
        *,
        flow_id: int,
        node_id: int | str,
    ) -> TaskHandle:
        """Write the frame to its file destination."""

    # -- result cache ------------------------------------------------------------

    @abstractmethod
    def results_exist(self, file_ref: str) -> bool:
        """Whether a completed cached result exists for ``file_ref``."""

    @abstractmethod
    def get_cached_lazyframe(self, file_ref: str) -> pl.LazyFrame:
        """Load a completed cached result; raises when unavailable."""

    @abstractmethod
    def clear_result(self, file_ref: str) -> bool:
        """Drop the cached result for ``file_ref``."""
