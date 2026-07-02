"""ExecutionBackend: the seam deciding where node compute runs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

import polars as pl

from flowfile_core.flowfile.execution.handles import TaskHandle


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
