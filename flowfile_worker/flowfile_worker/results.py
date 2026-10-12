"""The results this worker holds for the ``/v1`` routes, by result id.

A result is either materialised (an Arrow IPC file the submit child wrote under ``CACHE_DIR``)
or deferred (the submitted plan, run again when rows or a count are asked for). Both keep the
columns the child read from the plan's schema. The registry lives in the FastAPI process only;
nothing a spawned child imports reaches it, and like ``status_dict`` it does not survive a restart.
"""

import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from flowfile_worker import CACHE_DIR
from shared.compute_protocol import ResultRef, WireColumn, WorkerLocation


@dataclass
class ResultRecord:
    """One held result: a file at ``path`` when materialised, the serialised ``plan`` when deferred.

    ``flow_id`` and ``node_id`` are those of the submit, so a later sample or count of a deferred
    result logs under the node that produced it.
    """

    result_id: str
    state: Literal["materialised", "deferred"]
    columns: list[WireColumn]
    row_count: int | None
    flow_id: int
    node_id: int | str
    path: str | None = None
    plan: bytes | None = None

    def ref(self) -> ResultRef:
        """The wire reference core keeps for this result; its ``location`` is a ``WorkerLocation``'s JSON."""
        if self.state == "materialised":
            location = WorkerLocation(kind="file", path=self.path)
        else:
            location = WorkerLocation(kind="plan", plan=self.plan)
        return ResultRef(
            result_id=self.result_id,
            columns=self.columns,
            row_count=self.row_count,
            state=self.state,
            location=location.model_dump_json().encode(),
        )


class ResultRegistry:
    """Held results by id, under a lock: request threads read and release while submits put."""

    def __init__(self) -> None:
        self._records: dict[str, ResultRecord] = {}
        self._lock = threading.Lock()

    def get(self, result_id: str) -> ResultRecord | None:
        with self._lock:
            return self._records.get(result_id)

    def put(self, record: ResultRecord) -> None:
        with self._lock:
            self._records[record.result_id] = record

    def pop(self, result_id: str) -> ResultRecord | None:
        """Forget the result and delete its file when it has one; ``None`` when it was not held."""
        with self._lock:
            record = self._records.pop(result_id, None)
        if record is not None and record.path:
            try:
                os.remove(record.path)
            except FileNotFoundError:
                pass
        return record


results = ResultRegistry()


def result_path(flow_id: int, result_id: str) -> Path:
    """Where a materialised result is written: ``CACHE_DIR/<flow_id>/<result_id>.arrow``, the legacy layout."""
    return CACHE_DIR / str(flow_id) / f"{result_id}.arrow"
