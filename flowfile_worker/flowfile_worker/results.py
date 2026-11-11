"""The results this worker holds for the ``/v1`` routes, by result id.

A result is materialised (an Arrow IPC file under ``CACHE_DIR``) or deferred (the submitted plan, run again when rows
or a count are asked for). A blob is a materialised record of a file of bytes, such as a fitted model, with no columns
and no row count, which the routes copy and never read as rows. The registry lives in the FastAPI process only and
does not survive a restart. It also stamps when the submit under an id ended, failed ones included, so ``expire`` can
drop what has been held too long.
"""

import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from flowfile_worker import CACHE_DIR
from flowfile_worker.configs import logger
from shared.compute_protocol import ResultRef, WireColumn, WorkerLocation


@dataclass
class ResultRecord:
    """One held result: a file at ``path`` when materialised, the serialised ``plan`` when deferred.

    ``flow_id`` and ``node_id`` are those of the submit, so a later sample or count of a deferred
    result logs under the node that produced it. A ``kind="blob"`` record is a path and nothing
    else: materialised, the file of its bytes at ``path``, no columns and no row count. A record
    whose fields do not fit its state is a ``ValueError`` at construction, never a failure in the
    child that later loads it.
    """

    result_id: str
    state: Literal["materialised", "deferred"]
    columns: list[WireColumn]
    row_count: int | None
    flow_id: int
    node_id: int | str
    path: str | None = None
    plan: bytes | None = None
    kind: Literal["table", "blob"] = "table"

    def __post_init__(self) -> None:
        if self.state == "materialised" and (not self.path or self.plan is not None):
            raise ValueError(f"A materialised result holds a file and no plan: {self.result_id}")
        if self.state == "deferred" and (self.plan is None or self.path is not None):
            raise ValueError(f"A deferred result holds a plan and no file: {self.result_id}")
        if self.kind == "blob" and (self.state != "materialised" or self.columns or self.row_count is not None):
            raise ValueError(f"A blob is a materialised file with no columns and no row count: {self.result_id}")

    def ref(self) -> ResultRef:
        """The wire reference core keeps for this result; its ``location`` is a ``WorkerLocation``'s JSON."""
        if self.kind == "blob":
            location = WorkerLocation(kind="blob", path=self.path)
        elif self.state == "materialised":
            location = WorkerLocation(kind="file", path=self.path)
        else:
            location = WorkerLocation(kind="plan", plan=self.plan)
        return ResultRef(
            result_id=self.result_id,
            columns=self.columns,
            row_count=self.row_count,
            state=self.state,
            location=location.model_dump_json().encode(),
            kind=self.kind,
        )


class ResultRegistry:
    """Held results by id, under a lock: request threads read and release while submits put.

    Beside the records it keeps, per id, the monotonic time the last submit under it ended. A
    failed submit has a stamp and no record, so its job entry ages out with the rest.
    """

    def __init__(self) -> None:
        self._records: dict[str, ResultRecord] = {}
        self._ended: dict[str, float] = {}
        self._lock = threading.Lock()

    def get(self, result_id: str) -> ResultRecord | None:
        with self._lock:
            return self._records.get(result_id)

    def put(self, record: ResultRecord) -> None:
        with self._lock:
            self._records[record.result_id] = record

    def mark_ended(self, result_id: str) -> None:
        """Stamp that the submit under *result_id* ended now; its age counts from here."""
        with self._lock:
            self._ended[result_id] = time.monotonic()

    def pop(self, result_id: str) -> ResultRecord | None:
        """Forget the result, its end stamp and its file; ``None`` when no record was held.

        The stamp goes whether or not a record was held, so a resubmit under the id is not expired
        while it runs.
        """
        with self._lock:
            record = self._records.pop(result_id, None)
            self._ended.pop(result_id, None)
        _remove_file(record)
        return record

    def expire(self, max_age: float) -> list[str]:
        """Forget every id whose submit ended *max_age* seconds ago or more, with its record and file.

        The files are deleted after the registry's lock is released, each on its own: one that cannot be removed is
        logged and the sweep goes on. Returns the ids.
        """
        cutoff = time.monotonic() - max_age
        with self._lock:
            expired = [result_id for result_id, ended in self._ended.items() if ended <= cutoff]
            records = [self._records.pop(result_id, None) for result_id in expired]
            for result_id in expired:
                del self._ended[result_id]
        for record in records:
            _remove_file(record)
        return expired


def _remove_file(record: ResultRecord | None) -> None:
    """Delete a materialised record's file; one already gone is fine, one that cannot be removed is logged and left."""
    if record is not None and record.path:
        try:
            os.remove(record.path)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning(f"Could not remove the file of result {record.result_id}: {e}")


results = ResultRegistry()


def result_path(flow_id: int, result_id: str) -> Path:
    """Where a materialised result is written: ``CACHE_DIR/<flow_id>/<result_id>.arrow``, the legacy layout."""
    return CACHE_DIR / str(flow_id) / f"{result_id}.arrow"
