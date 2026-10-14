"""Bounded reads of a plan through a compute backend: its row count, its first rows, one row, its distinct values.

Each helper submits the plan to the backend as a deferred result under a fresh result id, makes one bounded read of
it (``count`` or ``sample``) and releases it, whether the read succeeded or not. The submit asks for no
materialisation and no inline sample (``DEFERRED_HINTS``), which both of core's backends honour by holding the plan
as it is: ``LocalBackend`` holds every plan lazily, and ``WorkerBackend`` stores a plan only when it must be
materialised or is wide with a preview asked for. So nothing is computed before the one read, and the read computes
only what it returns: a count, or at most the rows asked for (``LocalBackend`` collects ``plan.head(n)`` in this
process, the worker runs ``plan.limit(n)`` in a child and core reads that many rows back from the file it wrote).

Where the work runs is the backend's to decide, never the helper's: a failure comes back as the ``ComputeError`` the
backend raised (``ComputeUnavailable``, ``TaskFailed``, ``PlanRejected``, ...), and nothing here computes the plan in
core instead. A plan crosses as the ``pl.LazyFrame`` itself (an eager frame is turned lazy first); a backend that runs
it elsewhere serialises it there. The jobs are filed under no flow and no node (``-1``).
"""

from __future__ import annotations

import io
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa

from flowfile_core.flowfile.execution.protocol import SubmitHints

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.protocol import ComputeBackend, ResultRef

DEFERRED_HINTS = SubmitHints(must_materialise=False, wide=False, fanout=1, preview_rows=None)
"""Hints that keep a submitted plan deferred with no inline sample on every core backend.

``must_materialise`` is off because nothing is cached, ``preview_rows`` is ``None`` because the one bounded read is the
only rows core takes, and ``wide`` is off because no consumer reads the result twice. ``fanout`` is the one read."""

_NO_FLOW = -1
_RESULT_ID_PREFIX = "bounded_"


def count_rows(backend: ComputeBackend, plan: pl.LazyFrame | pl.DataFrame) -> int:
    """The number of rows ``plan`` yields, counted by ``backend``.

    Raises:
        ComputeError: ``backend`` could not count them.
    """
    with _deferred(backend, plan) as ref:
        return backend.count(ref)


def head_rows(backend: ComputeBackend, plan: pl.LazyFrame | pl.DataFrame, n: int) -> pa.Table:
    """The first ``n`` rows of ``plan`` (all of them when it yields fewer), computed by ``backend``.

    Raises:
        ComputeError: ``backend`` could not compute them.
    """
    with _deferred(backend, plan) as ref:
        return _arrow_table(backend.sample(ref, n))


def single_row(backend: ComputeBackend, plan: pl.LazyFrame | pl.DataFrame, empty_message: str) -> dict[str, Any]:
    """The first row of ``plan``, computed by ``backend``, as Python values keyed by column name.

    The values are what ``pl.DataFrame.row`` gives for them, as for any row core reads in process.

    Raises:
        ComputeError: ``backend`` could not compute the row.
        ValueError: ``plan`` yields no rows; the message is ``empty_message``.
    """
    rows = pl.from_arrow(head_rows(backend, plan, 1))
    if rows.height == 0:
        raise ValueError(empty_message)
    return rows.row(0, named=True)


def unique_values(backend: ComputeBackend, plan: pl.LazyFrame | pl.DataFrame, limit: int) -> pl.Series:
    """The distinct values of ``plan``'s one column in ascending order (null first), at most ``limit`` of them.

    The plan computes the distinct values and sorts them (``plan.unique().sort(...)``) where ``backend`` runs it, and
    only the first ``limit`` reach core, in the column's own dtype: a caller that needs them as text casts the series,
    so they are ordered by the column's values, not by their text.

    Raises:
        ComputeError: ``backend`` could not compute them.
        ValueError: ``plan`` has more or fewer columns than one.
    """
    lazy = plan.lazy()
    columns = lazy.collect_schema().names()
    if len(columns) != 1:
        raise ValueError(f"Distinct values are read from a plan of one column, not {len(columns)}: {columns}")
    distinct = lazy.unique().sort(columns[0])
    return pl.from_arrow(head_rows(backend, distinct, limit)).to_series(0)


@contextmanager
def _deferred(backend: ComputeBackend, plan: pl.LazyFrame | pl.DataFrame) -> Iterator[ResultRef]:
    """Hold ``plan`` on ``backend`` as a deferred result for the block, releasing it afterwards."""
    result_id = f"{_RESULT_ID_PREFIX}{uuid.uuid4().hex}"
    ref = backend.submit(plan.lazy(), [], DEFERRED_HINTS, result_id, flow_id=_NO_FLOW, node_id=_NO_FLOW).ref
    try:
        yield ref
    finally:
        backend.release(ref)


def _arrow_table(sample: bytes) -> pa.Table:
    """A backend's sample, an Arrow IPC stream, as a table."""
    with pa.ipc.open_stream(io.BytesIO(sample)) as reader:
        return reader.read_all()
