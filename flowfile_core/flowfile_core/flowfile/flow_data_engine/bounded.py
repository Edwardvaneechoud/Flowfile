"""Bounded reads of a plan through a compute backend: its row count, its first rows, one row, its distinct values.

Each helper submits the plan as a deferred result (``DEFERRED_HINTS``), makes one bounded read of it (``count`` or
``sample``) and releases it, whether the read succeeded or not. Nothing is computed before that read, and the read
computes only what it returns: a count, or at most the rows asked for.

Where the work runs is the backend's choice. A failure is the ``ComputeError`` the backend raised, and nothing here
computes the plan in core instead. The jobs are filed under no flow and no node (``-1``).
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

Nothing is cached (``must_materialise`` off), the one bounded read is the only rows core takes (no ``preview_rows``),
and no consumer reads the result twice (``wide`` off, ``fanout`` the one read)."""

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


def unique_values(
    backend: ComputeBackend, plan: pl.LazyFrame | pl.DataFrame, limit: int, column: str | None = None
) -> pl.Series:
    """The distinct values of ``plan``'s one column in ascending order (null first), at most ``limit`` of them.

    The values are deduplicated and sorted where ``backend`` runs the plan, and come back in the column's own dtype,
    so they are ordered by value, not by text. Pass ``column`` when the caller knows it: the plan's schema is then not
    resolved in core, where its leaf may scan a file only ``backend`` can read.

    Raises:
        ComputeError: ``backend`` could not compute them.
        ValueError: ``plan`` has more or fewer columns than one (checked only without ``column``).
    """
    lazy = plan.lazy()
    if column is None:
        columns = lazy.collect_schema().names()
        if len(columns) != 1:
            raise ValueError(f"Distinct values are read from a plan of one column, not {len(columns)}: {columns}")
        column = columns[0]
    distinct = lazy.unique().sort(column)
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
