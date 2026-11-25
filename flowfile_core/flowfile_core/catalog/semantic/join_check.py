"""Check a join's direction against the data: the *to* side must hold one row per key.

The metric compiler LEFT JOINs the *to* table, so a *to* side that repeats its key multiplies every aggregate over the
base table. ``check_join`` measures both sides with one aggregate statement each through the catalog SQL path. Rows
whose key holds a null are ignored, a key of several columns counts distinct combinations.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from flowfile_core.catalog.semantic.compiler import quote_ident
from flowfile_core.catalog.semantic.snapshot import TableInfo
from flowfile_core.schemas.semantic_schema import JoinVerdict


@dataclass
class _Side:
    table: str
    columns: list[str]
    rows: int
    keys: int

    @property
    def unique(self) -> bool:
        return self.keys == self.rows

    @property
    def cols(self) -> str:
        return ", ".join(self.columns)

    def phrase(self) -> str:
        if self.unique:
            return f"is unique in {self.table} ({self.keys} of {self.rows} rows)"
        return f"repeats in {self.table} ({self.keys} distinct in {self.rows} rows)"


@dataclass
class JoinCheck:
    verdict: JoinVerdict
    message: str


class _Unreadable(Exception):
    pass


def _measure(table: TableInfo, columns: list[str], execute: Callable[[str], Any]) -> _Side:
    cols = ", ".join(quote_ident(c) for c in columns)
    not_null = " AND ".join(f"{quote_ident(c)} IS NOT NULL" for c in columns)
    result = execute(
        f'SELECT COUNT(*) AS "keys", COALESCE(SUM("n"), 0) AS "rows" FROM '
        f'(SELECT {cols}, COUNT(*) AS "n" FROM {quote_ident(table.key)} WHERE {not_null} GROUP BY {cols}) AS "g"'
    )
    if result.error or not result.rows:
        raise _Unreadable(f"{table.name} could not be read ({result.error or 'no answer'})")
    values = dict(zip(result.columns, result.rows[0], strict=False))
    side = _Side(table.name, list(columns), int(values["rows"] or 0), int(values["keys"] or 0))
    if side.rows == 0:
        raise _Unreadable(f"{table.name} has no rows with a key (an empty table)")
    return side


def unchecked(why: str) -> JoinCheck:
    return JoinCheck("unchecked", f"Direction not checked: {why}.")


def check_join(
    from_table: TableInfo,
    from_columns: list[str],
    to_table: TableInfo,
    to_columns: list[str],
    execute: Callable[[str], Any],
) -> JoinCheck:
    """Measure both sides of a join and say whether it runs from many to one.

    ``execute`` takes SQL and answers a ``SqlQueryResult``. Compute that cannot be reached (a ``ComputeError``, or the
    worker session's own connection error), or a side that cannot be read, is ``unchecked``, never a refusal.
    """
    from requests import RequestException

    from flowfile_core.flowfile.execution.errors import ComputeError

    try:
        to = _measure(to_table, to_columns, execute)
        fr = _measure(from_table, from_columns, execute)
    except (ComputeError, RequestException):
        return unchecked("compute was not available")
    except _Unreadable as exc:
        return unchecked(str(exc))
    from_cols = "" if [c.lower() for c in fr.columns] == [c.lower() for c in to.columns] else f"{fr.cols} "
    if to.unique and fr.unique:
        both = f"{to.cols} {to.phrase()} and {from_cols}in {fr.table} ({fr.keys} of {fr.rows} rows)"
        return JoinCheck("ok", f"{both}: one to one.")
    if to.unique:
        return JoinCheck("ok", f"{to.cols} {to.phrase()} and {from_cols}{fr.phrase()}.")
    if fr.unique:
        return JoinCheck(
            "reversed",
            f"This join is the wrong way round: {to.cols} {to.phrase()}, while {fr.cols} {fr.phrase()}. The from side "
            f"is the one with many rows: swap the two sides so the join goes from {to.table} to {fr.table}.",
        )
    return JoinCheck(
        "many_to_many",
        f"This join would multiply rows: {fr.cols} {fr.phrase()} and {to.cols} {to.phrase()}. "
        f"The to side needs columns that identify one row of {to.table}.",
    )
