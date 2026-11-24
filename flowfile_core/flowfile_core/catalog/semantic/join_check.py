"""Check a join's direction against the data: the *to* side must hold one row per key.

A join is stored from the many side to the one side, and the metric compiler trusts that: it LEFT JOINs the *to*
table, so a *to* side that repeats its key multiplies every aggregate over the base table. ``check_join`` measures
both sides with one aggregate statement each through the catalog SQL path (the worker computes, the caller's table
access applies, core never collects) and answers a verdict with a sentence a person can act on.

Rows whose key holds a null are ignored, a key of several columns counts distinct combinations.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from flowfile_core.catalog.semantic.compiler import quote_ident
from flowfile_core.catalog.semantic.snapshot import TableInfo

Verdict = Literal["ok", "reversed", "many_to_many", "unchecked"]


@dataclass
class SideCount:
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

    def unique_phrase(self) -> str:
        return f"is unique in {self.table} ({self.keys} of {self.rows} rows)"

    def repeat_phrase(self) -> str:
        return f"repeats in {self.table} ({self.keys} distinct in {self.rows} rows)"


@dataclass
class JoinCheck:
    verdict: Verdict
    message: str


def side_sql(table_key: str, columns: list[str]) -> str:
    """Rows with a non-null key (``rows``) and distinct keys (``keys``) of one side, as one statement."""
    cols = ", ".join(quote_ident(c) for c in columns)
    not_null = " AND ".join(f"{quote_ident(c)} IS NOT NULL" for c in columns)
    return (
        f'SELECT COUNT(*) AS "keys", COALESCE(SUM("n"), 0) AS "rows" FROM '
        f'(SELECT {cols}, COUNT(*) AS "n" FROM {quote_ident(table_key)} WHERE {not_null} GROUP BY {cols}) AS "g"'
    )


class _Unreadable(Exception):
    pass


def _measure(table: TableInfo, columns: list[str], execute: Callable[[str], Any]) -> SideCount:
    result = execute(side_sql(table.key, columns))
    if result.error or not result.rows:
        raise _Unreadable(f"{table.name} could not be read ({result.error or 'no answer'})")
    values = dict(zip(result.columns, result.rows[0], strict=False))
    return SideCount(table.name, list(columns), int(values.get("rows") or 0), int(values.get("keys") or 0))


def _same_names(a: SideCount, b: SideCount) -> bool:
    return [c.lower() for c in a.columns] == [c.lower() for c in b.columns]


def check_join(
    from_table: TableInfo,
    from_columns: list[str],
    to_table: TableInfo,
    to_columns: list[str],
    execute: Callable[[str], Any],
) -> JoinCheck:
    """Measure both sides of a join and say whether it runs from many to one.

    ``execute`` takes SQL and answers a ``SqlQueryResult``. Compute that cannot be reached (a ``ComputeError``, or the
    worker session's own connection error once the handshake gate has passed), or a side that cannot be read, is
    ``unchecked``, never a refusal.
    """
    from requests import RequestException

    from flowfile_core.flowfile.execution.errors import ComputeError

    try:
        to = _measure(to_table, to_columns, execute)
        if to.rows == 0:
            return JoinCheck("unchecked", f"Direction not checked: {to.table} has no rows with a key (an empty table).")
        fr = _measure(from_table, from_columns, execute)
    except (ComputeError, RequestException):
        return JoinCheck("unchecked", "Direction not checked: compute was not available.")
    except _Unreadable as exc:
        return JoinCheck("unchecked", f"Direction not checked: {exc}.")
    if fr.rows == 0:
        return JoinCheck("unchecked", f"Direction not checked: {fr.table} has no rows with a key (an empty table).")
    from_cols = "" if _same_names(fr, to) else f"{fr.cols} "
    if to.unique and fr.unique:
        both = f"{to.cols} {to.unique_phrase()} and {from_cols}in {fr.table} ({fr.keys} of {fr.rows} rows)"
        return JoinCheck("ok", f"{both}: one to one.")
    if to.unique:
        return JoinCheck("ok", f"{to.cols} {to.unique_phrase()} and {from_cols}{fr.repeat_phrase()}.")
    if fr.unique:
        return JoinCheck(
            "reversed",
            f"This join is the wrong way round: {to.cols} {to.repeat_phrase()}, while {fr.cols} "
            f"{fr.unique_phrase()}. The from side is the one with many rows: swap the two sides so the join goes "
            f"from {to.table} to {fr.table}.",
        )
    return JoinCheck(
        "many_to_many",
        f"This join would multiply rows: {fr.cols} {fr.repeat_phrase()} and {to.cols} {to.repeat_phrase()}. "
        f"The to side needs columns that identify one row of {to.table}.",
    )


__all__ = ["JoinCheck", "SideCount", "Verdict", "check_join", "side_sql"]
