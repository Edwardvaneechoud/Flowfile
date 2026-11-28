"""Data rules of the semantic model: a column's stored ``checks`` (glosslayer's name), checked and run as Polars.

A rule is one of ``KINDS``. Six are glosslayer's own checks; ``formula`` is Flowfile's, a formula line that is true
for a row that passes, registered with glosslayer here so glosslayer knows nothing about formulas. Rules are planned
as one lazy aggregate per table (``glosslayer.checks.plan``) and that plan is sent to the compute backend through a
bounded read, so core reads no rows; a proposed list is first run against an empty frame of the table's stored
columns, which reads no data and shows a rule that cannot work on the column's type.

``run`` takes tables and a ledger view, no request, so a caller other than a route can use it.

Every ``glosslayer`` import is function-local, so core imports without it.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, ClassVar

FORMULA = "formula"
KINDS = ("not_null", "unique", "matches", "one_of", "between", "length", FORMULA)
ATTRIBUTE = "checks"
UNCHECKED = "Not checked"

logger = logging.getLogger(__name__)

_LISTED_VALUES = re.compile(r"( values): \[.*$")

_formula_rule: Any = None


def register_formula_rule() -> Any:
    """Make the ``formula`` rule known to glosslayer (once per process) and return its class."""
    global _formula_rule
    if _formula_rule is not None:
        return _formula_rule
    from glosslayer import checks

    @checks.register
    @dataclass(frozen=True)
    class Formula(checks.Check):
        """Passes where the Flowfile formula ``expression`` is true; a null result fails."""

        expression: str = ""
        kind: ClassVar[str] = FORMULA

        def verify(self) -> None:
            if not isinstance(self.expression, str) or not self.expression.strip():
                raise ValueError("formula needs an expression")

        def describe(self) -> str:
            return self.expression

        def polars(self, col: str) -> Any:
            from polars_expr_transformer import simple_function_to_expr

            return simple_function_to_expr(self.expression)

    _formula_rule = Formula
    return Formula


def table_schema(table: Any) -> dict[str, Any]:
    """The Polars schema of a table from the columns the catalog stores for it; nothing is read.

    A column whose stored type names no Polars type (``untyped_columns``) is given as text.
    """
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type

    return {c.name: cast_str_to_polars_type(c.dtype or "") for c in table.columns}


def untyped_columns(table: Any) -> list[str]:
    """The columns whose stored type names no Polars type, so nothing can be said about a rule on them.

    The catalog stores Polars' own ``str(dtype)`` for most tables; a table a kernel wrote holds Delta's names
    (``long``, ``boolean``), which are not all Polars'.
    """
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import known_polars_type

    return [c.name for c in table.columns if known_polars_type(c.dtype or "") is None]


class Unchecked(Exception):
    """A proposed rules list could not be checked, for a reason that is not about the rules."""


class Refused(ValueError):
    """A proposed rules list the check proved wrong; its message says which rule and why."""


def check_proposed(table: Any, column: str, value: Any) -> str | None:
    """Refuse a proposed rules list for ``column`` that Flowfile cannot run; ``None`` when it was checked and holds.

    Only what the check proves wrong is refused. When the check itself cannot be made (any other error, a
    ``ValueError`` that is not about the rules included), the list is not refused: a note starting with
    ``UNCHECKED`` is answered for the stored fact's rationale, and the rules are judged when they run.

    Raises:
        Refused: A rule is of a kind Flowfile does not run, its parameters are not valid, it cannot be evaluated on
            the column's type, or a formula does not answer true or false. A ``ValueError``, so a 422.
    """
    try:
        _check(table, column, value)
    except Refused:
        raise
    except Unchecked as exc:
        return f"{UNCHECKED}: {exc}."
    except Exception as exc:  # noqa: BLE001
        logger.warning("Semantic model: data rules of %s/%s stored unchecked", table.key, column, exc_info=True)
        return f"{UNCHECKED}: the rules could not be checked against the column ({type(exc).__name__})."
    return None


def _check(table: Any, column: str, value: Any) -> None:
    import polars as pl
    from glosslayer import checks

    register_formula_rule()
    if not isinstance(value, list) or not all(isinstance(rule, dict) for rule in value):
        raise Refused("Data rules must be a list of rules, each with its 'check'")
    try:
        rules = checks.validate(value)
    except ValueError as exc:
        raise Refused(str(exc)) from None
    for rule in rules:
        if rule["check"] not in KINDS:
            raise Refused(f"{rule['check']!r} is not a data rule; use one of: {', '.join(KINDS)}")
    untyped = untyped_columns(table)
    if untyped and (column in untyped or any(rule["check"] == FORMULA for rule in rules)):
        raise Unchecked(f"the catalog stores no type Flowfile knows for {', '.join(untyped)}")
    schema = table_schema(table)
    frame = pl.LazyFrame(schema=schema)
    for rule in rules:
        if rule["check"] == FORMULA:
            _require_true_or_false(frame, rule)
    plan = checks.plan({column: [(rule, None) for rule in rules]}, table.key, frame, schema=schema)
    # collected here: the frame is the stored columns with no rows, so no data is read
    for result in checks.run_plan(plan, lambda planned: planned.collect()):
        if result.error:
            raise Refused(f"The {result.check['check']} rule cannot run on column {column!r}: {result.error}")


def _require_true_or_false(frame: Any, rule: dict[str, Any]) -> None:
    """Refuse a formula that answers something other than true or false; one that does not resolve is left to the
    dry run, which names what is wrong with it."""
    import polars as pl

    try:
        answer = frame.select(register_formula_rule()(**{"expression": rule["expression"]}).polars("").alias("answer"))
        dtype = answer.collect_schema()["answer"]
    except Exception:  # noqa: BLE001
        logger.debug("Semantic model: formula rule %r does not resolve; left to the dry run", rule["expression"])
        return
    if dtype != pl.Boolean:
        raise Refused(f"A formula rule must answer true or false; {rule['expression']!r} answers {dtype}")


@dataclass
class TableRun:
    """One table's part of a run: its results (glosslayer ``CheckResult``), or why it was skipped."""

    table: Any
    results: list[Any] = field(default_factory=list)
    skipped: str | None = None


def effective_rules(view: Any, key: str) -> dict[str, list[tuple[dict[str, Any], str | None]]]:
    """``{column: [(rule, origin), ...]}`` of a table's accepted rules; empty for a table with none."""
    from glosslayer import checks

    try:
        return checks.effective_checks(view, key)
    except KeyError:
        return {}


def _runs(rule: dict[str, Any]) -> bool:
    return rule["check"] in KINDS


def _safe_reason(reason: str) -> str:
    """Why a rule could not run, without what an engine quotes of the data and the server: the values a failed
    conversion lists are left out and a path is cut to its last part."""
    from flowfile_core.catalog.services.virtual_tables import _scrub_paths_not_urls

    return _scrub_paths_not_urls(_LISTED_VALUES.sub(r"\1", reason))


def run(view: Any, catalog: Any, tables: Iterable[Any], user_id: int | None) -> list[TableRun]:
    """Run the accepted rules of each of ``tables`` on the compute backend, one plan per table.

    A table with no rules is left out; one that holds no stored rows (a virtual table), or that is on this server's
    disk while the backend does not read it, is answered as skipped. A stored rule of a kind Flowfile does not run
    is answered with an error and never evaluated.

    Raises:
        ComputeError: The backend could not be reached, or is on another version.
    """
    from glosslayer import checks
    from glosslayer.dialect import EngineUnavailable
    from glosslayer.ids import SubjectId

    from flowfile_core.flowfile.execution.context import current_context
    from flowfile_core.flowfile.execution.errors import ComputeUnavailable, VersionMismatch
    from flowfile_core.flowfile.flow_data_engine import bounded

    register_formula_rule()
    backend = current_context().backend

    def collect(planned: Any) -> dict[str, Any]:
        try:
            return bounded.single_row(backend, planned, "The table answered no row")
        except (ComputeUnavailable, VersionMismatch) as exc:
            raise EngineUnavailable(str(exc)) from exc

    runs: list[TableRun] = []
    for table in tables:
        rules = effective_rules(view, table.key)
        if not rules:
            continue
        try:
            frame = catalog.stored_table_plan(table.uid, user_id, backend)
        except ValueError as exc:
            runs.append(TableRun(table, skipped=str(exc)))
            continue
        runnable = {col: [(rule, origin) for rule, origin in found if _runs(rule)] for col, found in rules.items()}
        plan = checks.plan(runnable, table.key, frame, schema=[c.name for c in table.columns])
        try:
            results = checks.run_plan(plan, collect)
        except EngineUnavailable as exc:
            raise exc.__cause__ or exc from None
        for result in results:
            if result.error:
                result.error = _safe_reason(result.error)
        total = results[0].total_rows if results else 0
        for col, found in rules.items():
            subject = str(SubjectId.for_field(table.key, col))
            results += [
                checks.CheckResult(
                    subject, rule, False, 0, total, origin, error=f"{rule['check']!r} is not a data rule"
                )
                for rule, origin in found
                if not _runs(rule)
            ]
        runs.append(TableRun(table, results))
    return runs
