"""The ordered entries a formula node evaluates, and the one implementation of applying them.

Both the run-time engine and the edit-time chain validator go through `apply_formula_entries`,
so a message, an error kind and an accumulated schema can only ever agree.

Kept out of `flow_data_engine` so `flow_graph`, `settings_validation` and the editor routes can
reach the mechanism without importing that (very large) module.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import polars as pl
from polars_expr_transformer import simple_function_to_expr as to_expr

from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.schemas import transform_schema

FormulaErrorKind = Literal["config", "parse", "missing_column", "type"]

_MISSING_COLUMN_RE = re.compile(r'unable to find column "(.*?)"')


@dataclass(frozen=True)
class FormulaEntry:
    """One formula-node row: where it sits, what column it writes, and how."""

    position: int
    """1-based row number as the user sees it; blank rows are counted but never evaluated."""

    output_name: str
    expression: str
    output_data_type: pl.DataType | None = None


def entry_label(position: int, output_name: str) -> str:
    """The row prefix every formula message carries, at edit time and at run time alike."""
    return f'Formula {position} ("{output_name}")' if output_name else f"Formula {position}"


class FormulaEntryError(Exception):
    """A formula entry that could not be built, attributed to its row and output column.

    `kind` distinguishes the causes a static check can tell apart: `"config"` (blank output
    name), `"parse"` (the expression does not parse), `"missing_column"` (it references a
    column the frame does not have at that point in the chain) and `"type"` (any other Polars
    error raised while resolving the step's schema).
    """

    def __init__(self, position: int, output_name: str, detail: str, kind: FormulaErrorKind):
        self.position = position
        self.output_name = output_name
        self.detail = detail
        self.kind = kind
        super().__init__(f"{entry_label(position, output_name)}: {detail}")


def first_line(message: str) -> str:
    """The first non-empty line of an exception message, so an error stays a single line."""
    for line in str(message).splitlines():
        if line.strip():
            return line.strip()
    return str(message).strip()


def missing_column_detail(message: str) -> str:
    """Name the column Polars could not find, falling back to its own first line."""
    match = _MISSING_COLUMN_RE.search(str(message))
    return f"column '{match.group(1)}' not found" if match else first_line(message)


def formula_entry(position: int, fn: transform_schema.FunctionInput) -> FormulaEntry:
    """The entry a settings row describes, with its declared cast target resolved.

    `Auto` (and an unset type) means no cast: the expression keeps whatever type it produces.
    """
    declared = fn.field.data_type
    cast_target = None if declared in (None, transform_schema.AUTO_DATA_TYPE) else cast_str_to_polars_type(declared)
    return FormulaEntry(position, fn.field.name, fn.function, cast_target)


def apply_formula_entries(lf: pl.LazyFrame, entries: Sequence[FormulaEntry]) -> pl.LazyFrame:
    """Applies entries SEQUENTIALLY, one `with_columns` step per entry.

    Entry N sees the base columns plus every column entries 1..N-1 produced — exactly
    equivalent to N chained single-formula nodes.

    Each step's schema is resolved eagerly (`collect_schema`, schema-only — no data) so a parse
    error, an unknown column or a type error is attributed to the row that caused it. That is
    also why this takes a LazyFrame: an eager frame would raise inside `with_columns` first,
    before there is anything to attribute. Data-dependent strict-cast failures cannot be
    attributed at all — they surface only when the frame is collected (in the worker), carrying
    Polars' own "conversion from ... failed" text and no entry position.

    Args:
        lf: The frame the first entry runs against.
        entries: The entries to apply, in evaluation order. Entries with a blank expression
            must already be filtered out by the caller.

    Returns:
        The chained frame; *lf* unchanged when *entries* is empty.

    Raises:
        FormulaEntryError: Naming the failing entry's position and output column.
    """
    for entry in entries:
        if not entry.output_name.strip():
            raise FormulaEntryError(entry.position, "", "output column name is empty", "config")
        try:
            expr = to_expr(entry.expression)
        except Exception as e:
            raise FormulaEntryError(entry.position, entry.output_name, first_line(str(e)), "parse") from e
        if entry.output_data_type is not None:
            expr = expr.cast(entry.output_data_type)
        lf = lf.with_columns(expr.alias(entry.output_name))
        try:
            lf.collect_schema()
        except pl.exceptions.ColumnNotFoundError as e:
            raise FormulaEntryError(
                entry.position, entry.output_name, missing_column_detail(str(e)), "missing_column"
            ) from e
        except pl.exceptions.PolarsError as e:
            raise FormulaEntryError(entry.position, entry.output_name, first_line(str(e)), "type") from e
    return lf
