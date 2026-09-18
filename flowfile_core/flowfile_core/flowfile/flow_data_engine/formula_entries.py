"""The ordered entries a formula node evaluates, and the one implementation of applying them.

Both the run-time engine and the edit-time chain validator go through `apply_formula_entries`,
so a message, an error kind and an accumulated schema can only ever agree.

Kept out of `flow_data_engine` so `flow_graph`, `settings_validation` and the editor routes can
reach the mechanism without importing that (very large) module.
"""

import difflib
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

import polars as pl
from polars_expr_transformer import simple_function_to_expr as to_expr
from polars_expr_transformer.exceptions import ExpressionSyntaxError

from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.schemas import transform_schema

FormulaErrorKind = Literal["config", "parse", "missing_column", "type"]

_MISSING_COLUMN_RE = re.compile(r'unable to find column "(.*?)"')
MAX_ISSUE_LENGTH = 200


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

    def __init__(
        self,
        position: int,
        output_name: str,
        detail: str,
        kind: FormulaErrorKind,
        suggestion: "ColumnSuggestion | None" = None,
    ):
        self.position = position
        self.output_name = output_name
        self.detail = detail
        self.kind = kind
        self.suggestion = suggestion
        """The near-match fix a ``missing_column`` error may carry; already worded into ``detail``."""
        super().__init__(f"{entry_label(position, output_name)}: {detail}")


@dataclass(frozen=True)
class ColumnSuggestion:
    """A one-click fix for a ``missing_column`` error: replace ``[from_name]`` with ``[to_name]``."""

    from_name: str
    to_name: str


def first_line(message: str) -> str:
    """The first non-empty line of an exception message, so an error stays a single line."""
    for line in str(message).splitlines():
        if line.strip():
            return line.strip()
    return str(message).strip()


# Date functions whose input must already be temporal; the second set also needs a time component.
_DATE_INPUT_FUNCTIONS = frozenset(
    {
        "year",
        "month",
        "day",
        "week",
        "weekday",
        "dayofweek",
        "quarter",
        "dayofyear",
        "format_date",
        "end_of_month",
        "start_of_month",
        "date_trim",
        "date_truncate",
        "add_days",
        "add_years",
        "add_months",
        "add_weeks",
        "date_diff_days",
    }
)
_DATETIME_INPUT_FUNCTIONS = frozenset(
    {
        "hour",
        "minute",
        "second",
        "add_hours",
        "add_minutes",
        "add_seconds",
        "datetime_diff_seconds",
        "datetime_diff_nanoseconds",
    }
)
_TEMPORAL_FUNCTIONS = _DATE_INPUT_FUNCTIONS | _DATETIME_INPUT_FUNCTIONS

_UNSUPPORTED_TEXT_DTYPE = re.compile(r"`\w+` operation not supported for dtype `str`")
_STRING_LITERAL = re.compile(r"\"[^\"]*\"|'[^']*'")
_COLUMN_REF = re.compile(r"\[([^\[\]]+)\]")
_CALL = re.compile(r"\b([A-Za-z_]\w*)\s*\(")


def text_dtype_hint(schema: Mapping[str, pl.DataType], func_string: str, message: str) -> str | None:
    """A message naming the text column and the cast that fixes it, or None to keep polars' wording.

    Polars reports its own method name and dtype ("`to_string` operation not supported for dtype
    `str`"), which names neither the user's function nor their column. This rewrites only the one
    shape it can read with certainty — a date function applied to exactly one String column —
    and returns None for anything less certain, so the caller falls back to polars' own line.
    """
    if not _UNSUPPORTED_TEXT_DTYPE.search(message):
        return None
    bare = _STRING_LITERAL.sub('""', func_string)
    columns = [c for c in dict.fromkeys(_COLUMN_REF.findall(bare)) if schema.get(c) == pl.String]
    calls = list(dict.fromkeys(c for c in _CALL.findall(bare) if c in _TEMPORAL_FUNCTIONS))
    if len(columns) != 1 or not calls:
        return None
    column = columns[0]
    if any(c in _DATETIME_INPUT_FUNCTIONS for c in calls):
        wanted, wrap = "a Datetime", f'to_datetime([{column}], "%Y-%m-%d %H:%M:%S")'
    else:
        wanted, wrap = "a Date or Datetime", f'to_date([{column}], "%Y-%m-%d")'
    subject = calls[0] if len(calls) == 1 else "this date function"
    hint = f"{subject} needs {wanted} column; '{column}' is text — wrap it in {wrap}"
    return hint[:MAX_ISSUE_LENGTH]


def missing_column_name(message: str) -> str | None:
    """The column Polars could not find, when its message names one."""
    match = _MISSING_COLUMN_RE.search(str(message))
    return match.group(1) if match else None


def missing_column_detail(message: str, suggestion: ColumnSuggestion | None = None) -> str:
    """Name the column Polars could not find, falling back to its own first line."""
    name = missing_column_name(message)
    if name is None:
        return first_line(message)
    hint = f", did you mean '{suggestion.to_name}'?" if suggestion else ""
    return f"column '{name}' not found{hint}"


def parse_detail(expression: str, exc: Exception) -> str:
    """The one-line reason an expression did not parse.

    The parser's own ``ExpressionSyntaxError`` messages are worth showing as they are. Anything
    else is an internal failure whose text must not reach the user: the tokenizer raises a bare
    ``IndexError`` ("string index out of range") on an unclosed ``[``, and a dangling operator
    surfaces as a ``TypeError`` from Polars' operator overloads.
    """
    if isinstance(exc, ExpressionSyntaxError):
        return first_line(str(exc))
    if expression.count("[") > expression.count("]"):
        return "column reference is not closed, add ]"
    return "expression could not be parsed"


def suggest_column(name: str, columns: Iterable[str]) -> str | None:
    """The closest existing column to a misspelled reference, or None when nothing is close."""
    matches = difflib.get_close_matches(name, list(columns), n=1, cutoff=0.6)
    return matches[0] if matches else None


def build_expression(entry: FormulaEntry) -> pl.Expr:
    """Parse the entry's expression, attributing a failure to its row.

    Raises:
        FormulaEntryError: With ``kind="parse"``.
    """
    try:
        return to_expr(entry.expression)
    except Exception as e:
        raise FormulaEntryError(entry.position, entry.output_name, parse_detail(entry.expression, e), "parse") from e


def classify_polars_error(
    entry: FormulaEntry,
    exc: pl.exceptions.PolarsError,
    columns: Iterable[str] = (),
    schema: Mapping[str, pl.DataType] | None = None,
) -> FormulaEntryError:
    """The typed error for a Polars failure raised while resolving or evaluating *entry*.

    Shared by the schema-only chain walk and the sampled preview so both describe the same
    failure with the same words. *columns* is what the entry could have referenced; a missing
    column close to one of them gets a "did you mean" hint and a structured suggestion. With
    *schema* a type error can also name the text column and the cast that fixes it.
    """
    if isinstance(exc, pl.exceptions.ColumnNotFoundError):
        message = str(exc)
        name = missing_column_name(message)
        match = suggest_column(name, columns) if name is not None else None
        suggestion = None if match is None else ColumnSuggestion(name, match)
        return FormulaEntryError(
            entry.position, entry.output_name, missing_column_detail(message, suggestion), "missing_column", suggestion
        )
    detail = first_line(str(exc))
    if schema is not None:
        detail = text_dtype_hint(schema, entry.expression, detail) or detail
    return FormulaEntryError(entry.position, entry.output_name, detail, "type")


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
        expr = build_expression(entry)
        if entry.output_data_type is not None:
            expr = expr.cast(entry.output_data_type)
        before = lf
        lf = lf.with_columns(expr.alias(entry.output_name))
        try:
            lf.collect_schema()
        except pl.exceptions.PolarsError as e:
            schema_before = before.collect_schema()
            raise classify_polars_error(entry, e, schema_before.names(), dict(schema_before)) from e
    return lf
