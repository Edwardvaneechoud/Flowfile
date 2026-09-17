from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, NamedTuple

import polars as pl
from polars.exceptions import ColumnNotFoundError, PolarsError
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_data_engine.formula_entries import missing_column_detail


@dataclass
class RealTimeResult:
    result_df: pl.DataFrame
    data_type: pl.DataType
    readable_result: str
    success: bool | None = None

    def __init__(self, result_value: pl.DataFrame, data_type: pl.DataType):
        self.result_df = result_value
        self.data_type = data_type
        if len(result_value) > 0:
            self.readable_result = str(result_value.item(0, 0))
            self.success = True
        else:
            self.readable_result = ""
            self.success = None

    def is_filterable_result(self):
        """
        This function is used to check if the result of the function can be used as a filter
        """
        if self.data_type == pl.Boolean:
            return True
        else:
            try:
                self.result_df.select(pl.col(self.result_df.columns[0]).cast(pl.Boolean))
                return True
            except Exception:
                return False


def get_realtime_func_results(df: pl.DataFrame | pl.LazyFrame, func_string: str, sample: int = 1) -> RealTimeResult:
    """
    This function is used to get the first result of a function applied to a dataframe.
    This is useful for debugging the users write
    example:
    df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [1, 2, 3], 'names': ['ham', 'spam', 'eggs']})
    print(get_first_result_of_function('year(today())', df))
    """
    if isinstance(df, pl.LazyFrame):
        logger.warning(
            "Performance in this case can be " "improved by using polars.DataFrame to ensure it returns instantly"
        )
        df = df.head(sample).collect()
    result = df.head(1).select(simple_function_to_expr(func_string))
    return RealTimeResult(result, result.dtypes[0])


@dataclass(frozen=True)
class ExpressionIssue:
    """Why an expression cannot run. ``kind`` lets callers skip classes they report elsewhere.

    ``config`` and ``duplicate`` only arise in a formula chain: a blank output name, and an
    output name a later entry overwrites. ``duplicate`` is a warning — the run still succeeds.
    """

    message: str
    kind: Literal["parse", "missing_column", "type", "config", "duplicate"]


_MAX_ISSUE_LENGTH = 200
_PROBE_ALIAS = "__ff_probe__"


def _first_line(exc: Exception) -> str:
    # Polars appends a multi-line "Resolved plan until failure:" dump that must not reach a tooltip.
    lines = str(exc).splitlines()
    message = lines[0].strip() if lines else ""
    return (message or type(exc).__name__)[:_MAX_ISSUE_LENGTH]


def check_expression(
    schema: Mapping[str, pl.DataType],
    func_string: str,
    *,
    as_predicate: bool = False,
) -> ExpressionIssue | None:
    """Why *func_string* cannot run against *schema*, or None when it resolves.

    Data-free: the expression is resolved against an empty LazyFrame, so no row is ever read
    or collected. Uses the same parser as execution (``simple_function_to_expr``), and applies
    it the same way execution does — ``filter`` for a predicate, ``with_columns`` otherwise.
    Returns None for a blank expression and for anything it cannot classify: silence over guessing.
    """
    if not func_string or not func_string.strip():
        return None
    try:
        expr = simple_function_to_expr(func_string)
    except Exception as exc:
        return ExpressionIssue(_first_line(exc), "parse")
    try:
        lf = pl.LazyFrame(schema=schema)
        frame = lf.filter(expr) if as_predicate else lf.with_columns(expr.alias(_PROBE_ALIAS))
        frame.collect_schema()
    except ColumnNotFoundError as exc:
        return ExpressionIssue(_first_line(exc), "missing_column")
    except PolarsError as exc:
        return ExpressionIssue(_first_line(exc), "type")
    except Exception:
        logger.debug("Expression check skipped for %r", func_string, exc_info=True)
        return None
    return None


AUTO_DATA_TYPE = "Auto"


class ChainEntry(NamedTuple):
    """One formula-node row as the editor holds it, before any schema is known."""

    output_name: str
    data_type: str | None
    expression: str


@dataclass(frozen=True)
class ChainResult:
    """Per-entry verdicts and the schema each entry leaves behind.

    ``issues`` and ``schemas`` are positional: slot *i* belongs to ``entries[i]``. ``schemas[i]``
    is the accumulated schema AFTER entry *i*, so ``schemas[-1]`` is the node's output schema.
    """

    base_schema: dict[str, pl.DataType]
    issues: list[ExpressionIssue | None]
    schemas: list[dict[str, pl.DataType]]


def entry_label(position: int, output_name: str) -> str:
    """The row prefix a run-time ``FormulaEntryError`` carries, so both messages read the same."""
    return f'Formula {position} ("{output_name}")' if output_name else f"Formula {position}"


def _declared_type(data_type: str | None) -> pl.DataType | None:
    """The entry's cast target, or None when it is Auto/blank (no cast, like the engine)."""
    if data_type in (None, "", AUTO_DATA_TYPE):
        return None
    try:
        return cast_str_to_polars_type(data_type)
    except Exception:
        return None


def _apply_chain_entry(
    current: dict[str, pl.DataType],
    output_name: str,
    declared: pl.DataType | None,
    expression: str,
) -> tuple[ExpressionIssue | None, dict[str, pl.DataType]]:
    """Resolve one entry against *current*, returning its issue and the schema that follows it.

    A failing entry still contributes its declared output column (String under Auto) so the
    entries below it are validated against the schema the user is building, not a truncated
    one — one broken row must not light up every row under it.
    """
    fallback = {**current, output_name: declared if declared is not None else pl.String}
    try:
        expr = simple_function_to_expr(expression)
    except Exception as exc:
        return ExpressionIssue(_first_line(exc), "parse"), fallback
    if declared is not None:
        expr = expr.cast(declared)
    try:
        resolved = dict(pl.LazyFrame(schema=current).with_columns(expr.alias(output_name)).collect_schema())
    except ColumnNotFoundError as exc:
        return ExpressionIssue(missing_column_detail(str(exc))[:_MAX_ISSUE_LENGTH], "missing_column"), fallback
    except PolarsError as exc:
        return ExpressionIssue(_first_line(exc), "type"), fallback
    except Exception:
        logger.debug("Formula chain entry check skipped for %r", expression, exc_info=True)
        return None, fallback
    return None, resolved


def check_expression_chain(
    schema: Mapping[str, pl.DataType],
    entries: Sequence[ChainEntry],
) -> ChainResult:
    """Validate formula entries the way ``FlowDataEngine.apply_sql_formulas`` evaluates them.

    Entry N is resolved against the base schema plus the outputs of entries 1..N-1, so a
    reference to an earlier entry's column is correct and a reference to a later one is not.
    Data-free: every step resolves against an empty LazyFrame.

    Messages are bare details (no row prefix) — callers add ``entry_label`` where the row is
    not already obvious. Blank expressions yield no issue and contribute no column; a blank
    output name with a non-blank expression is a ``config`` issue; an unclassifiable failure
    yields no issue at all, matching ``check_expression``'s silence-over-guessing rule.
    """
    base = dict(schema)
    current = dict(base)
    issues: list[ExpressionIssue | None] = []
    schemas: list[dict[str, pl.DataType]] = []
    produced: dict[str, int] = {}
    for position, entry in enumerate(entries, start=1):
        output_name = (entry.output_name or "").strip()
        expression = entry.expression or ""
        if not expression.strip():
            issues.append(None)
            schemas.append(dict(current))
            continue
        if not output_name:
            issues.append(ExpressionIssue("output column name is empty", "config"))
            schemas.append(dict(current))
            continue
        issue, current = _apply_chain_entry(current, output_name, _declared_type(entry.data_type), expression)
        if issue is None and output_name in produced:
            issue = ExpressionIssue(
                f"output column '{output_name}' is also produced by formula {produced[output_name]}",
                "duplicate",
            )
        produced[output_name] = position
        issues.append(issue)
        schemas.append(dict(current))
    return ChainResult(base, issues, schemas)


def apply_chain_prefix(
    df: pl.DataFrame,
    entries: Sequence[ChainEntry],
) -> tuple[pl.DataFrame, int | None, str]:
    """Evaluate *entries* in order on a preview frame, stopping at the first that fails.

    Returns the frame reached, the 1-based position of the failing entry (None when all
    succeeded) and its detail message. Blank expressions are skipped, as at run time.
    """
    frame = df
    for position, entry in enumerate(entries, start=1):
        expression = entry.expression or ""
        if not expression.strip():
            continue
        output_name = (entry.output_name or "").strip()
        if not output_name:
            return frame, position, "output column name is empty"
        declared = _declared_type(entry.data_type)
        try:
            expr = simple_function_to_expr(expression)
            if declared is not None:
                expr = expr.cast(declared)
            frame = frame.with_columns(expr.alias(output_name))
        except Exception as exc:
            return frame, position, _first_line(exc)
    return frame, None, ""
