from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Literal

import polars as pl
from polars.exceptions import ColumnNotFoundError, PolarsError
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.formula_entries import (
    MAX_ISSUE_LENGTH,
    ColumnSuggestion,
    FormulaEntryError,
    apply_formula_entries,
    classify_polars_error,
    formula_entry,
    parse_detail,
    text_dtype_hint,
)
from flowfile_core.schemas import transform_schema


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
    return get_realtime_expr_results(df, simple_function_to_expr(func_string), sample)


def get_realtime_expr_results(df: pl.DataFrame | pl.LazyFrame, expr: pl.Expr, sample: int = 1) -> RealTimeResult:
    """The first result of an already-parsed expression, so a caller can classify parse failures itself."""
    if isinstance(df, pl.LazyFrame):
        logger.warning(
            "Performance in this case can be " "improved by using polars.DataFrame to ensure it returns instantly"
        )
        df = df.head(sample).collect()
    result = df.head(1).select(expr)
    return RealTimeResult(result, result.dtypes[0])


@dataclass(frozen=True)
class ExpressionIssue:
    """Why an expression cannot run. ``kind`` lets callers skip classes they report elsewhere.

    ``config`` and ``duplicate`` only arise in a formula chain: a blank output name, and an
    output name a later entry overwrites. ``duplicate`` is a warning — the run still succeeds.
    ``suggestion`` accompanies a ``missing_column`` whose name is close to an existing column.
    """

    message: str
    kind: Literal["parse", "missing_column", "type", "config", "duplicate"]
    suggestion: ColumnSuggestion | None = None


_MAX_ISSUE_LENGTH = MAX_ISSUE_LENGTH
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

    Data-free: the expression runs against a zero-row LazyFrame built from *schema* alone — no
    source, no rows — so the collect type-checks every namespace (``.dt``, ``.str``, ``.list``)
    without reading data. ``collect_schema`` alone answers those without type-checking them.
    Uses the same parser as execution (``simple_function_to_expr``), and applies it the same way
    execution does — ``filter`` for a predicate, ``with_columns`` otherwise.
    Returns None for a blank expression and for anything it cannot classify: silence over guessing.
    """
    if not func_string or not func_string.strip():
        return None
    try:
        expr = simple_function_to_expr(func_string)
    except Exception as exc:
        return ExpressionIssue(parse_detail(func_string, exc)[:_MAX_ISSUE_LENGTH], "parse")
    try:
        lf = pl.LazyFrame(schema=schema)
        frame = lf.filter(expr) if as_predicate else lf.with_columns(expr.alias(_PROBE_ALIAS))
        frame.collect()
    except ColumnNotFoundError as exc:
        return ExpressionIssue(_first_line(exc), "missing_column")
    except PolarsError as exc:
        message = _first_line(exc)
        return ExpressionIssue(text_dtype_hint(schema, func_string, message) or message, "type")
    except Exception:
        logger.debug("Expression check skipped for %r", func_string, exc_info=True)
        return None
    return None


@dataclass(frozen=True)
class ChainResult:
    """Per-entry verdicts and the schema each entry leaves behind.

    ``issues`` and ``schemas`` are positional: slot *i* belongs to ``entries[i]``. ``schemas[i]``
    is the accumulated schema AFTER entry *i*, so ``schemas[-1]`` is the node's output schema.
    """

    base_schema: dict[str, pl.DataType]
    issues: list[ExpressionIssue | None]
    schemas: list[dict[str, pl.DataType]]


def check_expression_chain(
    schema: Mapping[str, pl.DataType],
    entries: Sequence[transform_schema.FunctionInput],
) -> ChainResult:
    """Validate formula entries by running the engine's own step against an empty frame.

    Entry N is resolved against the base schema plus the outputs of entries 1..N-1, so a
    reference to an earlier entry's column is correct and a reference to a later one is not.
    Data-free: every step resolves AND collects against an empty LazyFrame — the collect is
    what type-checks the ``.dt``/``.str``/``.list`` namespaces the engine's schema resolution
    accepts unexamined, exactly as ``check_expression`` does for a single expression.

    A failing entry still contributes its declared output column (String under Auto) so the
    entries below it are validated against the schema the user is building, not a truncated
    one — one broken row must not light up every row under it.

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
    for position, fn in enumerate(entries, start=1):
        output_name = (fn.field.name or "").strip()
        if not (fn.function or "").strip():
            issues.append(None)
            schemas.append(dict(current))
            continue
        if not output_name:
            issues.append(ExpressionIssue("output column name is empty", "config"))
            schemas.append(dict(current))
            continue
        entry = replace(formula_entry(position, fn), output_name=output_name)
        declared = entry.output_data_type if entry.output_data_type is not None else pl.String
        fallback = {**current, output_name: declared}
        issue: ExpressionIssue | None = None
        try:
            stepped = apply_formula_entries(pl.LazyFrame(schema=current), [entry])
            # The engine step resolves the schema only; the collect also type-checks the
            # namespaces (.dt, .str, .list) that schema resolution accepts unexamined.
            stepped.collect()
            current = dict(stepped.collect_schema())
        except FormulaEntryError as exc:
            issue = ExpressionIssue(exc.detail[:_MAX_ISSUE_LENGTH], exc.kind, exc.suggestion)
            current = fallback
        except PolarsError as exc:
            # The collect's own failure, classified by the same words the engine step uses.
            failure = classify_polars_error(entry, exc, list(current), current)
            issue = ExpressionIssue(failure.detail[:_MAX_ISSUE_LENGTH], failure.kind, failure.suggestion)
            current = fallback
        except Exception:
            logger.debug("Formula chain entry check skipped for %r", fn.function, exc_info=True)
            current = fallback
        if issue is None and output_name in produced:
            issue = ExpressionIssue(
                f"output column '{output_name}' is also produced by formula {produced[output_name]}",
                "duplicate",
            )
        produced[output_name] = position
        issues.append(issue)
        schemas.append(dict(current))
    return ChainResult(base, issues, schemas)
