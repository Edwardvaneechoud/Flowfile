from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import polars as pl
from polars.exceptions import ColumnNotFoundError, PolarsError
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.configs import logger


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
    """Why an expression cannot run. ``kind`` lets callers skip classes they report elsewhere."""

    message: str
    kind: Literal["parse", "missing_column", "type"]


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
