"""Translate Flowfile GA4 filter specs into ``google.analytics.data_v1beta``
``FilterExpression`` objects.

Kept in its own module so it can be unit-tested without importing the full
``read_google_analytics`` path (which hits the Google SDK on import). The
Google SDK is imported lazily inside ``build_filter_expressions`` so that a
worker without the optional GA extra installed can still start.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowfile_worker.external_sources.google_analytics_source.models import GoogleAnalyticsFilter

if TYPE_CHECKING:  # pragma: no cover — just for type hints.
    from google.analytics.data_v1beta.types import FilterExpression


# String operators valid for dimension filters.
_STRING_OPS: set[str] = {"equals", "contains", "begins_with", "ends_with", "regex"}
# Numeric operators valid for metric filters.
_NUMERIC_OPS: set[str] = {
    "equals",
    "less_than",
    "less_equal",
    "greater_than",
    "greater_equal",
}


def _numeric_value(raw: str):
    """Return the GA ``NumericValue`` best matching the literal form.

    Integers use ``int64_value``; anything with a decimal point or scientific
    notation uses ``double_value``.
    """
    from google.analytics.data_v1beta.types import NumericValue

    s = raw.strip()
    if not s:
        raise ValueError("Numeric filter value cannot be empty")
    try:
        if "." not in s and "e" not in s and "E" not in s:
            return NumericValue(int64_value=int(s))
        return NumericValue(double_value=float(s))
    except ValueError as e:
        raise ValueError(f"Cannot parse '{raw}' as a number") from e


def _string_match_type(op: str):
    """Map a string-filter operator to the GA4 ``StringFilter.MatchType`` enum."""
    from google.analytics.data_v1beta.types import Filter

    mapping = {
        "equals": Filter.StringFilter.MatchType.EXACT,
        "contains": Filter.StringFilter.MatchType.CONTAINS,
        "begins_with": Filter.StringFilter.MatchType.BEGINS_WITH,
        "ends_with": Filter.StringFilter.MatchType.ENDS_WITH,
        "regex": Filter.StringFilter.MatchType.FULL_REGEXP,
    }
    if op not in mapping:
        raise ValueError(f"Unsupported string operator: {op}")
    return mapping[op]


def _numeric_operation(op: str):
    """Map a numeric-filter operator to the GA4 ``NumericFilter.Operation`` enum."""
    from google.analytics.data_v1beta.types import Filter

    mapping = {
        "equals": Filter.NumericFilter.Operation.EQUAL,
        "less_than": Filter.NumericFilter.Operation.LESS_THAN,
        "less_equal": Filter.NumericFilter.Operation.LESS_THAN_OR_EQUAL,
        "greater_than": Filter.NumericFilter.Operation.GREATER_THAN,
        "greater_equal": Filter.NumericFilter.Operation.GREATER_THAN_OR_EQUAL,
    }
    if op not in mapping:
        raise ValueError(f"Unsupported numeric operator: {op}")
    return mapping[op]


def _build_single_filter(spec: GoogleAnalyticsFilter, is_metric: bool):
    """Return a ``FilterExpression`` for one ``GoogleAnalyticsFilter`` spec.

    ``not_equals`` / ``not_in_list`` are realised as a ``not_expression`` wrapping
    the positive form, since GA4 has no inverse operators.
    """
    from google.analytics.data_v1beta.types import Filter, FilterExpression

    op = spec.operator
    field = spec.field
    value = spec.value

    # Negations reuse the positive-form builder.
    if op == "not_equals":
        positive = GoogleAnalyticsFilter(
            field=field, operator="equals", value=value, case_sensitive=spec.case_sensitive
        )
        return FilterExpression(not_expression=_build_single_filter(positive, is_metric))
    if op == "not_in_list":
        positive = GoogleAnalyticsFilter(
            field=field, operator="in_list", value=value, case_sensitive=spec.case_sensitive
        )
        return FilterExpression(not_expression=_build_single_filter(positive, is_metric))

    # --- Metric (numeric) filters ------------------------------------------
    if is_metric:
        if op in _NUMERIC_OPS:
            return FilterExpression(
                filter=Filter(
                    field_name=field,
                    numeric_filter=Filter.NumericFilter(
                        operation=_numeric_operation(op),
                        value=_numeric_value(value),
                    ),
                )
            )
        if op == "between":
            parts = [p.strip() for p in value.split(",")]
            if len(parts) != 2:
                raise ValueError("`between` operator requires 'low,high' (two comma-separated numbers)")
            return FilterExpression(
                filter=Filter(
                    field_name=field,
                    between_filter=Filter.BetweenFilter(
                        from_value=_numeric_value(parts[0]),
                        to_value=_numeric_value(parts[1]),
                    ),
                )
            )
        raise ValueError(f"Unsupported metric operator: {op}")

    # --- Dimension (string) filters ----------------------------------------
    if op in _STRING_OPS:
        return FilterExpression(
            filter=Filter(
                field_name=field,
                string_filter=Filter.StringFilter(
                    value=value,
                    match_type=_string_match_type(op),
                    case_sensitive=spec.case_sensitive,
                ),
            )
        )
    if op == "in_list":
        values = [v.strip() for v in value.split(",") if v.strip()]
        if not values:
            raise ValueError("`in_list` operator requires at least one value")
        return FilterExpression(
            filter=Filter(
                field_name=field,
                in_list_filter=Filter.InListFilter(values=values, case_sensitive=spec.case_sensitive),
            )
        )
    raise ValueError(f"Unsupported dimension operator: {op}")


def _and_group(exprs: list[FilterExpression]):
    """Wrap multiple expressions in an ``and_group``; return the single
    expression unchanged when only one is provided."""
    from google.analytics.data_v1beta.types import FilterExpression, FilterExpressionList

    if len(exprs) == 1:
        return exprs[0]
    return FilterExpression(and_group=FilterExpressionList(expressions=exprs))


def build_filter_expressions(
    filters: list[GoogleAnalyticsFilter],
    dimensions: list[str],
    metrics: list[str],
) -> tuple[FilterExpression | None, FilterExpression | None]:
    """Return ``(dimension_filter, metric_filter)`` ready for ``RunReportRequest``.

    Each input filter is routed based on whether its ``field`` is in
    ``dimensions`` (string filter) or ``metrics`` (numeric filter). Filters on
    unknown fields raise ``ValueError`` early so users get a clear error instead
    of a confusing GA API response.
    """
    if not filters:
        return None, None

    dim_set = set(dimensions)
    met_set = set(metrics)

    dim_exprs: list = []
    met_exprs: list = []
    for spec in filters:
        if spec.field in met_set:
            met_exprs.append(_build_single_filter(spec, is_metric=True))
        elif spec.field in dim_set:
            dim_exprs.append(_build_single_filter(spec, is_metric=False))
        else:
            raise ValueError(f"Filter field '{spec.field}' is not in the selected metrics or dimensions")

    dim_filter = _and_group(dim_exprs) if dim_exprs else None
    met_filter = _and_group(met_exprs) if met_exprs else None
    return dim_filter, met_filter
