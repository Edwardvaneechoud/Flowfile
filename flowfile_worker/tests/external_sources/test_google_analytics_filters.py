"""Tests for the GA4 filter-spec -> ``FilterExpression`` translator.

These tests require ``google-analytics-data`` to be importable (it is listed
as a core dependency in ``pyproject.toml``). They exercise the translator in
isolation — no real API calls are made.
"""

from __future__ import annotations

import pytest

from flowfile_worker.external_sources.google_analytics_source.filters import (
    build_filter_expressions,
)
from flowfile_worker.external_sources.google_analytics_source.models import GoogleAnalyticsFilter

pytest.importorskip("google.analytics.data_v1beta")

DIMENSIONS = ["date", "country", "deviceCategory"]
METRICS = ["sessions", "bounceRate"]


def test_no_filters_returns_none_pair() -> None:
    dim_f, met_f = build_filter_expressions([], DIMENSIONS, METRICS)
    assert dim_f is None
    assert met_f is None


def test_unknown_field_raises() -> None:
    filters = [GoogleAnalyticsFilter(field="pageTitle", operator="equals", value="Home")]
    with pytest.raises(ValueError, match="not in the selected metrics or dimensions"):
        build_filter_expressions(filters, DIMENSIONS, METRICS)


def test_single_dimension_equals_filter() -> None:
    dim_f, met_f = build_filter_expressions(
        [GoogleAnalyticsFilter(field="country", operator="equals", value="NL")],
        DIMENSIONS,
        METRICS,
    )
    assert met_f is None
    # With one filter, the result is an unwrapped FilterExpression (no and_group).
    assert dim_f.filter.field_name == "country"
    assert dim_f.filter.string_filter.value == "NL"


def test_dimension_and_metric_routed_separately() -> None:
    filters = [
        GoogleAnalyticsFilter(field="country", operator="equals", value="NL"),
        GoogleAnalyticsFilter(field="sessions", operator="greater_than", value="100"),
    ]
    dim_f, met_f = build_filter_expressions(filters, DIMENSIONS, METRICS)

    assert dim_f is not None and met_f is not None
    assert dim_f.filter.field_name == "country"
    assert met_f.filter.field_name == "sessions"
    assert met_f.filter.numeric_filter.value.int64_value == 100


def test_multiple_dimension_filters_are_and_grouped() -> None:
    filters = [
        GoogleAnalyticsFilter(field="country", operator="equals", value="NL"),
        GoogleAnalyticsFilter(field="deviceCategory", operator="equals", value="mobile"),
    ]
    dim_f, met_f = build_filter_expressions(filters, DIMENSIONS, METRICS)
    assert met_f is None
    # Two filters are wrapped in an and_group.
    exprs = list(dim_f.and_group.expressions)
    assert len(exprs) == 2
    fields = {e.filter.field_name for e in exprs}
    assert fields == {"country", "deviceCategory"}


def test_not_equals_uses_not_expression() -> None:
    filters = [GoogleAnalyticsFilter(field="country", operator="not_equals", value="NL")]
    dim_f, _ = build_filter_expressions(filters, DIMENSIONS, METRICS)
    # not_equals is implemented as NOT(equals)
    assert dim_f.not_expression.filter.field_name == "country"
    assert dim_f.not_expression.filter.string_filter.value == "NL"


def test_in_list_splits_on_commas() -> None:
    filters = [
        GoogleAnalyticsFilter(field="country", operator="in_list", value="NL, BE, DE"),
    ]
    dim_f, _ = build_filter_expressions(filters, DIMENSIONS, METRICS)
    assert list(dim_f.filter.in_list_filter.values) == ["NL", "BE", "DE"]


def test_not_in_list_wraps_in_list_in_not() -> None:
    filters = [
        GoogleAnalyticsFilter(field="country", operator="not_in_list", value="NL,BE"),
    ]
    dim_f, _ = build_filter_expressions(filters, DIMENSIONS, METRICS)
    assert list(dim_f.not_expression.filter.in_list_filter.values) == ["NL", "BE"]


def test_case_sensitive_flag_propagates() -> None:
    filters = [
        GoogleAnalyticsFilter(field="country", operator="contains", value="Nl", case_sensitive=True),
    ]
    dim_f, _ = build_filter_expressions(filters, DIMENSIONS, METRICS)
    assert dim_f.filter.string_filter.case_sensitive is True


def test_metric_numeric_operators() -> None:
    filters = [
        GoogleAnalyticsFilter(field="sessions", operator="greater_equal", value="10"),
    ]
    _, met_f = build_filter_expressions(filters, DIMENSIONS, METRICS)
    assert met_f.filter.numeric_filter.value.int64_value == 10


def test_metric_between_requires_two_values() -> None:
    bad = [GoogleAnalyticsFilter(field="sessions", operator="between", value="10")]
    with pytest.raises(ValueError, match="two comma-separated numbers"):
        build_filter_expressions(bad, DIMENSIONS, METRICS)

    ok = [GoogleAnalyticsFilter(field="sessions", operator="between", value="10, 100")]
    _, met_f = build_filter_expressions(ok, DIMENSIONS, METRICS)
    assert met_f.filter.between_filter.from_value.int64_value == 10
    assert met_f.filter.between_filter.to_value.int64_value == 100


def test_metric_value_promoted_to_double_when_decimal() -> None:
    filters = [
        GoogleAnalyticsFilter(field="bounceRate", operator="less_than", value="0.5"),
    ]
    _, met_f = build_filter_expressions(filters, DIMENSIONS, METRICS)
    assert met_f.filter.numeric_filter.value.double_value == pytest.approx(0.5)


def test_metric_value_rejects_garbage() -> None:
    filters = [
        GoogleAnalyticsFilter(field="sessions", operator="equals", value="not-a-number"),
    ]
    with pytest.raises(ValueError, match="Cannot parse"):
        build_filter_expressions(filters, DIMENSIONS, METRICS)


def test_unsupported_operator_on_dimension() -> None:
    filters = [GoogleAnalyticsFilter(field="country", operator="greater_than", value="NL")]
    with pytest.raises(ValueError, match="Unsupported dimension operator"):
        build_filter_expressions(filters, DIMENSIONS, METRICS)


def test_unsupported_operator_on_metric() -> None:
    filters = [GoogleAnalyticsFilter(field="sessions", operator="contains", value="10")]
    with pytest.raises(ValueError, match="Unsupported metric operator"):
        build_filter_expressions(filters, DIMENSIONS, METRICS)
