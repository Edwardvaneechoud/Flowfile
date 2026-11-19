"""Tests for ``POST /ai/generate_chart``: the chart request rules, the engine and the route.

One test per row of the plan's "Python validation rules" table (repairs assert the repaired
request, rejects the message), the shape repairs, the model loop with a scripted provider
(valid, repaired, retried, given up, timeout, provider error, unparsable, follow-ups), the
route's status codes and access refusal, the prompt log surface and the lazy-litellm contract.
"""

from __future__ import annotations

import ast
import asyncio
import importlib
import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import chart_profile, chart_routes, node_codegen_routes
from flowfile_core.ai import chart_request as cr
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.chart_request import (
    STAR_MESSAGE,
    ChartRequest,
    ColumnInfo,
    generate_chart,
    normalize_answer,
    normalize_chart_shape,
    review_chart,
)
from flowfile_core.ai.providers.base import ChatResponse
from flowfile_core.ai.scheduler import RateLimitScheduler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable

COLUMNS = [
    ColumnInfo("order_date", "date"),
    ColumnInfo("created_at", "datetime"),
    ColumnInfo("date_str", "string"),
    ColumnInfo("region", "string", "dimension", ("East", "North", "South", "West")),
    ColumnInfo("category", "string", "dimension"),
    ColumnInfo("revenue", "float", "measure"),
    ColumnInfo("price", "float", "measure"),
    ColumnInfo("quantity", "integer", "dimension"),
    ColumnInfo("is_active", "boolean", "dimension"),
    ColumnInfo("tags", "other"),
]
NAMES = ", ".join(c.name for c in COLUMNS)


def ok(raw: dict) -> dict:
    chart, problems = review_chart(raw, COLUMNS)
    assert problems == [], problems
    assert chart is not None
    return chart.model_dump(mode="json")


def rejected(raw: dict) -> list[str]:
    chart, problems = review_chart(raw, COLUMNS)
    assert chart is None
    assert problems
    return problems


BAR = {"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenue", "aggregate": "sum"}}


# The rule table, row by row.


def test_valid_bar_passes_unchanged() -> None:
    assert ok(BAR) == BAR


def test_field_case_insensitive_match_is_repaired() -> None:
    out = ok({"mark": "bar", "x": {"field": "Region "}, "y": {"field": "REVENUE", "aggregate": "sum"}})
    assert out == BAR


def test_field_not_a_column_is_rejected_with_closest_and_columns() -> None:
    problems = rejected({"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenu", "aggregate": "sum"}})
    assert problems == [f'y.field "revenu" is not a column. Closest: "revenue". Columns: {NAMES}']


def test_shorthand_string_is_repaired_to_a_reference() -> None:
    assert ok({"mark": "bar", "x": "region", "y": "sum(revenue)"}) == BAR


def test_count_shorthand_is_the_row_count() -> None:
    out = ok({"mark": "bar", "x": "region", "y": "count()"})
    assert out["y"] == {"field": "*", "aggregate": "count"}
    assert ok({"mark": "bar", "x": "region", "y": "COUNT(*)"})["y"] == {"field": "*", "aggregate": "count"}


def test_column_literally_named_like_a_shorthand_wins() -> None:
    columns = [*COLUMNS, ColumnInfo("count()", "integer")]
    chart, problems = review_chart({"mark": "bar", "x": "region", "y": "count()"}, columns)
    assert problems == []
    assert chart.model_dump()["y"] == {"field": "count()", "aggregate": "sum"}


@pytest.mark.parametrize(
    "ref",
    [
        {"field": "*"},
        {"field": "*", "aggregate": "sum"},
        {"field": "*", "aggregate": "count", "timeUnit": "month"},
        {"field": "*", "aggregate": "count", "bin": 10},
    ],
)
def test_star_without_count_or_with_time_unit_or_bin_is_rejected(ref: dict) -> None:
    assert STAR_MESSAGE in rejected({"mark": "bar", "x": {"field": "region"}, "y": ref})


def test_star_in_a_filter_is_rejected() -> None:
    assert STAR_MESSAGE in rejected({**BAR, "filters": [{"field": "*", "oneOf": [1]}]})


def test_star_count_on_x_is_allowed_for_horizontal_bars() -> None:
    raw = {"mark": "bar", "x": {"field": "*", "aggregate": "count"}, "y": {"field": "region"}}
    assert ok(raw) == raw


def test_number_aggregate_on_text_is_rejected() -> None:
    problems = rejected({"mark": "bar", "x": {"field": "category"}, "y": {"field": "region", "aggregate": "sum"}})
    assert '"region" is text. Use "count" or "distinctCount", or pick a numeric column.' in problems


def test_number_aggregate_on_a_date_names_the_type() -> None:
    problems = rejected({"mark": "bar", "x": {"field": "region"}, "y": {"field": "order_date", "aggregate": "mean"}})
    assert '"order_date" is a date. Use "count" or "distinctCount", or pick a numeric column.' in problems


def test_decimal_opposite_a_dimension_is_repaired_to_sum() -> None:
    assert ok({"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenue"}}) == BAR


def test_decimal_on_theta_or_size_is_repaired_to_sum() -> None:
    pie = ok({"mark": "arc", "theta": {"field": "revenue"}, "color": {"field": "region"}})
    assert pie["theta"] == {"field": "revenue", "aggregate": "sum"}
    point = ok(
        {
            "mark": "point",
            "x": {"field": "region"},
            "y": {"field": "quantity", "aggregate": "sum"},
            "size": {"field": "price"},
        }
    )
    assert point["size"] == {"field": "price", "aggregate": "sum"}


def test_decimal_with_no_dimension_opposite_is_rejected() -> None:
    problems = rejected({"mark": "bar", "x": {"field": "price"}, "y": {"field": "*", "aggregate": "count"}})
    assert problems == [
        '"price" is a decimal used as a category. Add "bin": 10, or an aggregate, '
        'or set "aggregate": false for raw rows.'
    ]


def test_same_column_grouped_and_aggregated_is_rejected() -> None:
    problems = rejected({"mark": "bar", "x": {"field": "quantity"}, "y": {"field": "quantity", "aggregate": "sum"}})
    assert '"quantity" is both grouped and aggregated. Use "bin" on one, or drop one.' in problems


def test_same_column_binned_and_counted_is_fine() -> None:
    raw = {"mark": "bar", "x": {"field": "price", "bin": 10}, "y": {"field": "price", "aggregate": "count"}}
    assert ok(raw) == raw


def test_bar_without_a_measure_repairs_one_plain_number_to_sum() -> None:
    out = ok({"mark": "bar", "x": {"field": "region"}, "y": {"field": "quantity"}})
    assert out["y"] == {"field": "quantity", "aggregate": "sum"}


def test_bar_without_a_measure_and_no_number_is_rejected() -> None:
    problems = rejected({"mark": "bar", "x": {"field": "region"}, "y": {"field": "category"}})
    assert problems == ["A bar needs one number axis: give y an aggregate."]


def test_line_without_a_measure_names_the_mark() -> None:
    problems = rejected({"mark": "line", "x": {"field": "region"}, "y": {"field": "category"}})
    assert problems == ["A line needs one number axis: give y an aggregate."]


def test_bar_with_measures_on_both_axes_is_rejected() -> None:
    problems = rejected(
        {"mark": "bar", "x": {"field": "revenue", "aggregate": "sum"}, "y": {"field": "quantity", "aggregate": "sum"}}
    )
    assert 'A bar has one number axis. Move one measure to color or use mark "point".' in problems


def test_several_measures_on_one_axis_are_rejected_outside_a_table() -> None:
    problems = rejected(
        {
            "mark": "bar",
            "x": {"field": "region"},
            "y": [{"field": "revenue", "aggregate": "sum"}, {"field": "quantity", "aggregate": "sum"}],
        }
    )
    assert 'One measure per axis: y has 2. Keep one; only a table (mark "table") shows several.' in problems


def test_time_unit_on_a_non_date_column_is_rejected() -> None:
    problems = rejected({**BAR, "x": {"field": "date_str", "timeUnit": "month"}})
    assert '"date_str" is text; timeUnit needs a date column: order_date, created_at.' in problems


def test_hour_on_a_date_column_is_rejected() -> None:
    problems = rejected({**BAR, "mark": "line", "x": {"field": "order_date", "timeUnit": "hour"}})
    assert '"order_date" has no time of day; use "day" or coarser.' in problems


def test_hour_on_a_datetime_column_is_fine() -> None:
    raw = {**BAR, "mark": "line", "x": {"field": "created_at", "timeUnit": "hour"}}
    assert ok(raw) == raw


BIN_MESSAGE = "bin needs a numeric column and a count between 2 and 50, with no aggregate."


@pytest.mark.parametrize(
    "ref",
    [
        {"field": "region", "bin": 10},
        {"field": "price", "bin": 10, "aggregate": "sum"},
        {"field": "price", "bin": 1},
        {"field": "price", "bin": 51},
    ],
)
def test_bad_bins_are_rejected(ref: dict) -> None:
    assert BIN_MESSAGE in rejected({"mark": "bar", "x": ref, "y": {"field": "*", "aggregate": "count"}})


def test_pie_on_x_and_y_is_repaired_to_color_and_theta() -> None:
    out = ok({"mark": "arc", "x": {"field": "region"}, "y": {"field": "revenue", "aggregate": "sum"}})
    assert out == {
        "mark": "arc",
        "color": {"field": "region"},
        "theta": {"field": "revenue", "aggregate": "sum"},
    }


PIE_MESSAGE = "A pie takes theta (aggregated number) and color (category) only; leave x and y empty."


@pytest.mark.parametrize(
    "raw",
    [
        {"mark": "arc", "theta": {"field": "revenue", "aggregate": "sum"}},
        {"mark": "arc", "theta": {"field": "quantity"}, "color": {"field": "region"}},
        {"mark": "arc", "x": {"field": "region"}, "theta": {"field": "revenue", "aggregate": "sum"}},
    ],
)
def test_pie_without_theta_and_color_or_with_axes_is_rejected(raw: dict) -> None:
    assert PIE_MESSAGE in rejected(raw)


def test_pie_with_sort_or_limit_is_rejected() -> None:
    pie = {"mark": "arc", "theta": {"field": "revenue", "aggregate": "sum"}, "color": {"field": "region"}}
    assert rejected({**pie, "sort": "descending", "limit": 3}) == [
        "A pie cannot be sorted or limited; filter with oneOf instead."
    ]


def test_heatmap_without_an_aggregated_color_is_rejected() -> None:
    problems = rejected({"mark": "rect", "x": {"field": "region"}, "y": {"field": "category"}})
    assert problems == ["A heatmap needs a category on x, a category on y and an aggregated number on color."]


def test_valid_heatmap_passes() -> None:
    raw = {
        "mark": "rect",
        "x": {"field": "region"},
        "y": {"field": "category"},
        "color": {"field": "revenue", "aggregate": "sum"},
    }
    assert ok(raw) == raw


def test_scatter_of_plain_numbers_is_repaired_to_raw_rows() -> None:
    out = ok({"mark": "point", "x": {"field": "price"}, "y": {"field": "quantity"}})
    assert out == {"mark": "point", "x": {"field": "price"}, "y": {"field": "quantity"}, "aggregate": False}


def test_aggregated_scatter_without_a_dimension_is_rejected() -> None:
    problems = rejected(
        {"mark": "point", "x": {"field": "price", "aggregate": "mean"}, "y": {"field": "quantity", "aggregate": "sum"}}
    )
    assert problems == [
        'This would draw one point. Add a color dimension, or remove the aggregates and set "aggregate": false.'
    ]


def test_boxplot_is_repaired_to_raw_rows_without_aggregates() -> None:
    out = ok({"mark": "boxplot", "x": {"field": "category"}, "y": {"field": "price", "aggregate": "mean"}})
    assert out == {"mark": "boxplot", "x": {"field": "category"}, "y": {"field": "price"}, "aggregate": False}


def test_boxplot_without_a_numeric_axis_is_rejected() -> None:
    problems = rejected({"mark": "boxplot", "x": {"field": "category"}, "y": {"field": "region"}})
    assert problems == ["A box plot takes categories on one axis and one plain numeric column on the other."]


def test_boxplot_with_a_numeric_color_is_rejected() -> None:
    problems = rejected(
        {"mark": "boxplot", "x": {"field": "category"}, "y": {"field": "price"}, "color": {"field": "quantity"}}
    )
    assert problems == ["A box plot takes categories on one axis and one plain numeric column on the other."]


def test_table_with_raw_rows_is_rejected() -> None:
    problems = rejected(
        {"mark": "table", "y": {"field": "region"}, "x": {"field": "revenue", "aggregate": "sum"}, "aggregate": False}
    )
    assert "A table shows aggregated values; give each number an aggregate." in problems


def test_table_takes_several_measures() -> None:
    raw = {
        "mark": "table",
        "y": {"field": "region"},
        "x": [{"field": "revenue", "aggregate": "sum"}, {"field": "quantity", "aggregate": "sum"}],
    }
    assert ok(raw) == raw


def test_raw_rows_with_a_reference_aggregate_are_rejected() -> None:
    problems = rejected({**BAR, "aggregate": False})
    assert problems == ['"aggregate": false plots raw rows, but y has "sum". Remove one.']


def test_misplaced_theta_size_and_stack_are_dropped() -> None:
    out = ok(
        {
            **BAR,
            "mark": "line",
            "theta": {"field": "revenue", "aggregate": "sum"},
            "size": {"field": "price"},
            "stack": "stack",
        }
    )
    assert out == {**BAR, "mark": "line"}


def test_stack_is_kept_on_a_bar() -> None:
    raw = {**BAR, "color": {"field": "category"}, "stack": "normalize"}
    assert ok(raw) == raw


TIME_AXIS_MESSAGE = "A time or number axis is always in order; remove sort and limit, or use dateRange."


@pytest.mark.parametrize(
    "x",
    [
        {"field": "order_date", "timeUnit": "month"},
        {"field": "order_date"},
        {"field": "quantity"},
        {"field": "price", "bin": 10},
    ],
)
def test_sort_on_a_time_or_number_axis_is_rejected(x: dict) -> None:
    y = {"field": "*", "aggregate": "count"}
    assert TIME_AXIS_MESSAGE in rejected({"mark": "bar", "x": x, "y": y, "sort": "descending"})


def test_top_five_passes() -> None:
    raw = {**BAR, "sort": "descending", "limit": 5}
    assert ok(raw) == raw


def test_limit_without_sort_is_rejected() -> None:
    problems = rejected({**BAR, "limit": 5})
    assert problems == ['limit needs sort: "descending" for the top 5, "ascending" for the bottom 5.']


def test_limit_with_a_color_split_is_rejected() -> None:
    problems = rejected({**BAR, "color": {"field": "category"}, "sort": "descending", "limit": 5})
    assert problems == [
        'With color "category" the limit keeps the 5 largest (region, category) cells, '
        'not 5 "region" values. Remove color or limit.'
    ]


@pytest.mark.parametrize("values", [[], [None], ["West", None]])
def test_empty_or_null_value_lists_are_rejected(values: list) -> None:
    problems = rejected({**BAR, "filters": [{"field": "category", "oneOf": values}]})
    assert problems == ["oneOf needs at least one value; null is not matched."]


def test_not_in_takes_the_same_value_rule() -> None:
    problems = rejected({**BAR, "filters": [{"field": "category", "notIn": []}]})
    assert problems == ["notIn needs at least one value; null is not matched."]


@pytest.mark.parametrize(
    ("field", "given", "expected"),
    [
        ("is_active", ["true", 0], [True, False]),
        ("quantity", ["3", 4.0], [3, 4]),
        ("price", ["9.5", 3], [9.5, 3]),
        ("category", [2025, 1.5], ["2025", "1.5"]),
    ],
)
def test_filter_values_are_coerced_to_the_column_type(field: str, given: list, expected: list) -> None:
    out = ok({**BAR, "filters": [{"field": field, "oneOf": given}]})
    assert out["filters"] == [{"field": field, "oneOf": expected}]


@pytest.mark.parametrize(
    ("field", "given", "message"),
    [
        ("is_active", ["maybe"], '"is_active" is boolean; use true or false.'),
        ("quantity", ["three"], '"quantity" holds whole numbers; use numbers such as 3.'),
        ("price", [True], '"price" holds numbers; use numbers such as 9.5.'),
        ("category", [{"a": 1}], '"category" is text; use strings such as "West".'),
    ],
)
def test_uncoercible_filter_values_are_rejected(field: str, given: list, message: str) -> None:
    assert rejected({**BAR, "filters": [{"field": field, "oneOf": given}]}) == [message]


def test_filter_value_in_another_case_is_repaired_to_the_known_value() -> None:
    out = ok({**BAR, "filters": [{"field": "region", "oneOf": ["west"]}]})
    assert out["filters"] == [{"field": "region", "oneOf": ["West"]}]


def test_filter_value_not_in_the_known_values_is_rejected() -> None:
    problems = rejected({**BAR, "filters": [{"field": "region", "oneOf": ["Wst"]}]})
    assert problems == ['"Wst" is not a value of "region". Values: "East", "North", "South", "West".']


@pytest.mark.parametrize(
    "flt",
    [{"field": "order_date", "oneOf": ["2025-01-01"]}, {"field": "order_date", "range": [0, 10]}],
)
def test_one_of_or_range_on_a_date_is_rejected(flt: dict) -> None:
    problems = rejected({**BAR, "filters": [flt]})
    assert problems == ['Filter dates with {"field":"order_date","dateRange":["2024-01-01","2024-12-31"]}.']


RANGE_MESSAGE = "range needs a numeric column and [min,max] with at least one bound."


@pytest.mark.parametrize(
    "flt",
    [
        {"field": "region", "range": [0, 10]},
        {"field": "price", "range": [None, None]},
        {"field": "price", "range": [10, 1]},
    ],
)
def test_bad_ranges_are_rejected(flt: dict) -> None:
    assert rejected({**BAR, "filters": [flt]}) == [RANGE_MESSAGE]


def test_open_range_passes() -> None:
    raw = {**BAR, "filters": [{"field": "price", "range": [None, 100.0]}]}
    assert ok(raw) == raw


DATE_RANGE_MESSAGE = (
    'dateRange takes "YYYY-MM-DD" or null on a date column, with at least one bound and start before end.'
)


@pytest.mark.parametrize(
    "flt",
    [
        {"field": "region", "dateRange": ["2025-01-01", None]},
        {"field": "order_date", "dateRange": ["last year", None]},
        {"field": "order_date", "dateRange": [None, None]},
        {"field": "order_date", "dateRange": ["2025-12-31", "2025-01-01"]},
    ],
)
def test_bad_date_ranges_are_rejected(flt: dict) -> None:
    assert rejected({**BAR, "filters": [flt]}) == [DATE_RANGE_MESSAGE]


def test_full_iso_datetime_in_a_date_range_is_repaired_to_its_date() -> None:
    out = ok({**BAR, "filters": [{"field": "created_at", "dateRange": ["2025-01-01T00:00:00Z", None]}]})
    assert out["filters"] == [{"field": "created_at", "dateRange": ["2025-01-01", None]}]


FILTER_KEYS_MESSAGE = (
    "Each filter has exactly one of oneOf, notIn, range, dateRange; filters apply to rows, not to sums."
)


@pytest.mark.parametrize(
    "flt",
    [
        {"field": "region"},
        {"field": "region", "oneOf": ["West"], "notIn": ["East"]},
        {"field": "revenue", "range": [0, 10], "aggregate": "sum"},
    ],
)
def test_filter_with_zero_or_several_rules_or_an_aggregate_is_rejected(flt: dict) -> None:
    assert rejected({**BAR, "filters": [flt]}) == [f"filters[0]: {FILTER_KEYS_MESSAGE}"]


def test_column_that_cannot_be_charted_is_rejected() -> None:
    assert '"tags" cannot be charted.' in rejected({**BAR, "x": {"field": "tags"}})
    assert '"tags" cannot be charted.' in rejected({**BAR, "filters": [{"field": "tags", "oneOf": ["a"]}]})


def test_every_problem_of_one_answer_comes_back_together() -> None:
    problems = rejected(
        {
            "mark": "bar",
            "x": {"field": "regoin"},
            "y": {"field": "revenue", "aggregate": "sum"},
            "limit": 5,
            "filters": [{"field": "order_date", "oneOf": ["2025"]}],
        }
    )
    assert len(problems) == 3
    assert problems[0].startswith('x.field "regoin" is not a column. Closest: "region".')
    assert 'limit needs sort: "descending" for the top 5, "ascending" for the bottom 5.' in problems
    assert any(p.startswith("Filter dates with") for p in problems)


def test_shape_problems_are_sentences() -> None:
    problems = rejected({"mark": "pie", "x": {"field": "region", "agg": "sum"}, "opacity": 0.5})
    assert any(p.startswith('"opacity" is not a chart key.') for p in problems)
    assert any(p.startswith("x.agg: unknown key.") for p in problems)
    assert any(p.startswith('mark: "pie" is not allowed; use') for p in problems)


def test_no_fields_at_all_is_rejected() -> None:
    assert rejected({"mark": "bar"})[0].startswith("The chart has no fields.")


# Shape repairs: each normaliser returns the same object when nothing changes.


def test_normalizers_return_the_same_object_on_a_canonical_shape() -> None:
    raw = {"mark": "bar", "x": [{"field": "region"}], "y": {"field": "revenue", "aggregate": "sum"}}
    assert normalize_chart_shape(raw, ["region", "revenue"]) is raw
    answer = {"chart": raw, "title": "t", "explanation": "e"}
    assert normalize_answer(answer) is answer


def test_aggregate_and_time_unit_spellings_are_canonicalised() -> None:
    out = ok(
        {
            "mark": "line",
            "x": {"field": "order_date", "timeUnit": "yearmonth"},
            "y": {"field": "revenue", "aggregate": "avg"},
        }
    )
    assert out["x"] == {"field": "order_date", "timeUnit": "month"}
    assert out["y"] == {"field": "revenue", "aggregate": "mean"}
    assert ok({**BAR, "y": {"field": "category", "aggregate": "count_distinct"}})["y"]["aggregate"] == "distinctCount"
    assert ok({**BAR, "y": {"field": "revenue", "aggregate": "Sum"}})["y"]["aggregate"] == "sum"


def test_sort_on_a_reference_moves_to_the_request() -> None:
    out = ok({**BAR, "y": {"field": "revenue", "aggregate": "sum", "sort": "descending"}, "limit": 5})
    assert out == {**BAR, "sort": "descending", "limit": 5}


def test_conflicting_sort_on_a_reference_is_left_for_the_message() -> None:
    raw = {**BAR, "y": {"field": "revenue", "aggregate": "sum", "sort": "descending"}, "sort": "ascending"}
    assert any(p.startswith("y.sort: unknown key.") for p in rejected(raw))


def test_one_element_list_on_color_is_its_element() -> None:
    out = ok({**BAR, "color": [{"field": "category"}]})
    assert out["color"] == {"field": "category"}


def test_bare_chart_at_the_top_level_is_wrapped() -> None:
    assert normalize_answer({**BAR, "title": "T"}) == {"chart": BAR, "title": "T", "explanation": None}


def test_chart_sent_as_a_json_string_is_parsed() -> None:
    assert normalize_answer({"chart": json.dumps(BAR), "title": "T"}) == {"chart": BAR, "title": "T"}


def test_x_list_form_is_kept_and_serialisation_omits_absent_keys() -> None:
    chart, _ = review_chart({"mark": "bar", "x": [{"field": "region"}], "y": "sum(revenue)"}, COLUMNS)
    assert chart.model_dump() == {"mark": "bar", "x": [{"field": "region"}], "y": BAR["y"]}
    assert "null" not in ChartRequest.model_validate(BAR).model_dump_json()


# Repair codes: each silent repair names itself, and a canonical answer names none.


def repairs_of(raw: dict, columns: list[ColumnInfo] = COLUMNS) -> list[str]:
    repairs: list[str] = []
    chart, problems = review_chart(raw, columns, repairs)
    assert chart is not None, problems
    return repairs


def test_a_canonical_answer_has_no_repairs() -> None:
    assert repairs_of(BAR) == []
    assert repairs_of({"mark": "point", "x": {"field": "price"}, "y": {"field": "quantity"}, "aggregate": False}) == []
    assert (
        repairs_of({"mark": "boxplot", "x": {"field": "category"}, "y": {"field": "price"}, "aggregate": False}) == []
    )


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        ({"mark": "bar", "x": "region", "y": "sum(revenue)"}, "shorthand"),
        ({**BAR, "y": {"field": "revenue", "aggregate": "avg"}}, "aggregate_alias"),
        ({**BAR, "x": {"field": "order_date", "timeUnit": "yearmonth"}}, "time_unit_alias"),
        ({**BAR, "color": [{"field": "category"}]}, "list_unwrapped"),
        ({**BAR, "y": {"field": "revenue", "aggregate": "sum", "sort": "descending"}, "limit": 5}, "sort_lifted"),
        ({**BAR, "x": {"field": "Region"}}, "column_case"),
        ({**BAR, "filters": [{"field": "REGION", "oneOf": ["West"]}]}, "column_case"),
        ({**BAR, "theta": {"field": "revenue", "aggregate": "sum"}}, "theta_dropped"),
        ({**BAR, "size": {"field": "revenue", "aggregate": "sum"}}, "size_dropped"),
        ({**BAR, "mark": "line", "stack": "stack"}, "stack_dropped"),
        ({"mark": "point", "x": {"field": "price"}, "y": {"field": "quantity"}}, "scatter_raw"),
        ({"mark": "boxplot", "x": {"field": "category"}, "y": {"field": "price"}}, "boxplot_raw"),
        (
            {
                "mark": "boxplot",
                "x": {"field": "category"},
                "y": {"field": "price", "aggregate": "mean"},
                "aggregate": False,
            },
            "boxplot_raw",
        ),
        ({"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenue"}}, "decimal_sum"),
        ({"mark": "bar", "x": {"field": "region"}, "y": {"field": "quantity"}}, "measure_sum"),
        ({"mark": "arc", "x": {"field": "region"}, "y": {"field": "revenue", "aggregate": "sum"}}, "pie_channels"),
        ({**BAR, "filters": [{"field": "quantity", "oneOf": ["3"]}]}, "filter_value_coerced"),
        ({**BAR, "filters": [{"field": "is_active", "oneOf": [1]}]}, "filter_value_coerced"),
        ({**BAR, "filters": [{"field": "region", "oneOf": ["west"]}]}, "filter_value_case"),
        (
            {**BAR, "filters": [{"field": "order_date", "dateRange": ["2024-01-01T00:00:00Z", None]}]},
            "date_range_normalized",
        ),
    ],
)
def test_each_repair_names_its_code(raw: dict, code: str) -> None:
    assert repairs_of(raw) == [code]


def test_several_repairs_are_listed_once_each_in_order() -> None:
    raw = {"mark": "bar", "x": "Region", "y": "revenue", "filters": [{"field": "region", "oneOf": ["west", "EAST"]}]}
    assert repairs_of(raw) == ["shorthand", "column_case", "decimal_sum", "filter_value_case"]


def test_answer_shape_repairs_name_their_codes() -> None:
    repairs: list[str] = []
    normalize_answer({**BAR, "title": "T"}, repairs)
    normalize_answer({"chart": json.dumps(BAR)}, repairs)
    assert repairs == ["bare_chart", "chart_string"]
    assert cr.read_answer("```json\n" + json.dumps({"chart": BAR}) + "\n```", COLUMNS).response.repairs == [
        "json_extracted"
    ]
    assert cr.read_answer(json.dumps(BAR), COLUMNS).response.repairs == ["bare_chart"]


def test_every_repair_code_is_in_the_vocabulary_and_used() -> None:
    source = Path(cr.__file__).read_text(encoding="utf-8")
    noted = set(re.findall(r'note\([^"]*"([a-z_]+)"\)', source))
    assert noted <= set(cr.REPAIRS)
    assert all(source.count(f'"{code}"') >= 2 for code in cr.REPAIRS)


# The model loop, with a scripted provider.


class ScriptedProvider:
    """Answers each ``chat`` with the next reply (or raises/sleeps) and records every call."""

    name = "fake"
    model = "fake-model"

    def __init__(self, *replies: str, delay: float = 0.0, error: BaseException | None = None) -> None:
        self.replies = list(replies)
        self.delay = delay
        self.error = error
        self.calls: list[dict[str, Any]] = []

    async def chat(self, messages, tools=None, max_tokens=None, response_format=None, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append({"messages": list(messages), "response_format": response_format, **kwargs})
        if self.error is not None:
            raise self.error
        if self.delay:
            await asyncio.sleep(self.delay)
        return ChatResponse(model=self.model, content=self.replies.pop(0))


def _scheduler() -> RateLimitScheduler:
    return RateLimitScheduler(time_source=lambda: 0.0, sleep=lambda *_a, **_k: asyncio.sleep(0))


def _answer(chart: Any, title: str = "Revenue by region", explanation: str = "Total per region.") -> str:
    return json.dumps({"chart": chart, "title": title, "explanation": explanation})


async def _generate(provider: ScriptedProvider, **kwargs: Any):
    return await generate_chart(
        prompt=kwargs.pop("prompt", "revenue by region"),
        columns=COLUMNS,
        profile_text='Table "sales": 10 columns.',
        provider=provider,
        user_id=7,
        scheduler=_scheduler(),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_valid_answer_is_returned_with_json_mode_and_the_surface() -> None:
    provider = ScriptedProvider(_answer(BAR))
    resp = await _generate(provider)
    assert resp.degraded is False
    assert resp.chart.model_dump() == BAR
    assert resp.title == "Revenue by region"
    assert resp.explanation == "Total per region."
    [call] = provider.calls
    assert call["response_format"] == {"type": "json_object"}
    assert call["surface"] == "chart"
    assert call["user_id"] == 7
    assert (resp.attempts, resp.repairs) == (1, [])
    assert [m.role for m in call["messages"]] == ["system", "user"]
    assert "## Request\n\nrevenue by region" in call["messages"][1].content


@pytest.mark.asyncio
async def test_repairable_answer_is_repaired_without_a_retry() -> None:
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "Region", "y": "sum(revenue)"}))
    resp = await _generate(provider)
    assert resp.degraded is False
    assert resp.chart.model_dump() == BAR
    assert len(provider.calls) == 1
    assert (resp.attempts, resp.repairs) == (1, ["shorthand", "column_case"])


@pytest.mark.asyncio
async def test_rejected_answer_is_fixed_by_the_retry() -> None:
    bad = _answer({**BAR, "limit": 5})
    provider = ScriptedProvider(bad, _answer({**BAR, "sort": "descending", "limit": 5}))
    resp = await _generate(provider)
    assert resp.degraded is False
    assert resp.chart.model_dump() == {**BAR, "sort": "descending", "limit": 5}
    assert (resp.attempts, resp.repairs) == (2, [])
    second = provider.calls[1]["messages"]
    assert [m.role for m in second] == ["system", "user", "assistant", "user"]
    assert second[2].content == bad
    assert 'limit needs sort: "descending" for the top 5' in second[3].content


@pytest.mark.asyncio
async def test_unparsable_first_answer_is_retried() -> None:
    provider = ScriptedProvider("Sure! Here is a chart.", _answer(BAR))
    resp = await _generate(provider)
    assert resp.degraded is False
    assert "not valid JSON" in provider.calls[1]["messages"][3].content


@pytest.mark.asyncio
async def test_answer_with_prose_around_the_object_is_parsed() -> None:
    provider = ScriptedProvider("```json\n" + _answer(BAR) + "\n```")
    assert (await _generate(provider)).chart.model_dump() == BAR
    provider = ScriptedProvider("Here you go: " + _answer(BAR) + " Enjoy.")
    assert (await _generate(provider)).chart.model_dump() == BAR


@pytest.mark.asyncio
async def test_still_failing_after_the_retry_gives_up_with_the_message() -> None:
    bad = _answer({**BAR, "limit": 5})
    provider = ScriptedProvider(bad, bad)
    resp = await _generate(provider)
    assert resp.degraded is True
    assert resp.reason == "invalid_chart"
    assert resp.chart is None
    assert resp.message.startswith("The chart request has 1 problem:")
    assert len(provider.calls) == 2
    assert (resp.attempts, resp.repairs) == (2, [])


@pytest.mark.asyncio
async def test_repairs_are_those_of_the_accepted_answer_only() -> None:
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "Region", "y": "revenue", "limit": 5}), _answer(BAR))
    resp = await _generate(provider)
    assert (resp.attempts, resp.repairs) == (2, [])


@pytest.mark.asyncio
async def test_unparsable_twice_is_a_parse_error() -> None:
    resp = await _generate(ScriptedProvider("no json", "still none"))
    assert (resp.degraded, resp.reason) == (True, "parse_error")


@pytest.mark.asyncio
async def test_empty_answer_twice_is_an_empty_response() -> None:
    resp = await _generate(ScriptedProvider("", " "))
    assert (resp.degraded, resp.reason) == (True, "empty_response")


@pytest.mark.asyncio
async def test_answer_without_a_chart_key_twice_is_a_validation_error() -> None:
    resp = await _generate(ScriptedProvider('{"foo": 1}', "[1, 2]"))
    assert (resp.degraded, resp.reason) == (True, "validation_error")


@pytest.mark.asyncio
async def test_declined_request_is_no_chart_without_a_retry() -> None:
    provider = ScriptedProvider(json.dumps({"chart": None, "title": None, "explanation": "This table has no weather."}))
    resp = await _generate(provider, prompt="weather tomorrow")
    assert (resp.degraded, resp.reason) == (True, "no_chart")
    assert resp.explanation == "This table has no weather."
    assert len(provider.calls) == 1
    assert (resp.attempts, resp.repairs) == (1, [])


@pytest.mark.asyncio
async def test_timeout_is_degraded() -> None:
    resp = await _generate(ScriptedProvider(_answer(BAR), delay=1.0), timeout=0.05)
    assert (resp.degraded, resp.reason) == (True, "timeout")
    assert resp.attempts == 1


@pytest.mark.asyncio
async def test_provider_error_is_degraded_without_its_text() -> None:
    resp = await _generate(ScriptedProvider(error=RuntimeError("secret-key-sk-123 rejected")))
    assert (resp.degraded, resp.reason) == (True, "provider_error")
    assert resp.message == "The AI provider call failed (RuntimeError)."
    assert resp.attempts == 1


@pytest.mark.asyncio
async def test_empty_prompt_is_degraded_without_a_call() -> None:
    provider = ScriptedProvider()
    resp = await _generate(provider, prompt="   ")
    assert (resp.degraded, resp.reason) == (True, "empty_prompt")
    assert provider.calls == []
    assert (resp.attempts, resp.repairs) == (0, [])


@pytest.mark.asyncio
async def test_follow_up_sends_the_valid_current_chart() -> None:
    line = {**BAR, "mark": "line", "x": {"field": "order_date", "timeUnit": "month"}}
    current = {**line, "mark": "bar"}
    provider = ScriptedProvider(_answer(line, title="Monthly revenue"))
    resp = await _generate(provider, prompt="make it a line", current_chart=current)
    assert resp.degraded is False
    assert resp.warnings == []
    assert resp.chart.model_dump() == line
    assert resp.repairs == []
    user = provider.calls[0]["messages"][1].content
    assert "## Current chart\n\n" + json.dumps(current) in user


@pytest.mark.asyncio
async def test_follow_up_with_an_invalid_current_chart_goes_on_without_it() -> None:
    provider = ScriptedProvider(_answer(BAR))
    resp = await _generate(provider, prompt="only 2025", current_chart={**BAR, "x": {"field": "nope"}})
    assert resp.degraded is False
    [warning] = resp.warnings
    assert warning.startswith("The current chart could not be used for a change (")
    assert 'x.field "nope" is not a column.' in warning
    assert "## Current chart\n\nNone: build a new chart." in provider.calls[0]["messages"][1].content


def test_reason_vocabulary_is_stable() -> None:
    assert cr.REASONS == (
        "empty_prompt",
        "no_columns",
        "timeout",
        "provider_error",
        "empty_response",
        "parse_error",
        "validation_error",
        "no_chart",
        "invalid_chart",
    )


def test_system_prompt_carries_the_worked_examples() -> None:
    prompt = cr.system_prompt()
    for request in (
        "revenue by region",
        "monthly revenue trend",
        "top 5 products by revenue",
        "share of revenue per category as a pie",
        "price against quantity",
        "histogram of price",
        "revenue by category in the West during 2024",
        "make it a line",
        "only 2025",
    ):
        assert f"Request: {request}\n" in prompt
    examples = [json.loads(block) for block in _example_answers(prompt)]
    assert len(examples) >= 9
    for example in examples:
        if example["chart"] is None:
            continue
        chart, problems = review_chart(example["chart"], _SALES_COLUMNS)
        assert problems == [], (example, problems)
        assert chart.model_dump(mode="json") == example["chart"], "a worked example must need no repair"


_SALES_COLUMNS = [
    ColumnInfo("order_date", "date"),
    ColumnInfo("region", "string", values=("East", "North", "South", "West")),
    ColumnInfo("category", "string"),
    ColumnInfo("product", "string"),
    ColumnInfo("revenue", "float"),
    ColumnInfo("quantity", "integer"),
    ColumnInfo("price", "float"),
    ColumnInfo("is_return", "boolean"),
]


def _example_answers(prompt: str) -> list[str]:
    """Each worked answer: the lines after a ``Request:`` line up to the blank line."""
    blocks: list[str] = []
    lines = prompt.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("Request: "):
            body = []
            for nxt in lines[i + 1 :]:
                if not nxt.strip():
                    break
                body.append(nxt)
            blocks.append("\n".join(body))
    return blocks


# The route.


@pytest.fixture(autouse=True)
def _fresh_profile_cache() -> Iterator[None]:
    chart_profile.clear_cache()
    yield
    chart_profile.clear_cache()


def _make_table() -> tuple[int, int]:
    with get_db_context() as db:
        ns = CatalogNamespace(name="ChartAiNs", parent_id=None, level=0, owner_id=1)
        db.add(ns)
        db.commit()
        db.refresh(ns)
        table = CatalogTable(
            name="ai_sales",
            namespace_id=ns.id,
            owner_id=1,
            file_path="/tmp/flowfile-test/ai_sales",
            storage_format="delta",
            schema_json=json.dumps(
                [
                    {"name": "region", "dtype": "String"},
                    {"name": "revenue", "dtype": "Float64"},
                    {"name": "order_date", "dtype": "Date"},
                ]
            ),
            table_type="physical",
        )
        db.add(table)
        db.commit()
        db.refresh(table)
        return table.id, ns.id


@pytest.fixture
def table_id() -> Iterator[int]:
    tid, ns_id = _make_table()
    yield tid
    with get_db_context() as db:
        db.query(CatalogTable).filter(CatalogTable.id == tid).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.id == ns_id).delete()
        db.commit()


def _fake_fields(worker_source, *, worker_url):
    return {
        "fields": [
            {"fid": "region", "analyticType": "dimension"},
            {"fid": "revenue", "analyticType": "measure"},
            {"fid": "order_date", "analyticType": "dimension"},
        ]
    }


def _fake_query(worker_source, payload, max_rows, *, worker_url):
    query = payload["workflow"][0]["query"][0]
    if query["op"] == "raw":
        rows = [{"region": "West", "revenue": 10.5, "order_date": "2025-01-02"}]
    elif query["groupBy"]:
        rows = [{"region": v} for v in ("West", "East")]
    else:
        rows = [
            {
                "__rows": 2,
                "c0_nonnull": 2,
                "c0_distinct": 2,
                "c1_nonnull": 2,
                "c1_distinct": 2,
                "c1_min": 1.0,
                "c1_max": 10.5,
                "c2_nonnull": 2,
                "c2_distinct": 2,
                "c2_min": "2025-01-01",
                "c2_max": "2025-01-02",
            }
        ]
    return {"rows": rows, "total_rows": len(rows), "truncated": False, "elapsed_ms": 1.0, "cache_hit": False}


@pytest.fixture
def worker() -> Iterator[None]:
    with (
        patch("flowfile_core.catalog.service.trigger_visualize_fields", side_effect=_fake_fields),
        patch("flowfile_core.catalog.service.trigger_visualize_query", side_effect=_fake_query),
    ):
        yield


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    main.app.dependency_overrides[get_current_active_user] = lambda: PydanticUser(id=1, username="local_user")
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def _use_provider(monkeypatch: pytest.MonkeyPatch, provider: ScriptedProvider) -> None:
    monkeypatch.setattr(chart_routes, "_resolve_provider", lambda *_a, **_kw: provider)


def test_route_generates_a_chart_from_the_profile(
    authed_client: TestClient, table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "region", "y": "sum(revenue)"}))
    _use_provider(monkeypatch, provider)
    resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "revenue by region"})
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "chart": {"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenue", "aggregate": "sum"}},
        "title": "Revenue by region",
        "explanation": "Total per region.",
        "warnings": [],
        "degraded": False,
        "reason": None,
        "message": None,
        "attempts": 1,
        "repairs": ["shorthand"],
    }
    user = provider.calls[0]["messages"][1].content
    assert 'Table "ai_sales": 2 rows, 3 columns.' in user
    assert '- "region": string, dimension; 2 non-null, 2 distinct; values: "East", "West"' in user


def test_route_follow_up_with_a_current_chart(
    authed_client: TestClient, table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    line = {
        "mark": "line",
        "x": {"field": "order_date", "timeUnit": "month"},
        "y": {"field": "revenue", "aggregate": "sum"},
    }
    provider = ScriptedProvider(_answer(line))
    _use_provider(monkeypatch, provider)
    resp = authed_client.post(
        "/ai/generate_chart",
        json={"table_id": table_id, "prompt": "make it a line", "current_chart": {**line, "mark": "bar"}},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["chart"] == line
    assert '"mark": "bar"' in provider.calls[0]["messages"][1].content


def test_route_warns_when_the_profile_fails(
    authed_client: TestClient, table_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "region", "y": "sum(revenue)"}))
    _use_provider(monkeypatch, provider)
    with patch("flowfile_core.catalog.service.trigger_visualize_fields", side_effect=RuntimeError("worker down")):
        resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "revenue by region"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["degraded"] is False
    assert body["warnings"] == [
        "Column statistics and example values were not available (failed); "
        "the model saw the column names, types and roles only."
    ]
    assert "Statistics and example values are not available" in provider.calls[0]["messages"][1].content


def test_route_404_on_unknown_provider(authed_client: TestClient, table_id: int) -> None:
    resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "x", "provider": "imaginary"})
    assert resp.status_code == 404
    assert "imaginary" in resp.json()["detail"]


def test_route_404_on_unknown_table(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = ScriptedProvider()
    _use_provider(monkeypatch, provider)
    resp = authed_client.post("/ai/generate_chart", json={"table_id": 987654321, "prompt": "x"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Catalog table not found"
    assert provider.calls == []


def test_route_409_when_no_provider_is_configured(
    authed_client: TestClient, table_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(*_a: Any, **_kw: Any):
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(node_codegen_routes, "get_configured_provider", _raise)
    resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "x"})
    assert resp.status_code == 409


def test_route_503_when_ai_is_off(authed_client: TestClient, table_id: int) -> None:
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "x"})
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert resp.status_code == 503


def test_route_422_on_a_blank_or_long_prompt(authed_client: TestClient, table_id: int) -> None:
    assert authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": ""}).status_code == 422
    long = "x" * 2001
    assert authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": long}).status_code == 422


def test_route_refuses_a_table_the_caller_may_not_read(
    table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    provider = ScriptedProvider(_answer(BAR))
    _use_provider(monkeypatch, provider)
    stranger = PydanticUser(id=987654, username="stranger", is_admin=False)
    main.app.dependency_overrides[get_current_active_user] = lambda: stranger
    try:
        resp = TestClient(main.app).post("/ai/generate_chart", json={"table_id": table_id, "prompt": "x"})
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Not authorized to access this catalog_table"
    assert provider.calls == []


def test_route_degrades_on_a_provider_error(
    authed_client: TestClient, table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _use_provider(monkeypatch, ScriptedProvider(error=ConnectionError("down")))
    resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "revenue by region"})
    assert resp.status_code == 200
    assert (resp.json()["degraded"], resp.json()["reason"]) == (True, "provider_error")
    assert (resp.json()["attempts"], resp.json()["repairs"]) == (1, [])


# The prompt log and the lazy-litellm contract.


def test_prompt_log_records_the_chart_surface(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from flowfile_core.ai import prompt_log
    from flowfile_core.ai.providers import _litellm_base
    from flowfile_core.ai.providers.openai import OpenAIProvider
    from shared.storage_config import storage

    async def acompletion(**_kwargs: Any) -> Any:
        message = SimpleNamespace(content=_answer(BAR), tool_calls=None)
        return SimpleNamespace(choices=[SimpleNamespace(message=message, finish_reason="stop")], usage=None)

    monkeypatch.setattr(_litellm_base, "_lazy_litellm", lambda: SimpleNamespace(acompletion=acompletion))
    monkeypatch.setattr(storage, "_base_dir", tmp_path)
    original = core_settings.FLOWFILE_AI_LOG_PROMPTS.value
    core_settings.FLOWFILE_AI_LOG_PROMPTS.set(True)
    try:
        provider = OpenAIProvider(model="gpt-test", api_key="sk-test")
        resp = asyncio.run(
            generate_chart(
                prompt="revenue by region",
                columns=COLUMNS,
                profile_text="Table",
                provider=provider,
                user_id=42,
                scheduler=_scheduler(),
            )
        )
    finally:
        core_settings.FLOWFILE_AI_LOG_PROMPTS.set(original)
    assert resp.degraded is False
    [entry] = list(prompt_log.grep(r"revenue by region", surface="chart"))
    assert entry.surface == "chart"
    assert entry.user_id == 42


_NEW_MODULES = (
    "flowfile_core.ai.chart_request",
    "flowfile_core.ai.chart_profile",
    "flowfile_core.ai.chart_routes",
)


@pytest.mark.parametrize("module_name", _NEW_MODULES)
def test_lazy_litellm_contract(module_name: str) -> None:
    """Importing the module pulls in no litellm, and its source has no module-level litellm import."""
    module = importlib.import_module(module_name)
    tree = ast.parse(Path(module.__file__).read_text())
    for node in tree.body:
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        assert not any(n == "litellm" or n.startswith("litellm.") for n in names), module_name

    already = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)
    saved = {k: v for k, v in sys.modules.items() if k == module_name}
    sys.modules.pop(module_name, None)
    try:
        importlib.import_module(module_name)
        if not already:
            assert "litellm" not in sys.modules, f"{module_name} pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)


def test_openapi_schema_lists_the_route() -> None:
    schema = main.app.openapi()
    assert "/ai/generate_chart" in schema["paths"]
