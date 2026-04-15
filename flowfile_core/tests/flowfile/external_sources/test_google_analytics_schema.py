"""Unit tests for the GA4 schema derivation helper.

These tests are pure Python — no network, no database, no Google SDK. They
verify that the lazy ``schema_callback`` used by the GA reader node produces
the correct Polars dtypes purely from the selected metric/dimension names.
"""

from __future__ import annotations

import polars as pl
import pytest

from flowfile_core.flowfile.sources.external_sources.google_analytics_source.google_analytics_source import (
    derive_schema,
    polars_schema_dict,
)


@pytest.mark.parametrize(
    ("dimension_name", "expected"),
    [
        ("country", pl.String),
        ("deviceCategory", pl.String),
        ("sessionDefaultChannelGroup", pl.String),
        ("date", pl.Date),
        ("dateHour", pl.Datetime),
        ("dateHourMinute", pl.Datetime),
        ("year", pl.Int64),
        ("month", pl.Int64),
        ("week", pl.Int64),
        ("day", pl.Int64),
        ("hour", pl.Int64),
        ("yearMonth", pl.Int64),
        ("nthDay", pl.Int64),
    ],
)
def test_dimension_dtypes(dimension_name: str, expected: pl.DataType) -> None:
    schema = derive_schema(metrics=[], dimensions=[dimension_name])
    assert len(schema) == 1
    assert schema[0].column_name == dimension_name
    assert str(schema[0].data_type).replace("()", "") in str(expected)


@pytest.mark.parametrize(
    ("metric_name", "expected_group"),
    [
        # Integer counts
        ("sessions", "int"),
        ("activeUsers", "int"),
        ("totalUsers", "int"),
        ("newUsers", "int"),
        ("screenPageViews", "int"),
        ("eventCount", "int"),
        ("transactions", "int"),
        # Float-valued metrics
        ("bounceRate", "float"),
        ("engagementRate", "float"),
        ("userEngagementDuration", "float"),
        ("averageSessionDuration", "float"),
        ("totalRevenue", "float"),
        ("eventCountPerUser", "float"),
        # Unknown → default to float
        ("someUnheardOfMetric", "float"),
    ],
)
def test_metric_dtypes(metric_name: str, expected_group: str) -> None:
    schema = derive_schema(metrics=[metric_name], dimensions=[])
    assert len(schema) == 1
    assert schema[0].column_name == metric_name

    dtype_str = str(schema[0].data_type).lower()
    if expected_group == "int":
        assert "int" in dtype_str, f"Expected integer dtype for {metric_name}, got {dtype_str}"
    else:
        assert "float" in dtype_str, f"Expected float dtype for {metric_name}, got {dtype_str}"


def test_dimensions_come_before_metrics() -> None:
    """Column order must mirror ``run_report`` output: dimensions first."""
    schema = derive_schema(
        metrics=["sessions", "bounceRate"],
        dimensions=["date", "country"],
    )
    names = [col.column_name for col in schema]
    assert names == ["date", "country", "sessions", "bounceRate"]


def test_empty_selection_returns_empty_schema() -> None:
    assert derive_schema(metrics=[], dimensions=[]) == []


def test_polars_schema_dict_matches_derive_schema() -> None:
    dims = ["date", "country"]
    metrics = ["sessions", "bounceRate"]
    schema_dict = polars_schema_dict(metrics=metrics, dimensions=dims)

    assert list(schema_dict.keys()) == ["date", "country", "sessions", "bounceRate"]
    assert schema_dict["date"] == pl.Date
    assert schema_dict["country"] == pl.String
    assert schema_dict["sessions"] == pl.Int64
    assert schema_dict["bounceRate"] == pl.Float64
