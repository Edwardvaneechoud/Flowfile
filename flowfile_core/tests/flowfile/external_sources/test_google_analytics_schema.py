"""Unit tests for the GA4 schema derivation helper.

These tests are pure Python — no network, no database, no Google SDK. They
verify that the lazy ``schema_callback`` used by the GA reader node produces
the correct Polars dtypes purely from the selected metric/dimension names.
"""

from __future__ import annotations

from unittest.mock import patch

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


# --- Integration with FlowGraph ----------------------------------------------
# Verifies the placeholder-caching contract: ``add_google_analytics_reader``
# must stamp ``node_ga_reader.fields`` and a ``user_provided_schema_callback``
# at add-time so downstream schema lookups never reach ``_func`` (which would
# trigger a worker → Google round-trip).
#
# Real DB + real master key (matches the rest of the GA test suite); only the
# worker fetcher is patched, so we can assert it is never instantiated.

_TEST_USER_ID = 1
_TEST_CONNECTION_NAME = "ga-test-flow-graph"
_TEST_REFRESH_TOKEN = "1//0g-fake-refresh-token-for-flow-graph-tests"


@pytest.fixture
def _ga_connection_and_oauth_config():
    """Stand up a GA connection + Google OAuth client config in the real DB."""
    from flowfile_core.configs.app_settings import (
        clear_google_oauth_config,
        set_google_oauth_config,
    )
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.flowfile.database_connection_manager.ga_connections import (
        delete_ga_connection,
        upsert_ga_connection_with_refresh_token,
    )

    with get_db_context() as db:
        upsert_ga_connection_with_refresh_token(
            db,
            connection_name=_TEST_CONNECTION_NAME,
            user_id=_TEST_USER_ID,
            refresh_token=_TEST_REFRESH_TOKEN,
            oauth_user_email="user@example.com",
            default_property_id="123456789",
        )
        set_google_oauth_config(
            db,
            user_id=_TEST_USER_ID,
            client_id="fake-client-id",
            client_secret="fake-client-secret",
            redirect_uri="http://localhost/callback",
        )

    yield

    with get_db_context() as db:
        delete_ga_connection(db, _TEST_CONNECTION_NAME, _TEST_USER_ID)
        clear_google_oauth_config(db, _TEST_USER_ID)


def _build_settings(node_id: int, metrics: list[str], dimensions: list[str]):
    from flowfile_core.schemas import input_schema

    return input_schema.NodeGoogleAnalyticsReader(
        node_id=node_id,
        flow_id=1,
        user_id=_TEST_USER_ID,
        google_analytics_settings=input_schema.GoogleAnalyticsSettings(
            ga_connection_name=_TEST_CONNECTION_NAME,
            property_id="123456789",
            metrics=metrics,
            dimensions=dimensions,
        ),
    )


def _new_graph(flow_id: int = 1):
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.schemas.schemas import FlowSettings

    handler = FlowfileHandler()
    handler.register_flow(
        FlowSettings(flow_id=flow_id, name="ga-test", path=".", execution_location="remote")
    )
    return handler.get_flow(flow_id)


def test_add_google_analytics_reader_stamps_fields_and_skips_worker(
    _ga_connection_and_oauth_config,
) -> None:
    """``add_google_analytics_reader`` must populate ``fields`` from
    ``derive_schema`` (no worker call) and register
    ``user_provided_schema_callback`` so schema introspection stays local."""

    metrics = ["sessions", "bounceRate"]
    dimensions = ["date", "country"]
    node_settings = _build_settings(node_id=1, metrics=metrics, dimensions=dimensions)

    graph = _new_graph(flow_id=1)

    with patch(
        "flowfile_core.flowfile.flow_graph.ExternalGoogleAnalyticsFetcher"
    ) as mock_fetcher:
        graph.add_google_analytics_reader(node_settings)

        # 1. ``fields`` is stamped at add-time, before any execution.
        assert node_settings.fields is not None
        assert [f.name for f in node_settings.fields] == [*dimensions, *metrics]

        # 2. ``user_provided_schema_callback`` is set so the engine never
        #    falls back to invoking ``_func`` for schema lookups.
        node = graph.get_node(node_settings.node_id)
        assert node is not None
        assert node.user_provided_schema_callback is not None

        # 3. The predicted schema resolves without instantiating the worker
        #    fetcher (which would talk to Google).
        predicted_schema = node.get_predicted_schema()
        assert [c.column_name for c in predicted_schema] == [*dimensions, *metrics]
        mock_fetcher.assert_not_called()


def test_add_google_analytics_reader_refreshes_fields_on_resettle(
    _ga_connection_and_oauth_config,
) -> None:
    """Re-adding the same node with new metrics/dimensions must overwrite the
    cached ``fields`` so stale columns don't survive an edit."""

    graph = _new_graph(flow_id=1)

    with patch("flowfile_core.flowfile.flow_graph.ExternalGoogleAnalyticsFetcher"):
        first = _build_settings(node_id=1, metrics=["sessions"], dimensions=["date"])
        graph.add_google_analytics_reader(first)
        assert [f.name for f in first.fields] == ["date", "sessions"]

        # Same node_id, different selection — re-add must refresh fields.
        second = _build_settings(
            node_id=1, metrics=["totalUsers", "bounceRate"], dimensions=["country"]
        )
        graph.add_google_analytics_reader(second)
        assert [f.name for f in second.fields] == ["country", "totalUsers", "bounceRate"]
