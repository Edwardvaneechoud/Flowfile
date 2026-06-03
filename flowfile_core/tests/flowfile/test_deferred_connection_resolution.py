"""Regression tests for deferred connection resolution on source/sink nodes.

Background — two bugs, same root cause (connection resolution happening eagerly
at graph-build time, keyed on a ``user_id`` that is wrong or absent):

1. Opening a flow authored by another user failed: ``open_flow`` re-stamps every
   node's ``user_id`` to the opener, then each ``add_<type>`` method resolved its
   connection against the opener's account and raised, aborting the whole open.
2. Undo failed even for the connection's owner: the history snapshot drops
   ``user_id`` (kept out so on-disk flows stay portable) and ``restore_from_snapshot``
   replayed ``add_<type>`` with ``user_id=None``.

The fix defers connection/credential resolution into the deferred ``_func`` /
``schema_callback`` closures (mirroring the cloud-storage nodes), and re-stamps
``user_id`` from the live graph during undo/redo restore. These tests pin both.

Pure-Python: no network, no real connection — the whole point is that adding /
opening / undoing these nodes never needs one. Only ``_func`` (run time) does.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import HTTPException

from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import FlowSettings

# A user_id that owns no connections — stands in for "opening someone else's flow".
_FOREIGN_USER_ID = 424242
_MISSING_GA_CONN = "conn-that-does-not-exist-ga"
_MISSING_DB_CONN = "conn-that-does-not-exist-db"


def _new_graph(flow_id: int = 1, track_history: bool = False):
    handler = FlowfileHandler()
    handler.register_flow(
        FlowSettings(
            flow_id=flow_id,
            name="deferred-conn-test",
            path=".",
            execution_location="remote",
            track_history=track_history,
        )
    )
    return handler.get_flow(flow_id)


def _ga_settings(node_id: int, metrics: list[str], dimensions: list[str], user_id: int = _FOREIGN_USER_ID):
    return input_schema.NodeGoogleAnalyticsReader(
        node_id=node_id,
        flow_id=1,
        user_id=user_id,
        google_analytics_settings=input_schema.GoogleAnalyticsSettings(
            ga_connection_name=_MISSING_GA_CONN,
            property_id="123456789",
            metrics=metrics,
            dimensions=dimensions,
        ),
    )


def _db_reader_settings(node_id: int, fields: list[tuple[str, str]], user_id: int = _FOREIGN_USER_ID):
    return input_schema.NodeDatabaseReader(
        node_id=node_id,
        flow_id=1,
        user_id=user_id,
        database_settings=input_schema.DatabaseSettings(
            connection_mode="reference",
            database_connection_name=_MISSING_DB_CONN,
            schema_name="some_schema",
            table_name="some_table",
            query_mode="table",
        ),
        fields=[input_schema.MinimalFieldInfo(name=n, data_type=t) for n, t in fields],
    )


# ---------------------------------------------------------------------------
# Bug 1 — opening a flow that references a connection the session can't resolve
# ---------------------------------------------------------------------------


def test_ga_reader_add_does_not_resolve_connection_and_fails_only_at_run():
    """GA reader: adding the node must not touch the connection (so flow-open
    never fails); resolution + the clear error happen only when ``_func`` runs."""
    graph = _new_graph()
    metrics, dimensions = ["sessions", "bounceRate"], ["date", "country"]
    node_settings = _ga_settings(node_id=1, metrics=metrics, dimensions=dimensions)

    with patch("flowfile_core.flowfile.flow_graph.ExternalGoogleAnalyticsFetcher") as mock_fetcher:
        # add must NOT raise even though the connection is missing for this user.
        graph.add_google_analytics_reader(node_settings)

        node = graph.get_node(1)
        assert node is not None
        # Schema is available at open with no connection (pure-Python derive_schema).
        assert [f.name for f in node_settings.fields] == [*dimensions, *metrics]
        assert [c.column_name for c in node.get_predicted_schema()] == [*dimensions, *metrics]
        mock_fetcher.assert_not_called()

        # Resolution is deferred to run time → raises the clear 400, before the
        # worker fetcher is ever constructed.
        with pytest.raises(HTTPException) as exc:
            node.function()
        assert exc.value.status_code == 400
        assert "not found" in str(exc.value.detail).lower()
        mock_fetcher.assert_not_called()


def test_database_reader_opens_with_stored_fields_then_clear_error_at_run():
    """Database reader: adding the node (and predicting its schema from stored
    ``fields``) needs no connection; run raises a clear message, not AttributeError."""
    graph = _new_graph()
    fields = [("a", "Int64"), ("b", "String")]
    node_settings = _db_reader_settings(node_id=1, fields=fields)

    # add must NOT raise — eager resolution is gone.
    graph.add_database_reader(node_settings)

    node = graph.get_node(1)
    assert node is not None
    # schema_callback prefers stored fields → renders columns with no connection.
    assert [c.column_name for c in node.get_predicted_schema()] == ["a", "b"]

    # Run resolves the (missing) connection → clean HTTPException, not AttributeError.
    with pytest.raises(HTTPException) as exc:
        node.function()
    assert exc.value.status_code == 400
    assert "not found or not accessible" in str(exc.value.detail).lower()


def test_database_reader_missing_connection_raises_clean_error_not_attributeerror():
    """The missing-reference path must raise a typed HTTPException (regression: it
    used to raise AttributeError on ``None.password``)."""
    from flowfile_core.flowfile.flow_graph import _resolve_database_credentials

    settings = input_schema.DatabaseSettings(
        connection_mode="reference",
        database_connection_name=_MISSING_DB_CONN,
        table_name="t",
        query_mode="table",
    )
    with pytest.raises(HTTPException) as exc:
        _resolve_database_credentials(settings, _FOREIGN_USER_ID)
    assert exc.value.status_code == 400
    assert "not found or not accessible" in str(exc.value.detail).lower()


# ---------------------------------------------------------------------------
# Bug 2 — undo/redo must preserve user_id and not re-trigger eager resolution
# ---------------------------------------------------------------------------


def test_undo_with_ga_reader_succeeds_and_preserves_user_id():
    """Undoing a settings change on a connection-backed node must succeed (no eager
    resolution) AND restore the owning ``user_id`` (dropped from the snapshot)."""
    graph = _new_graph(track_history=True)

    with patch("flowfile_core.flowfile.flow_graph.ExternalGoogleAnalyticsFetcher"):
        first = _ga_settings(node_id=1, metrics=["sessions"], dimensions=["date"])
        graph.add_google_analytics_reader(first)

        # A settings change on the same node → captures the prior state for undo.
        second = _ga_settings(node_id=1, metrics=["totalUsers", "bounceRate"], dimensions=["country"])
        graph.add_google_analytics_reader(second)

        result = graph.undo()

    assert result.success is True, result.error_message
    assert result.error_message is None

    node = graph.get_node(1)
    assert node is not None
    # user_id survived the snapshot round-trip via the restore re-stamp.
    assert node.setting_input.user_id == _FOREIGN_USER_ID
    # Restored to the first selection.
    assert [f.name for f in node.setting_input.fields] == ["date", "sessions"]


def test_redo_with_ga_reader_succeeds_and_preserves_user_id():
    """Redo replays the add the same way — it must also keep ``user_id``."""
    graph = _new_graph(track_history=True)

    with patch("flowfile_core.flowfile.flow_graph.ExternalGoogleAnalyticsFetcher"):
        graph.add_google_analytics_reader(_ga_settings(node_id=1, metrics=["sessions"], dimensions=["date"]))
        graph.add_google_analytics_reader(
            _ga_settings(node_id=1, metrics=["totalUsers"], dimensions=["country"])
        )
        undo_result = graph.undo()
        redo_result = graph.redo()

    assert undo_result.success is True, undo_result.error_message
    assert redo_result.success is True, redo_result.error_message

    node = graph.get_node(1)
    assert node is not None
    assert node.setting_input.user_id == _FOREIGN_USER_ID
    assert [f.name for f in node.setting_input.fields] == ["country", "totalUsers"]
