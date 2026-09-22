"""Change-feed kwargs on the catalog reader and writer frame API."""

from __future__ import annotations

from datetime import datetime

import pytest

import flowfile_frame as ff
from flowfile_frame.catalog import _resolve_change_mode

TABLE = "frame_change_feed_test"


def test_resolve_change_mode_off():
    assert _resolve_change_mode(None) == ("off", None, None)


def test_resolve_change_mode_version():
    assert _resolve_change_mode(7) == ("since_version", 7, None)


def test_resolve_change_mode_last_run():
    assert _resolve_change_mode("last_run") == ("since_last_run", None, None)


def test_resolve_change_mode_timestamp_string():
    assert _resolve_change_mode("2024-01-01T00:00:00+00:00") == (
        "since_timestamp",
        None,
        "2024-01-01T00:00:00+00:00",
    )


def test_resolve_change_mode_timestamp_datetime():
    moment = datetime(2024, 1, 1, 12, 30)
    assert _resolve_change_mode(moment) == ("since_timestamp", None, moment.isoformat())


def test_resolve_change_mode_rejects_bool():
    with pytest.raises(TypeError):
        _resolve_change_mode(True)


def test_resolve_change_mode_rejects_unknown_type():
    with pytest.raises(TypeError):
        _resolve_change_mode(1.5)


def _write(df: ff.FlowFrame, **kwargs) -> None:
    ff.write_catalog_table(
        df,
        TABLE,
        schema=ff.default_schema(),
        write_mode="upsert",
        merge_keys=["id"],
        track_changes=True,
        **kwargs,
    )


def test_track_changes_reaches_the_writer_settings():
    df = ff.from_dict({"id": [1, 2], "v": ["a", "b"]})
    child = df.write_catalog_table(
        TABLE,
        schema=ff.default_schema(),
        write_mode="upsert",
        merge_keys=["id"],
        track_changes=True,
    )
    settings = df.flow_graph.get_node(child.node_id).setting_input.catalog_write_settings
    assert settings.track_changes is True


def test_track_changes_rejected_for_overwrite():
    df = ff.from_dict({"id": [1, 2], "v": ["a", "b"]})
    with pytest.raises(ValueError):
        df.write_catalog_table(TABLE, schema=ff.default_schema(), write_mode="overwrite", track_changes=True)


def test_change_read_returns_the_feed_columns():
    _write(ff.from_dict({"id": [1, 2], "v": ["a", "b"]}))
    _write(ff.from_dict({"id": [2, 3], "v": ["B", "c"]}))

    changes = ff.read_catalog_table(
        TABLE,
        schema=ff.default_schema(),
        changes_since="last_run",
        changes_consumer="frame-change-feed-test",
        changes_start="beginning",
    )
    settings = changes.flow_graph.get_node(changes.node_id).setting_input
    assert settings.cdc_mode == "since_last_run"
    assert settings.cdc_consumer_name == "frame-change-feed-test"
    assert settings.cdc_start == "beginning"

    feed = changes.collect()
    assert {"_change_type", "_commit_version", "_commit_timestamp"} <= set(feed.columns)
    assert "update_preimage" not in feed["_change_type"].to_list()

    latest = feed.filter(feed["_commit_version"] == feed["_commit_version"].max())
    assert sorted(latest["id"].to_list()) == [2, 3]
    assert sorted(latest["v"].to_list()) == ["B", "c"]


def test_change_read_include_preimage_keeps_the_before_image():
    _write(ff.from_dict({"id": [1, 2], "v": ["a", "b"]}))
    _write(ff.from_dict({"id": [1, 2], "v": ["changed", "changed"]}))

    changes = ff.read_catalog_table(
        TABLE,
        schema=ff.default_schema(),
        changes_since="last_run",
        changes_consumer="frame-change-feed-preimage",
        changes_start="beginning",
        include_change_preimage=True,
    )
    feed = changes.collect()
    latest = feed.filter(feed["_commit_version"] == feed["_commit_version"].max())
    assert "update_preimage" in latest["_change_type"].to_list()
    assert "update_postimage" in latest["_change_type"].to_list()


def test_changes_since_version_reads_only_later_commits():
    _write(ff.from_dict({"id": [1, 2], "v": ["a", "b"]}))
    baseline = ff.read_catalog_table(
        TABLE,
        schema=ff.default_schema(),
        changes_since="last_run",
        changes_consumer="frame-change-feed-version",
        changes_start="beginning",
    ).collect()
    pinned = int(baseline["_commit_version"].max())

    _write(ff.from_dict({"id": [9, 10], "v": ["nine", "ten"]}))
    later = ff.read_catalog_table(TABLE, schema=ff.default_schema(), changes_since=pinned)
    settings = later.flow_graph.get_node(later.node_id).setting_input
    assert settings.cdc_mode == "since_version"
    assert settings.cdc_from_version == pinned

    rows = later.collect()
    assert rows["_commit_version"].min() > pinned
    assert 9 in rows["id"].to_list()


def test_last_run_without_consumer_on_an_unregistered_flow_raises():
    with pytest.raises(ValueError, match="changes_consumer"):
        ff.read_catalog_table(TABLE, schema=ff.default_schema(), changes_since="last_run")
