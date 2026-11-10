"""Snapshot totals use complete log statistics without requiring a full data scan."""

import json

import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from shared.delta_utils import delta_row_count, read_delta_preview


@pytest.mark.parametrize("size", [0, 20])
def test_complete_statistics_need_no_data_scan(tmp_path, monkeypatch, size):
    write_deltalake(tmp_path, pa.table({"a": pa.array(range(size), type=pa.int64())}))
    snapshot = DeltaTable(str(tmp_path))

    def unexpected_scan(*args, **kwargs):
        pytest.fail("Complete snapshot statistics must not scan the data")

    monkeypatch.setattr(pl, "scan_delta", unexpected_scan)
    assert delta_row_count(snapshot) == size


@pytest.mark.parametrize("with_statistics", [True, False])
def test_preview_pins_rows_count_and_storage_options(tmp_path, monkeypatch, with_statistics):
    write_deltalake(tmp_path, pa.table({"a": range(20)}))
    if not with_statistics:
        log = next((tmp_path / "_delta_log").glob("*.json"))
        actions = [json.loads(line) for line in log.read_text().splitlines()]
        for action in actions:
            if "add" in action:
                action["add"].pop("stats", None)
        log.write_text("\n".join(json.dumps(action) for action in actions) + "\n")
    snapshot = DeltaTable(str(tmp_path))
    write_deltalake(tmp_path, pa.table({"a": range(100, 103)}), mode="overwrite")
    scan = pl.scan_delta
    calls = []
    options = {}

    def pinned_scan(path, **kwargs):
        calls.append((path, kwargs))
        return scan(path, **kwargs)

    monkeypatch.setattr(pl, "scan_delta", pinned_scan)
    page, total = read_delta_preview(snapshot, 2, options)
    assert page.to_pydict() == {"a": [0, 1]}
    assert total == 20
    assert len(calls) == (1 if with_statistics else 2)
    assert all(path == snapshot.table_uri for path, _ in calls)
    assert all(kwargs["version"] == 0 and kwargs["storage_options"] is options for _, kwargs in calls)
