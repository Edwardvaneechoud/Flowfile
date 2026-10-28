"""Latest and pinned previews count the snapshot even without optional statistics."""

import json

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaProtocolError

from flowfile_worker.funcs import _delta_preview_payload, read_delta_preview, read_delta_version_preview


@pytest.mark.parametrize("version, expected", [(None, 250), (0, 20), (1, 250)])
@pytest.mark.parametrize("with_statistics", [True, False])
def test_delta_preview_snapshot_total(tmp_path, version, expected, with_statistics):
    path = tmp_path / "table"
    write_deltalake(path, pa.table({"a": range(20)}))
    write_deltalake(path, pa.table({"a": range(250)}), mode="overwrite")

    if not with_statistics:
        # Delta add-action statistics are optional; keep the real Parquet data.
        for log in (path / "_delta_log").glob("*.json"):
            actions = [json.loads(line) for line in log.read_text().splitlines()]
            for action in actions:
                if "add" in action:
                    action["add"].pop("stats", None)
            log.write_text("\n".join(json.dumps(action) for action in actions) + "\n")

    if version is None:
        result = read_delta_preview("table", n_rows=7, base_uri=str(tmp_path))
    else:
        result = read_delta_version_preview("table", version, n_rows=7, base_uri=str(tmp_path))
        assert result.version == version

    assert result.columns == ["a"]
    assert result.dtypes == ["int64"]
    assert result.rows == [[i] for i in range(7)]
    assert result.total_rows == expected


@pytest.mark.parametrize("raw_count", [1.5, "20"])
def test_malformed_raw_statistics_count_actual_rows(tmp_path, raw_count):
    path = tmp_path / "table"
    write_deltalake(path, pa.table({"a": range(23)}))
    log = next((path / "_delta_log").glob("*.json"))
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "add" in action:
            action["add"]["stats"] = json.dumps({"numRecords": raw_count})
    log.write_text("\n".join(json.dumps(action) for action in actions) + "\n")
    result = read_delta_preview("table", n_rows=7, base_uri=str(tmp_path))
    assert len(result.rows) == 7
    assert result.total_rows == 23


@pytest.fixture
def fragmented_delta(tmp_path):
    path = tmp_path / "table"
    for i in range(40):
        write_deltalake(path, pa.table({"a": [i]}), mode="append")
    return path


@pytest.mark.parametrize("version", [None, 39])
def test_unreadable_later_fragment_propagates_error(fragmented_delta, version):
    path = fragmented_delta
    dt = DeltaTable(str(path), version=version)
    dataset = dt.to_pyarrow_dataset()
    victim = path / list(dataset.get_fragments())[-1].path
    hidden = victim.with_suffix(".unavailable")
    victim.rename(hidden)
    try:
        assert dataset.head(1).num_rows == 1
        with pytest.raises(FileNotFoundError):
            if version is None:
                read_delta_preview("table", n_rows=1, base_uri=str(path.parent))
            else:
                read_delta_version_preview("table", version, n_rows=1, base_uri=str(path.parent))
    finally:
        hidden.rename(victim)


@pytest.mark.parametrize("statistics", ["partial", "none"])
def test_incomplete_statistics_count_actual_rows(fragmented_delta, statistics):
    path = fragmented_delta
    logs = sorted((path / "_delta_log").glob("*.json"))
    for log in logs[:1] if statistics == "partial" else logs:
        actions = [json.loads(line) for line in log.read_text().splitlines()]
        for action in actions:
            if "add" in action:
                action["add"].pop("stats", None)
        log.write_text("\n".join(json.dumps(action) for action in actions) + "\n")
    assert _delta_preview_payload(DeltaTable(str(path)), 1)[3] == 40


def test_payload_retains_open_snapshot_after_overwrite(tmp_path):
    path = tmp_path / "table"
    write_deltalake(path, pa.table({"a": range(20)}))
    dt = DeltaTable(str(path), version=0)
    write_deltalake(path, pa.table({"a": range(3)}), mode="overwrite")
    assert _delta_preview_payload(dt, 1)[3] == 20
    assert dt.version() == 0
    assert read_delta_preview("table", 1, base_uri=str(tmp_path)).total_rows == 3


@pytest.mark.parametrize("feature", ["deletionVectors", "columnMapping"])
def test_payload_preserves_arrow_protocol_refusal(tmp_path, feature):
    path = tmp_path / "table"
    write_deltalake(path, pa.table({"a": [1]}))
    log = next((path / "_delta_log").glob("*.json"))
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "protocol" in action:
            action["protocol"] = {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": [feature],
                "writerFeatures": [feature],
            }
    log.write_text("\n".join(json.dumps(action) for action in actions) + "\n")
    with pytest.raises(DeltaProtocolError):
        _delta_preview_payload(DeltaTable(str(path)), 1)
