"""``shared.flow_merge.records``: a flow's schedule file merged field by field, schedule by schedule."""

import copy
import subprocess
import sys

from shared.flow_merge.conflict import conflict_document, is_conflicted, open_conflicts, resolve
from shared.flow_merge.records import merge_records
from shared.flow_merge.yaml_io import dump, load

from .fixtures import REPO_ROOT


def _base() -> dict:
    return {
        "flow_uuid": "6b1d0c2e-0000-0000-0000-00000000000a",
        "schedules": [
            {
                "id": "s-nightly",
                "name": "nightly",
                "enabled": True,
                "schedule_type": "cron",
                "interval_seconds": None,
                "cron_expression": "0 2 * * *",
                "cron_timezone": "UTC",
                "description": None,
            },
            {
                "id": "s-hourly",
                "name": "hourly",
                "enabled": True,
                "schedule_type": "interval",
                "interval_seconds": 3600,
                "cron_expression": None,
                "cron_timezone": None,
                "description": None,
            },
        ],
    }


def _entry(document: dict, schedule_id: str) -> dict:
    return next(e for e in document["schedules"] if e["id"] == schedule_id)


def test_two_fields_of_one_schedule_combine():
    base = _base()
    ours, theirs = copy.deepcopy(base), copy.deepcopy(base)
    _entry(ours, "s-nightly")["cron_expression"] = "0 3 * * *"
    _entry(theirs, "s-nightly")["enabled"] = False
    result = merge_records(base, ours, theirs)
    assert result.clean
    nightly = _entry(result.document, "s-nightly")
    assert (nightly["cron_expression"], nightly["enabled"]) == ("0 3 * * *", False)


def test_same_field_conflicts():
    base = _base()
    ours, theirs = copy.deepcopy(base), copy.deepcopy(base)
    _entry(ours, "s-nightly")["cron_expression"] = "0 3 * * *"
    _entry(theirs, "s-nightly")["cron_expression"] = "0 4 * * *"
    result = merge_records(base, ours, theirs)
    [conflict] = result.conflicts
    assert (conflict.key, conflict.kind) == ("schedule:s-nightly", "both_changed")
    assert "cron_expression" in conflict.message
    document = conflict_document(result)
    assert is_conflicted(document) and document["flow_uuid"] == base["flow_uuid"]
    settled = resolve(document, {"schedule:s-nightly": "theirs"})
    assert _entry(settled, "s-nightly")["cron_expression"] == "0 4 * * *"


def test_a_schedule_added_on_each_side_is_kept_twice():
    base = _base()
    ours, theirs = copy.deepcopy(base), copy.deepcopy(base)
    ours["schedules"].append({**_entry(base, "s-hourly"), "id": "s-a", "name": "a"})
    theirs["schedules"].append({**_entry(base, "s-hourly"), "id": "s-b", "name": "b"})
    result = merge_records(base, ours, theirs)
    assert result.clean
    assert [e["id"] for e in result.document["schedules"]] == ["s-nightly", "s-hourly", "s-a", "s-b"]


def test_removed_on_one_side_and_changed_on_the_other():
    base = _base()
    ours, theirs = copy.deepcopy(base), copy.deepcopy(base)
    ours["schedules"] = [e for e in ours["schedules"] if e["id"] != "s-hourly"]
    _entry(theirs, "s-hourly")["interval_seconds"] = 60
    result = merge_records(base, ours, theirs)
    assert [(c.key, c.kind) for c in result.conflicts] == [("schedule:s-hourly", "delete_vs_change")]
    assert [e["id"] for e in result.document["schedules"]] == ["s-nightly"]
    settled = resolve(conflict_document(result), {"schedule:s-hourly": "theirs"})
    assert _entry(settled, "s-hourly")["interval_seconds"] == 60


def test_a_removal_on_one_side_alone_removes():
    base = _base()
    ours = copy.deepcopy(base)
    ours["schedules"] = ours["schedules"][:1]
    result = merge_records(base, ours, copy.deepcopy(base))
    assert result.clean and [e["id"] for e in result.document["schedules"]] == ["s-nightly"]


def test_merge_file_merges_a_schedule_file(tmp_path):
    base = _base()
    ours, theirs = copy.deepcopy(base), copy.deepcopy(base)
    _entry(ours, "s-nightly")["cron_expression"] = "0 3 * * *"
    _entry(theirs, "s-hourly")["enabled"] = False
    paths = {}
    for name, data in (("base", base), ("ours", ours), ("theirs", theirs)):
        paths[name] = tmp_path / f"{name}.yaml"
        paths[name].write_text(dump(data, "project"))
    result = subprocess.run(
        [sys.executable, "-m", "shared.flow_merge", "merge-file", str(paths["base"]), str(paths["ours"]), str(paths["theirs"])],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    merged = load(paths["ours"].read_text())
    assert _entry(merged, "s-nightly")["cron_expression"] == "0 3 * * *"
    assert _entry(merged, "s-hourly")["enabled"] is False
    assert not open_conflicts(merged)
