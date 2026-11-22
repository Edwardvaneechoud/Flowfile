"""``shared.flow_merge.diff`` and its review text: what changed, step by step, with layout and noise kept apart."""

import copy
import json
import subprocess
import sys

import pytest

from shared.flow_merge.diff import MISSING, diff
from shared.flow_merge.render import render_diff, show, summary
from shared.flow_merge.yaml_io import dump

from .fixtures import REPO_ROOT, fixture_id, flow_files, load

BASE = REPO_ROOT / "docs/assets/flows/sales_dashboard.yaml"


def _base() -> dict:
    return load(BASE)


def _step(document: dict, step_id: int) -> dict:
    return next(n for n in document["nodes"] if n["id"] == step_id)


def _changes(result) -> list[tuple]:
    return [(c.kind, c.item, c.id, c.aspects) for c in result.changes]


@pytest.mark.parametrize("path", flow_files(), ids=fixture_id)
def test_a_flow_against_itself_is_no_change(path):
    document = load(path)
    result = diff(document, copy.deepcopy(document))
    assert result.empty and not result.layout and not result.format


def test_added_removed_changed():
    a, b = _base(), _base()
    _step(b, 6)["setting_input"]["join_input"]["how"] = "inner"
    removed = _step(b, 10)
    b["nodes"].remove(removed)
    for node in b["nodes"]:
        node["outputs"] = [t for t in node.get("outputs") or [] if t != 10]
    b["nodes"].append({**copy.deepcopy(_step(a, 3)), "id": 50, "input_ids": [2], "outputs": []})
    result = diff(a, b)
    assert ("changed", "step", 6, ("settings",)) in _changes(result)
    assert ("removed", "step", 10, ()) in _changes(result)
    assert ("added", "step", 50, ()) in _changes(result)
    join = next(c for c in result.changes if c.id == 6)
    assert [(f.path, f.before, f.after) for f in join.fields] == [("settings.join_input.how", "left", "inner")]


def test_move_is_layout_only():
    a, b = _base(), _base()
    _step(b, 6)["x_position"] += 300
    _step(b, 6)["y_position"] -= 50
    result = diff(a, b)
    assert result.empty
    assert [(c.id, c.aspects) for c in result.layout] == [(6, ("position",))]


def test_rewire_reported_on_target():
    """Changing which step feeds a join is one change, on the join, whichever end of the edge the file changed."""
    a, b = _base(), _base()
    join = _step(b, 6)
    old_right = join["right_input_id"]
    join["right_input_id"] = 4
    _step(b, old_right)["outputs"] = [t for t in _step(b, old_right)["outputs"] if t != 6]
    result = diff(a, b)
    assert _changes(result) == [("changed", "step", 6, ("inputs",))]
    assert [(f.path, f.before, f.after) for f in result.changes[0].fields] == [
        ("inputs.input-1", [[old_right, "output-0"]], [[4, "output-0"]])
    ]


def test_description_only_change():
    a, b = _base(), _base()
    _step(b, 7)["description"] = "Products ranked by revenue"
    assert _changes(diff(a, b)) == [("changed", "step", 7, ("description",))]


def test_a_description_that_follows_the_settings_is_not_its_own_change():
    a, b = _base(), _base()
    _step(b, 6)["setting_input"]["join_input"]["how"] = "inner"
    _step(b, 6)["description"] = "inner join on product_line"
    assert _changes(diff(a, b)) == [("changed", "step", 6, ("settings",))]


def test_type_change_is_removed_and_added():
    a, b = _base(), _base()
    _step(b, 7)["type"] = "sort"
    assert _changes(diff(a, b)) == [("removed", "step", 7, ("type",)), ("added", "step", 7, ("type",))]


def test_noise_is_left_out():
    a, b = _base(), _base()
    b["flowfile_id"] = 1
    b["flowfile_settings"]["source_registration_id"] = 42
    b["flowfile_settings"]["hub_origin"] = {"hub_url": "https://hub", "flow_uuid": "u", "version": 3}
    _step(b, 1)["is_start_node"] = not _step(b, 1)["is_start_node"]
    _step(b, 1)["setting_input"]["received_file"]["abs_file_path"] = "/home/someone/supermarket_sales.csv"
    result = diff(a, b)
    assert result.empty and not result.layout


def test_a_schema_a_source_cached_is_noise():
    step = {"id": 1, "type": "database_reader", "setting_input": {"fields": [{"name": "a"}], "query": "select 1"}}
    a = {"nodes": [step]}
    b = {"nodes": [{**step, "setting_input": {"fields": [{"name": "a"}, {"name": "b"}], "query": "select 1"}}]}
    assert diff(a, b).empty


def test_keys_of_a_newer_version_are_format():
    a, b = _base(), _base()
    b["flowfile_version"] = "9.9.9"
    _step(b, 6)["setting_input"]["new_option"] = False
    result = diff(a, b)
    assert result.empty
    assert [(c.id, [f.path for f in c.fields]) for c in result.format] == [(6, ["settings.new_option"])]


def test_a_key_added_in_the_same_version_is_a_change():
    a, b = _base(), _base()
    _step(b, 6)["setting_input"]["new_option"] = False
    change = diff(a, b).changes[0]
    assert (change.id, change.fields[0].before, change.fields[0].after) == (6, MISSING, False)


def test_parameter_and_setting_changes():
    a, b = _base(), _base()
    a["flowfile_settings"]["parameters"] = [{"name": "region", "default_value": "North"}]
    b["flowfile_settings"]["parameters"] = [
        {"name": "region", "default_value": "South"},
        {"name": "year", "default_value": "2026"},
    ]
    b["flowfile_settings"]["execution_mode"] = "Performance"
    assert _changes(diff(a, b)) == [
        ("changed", "setting", "execution_mode", ("execution_mode",)),
        ("changed", "parameter", "region", ("default_value",)),
        ("added", "parameter", "year", ()),
    ]


def test_group_rename_is_a_change_and_its_box_is_layout():
    a = {"nodes": [], "groups": [{"id": 1, "name": "clean", "x_position": 0, "y_position": 0, "width": 10}]}
    b = copy.deepcopy(a)
    b["groups"][0]["name"] = "clean up"
    b["groups"][0]["width"] = 20
    result = diff(a, b)
    assert _changes(result) == [("changed", "group", 1, ("name",))]
    assert [(c.item, c.id) for c in result.layout] == [("group", 1)]


def test_summary_counts_by_kind():
    a, b = _base(), _base()
    _step(b, 6)["setting_input"]["join_input"]["how"] = "inner"
    _step(b, 7)["description"] = "x"
    b["nodes"].append({**copy.deepcopy(_step(a, 3)), "id": 50, "input_ids": [], "outputs": []})
    assert summary(diff(a, b)) == "3 steps (2 changed, 1 added)"
    assert summary(diff(a, a)) == "no change"


def test_review_text_names_the_value_that_changed():
    a, b = _base(), _base()
    _step(b, 6)["setting_input"]["join_input"]["how"] = "inner"
    _step(b, 6)["x_position"] += 10
    text = render_diff(diff(a, b), name="sales_dashboard")
    assert "settings.join_input.how: 'left' → 'inner'" in text
    assert "1 layout change not shown" in text
    assert "x_position" not in text
    assert "x_position: " in render_diff(diff(a, b), layout=True)
    md = render_diff(diff(a, b), "md", name="sales_dashboard")
    assert md.startswith("**sales_dashboard: 1 step (1 changed)**")
    data = json.loads(render_diff(diff(a, b), "json"))
    assert data["changes"][0]["fields"][0] == {
        "path": "settings.join_input.how",
        "before": "left",
        "after": "inner",
        "before_absent": False,
        "after_absent": False,
    }


def test_show_has_no_positions_or_noise():
    a, b = _base(), _base()
    for node in b["nodes"]:
        node["x_position"] += 100
    b["flowfile_id"] = 7
    assert show(a) == show(b)
    assert "step 6 join" in show(a)
    assert "x_position" not in show(a)


def _cli(*args, cwd=REPO_ROOT):
    return subprocess.run(
        [sys.executable, "-m", "shared.flow_merge", *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=str(cwd),
        timeout=60,
    )


def test_cli_diff_markdown(tmp_path):
    b = _base()
    _step(b, 6)["setting_input"]["join_input"]["how"] = "inner"
    changed = tmp_path / "sales_dashboard.flow.yaml"
    changed.write_text(dump(b), encoding="utf-8")
    result = _cli("diff", "--format", "md", str(BASE), str(changed))
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("**sales_dashboard: 1 step (1 changed)**")
    assert _cli("diff", "--exit-code", str(BASE), str(changed)).returncode == 1
    assert _cli("diff", "--exit-code", str(BASE), str(BASE)).returncode == 0


def test_cli_show_and_an_unreadable_file(tmp_path):
    result = _cli("show", str(BASE))
    assert result.returncode == 0 and "step 6 join" in result.stdout
    broken = tmp_path / "broken.yaml"
    broken.write_text("nodes: [", encoding="utf-8")
    result = _cli("diff", str(BASE), str(broken))
    assert result.returncode == 2
    assert "cannot read" in result.stderr
