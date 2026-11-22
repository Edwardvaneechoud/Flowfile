"""``shared.flow_merge.merge``: two sets of changes to one flow combined step by step, and the true conflicts named."""

from shared.flow_merge.canonical import canonical
from shared.flow_merge.diff import diff
from shared.flow_merge.edges import MAIN, RIGHT
from shared.flow_merge.merge import merge

from .editing import add_step, copied, remove_step, sales, set_inputs, settings, step


def _ids(document: dict) -> list[int]:
    return [n["id"] for n in document["nodes"]]


def _inputs(document: dict, step_id: int) -> dict:
    flow, _ = canonical(document)
    return {h: [s for s, _ in es] for h, es in flow.inputs.get(step_id, {}).items()}


def test_changes_to_different_steps_combine():
    base = sales()
    ours, theirs = copied(base), copied(base)
    settings(ours, 6)["join_input"]["how"] = "inner"
    settings(theirs, 10)["pivot_input"]["aggregations"] = ["mean"]
    result = merge(base, ours, theirs)
    assert result.clean
    assert settings(result.document, 6)["join_input"]["how"] == "inner"
    assert settings(result.document, 10)["pivot_input"]["aggregations"] == ["mean"]
    assert [(c.id, c.aspects) for c in result.changes.changes] == [(10, ("settings",))]


def test_the_same_change_on_both_sides_is_one_change():
    base = sales()
    ours, theirs = copied(base), copied(base)
    for side in (ours, theirs):
        settings(side, 6)["join_input"]["how"] = "inner"
    result = merge(base, ours, theirs)
    assert result.clean and result.changes.empty
    assert result.document == ours


def test_merge_is_idempotent():
    base = sales()
    ours = copied(base)
    settings(ours, 6)["join_input"]["how"] = "inner"
    ours = add_step(ours, 16, "sort", {MAIN: [8]})
    assert merge(base, ours, ours).document == ours
    assert merge(base, ours, base).document == ours
    assert merge(base, base, ours).document == ours


def test_both_add_same_id_renumbers_incoming():
    base = sales()
    ours = add_step(copied(base), 16, "sort", {MAIN: [8]}, setting_input={"mine": True})
    theirs = add_step(copied(base), 16, "filter", {MAIN: [14]}, setting_input={"theirs": True})
    result = merge(base, ours, theirs)
    assert result.clean
    assert result.id_map.nodes == {16: 17}
    assert step(result.document, 16)["type"] == "sort"
    assert step(result.document, 17)["type"] == "filter"
    assert _inputs(result.document, 17) == {MAIN: [14]}
    assert 17 in step(result.document, 14)["outputs"]


def test_both_adding_the_same_step_is_one_step():
    base = sales()
    ours = add_step(copied(base), 16, "sort", {MAIN: [8]})
    theirs = add_step(copied(base), 16, "sort", {MAIN: [8]})
    result = merge(base, ours, theirs)
    assert result.clean and result.id_map.nodes == {}
    assert _ids(result.document).count(16) == 1


def test_renumber_rewrites_every_reference():
    """A chain theirs added, a model step naming its training step, and a group: all follow the new ids."""
    base = sales()
    base["groups"] = []
    ours = add_step(copied(base), 16, "sort", {MAIN: [8]})
    theirs = copied(base)
    theirs["groups"] = [{"id": 1, "name": "scoring", "x_position": 0, "y_position": 0, "width": 10, "height": 10}]
    theirs = add_step(theirs, 16, "train_model", {MAIN: [14]}, group_id=1)
    theirs = add_step(
        theirs, 17, "apply_model", {MAIN: [16]}, setting_input={"apply_input": {"source": "upstream", "upstream_node_id": 16}}
    )
    result = merge(base, ours, theirs)
    assert result.clean
    assert result.id_map.nodes == {16: 18}, "only the clashing id moves; theirs' step 17 clashes with nothing"
    assert _inputs(result.document, 17) == {MAIN: [18]}
    assert settings(result.document, 17)["apply_input"]["upstream_node_id"] == 18
    assert step(result.document, 18)["group_id"] == 1
    assert step(result.document, 16)["type"] == "sort"


def test_base_ids_never_change():
    base = sales()
    ours = add_step(copied(base), 16, "sort", {MAIN: [8]})
    theirs = add_step(copied(base), 16, "sort", {MAIN: [9]})
    settings(theirs, 3)["functions"] = []
    result = merge(base, ours, theirs)
    for sid in _ids(base):
        assert step(result.document, sid)["type"] == step(base, sid)["type"]
    assert set(result.id_map.nodes) == {16}


def test_keep_ids_theirs():
    """Merging production into a feature keeps production's ids: the feature's new step moves."""
    base = sales()
    ours = add_step(copied(base), 16, "sort", {MAIN: [8]})
    theirs = add_step(copied(base), 16, "filter", {MAIN: [14]})
    result = merge(base, ours, theirs, keep_ids="theirs")
    assert step(result.document, 16)["type"] == "filter"
    assert step(result.document, 17)["type"] == "sort"


def test_deleted_step_takes_its_edges():
    base = sales()
    ours = remove_step(copied(base), 11)
    theirs = copied(base)
    settings(theirs, 6)["join_input"]["how"] = "inner"
    result = merge(base, ours, theirs)
    assert result.clean
    assert 11 not in _ids(result.document)
    assert 11 not in step(result.document, 10)["outputs"]


def test_a_rewire_wins_over_an_edge_lost_to_a_removal():
    """Ours removes the manual input feeding the join; theirs feeds the join from another step instead."""
    base = sales()
    ours = remove_step(copied(base), 5)
    theirs = set_inputs(copied(base), 6, RIGHT, (12,))
    result = merge(base, ours, theirs)
    assert result.clean
    assert 5 not in _ids(result.document)
    assert _inputs(result.document, 6)[RIGHT] == [12]


def test_additive_resave_is_not_a_change():
    """Theirs was saved by a newer Flowfile that writes one more key; ours changed the step."""
    base = sales()
    ours, theirs = copied(base), copied(base)
    settings(ours, 6)["join_input"]["how"] = "inner"
    settings(theirs, 6)["new_option"] = False
    result = merge(base, ours, theirs)
    assert result.clean
    assert settings(result.document, 6)["join_input"]["how"] == "inner"
    assert settings(result.document, 6)["new_option"] is False
    assert "mixed_versions" in [w.code for w in result.warnings]


def test_union_inputs_from_both_sides():
    base = add_step(sales(), 16, "union", {MAIN: [8, 14]})
    ours = set_inputs(copied(base), 16, MAIN, (8, 14, 4))
    theirs = set_inputs(copied(base), 16, MAIN, (14, 10))
    result = merge(base, ours, theirs)
    assert result.clean
    assert _inputs(result.document, 16) == {MAIN: [14, 4, 10]}
    assert "union_inputs" in [w.code for w in result.warnings]


def test_group_and_comment_id_collisions():
    base = sales()
    base["groups"], base["comments"] = [], []
    ours, theirs = copied(base), copied(base)
    ours["groups"] = [{"id": 1, "name": "ours", "x_position": 0, "y_position": 0, "width": 1, "height": 1}]
    theirs["groups"] = [{"id": 1, "name": "theirs", "x_position": 0, "y_position": 0, "width": 1, "height": 1}]
    ours["comments"] = [{"id": 1, "text": "ours", "x_position": 0, "y_position": 0, "width": 1, "height": 1}]
    theirs["comments"] = [{"id": 1, "text": "theirs", "x_position": 0, "y_position": 0, "width": 1, "height": 1}]
    step(theirs, 3)["group_id"] = 1
    result = merge(base, ours, theirs)
    assert [(g["id"], g["name"]) for g in result.document["groups"]] == [(1, "ours"), (2, "theirs")]
    assert [(c["id"], c["text"]) for c in result.document["comments"]] == [(1, "ours"), (2, "theirs")]
    assert step(result.document, 3)["group_id"] == 2
    assert result.id_map.groups == {1: 2} and result.id_map.comments == {1: 2}


def test_reference_collision_renames_incoming():
    base = sales()
    ours, theirs = copied(base), copied(base)
    step(ours, 6)["node_reference"] = "revenue"
    step(theirs, 12)["node_reference"] = "revenue"
    result = merge(base, ours, theirs)
    assert step(result.document, 6)["node_reference"] == "revenue"
    assert step(result.document, 12)["node_reference"] == "revenue_2"
    assert "reference_renamed" in [w.code for w in result.warnings]


def test_df_id_in_code_warns():
    base = sales()
    ours = add_step(copied(base), 16, "sort", {MAIN: [8]})
    theirs = add_step(copied(base), 16, "sort", {MAIN: [14]}, setting_input={"x": 1})
    theirs = add_step(
        theirs, 17, "python_script", {MAIN: [16]}, setting_input={"python_script_input": {"code": "out = df_16.head()"}}
    )
    result = merge(base, ours, theirs)
    codes = {w.code: w for w in result.warnings}
    assert "code_names_renumbered_step" in codes
    assert "df_16" in codes["code_names_renumbered_step"].message


def test_a_new_step_does_not_cover_another():
    base = sales()
    ours = add_step(copied(base), 16, "sort", {MAIN: [8]}, x_position=2000, y_position=600)
    theirs = add_step(copied(base), 16, "filter", {MAIN: [14]}, x_position=2000, y_position=600)
    result = merge(base, ours, theirs)
    placed = step(result.document, 17)
    assert (placed["x_position"], placed["y_position"]) != (2000, 600)


def test_layout_and_descriptions_merge_per_field():
    base = sales()
    ours, theirs = copied(base), copied(base)
    step(ours, 6)["x_position"] = 900
    step(theirs, 6)["y_position"] = 500
    step(theirs, 7)["description"] = "Products ranked by revenue"
    result = merge(base, ours, theirs)
    assert result.clean
    assert (step(result.document, 6)["x_position"], step(result.document, 6)["y_position"]) == (900, 500)
    assert step(result.document, 7)["description"] == "Products ranked by revenue"


def test_both_changed_step_is_a_conflict():
    base = sales()
    ours, theirs = copied(base), copied(base)
    settings(ours, 6)["join_input"]["how"] = "inner"
    settings(theirs, 6)["join_input"]["how"] = "outer"
    result = merge(base, ours, theirs)
    [conflict] = result.conflicts
    assert (conflict.key, conflict.kind) == ("step:6", "both_changed")
    assert conflict.ours["step"]["setting_input"]["join_input"]["how"] == "inner"
    assert conflict.theirs["step"]["setting_input"]["join_input"]["how"] == "outer"
    assert settings(result.document, 6)["join_input"]["how"] == "inner", "the document keeps ours while it is open"


def test_delete_vs_change():
    base = sales()
    ours = remove_step(copied(base), 10)
    theirs = copied(base)
    settings(theirs, 10)["pivot_input"]["aggregations"] = ["mean"]
    result = merge(base, ours, theirs)
    [conflict] = result.conflicts
    assert (conflict.key, conflict.kind) == ("step:10", "delete_vs_change")
    assert conflict.ours is None or conflict.ours["step"] is None
    assert conflict.theirs["step"]["setting_input"]["pivot_input"]["aggregations"] == ["mean"]
    assert conflict.theirs["readers"] == {11: {MAIN: [[10, "output-0"]]}}
    assert 10 not in _ids(result.document)


def test_deleted_but_connected():
    base = sales()
    ours = remove_step(copied(base), 5)
    theirs = set_inputs(copied(base), 13, "input-1", (5,))
    result = merge(base, ours, theirs)
    [conflict] = result.conflicts
    assert (conflict.key, conflict.kind, conflict.related) == ("step:5", "deleted_but_connected", (13,))


def test_inputs_both_changed():
    base = sales()
    ours = set_inputs(copied(base), 6, RIGHT, (12,))
    theirs = set_inputs(copied(base), 6, RIGHT, (13,))
    result = merge(base, ours, theirs)
    [conflict] = result.conflicts
    assert (conflict.key, conflict.kind) == ("step:6", "inputs_both_changed")
    assert _inputs(result.document, 6)[RIGHT] == [12]


def test_parameter_conflict():
    base = sales()
    base["flowfile_settings"]["parameters"] = [{"name": "region", "default_value": "North"}]
    ours, theirs = copied(base), copied(base)
    ours["flowfile_settings"]["parameters"][0]["default_value"] = "South"
    theirs["flowfile_settings"]["parameters"][0]["default_value"] = "East"
    result = merge(base, ours, theirs)
    assert [(c.key, c.kind) for c in result.conflicts] == [("parameter:region", "parameter_both_changed")]


def test_a_merged_flow_reads_once_without_repairs():
    base = sales()
    ours = add_step(remove_step(copied(base), 11), 16, "sort", {MAIN: [8]})
    theirs = add_step(set_inputs(copied(base), 6, RIGHT, (12,)), 16, "filter", {MAIN: [14]})
    result = merge(base, ours, theirs)
    _, issues = canonical(result.document)
    assert issues == []
    assert diff(result.document, result.document).empty
