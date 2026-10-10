"""``reconcile``: live vs relabelled clean-run payload -> editor ops, over hand-built pairs."""

import copy

from flowfile_core.flowfile.util.layout.placement import Box, node_box
from flowfile_core.notebook.reconcile import incoming_edges, reconcile, user_description


def _node(node_id, node_type, settings=None, *, inputs=None, left=None, right=None, keyed=None, **extra):
    node = {
        "id": node_id,
        "type": node_type,
        "description": extra.pop("description", ""),
        "node_reference": extra.pop("node_reference", None),
        "x_position": extra.pop("x", 100 * node_id),
        "y_position": extra.pop("y", 50),
        "left_input_id": left,
        "right_input_id": right,
        "input_ids": inputs,
        "outputs": [],
        "output_handles": [],
        "input_connections": keyed,
        "setting_input": settings,
    }
    node.update(extra)
    return node


def _payload(*nodes, parameters=None):
    """Fill every source's ``outputs``/``output_handles`` from the targets' inputs (handles via ``handles``)."""
    by_id = {n["id"]: n for n in nodes}
    for node in nodes:
        for source in node.get("input_ids") or []:
            by_id[source]["outputs"].append(node["id"])
            by_id[source]["output_handles"].append(node.get("_handles", {}).get(source, "output-0"))
        for field in ("left_input_id", "right_input_id"):
            if node.get(field) is not None:
                by_id[node[field]]["outputs"].append(node["id"])
                by_id[node[field]]["output_handles"].append(node.get("_handles", {}).get(node[field], "output-0"))
        for conn in node.get("input_connections") or []:
            by_id[conn["from_id"]]["outputs"].append(node["id"])
            by_id[conn["from_id"]]["output_handles"].append(conn["source_handle"])
    for node in nodes:
        node.pop("_handles", None)
    return {"flowfile_id": 7, "flowfile_settings": {"parameters": parameters or []}, "nodes": list(nodes)}


def _manual(node_id=1, values=(1, 2, 3)):
    return _node(
        node_id,
        "manual_input",
        {"raw_data_format": {"columns": [{"name": "a", "data_type": "Int64"}], "data": [list(values)]}},
    )


def _filter(node_id, source, formula="[a] > 1", **extra):
    settings = {"filter_input": {"mode": "advanced", "advanced_filter": formula}, "split_mode": False}
    return _node(node_id, "filter", settings, inputs=[source], **extra)


def _sort(node_id, source, column="a", **extra):
    settings = {"sort_input": [{"column": column, "how": "asc"}]}
    return _node(node_id, "sort", settings, inputs=[source], **extra)


def _cells(*node_ids, extra=None):
    cells = {f"node-{i}": [i] for i in node_ids}
    cells.update(extra or {})
    return cells


def _ops(plan):
    return [op.model_dump(mode="json") for op in plan.operations]


def _kinds(plan):
    return [op.op for op in plan.operations]


def _edge(op):
    c = op["connection"]
    return (
        c["output_connection"]["node_id"],
        c["output_connection"]["connection_class"],
        c["input_connection"]["node_id"],
        c["input_connection"]["connection_class"],
    )


def test_unchanged_flow_reconciles_to_no_ops():
    live = _payload(_manual(), _filter(2, 1), _sort(3, 2))
    session = copy.deepcopy(live)
    plan = reconcile(live, session, ["node-1", "node-2", "node-3"], _cells(1, 2, 3))
    assert plan.operations == [] and plan.warnings == [] and plan.deletions == []


def test_cosmetic_formula_difference_is_no_op():
    live = _payload(_manual(), _filter(2, 1, "[a] > 1"))
    session = _payload(_manual(), _filter(2, 1, "([a] > 1)"))
    assert reconcile(live, session, ["node-2"], _cells(1, 2)).operations == []


def test_edited_cell_updates_only_its_node():
    live = _payload(_manual(), _filter(2, 1, "[a] > 1"), _sort(3, 2))
    session = _payload(_manual(), _filter(2, 1, "[a] > 2"), _sort(3, 2))
    ops = _ops(reconcile(live, session, ["node-2"], _cells(1, 2, 3)))
    assert [op["op"] for op in ops] == ["update_settings"]
    body = ops[0]["settings"]
    assert ops[0]["node_type"] == "filter"
    assert body["node_id"] == 2 and body["flow_id"] == 7 and body["depending_on_id"] == 1
    assert body["filter_input"]["advanced_filter"] == "[a] > 2"


def test_untouched_cell_pins_its_node():
    live = _payload(_manual(), _filter(2, 1, "[a] > 1"))
    session = _payload(_manual(), _filter(2, 1, "[a] > 2"))
    assert reconcile(live, session, [], _cells(1, 2)).operations == []


def test_new_node_is_added_next_to_its_input_connected_then_configured():
    live = _payload(_manual(), _filter(2, 1, x=300, y=80))
    session = _payload(_manual(), _filter(2, 1), _sort(5, 2))
    ops = _ops(reconcile(live, session, ["node-2"], _cells(1, 2, extra={"node-2": [2, 5]})))
    assert [op["op"] for op in ops] == ["add_node", "connect", "update_settings"]
    assert ops[0] == {"op": "add_node", "node_id": 5, "node_type": "sort", "pos_x": 550.0, "pos_y": 80.0}
    assert _edge(ops[1]) == (2, "output-0", 5, "input-0")
    assert ops[2]["settings"]["sort_input"] == [{"column": "a", "how": "asc"}]


def test_deleted_cell_deletes_its_node_with_a_warning():
    live = _payload(_manual(), _filter(2, 1), _sort(3, 2))
    session = _payload(_manual(), _filter(2, 1))
    plan = reconcile(live, session, [], {"node-1": [1], "node-2": [2]})
    assert _kinds(plan) == ["delete_node"] and plan.deletions == [3]
    assert any("Deletes node 3" in w for w in plan.warnings)


def test_pinned_node_missing_from_the_session_is_kept():
    live = _payload(_manual(), _filter(2, 1), _sort(3, 2))
    session = _payload(_manual(), _filter(2, 1))
    assert reconcile(live, session, [], _cells(1, 2, 3)).operations == []


def test_rewire_deletes_the_old_edge_and_connects_the_new_one():
    live = _payload(_manual(1), _manual(2, (4, 5)), _filter(3, 1))
    session = _payload(_manual(1), _manual(2, (4, 5)), _filter(3, 2))
    ops = _ops(reconcile(live, session, ["node-3"], _cells(1, 2, 3)))
    assert [op["op"] for op in ops] == ["delete_connection", "connect"]
    assert _edge(ops[0]) == (1, "output-0", 3, "input-0")
    assert _edge(ops[1]) == (2, "output-0", 3, "input-0")


def test_second_output_handle_is_part_of_the_edge():
    split = {"filter_input": {"mode": "advanced", "advanced_filter": "[a] > 1"}, "split_mode": True}
    live = _payload(_manual(), _node(2, "filter", split, inputs=[1]), _sort(3, 2))
    session = _payload(_manual(), _node(2, "filter", split, inputs=[1]), _sort(3, 2, _handles={2: "output-1"}))
    ops = _ops(reconcile(live, session, ["node-3"], _cells(1, 2, 3)))
    assert [op["op"] for op in ops] == ["delete_connection", "connect"]
    assert _edge(ops[0]) == (2, "output-0", 3, "input-0")
    assert _edge(ops[1]) == (2, "output-1", 3, "input-0")


def test_keyed_input_changes_by_handle():
    def run_flow(keyed):
        return _node(4, "run_flow", {"flow_path": "child.yaml"}, keyed=keyed)

    live = _payload(
        _manual(1), _manual(2), run_flow([{"from_id": 1, "input_handle": "input-0", "source_handle": "output-0"}])
    )
    session = _payload(
        _manual(1),
        _manual(2),
        run_flow(
            [
                {"from_id": 1, "input_handle": "input-0", "source_handle": "output-0"},
                {"from_id": 2, "input_handle": "input-3", "source_handle": "output-0"},
            ]
        ),
    )
    ops = _ops(reconcile(live, session, ["node-4"], _cells(1, 2, 4)))
    assert [op["op"] for op in ops] == ["connect"]
    assert _edge(ops[0]) == (2, "output-0", 4, "input-3")


def test_positional_reorder_re_adds_every_input():
    union = {"union_input": {"mode": "relaxed"}}
    live = _payload(_manual(1), _manual(2), _manual(3), _node(4, "union", union, inputs=[1, 2, 3]))
    session = _payload(_manual(1), _manual(2), _manual(3), _node(4, "union", union, inputs=[2, 1, 3]))
    ops = _ops(reconcile(live, session, ["node-4"], _cells(1, 2, 3, 4)))
    assert [op["op"] for op in ops] == ["delete_connection"] * 3 + ["connect"] * 3
    assert [_edge(op)[0] for op in ops[3:]] == [2, 1, 3]


def test_appended_positional_input_only_connects_it():
    union = {"union_input": {"mode": "relaxed"}}
    live = _payload(_manual(1), _manual(2), _manual(3), _node(4, "union", union, inputs=[1, 2]))
    session = _payload(_manual(1), _manual(2), _manual(3), _node(4, "union", union, inputs=[1, 2, 3]))
    ops = _ops(reconcile(live, session, ["node-4"], _cells(1, 2, 3, 4)))
    assert [op["op"] for op in ops] == ["connect"] and _edge(ops[0])[0] == 3


def test_join_slots_are_compared_per_handle():
    join = {"join_input": {"join_mapping": [{"left_col": "a", "right_col": "a"}], "how": "inner"}}
    live = _payload(_manual(1), _manual(2), _manual(3), _node(4, "join", join, inputs=[1], right=2))
    session = _payload(_manual(1), _manual(2), _manual(3), _node(4, "join", join, inputs=[1], right=3))
    ops = _ops(reconcile(live, session, ["node-4"], _cells(1, 2, 3, 4)))
    assert [op["op"] for op in ops] == ["delete_connection", "connect"]
    assert _edge(ops[0]) == (2, "output-0", 4, "input-1")
    assert _edge(ops[1]) == (3, "output-0", 4, "input-1")


def test_type_change_is_delete_add_and_reconnect_of_every_edge():
    live = _payload(_manual(), _filter(2, 1, x=321, y=12), _sort(3, 2))
    session = _payload(_manual(), _sort(2, 1, column="a"), _sort(3, 2))
    plan = reconcile(live, session, ["node-2"], _cells(1, 2, 3))
    ops = _ops(plan)
    assert [op["op"] for op in ops] == ["delete_node", "add_node", "connect", "update_settings", "connect"]
    assert ops[1] == {"op": "add_node", "node_id": 2, "node_type": "sort", "pos_x": 321.0, "pos_y": 12.0}
    assert _edge(ops[2]) == (1, "output-0", 2, "input-0")
    assert _edge(ops[4]) == (2, "output-0", 3, "input-0")
    assert plan.deletions == [] and any("changes type" in w for w in plan.warnings)


def test_custom_node_uses_its_own_operation():
    def custom(value):
        return _node(2, "mood_emoji", {"is_user_defined": True, "settings": {"main": {"mood": value}}}, inputs=[1])

    live = _payload(_manual(), custom("happy"))
    session = _payload(_manual(), custom("sad"))
    ops = _ops(reconcile(live, session, ["node-2"], _cells(1, 2)))
    assert [op["op"] for op in ops] == ["update_user_defined_settings"]
    assert ops[0]["node_type"] == "mood_emoji"
    assert ops[0]["settings"]["is_user_defined"] is True and ops[0]["settings"]["depending_on_ids"] == [1]


def test_parameter_change_is_one_leading_operation_with_a_warning():
    params = [{"name": "limit", "default_value": "5"}]
    live = _payload(_manual(), parameters=params)
    session = _payload(_manual(), parameters=[{"name": "limit", "default_value": "6"}])
    plan = reconcile(live, session, ["parameters"], _cells(1, extra={"parameters": []}))
    assert _kinds(plan) == ["set_flow_parameters"] and plan.parameter_changes
    assert _ops(plan)[0]["parameters"][0]["default_value"] == "6"
    same = reconcile(live, _payload(_manual(), parameters=[{"name": "limit", "default_value": "5"}]), [], _cells(1))
    assert same.operations == [] and not same.parameter_changes


def test_reference_rename_is_sent_and_a_label_counts_as_none():
    live = _payload(_manual(), _filter(2, 1, node_reference="orders"))
    renamed = _payload(_manual(), _filter(2, 1, node_reference="big_orders"))
    ops = _ops(reconcile(live, renamed, ["node-2"], _cells(1, 2)))
    assert [op["op"] for op in ops] == ["update_settings"] and ops[0]["settings"]["node_reference"] == "big_orders"
    unnamed = _payload(_manual(), _filter(2, 1, node_reference=None))
    labelled = _payload(_manual(), _filter(2, 1, node_reference="filtered_2"))
    assert reconcile(unnamed, labelled, ["node-2"], _cells(1, 2)).operations == []


def test_reference_uniqueness_is_enforced():
    live = _payload(_manual(), _filter(2, 1), _sort(3, 1, node_reference="orders"))
    session = _payload(_manual(), _filter(2, 1, node_reference="orders"), _sort(3, 1, node_reference="orders"))
    plan = reconcile(live, session, ["node-2"], _cells(1, 2, 3))
    ops = _ops(plan)
    assert [(op["op"], op["settings"]["node_id"], op["settings"]["node_reference"]) for op in ops] == [
        ("update_settings", 2, "orders"),
        ("update_settings", 3, None),
    ]
    assert any("'orders'" in w for w in plan.warnings)


def test_user_description_is_compared_and_the_auto_one_ignored():
    live = _payload(_manual(), _filter(2, 1, description="[a] > 1"))
    session = _payload(_manual(), _filter(2, 1, description="[a] > 1"))
    assert user_description(live["nodes"][1]) == ""
    assert reconcile(live, session, ["node-2"], _cells(1, 2)).operations == []
    typed = _payload(_manual(), _filter(2, 1, description="keep the big ones"))
    ops = _ops(reconcile(live, typed, ["node-2"], _cells(1, 2)))
    assert [op["op"] for op in ops] == ["update_settings"] and ops[0]["settings"]["description"] == "keep the big ones"


def test_python_script_input_key_rename_warns():
    script = {"python_script_input": {"code": "", "kernel_id": None}, "output_names": ["main"]}
    live = _payload(_manual(), _node(2, "python_script", script, inputs=[1]))
    session = _payload(
        _node(1, "manual_input", _manual()["setting_input"], node_reference="orders"),
        _node(2, "python_script", script, inputs=[1]),
    )
    plan = reconcile(live, session, ["node-1"], _cells(1, 2))
    assert any("Python Script node 2" in w and "orders" in w for w in plan.warnings)


def test_new_node_of_a_pinned_cell_is_not_added_and_its_reader_keeps_its_inputs():
    live = _payload(_manual(), _filter(2, 1), _sort(3, 2))
    session = _payload(_manual(), _filter(2, 1), _sort(9, 2), _sort(3, 9))
    plan = reconcile(live, session, [], _cells(1, 2, 3, extra={"node-2": [2, 9]}))
    assert plan.operations == []


def test_incoming_edges_keep_two_handles_of_one_source():
    split = {"filter_input": {"mode": "advanced", "advanced_filter": "[a] > 1"}, "split_mode": True}
    join = {"join_input": {"join_mapping": [], "how": "inner"}}
    payload = _payload(
        _manual(),
        _node(2, "filter", split, inputs=[1]),
        _node(3, "join", join, inputs=[2], right=2, _handles={2: "output-0"}),
    )
    payload["nodes"][1]["output_handles"] = ["output-0", "output-1"]
    assert incoming_edges(payload["nodes"][2], {n["id"]: n for n in payload["nodes"]}) == {
        "input-0": [(2, "output-0")],
        "input-1": [(2, "output-1")],
    }


def _at(node, x, y):
    node.update(x_position=x, y_position=y)
    return node


def _added(plan):
    return {op.node_id: (op.pos_x, op.pos_y) for op in plan.operations if op.op == "add_node"}


def _assert_added_nodes_cover_nothing(live, plan):
    """Every added node's box is clear of the other nodes left on the canvas, its comments and collapsed groups."""
    added = _added(plan)
    left = {n["id"]: (n["x_position"], n["y_position"]) for n in live["nodes"] if n["id"] not in plan.deletions}
    positions = {**left, **added}
    blocking = [g for g in live.get("groups", []) if g.get("collapsed")] + live.get("comments", [])
    boxes = [Box(b["x_position"], b["y_position"], b["width"], b["height"]) for b in blocking]
    for nid, (x, y) in added.items():
        others = [node_box(*p) for other, p in positions.items() if other != nid] + boxes
        assert not any(node_box(x, y).overlaps(box) for box in others), (nid, x, y)


def _group(group_id, x, y, width, height, *, collapsed=False):
    return {"id": group_id, "name": "g", "x_position": x, "y_position": y, "width": width, "height": height, "collapsed": collapsed}


def test_a_node_inserted_into_a_chain_does_not_cover_the_next_node():
    live = _payload(_at(_manual(), 0, 50), _filter(2, 1, x=250, y=50), _sort(3, 2, x=500, y=50))
    session = _payload(_manual(), _sort(5, 1), _filter(2, 5), _sort(3, 2))
    plan = reconcile(live, session, ["node-2"], _cells(1, 2, 3, extra={"node-2": [5, 2]}))
    assert _added(plan) == {5: (250.0, 150.0)}
    _assert_added_nodes_cover_nothing(live, plan)


def test_a_second_branch_does_not_cover_the_first():
    live = _payload(_at(_manual(), 0, 50), _filter(2, 1, x=250, y=50))
    session = _payload(_manual(), _filter(2, 1), _sort(5, 1))
    plan = reconcile(live, session, ["cell-5"], _cells(1, 2, extra={"cell-5": [5]}))
    assert _added(plan) == {5: (250.0, 150.0)}
    _assert_added_nodes_cover_nothing(live, plan)


def test_a_new_reader_goes_below_the_canvas_not_onto_its_first_node():
    live = _payload(_at(_manual(), 50, 50), _filter(2, 1, x=300, y=50))
    session = _payload(_manual(), _filter(2, 1), _manual(5))
    plan = reconcile(live, session, ["cell-5"], _cells(1, 2, extra={"cell-5": [5]}))
    assert _added(plan) == {5: (50.0, 230.0)}
    _assert_added_nodes_cover_nothing(live, plan)


def test_a_new_reader_joined_into_the_chain_lands_beside_the_other_join_input():
    join = {"join_input": {"join_mapping": [{"left_col": "a", "right_col": "a"}], "how": "inner"}}
    live = _payload(_at(_manual(), 0, 50), _filter(2, 1, x=250, y=50))
    session = _payload(_manual(), _filter(2, 1), _manual(5), _node(6, "join", join, inputs=[2], right=5))
    plan = reconcile(live, session, ["cell-5"], _cells(1, 2, extra={"cell-5": [5, 6]}))
    assert _added(plan) == {6: (500.0, 50.0), 5: (250.0, 150.0)}
    _assert_added_nodes_cover_nothing(live, plan)


def test_a_deleted_nodes_slot_is_free_for_its_replacement():
    live = _payload(_at(_manual(), 0, 50), _filter(2, 1, x=250, y=50))
    session = _payload(_manual(), _sort(5, 1))
    plan = reconcile(live, session, ["node-2"], _cells(1, extra={"node-2": [5]}))
    assert plan.deletions == [2]
    assert _added(plan) == {5: (250.0, 50.0)}


def test_a_node_inserted_into_a_grouped_chain_stays_in_the_group():
    live = _payload(_at(_manual(), 0, 50), _filter(2, 1, x=250, y=50), _sort(3, 2, x=500, y=50))
    live["groups"] = [_group(1, -40, -20, 600, 300)]
    session = _payload(_manual(), _sort(5, 1), _filter(2, 5), _sort(3, 2))
    plan = reconcile(live, session, ["node-2"], _cells(1, 2, 3, extra={"node-2": [5, 2]}))
    assert _added(plan) == {5: (250.0, 150.0)}
    _assert_added_nodes_cover_nothing(live, plan)


def test_a_collapsed_group_in_the_way_is_avoided():
    live = _payload(_at(_manual(), 0, 50), _filter(2, 1, x=250, y=50))
    live["groups"] = [_group(1, 500, 0, 240, 200, collapsed=True)]
    session = _payload(_manual(), _filter(2, 1), _sort(5, 2))
    plan = reconcile(live, session, ["node-2"], _cells(1, 2, extra={"node-2": [2, 5]}))
    assert _added(plan) == {5: (500.0, 250.0)}
    _assert_added_nodes_cover_nothing(live, plan)


def test_a_comment_in_the_way_is_avoided():
    live = _payload(_at(_manual(), 0, 50), _filter(2, 1, x=250, y=50))
    live["comments"] = [{"id": 1, "text": "note", "x_position": 500, "y_position": 0, "width": 240, "height": 200}]
    session = _payload(_manual(), _filter(2, 1), _sort(5, 2))
    plan = reconcile(live, session, ["node-2"], _cells(1, 2, extra={"node-2": [2, 5]}))
    assert _added(plan) == {5: (500.0, 250.0)}
    _assert_added_nodes_cover_nothing(live, plan)



# visual groups: the group routes' own ops, last


def _vgroup(group_id, name, parent=None, color=None):
    return {
        "id": group_id,
        "name": name,
        "color": color,
        "parent_group_id": parent,
        "x_position": 0.0,
        "y_position": 0.0,
        "width": 400.0,
        "height": 250.0,
        "collapsed": False,
    }


def _grouped(*nodes, groups):
    payload = _payload(*nodes)
    payload["groups"] = list(groups)
    return payload


def _in(node, group_id):
    return {**node, "group_id": group_id}


def _group_ops(plan):
    """Every group op of the plan as ``(op, ...)``, which must come after every node op."""
    kinds = ("create_group", "update_group", "nest_group", "delete_group", "add_nodes_to_group", "remove_nodes_from_group")
    ops = [op.model_dump(mode="json", exclude_none=True) for op in plan.operations]
    first = next((i for i, op in enumerate(ops) if op["op"] in kinds), len(ops))
    assert all(op["op"] in kinds for op in ops[first:])
    return ops[first:]


def test_same_grouping_under_other_ids_is_no_op():
    live = _grouped(
        _manual(), _filter(2, 1, group_id=7), _sort(3, 2, group_id=8), groups=[_vgroup(7, "Outer"), _vgroup(8, "Inner", parent=7)]
    )
    session = _grouped(
        _manual(), _filter(2, 1, group_id=1), _sort(3, 2, group_id=2), groups=[_vgroup(1, "Outer"), _vgroup(2, "Inner", parent=1)]
    )
    assert reconcile(live, session, ["node-1", "node-2", "node-3"], _cells(1, 2, 3)).operations == []


def test_a_changed_cell_moves_its_node_between_groups_matched_by_membership():
    live = _grouped(
        _in(_manual(), 8), _filter(2, 1, group_id=7), _sort(3, 2, group_id=7), groups=[_vgroup(7, "A"), _vgroup(8, "B")]
    )
    session = _grouped(
        _in(_manual(), 1), _filter(2, 1, group_id=2), _sort(3, 2, group_id=1), groups=[_vgroup(1, "B"), _vgroup(2, "A")]
    )
    plan = reconcile(live, session, ["node-3"], _cells(1, 2, 3))
    assert _group_ops(plan) == [{"op": "add_nodes_to_group", "group_id": 8, "node_ids": [3]}]


def test_a_pinned_node_keeps_the_canvas_group_whatever_the_session_says():
    live = _grouped(_manual(), _filter(2, 1, group_id=7), groups=[_vgroup(7, "A")])
    session = _grouped(_in(_manual(), 1), _filter(2, 1, group_id=1), groups=[_vgroup(1, "A")])
    assert reconcile(live, session, [], _cells(1, 2)).operations == []


def test_a_new_group_is_created_above_the_ceiling_under_its_matched_parent():
    live = _grouped(
        _manual(), _filter(2, 1, group_id=7), _sort(3, 2, group_id=7), groups=[_vgroup(7, "Outer", color="blue")]
    )
    session = _grouped(
        _manual(),
        _filter(2, 1, group_id=1),
        _sort(3, 2, group_id=2),
        groups=[_vgroup(1, "Outer", color="blue"), _vgroup(2, "Inner", parent=1, color="rose")],
    )
    plan = reconcile(live, session, ["node-3"], _cells(1, 2, 3), group_id_ceiling=20)
    assert _group_ops(plan) == [
        {
            "op": "create_group",
            "group": {"group_id": 21, "name": "Inner", "color": "rose", "parent_group_id": 7, "node_ids": [3], "child_group_ids": []},
        }
    ]


def test_a_matched_group_takes_the_cells_name_colour_and_parent():
    live = _grouped(
        _manual(), _filter(2, 1, group_id=7), _sort(3, 2, group_id=8), groups=[_vgroup(7, "A"), _vgroup(8, "B", parent=7)]
    )
    session = _grouped(
        _manual(),
        _filter(2, 1, group_id=1),
        _sort(3, 2, group_id=2),
        groups=[_vgroup(1, "Renamed", color="green"), _vgroup(2, "B")],
    )
    plan = reconcile(live, session, ["node-2", "node-3"], _cells(1, 2, 3))
    assert _group_ops(plan) == [
        {"op": "update_group", "group_id": 7, "group": {"name": "Renamed", "color": "green"}},
        {"op": "nest_group", "group_id": 8},
    ]


def test_a_colour_the_cell_leaves_out_keeps_the_canvas_tint():
    live = _grouped(_manual(), _filter(2, 1, group_id=7), groups=[_vgroup(7, "A", color="blue")])
    session = _grouped(_manual(), _filter(2, 1, group_id=1), groups=[_vgroup(1, "A")])
    assert reconcile(live, session, ["node-2"], _cells(1, 2)).operations == []


def test_a_live_group_left_without_members_is_deleted_and_its_node_ungrouped():
    live = _grouped(
        _manual(),
        _filter(2, 1, group_id=8),
        _sort(3, 2, group_id=9),
        groups=[_vgroup(7, "Outer"), _vgroup(8, "Inner", parent=7), _vgroup(9, "Gone")],
    )
    session = _grouped(
        _manual(), _filter(2, 1, group_id=2), _sort(3, 2), groups=[_vgroup(1, "Outer"), _vgroup(2, "Inner", parent=1)]
    )
    plan = reconcile(live, session, ["node-3"], _cells(1, 2, 3))
    assert _group_ops(plan) == [
        {"op": "delete_group", "group_id": 9},
        {"op": "remove_nodes_from_group", "node_ids": [3]},
    ]


def test_a_parent_with_no_direct_members_matches_through_its_sub_groups():
    live = _grouped(_manual(), _filter(2, 1, group_id=8), groups=[_vgroup(7, "Outer"), _vgroup(8, "Inner", parent=7)])
    session = _grouped(
        _manual(), _filter(2, 1, group_id=2), groups=[_vgroup(1, "Outer renamed"), _vgroup(2, "Inner", parent=1)]
    )
    plan = reconcile(live, session, ["node-2"], _cells(1, 2))
    assert _group_ops(plan) == [{"op": "update_group", "group_id": 7, "group": {"name": "Outer renamed"}}]


def test_a_deleted_node_leaves_its_group_without_a_group_op():
    live = _grouped(_manual(), _filter(2, 1, group_id=7), _sort(3, 2, group_id=7), groups=[_vgroup(7, "A")])
    session = _grouped(_manual(), _filter(2, 1, group_id=1), groups=[_vgroup(1, "A")])
    plan = reconcile(live, session, ["node-3"], _cells(1, 2))
    assert [op.op for op in plan.operations] == ["delete_node"] and plan.deletions == [3]


def test_a_group_the_node_ops_prune_is_recreated_when_its_only_member_changes_type():
    live = _grouped(_manual(), _filter(2, 1, group_id=7), groups=[_vgroup(7, "A", color="cyan")])
    session = _grouped(_manual(), _sort(2, 1, group_id=1), groups=[_vgroup(1, "A", color="cyan")])
    plan = reconcile(live, session, ["node-2"], _cells(1, 2))
    assert [op.op for op in plan.operations][:2] == ["delete_node", "add_node"]
    assert _group_ops(plan) == [
        {
            "op": "create_group",
            "group": {"group_id": 8, "name": "A", "color": "cyan", "node_ids": [2], "child_group_ids": []},
        }
    ]
