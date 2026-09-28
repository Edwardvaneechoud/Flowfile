"""Notebook cells: provenance, the cell compiler, name capture, seeding, fl.canvas_node, display and the clean run."""

import linecache
from pathlib import Path

import pytest

import flowfile as fl
from flowfile_core.flowfile import schema_callbacks
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from flowfile_frame import callable_utils, notebook
from flowfile_frame.native import NativeNodeError
from flowfile_frame.notebook_cells import (
    SeededNode,
    canvas_node,
    clean_run,
    display_payload,
    execute_cell,
    new_namespace,
    seed_session,
)
from shared.notebook_display import TABLE_MIME
from shared.storage_config import storage

DATA = "fl.from_dict({'a': [1, 2, 3], 'g': ['x', 'x', 'y']})"


@pytest.fixture
def session():
    with notebook.notebook_mode() as mode:
        yield mode, new_namespace()


def run(namespace, code, cell_id="c"):
    result = execute_cell(cell_id, code, namespace)
    assert result.ok, result.error
    return result


def reference(mode, node_id):
    return mode.graph.get_node(node_id).setting_input.node_reference


def canvas_payload():
    """A small canvas flow: source -> filter -> parameter gate (then/else) -> sort on the else side."""
    graph = fl.create_flow_graph()
    fl.add_flow_parameter(graph, fl.Parameter("mode", default="full"))
    source = fl.from_dict({"a": [1, 2, 3], "g": ["x", "x", "y"]}, flow_graph=graph)
    big = source.filter(fl.col("a") > 1)
    gate = fl.Gate(big, parameter="mode", value="full")
    gate.otherwise.sort("a")
    return graph.get_flowfile_data().model_dump(mode="json"), graph, source, big, gate


def test_provenance_records_each_created_node_once_per_cell(session):
    mode, ns = session
    first = run(ns, f"df = {DATA}\nbig = df.filter(fl.col('a') > 1)", "one")
    second = run(ns, "ordered = big.sort('a')", "two")
    assert [t for t, _ in first.created] == ["manual_input", "filter"]
    assert [t for t, _ in second.created] == ["sort"]
    assert mode.provenance == [("one", t, i) for t, i in first.created] + [("two", t, i) for t, i in second.created]
    assert len({i for _, _, i in mode.provenance}) == len(mode.provenance)


def test_failed_native_build_is_not_recorded(session):
    mode, ns = session
    run(ns, f"df = {DATA}")
    result = execute_cell("bad", "fl.Gate(df, parameter='undeclared', value=1)", ns)
    assert not result.ok and "undeclared" in result.error
    assert result.created == []
    assert [c for c, _, _ in mode.provenance] == ["c"]


def test_cell_filename_is_unique_and_registered_in_linecache(session):
    _, ns = session
    first, second = run(ns, "x = 1", "k"), run(ns, "x = 2", "k")
    assert first.filename.startswith("<cell-k-") and first.filename != second.filename
    assert linecache.getline(second.filename, 1) == "x = 2"


def test_errors_come_back_with_the_cell_filename(session):
    _, ns = session
    result = execute_cell("err", "x = 1\n1 / 0", ns)
    assert not result.ok
    assert f'File "{result.filename}", line 2' in result.error and "ZeroDivisionError" in result.error
    syntax = execute_cell("syn", "def (", ns)
    assert "SyntaxError" in syntax.error and syntax.filename.startswith("<cell-syn-")


def test_inspect_getsource_works_for_a_function_defined_in_a_cell(session):
    _, ns = session
    run(ns, "import inspect\n\ndef add_one(x):\n    return x + 1\n\nsource = inspect.getsource(add_one)")
    assert ns["source"] == "def add_one(x):\n    return x + 1\n"


def test_polars_code_reads_a_function_defined_in_a_cell(session):
    mode, ns = session
    code = f"df = {DATA}\ndef top(input_df: fl.FlowFrame): output_df = input_df.head(2)\nout = df.polars_code(top)"
    run(ns, code)
    assert reference(mode, ns["out"].node_id) == "out"
    settings = mode.graph.get_node(ns["out"].node_id).setting_input
    assert settings.polars_code_input.polars_code == "output_df = input_df.head(2)"


def test_python_script_in_a_cell_needs_no_console_hook(session, monkeypatch):
    def no_console(func):
        raise AssertionError("the console-source hook was consulted")

    monkeypatch.setattr(callable_utils, "console_function_source", no_console)
    mode, ns = session
    run(
        ns,
        f"df = {DATA}\n\n@fl.python_script(kernel='lite')\ndef doubled(frame):\n"
        "    return frame.with_columns(b=frame['a'] * 2)\n\nout = doubled(df)",
    )
    node = mode.graph.get_node(ns["out"].node_id)
    assert node.node_type == "python_script"
    assert "frame.with_columns(b=frame['a'] * 2)" in node.setting_input.python_script_input.code


def test_names_bound_are_reported(session):
    _, ns = session
    run(ns, "x = 1")
    result = run(ns, "x = 2\ny = 3\nz = x")
    assert result.names == ["x", "y", "z"]


def test_a_name_bound_to_a_node_created_in_the_cell_becomes_its_reference(session):
    mode, ns = session
    result = run(ns, f"orders = {DATA}\nbig_orders = orders.filter(fl.col('a') > 1)")
    orders, big = ns["orders"].node_id, ns["big_orders"].node_id
    assert result.references == {orders: "orders", big: "big_orders"}
    assert reference(mode, orders) == "orders" and reference(mode, big) == "big_orders"


@pytest.mark.parametrize("name", ["Upper", "MODE", "2x", "_private", "list", "print", "fl", "pl", "main", "source_7"])
def test_names_outside_the_rules_are_not_captured(session, name):
    mode, ns = session
    result = run(ns, f"{name} = {DATA}" if name.isidentifier() else f"globals()[{name!r}] = {DATA}")
    assert result.references == {}
    assert reference(mode, result.created[0][1]) is None


def test_keyword_is_never_a_reference(session):
    mode, ns = session
    result = run(ns, f"globals()['lambda'] = {DATA}")
    assert result.references == {}


def test_generated_label_is_not_captured(session):
    mode, ns = session
    result = run(ns, f"src = {DATA}\nfiltered_99 = src.filter(fl.col('a') > 1)")
    assert result.references == {ns["src"].node_id: "src"}


@pytest.mark.parametrize(
    "code",
    [
        "filtered_{n}_pass, filtered_{n}_fail = src.filter_split(fl.col('a') > 1)",
        "random_split_{n}_train, random_split_{n}_test = src.random_split({{'train': 50, 'test': 50}}, seed=1)",
    ],
)
def test_a_split_label_derived_from_the_generated_one_is_not_captured(session, code):
    mode, ns = session
    run(ns, f"src = {DATA}")
    next_id = max(n.node_id for n in mode.graph.nodes) + 1
    result = run(ns, code.format(n=next_id))
    assert result.references == {}
    assert all(reference(mode, node.node_id) in (None, "src") for node in mode.graph.nodes)


def test_a_name_bound_to_an_existing_node_is_not_captured(session):
    mode, ns = session
    run(ns, f"df = {DATA}")
    result = run(ns, "alias = df")
    assert result.references == {}
    assert reference(mode, ns["df"].node_id) == "df"


def test_rebinding_moves_the_reference_and_clears_the_earlier_one(session):
    mode, ns = session
    run(ns, f"df = {DATA}")
    first = ns["df"].node_id
    run(ns, "df = df.filter(fl.col('a') > 1)")
    assert reference(mode, first) is None
    assert reference(mode, ns["df"].node_id) == "df"


def test_rebinding_to_a_non_frame_clears_the_reference(session):
    mode, ns = session
    run(ns, f"df = {DATA}")
    first = ns["df"].node_id
    run(ns, "df = 5")
    assert reference(mode, first) is None


def test_last_binding_wins_and_references_stay_unique(session):
    mode, ns = session
    run(ns, f"a = {DATA}\nb = a")
    node = ns["a"].node_id
    assert reference(mode, node) == "b"
    names = [n.setting_input.node_reference for n in mode.graph.nodes if n.setting_input.node_reference]
    assert len(names) == len(set(names))


def test_a_reference_held_elsewhere_moves_to_the_new_binding(session):
    mode, ns = session
    run(ns, f"df = {DATA}")
    first = ns["df"].node_id
    run(ns, f"keep = df\ndf = {DATA}")
    assert reference(mode, first) is None
    assert reference(mode, ns["df"].node_id) == "df"


def test_a_native_node_captures_its_name(session):
    mode, ns = session
    fl.add_flow_parameter(mode.graph, fl.Parameter("mode", default="full"))
    run(ns, f"df = {DATA}\nrouter = fl.Gate(df, parameter='mode', value='full')")
    assert reference(mode, ns["router"].node_id) == "router"


def test_a_failing_cell_captures_nothing(session):
    mode, ns = session
    result = execute_cell("c", f"df = {DATA}\n1 / 0", ns)
    assert not result.ok and result.references == {}
    assert reference(mode, result.created[0][1]) is None


def test_auto_display_of_a_live_frame_shows_schema_and_rows(session):
    _, ns = session
    result = run(ns, "fl.from_dict({'a': list(range(150))})")
    payload = result.display
    assert payload["schema"] == [{"name": "a", "data_type": "Int64"}] and payload["lazy_safe"]
    table = payload[TABLE_MIME]
    assert (table["loaded_rows"], table["total_rows"], table["truncated"]) == (100, 150, True)


def test_explicit_display_caps_at_2000_rows(session):
    _, ns = session
    result = run(ns, "display(fl.from_dict({'a': list(range(2500))}))\nNone")
    assert result.display is None
    [payload] = result.outputs
    assert payload[TABLE_MIME]["loaded_rows"] == 2000 and payload[TABLE_MIME]["total_rows"] == 2500


def test_a_deferred_frame_shows_only_its_schema(session, tmp_path):
    _, ns = session
    result = run(ns, f"{DATA}.write_csv({str(tmp_path / 'x.csv')!r})")
    assert result.display["schema"] == [{"name": "a", "data_type": "Int64"}, {"name": "g", "data_type": "String"}]
    assert not result.display["lazy_safe"] and TABLE_MIME not in result.display


def test_a_frame_below_a_gate_shows_only_its_schema(session):
    mode, ns = session
    fl.add_flow_parameter(mode.graph, fl.Parameter("mode", default="full"))
    result = run(ns, f"fl.Gate({DATA}, parameter='mode', value='full').then.sort('a')")
    assert TABLE_MIME not in result.display


def test_display_of_a_node_and_of_a_plain_value(session):
    mode, ns = session
    fl.add_flow_parameter(mode.graph, fl.Parameter("mode", default="full"))
    gate = run(ns, f"fl.Gate({DATA}, parameter='mode', value='full')").display
    assert gate["kind"] == "node" and set(gate["outputs"]) == {"then", "else"}
    assert run(ns, "1 + 1").display == {"kind": "text", "text/plain": "2"}
    assert run(ns, "x = 1").display is None


def test_display_outside_a_cell_returns_the_payload(session):
    payload = display_payload(fl.from_dict({"a": [1, 2]}))
    assert payload[TABLE_MIME]["data"] == [{"a": 1}, {"a": 2}]


def test_seed_session_binds_live_and_deferred_variables():
    data, graph, source, big, gate = canvas_payload()
    sort_id = max(n.node_id for n in graph.nodes)
    schemas = {gate.node_id: {"output-0": [{"name": "a", "data_type": "Int64"}]}}
    try:
        bound = seed_session(data, [{"name": "mode", "default": "full"}], {big.node_id: "big"}, schemas)
        mode = notebook.current()
        assert mode is not None and bound["flow"] is mode.graph
        assert mode.graph.flow_id != graph.flow_id
        assert [p.name for p in mode.graph.flow_settings.parameters] == ["mode"]
        assert set(bound) == {"flow", f"source_{source.node_id}", "big", f"gate_{gate.node_id}", f"ordered_{sort_id}"}
        live_source, live_big = bound[f"source_{source.node_id}"], bound["big"]
        assert not live_source._deferred and not live_big._deferred
        assert live_big.collect()["a"].to_list() == [2, 3]
        seeded_gate = bound[f"gate_{gate.node_id}"]
        assert isinstance(seeded_gate, SeededNode) and seeded_gate.outputs == ["then", "else"]
        assert seeded_gate.then._deferred and seeded_gate.otherwise.output_handle == output_handle(1)
        assert seeded_gate.then.columns == ["a"]
        assert seeded_gate.otherwise.columns == ["a"]
        assert bound[f"ordered_{sort_id}"]._deferred
    finally:
        notebook.exit()


def test_seeded_node_handles_match_the_native_classes():
    data, graph, source, big, gate = canvas_payload()
    try:
        bound = seed_session(data, [], {gate.node_id: "router"}, {})
        router = bound["router"]
        assert router.output is router.then and router["then"] is router.then
        assert router.get_output("else") is router.otherwise and router.else_ is router.otherwise
        with pytest.raises(NativeNodeError, match="no output 'nope'"):
            router["nope"]
        downstream = router.otherwise.sort("a")
        assert downstream._deferred
    finally:
        notebook.exit()


def test_a_single_output_seeded_node_forwards_frame_methods():
    frame = fl.from_dict({"a": [1]})
    node = SeededNode(frame.flow_graph, frame.node_id, "python_script", ["main"], {"output-0": frame})
    assert node.columns == ["a"] and node.output is frame


def test_a_cell_builds_on_seeded_variables_without_renumbering():
    data, graph, *_ = canvas_payload()
    try:
        bound = seed_session(data, [], {}, {})
        mode = notebook.current()
        before = {n.node_id: n.node_type for n in mode.graph.nodes}
        ns = {**new_namespace(), **bound}
        filtered = next(k for k in bound if k.startswith("filtered"))
        run(ns, f"joined = fl.from_dict({{'a': [2, 3], 'z': [1, 2]}}).join({filtered}, on='a')")
        after = {n.node_id: n.node_type for n in mode.graph.nodes}
        assert {k: after[k] for k in before} == before
    finally:
        notebook.exit()


def test_canvas_node_adopts_settings_wires_inputs_and_seeds_outputs():
    data, graph, source, big, gate = canvas_payload()
    try:
        bound = seed_session(data, [], {source.node_id: "src"}, {big.node_id: {"output-0": [("a", "Int64")]}})
        mode = notebook.current()
        frame = canvas_node(big.node_id, bound["src"])
        node = mode.graph.get_node(frame.node_id)
        assert frame.node_id != big.node_id and node.node_type == "filter"
        assert node.setting_input.filter_input == graph.get_node(big.node_id).setting_input.filter_input
        assert [n.node_id for n in node.all_inputs] == [source.node_id]
        assert frame._deferred and frame.columns == ["a"]
        split = canvas_node(gate.node_id, frame)
        assert isinstance(split, SeededNode) and split.otherwise._deferred
        assert canvas_node(gate.node_id, frame, output="else").output_handle == output_handle(1)
    finally:
        notebook.exit()


def test_canvas_node_of_a_split_filter_exposes_pass_and_fail():
    graph = fl.create_flow_graph()
    source = fl.from_dict({"a": [1, 2, 3]}, flow_graph=graph)
    passed, _ = source.filter_split(fl.col("a") > 1)
    data = graph.get_flowfile_data().model_dump(mode="json")
    try:
        bound = seed_session(data, [], {source.node_id: "src"}, {})
        split = canvas_node(passed.node_id, bound["src"])
        assert isinstance(split, SeededNode) and split.outputs == ["pass", "fail"]
        assert split.then.output_handle == output_handle(0) and split["pass"] is split.then
        assert split.otherwise.output_handle == output_handle(1) and split["output-1"] is split.otherwise
        assert canvas_node(passed.node_id, bound["src"], output="fail").output_handle == output_handle(1)
        assert split.then._deferred and split.otherwise.columns == ["a"]
    finally:
        notebook.exit()


def _storage_files() -> set:
    return {
        path
        for path in Path(storage.base_directory).rglob("*")
        if path.is_file() and not any(part.endswith("logs") for part in path.parts) and ".db" not in path.name
    }


def test_seeding_never_predicts_a_pivot_and_writes_nothing_under_storage(monkeypatch):
    """A pivot predicts by collecting its pivot values, through the worker's cache when one is up."""
    graph = fl.create_flow_graph()
    source = fl.from_dict({"g": ["a", "a", "b"], "k": ["x", "y", "x"], "v": [1, 2, 3]}, flow_graph=graph)
    pivoted = source.pivot(on="k", index="g", values="v", aggregate_function="sum")
    pivoted.select("g", "x").sort("g")
    source.unpivot(["v"], index="g").group_by("g").agg(fl.col("value").std())
    data = graph.get_flowfile_data().model_dump(mode="json")
    predicted = []
    monkeypatch.setattr(schema_callbacks, "fetch_unique_values", lambda lf: predicted.append(lf) or ["x", "y"])
    before = _storage_files()
    for schemas in ({}, {pivoted.node_id: {"output-0": [{"name": "g", "data_type": "String"}]}}):
        try:
            bound = seed_session(data, [], {pivoted.node_id: "wide"}, schemas)
            assert bound["wide"]._deferred
            assert bound["wide"].columns == [c["name"] for c in schemas.get(pivoted.node_id, {}).get("output-0", [])]
        finally:
            notebook.exit()
    assert predicted == []
    assert _storage_files() - before == set()


def test_canvas_node_refuses_an_unknown_id_and_outside_a_session():
    with pytest.raises(NativeNodeError, match="seeded from the canvas"):
        canvas_node(1)
    data, *_ = canvas_payload()
    try:
        seed_session(data, [], {}, {})
        with pytest.raises(NativeNodeError, match="not in this session's snapshot"):
            canvas_node(987654)
    finally:
        notebook.exit()
    with pytest.raises(NativeNodeError, match="seeded from the canvas"):
        canvas_node(1)


def test_clean_run_prunes_relabels_and_restores_the_session(session):
    mode, ns = session
    fl.add_flow_parameter(mode.graph, fl.Parameter("interactive_only", default=1))
    cells = [
        ("node-5", f"df = {DATA}\nfl.from_dict({{'dropped': [1]}}).sort('dropped')"),
        ("node-6", "big = df.filter(fl.col('a') > 1)\nbig.write_csv('/nonexistent/never.csv')"),
    ]
    result = clean_run(cells, ceiling=40, provenance={"node-5": [("manual_input", 5)], "node-6": [("filter", 6)]})
    assert result["ok"], result.get("error")
    nodes = {n["id"]: n for n in result["flowfile_data"]["nodes"]}
    assert result["cells"] == {"node-5": [5], "node-6": [6, 41]}
    assert {i: n["type"] for i, n in nodes.items()} == {5: "manual_input", 6: "filter", 41: "output"}
    assert nodes[6]["input_ids"] == [5] and nodes[41]["input_ids"] == [6]
    assert result["names"] == {5: "df", 6: "big"}
    assert result["flowfile_data"]["flowfile_settings"]["parameters"] == []
    assert result["refusals"] == []
    assert notebook.current() is mode and "run_graph" in mode.graph.__dict__


def test_clean_run_aborts_on_the_first_failing_cell():
    result = clean_run([("a", f"df = {DATA}"), ("b", "raise ValueError('boom')"), ("c", "x = 1")], ceiling=0)
    assert result["ok"] is False and result["cell_id"] == "b" and "boom" in result["error"]
    assert notebook.current() is None


def test_clean_run_reports_a_caught_refusal():
    code = f"df = {DATA}\ntry:\n    fl.register_flow(df, name='x')\nexcept ValueError:\n    pass"
    result = clean_run([("a", code)], ceiling=0)
    assert result["ok"] and len(result["refusals"]) == 1 and "run it from a script" in result["refusals"][0]


def test_clean_run_reproduces_a_canvas_node_placeholder():
    data, graph, source, big, gate = canvas_payload()
    try:
        seed_session(data, [], {}, {})
        cells = [("s", f"src = {DATA}"), (f"node-{big.node_id}", f"kept = fl.canvas_node({big.node_id}, src)")]
        result = clean_run(cells, ceiling=100, provenance={f"node-{big.node_id}": [("filter", big.node_id)]})
        assert result["ok"], result.get("error")
        assert result["cells"][f"node-{big.node_id}"] == [big.node_id]
        [node] = [n for n in result["flowfile_data"]["nodes"] if n["id"] == big.node_id]
        assert node["type"] == "filter" and node["node_reference"] == "kept"
    finally:
        notebook.exit()
