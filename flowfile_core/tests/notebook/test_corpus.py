"""Sanity checks on the notebook corpus fixture itself (the renderer tests consume it)."""

from tests.notebook.corpus import load_expected_placeholders

NATIVE_TYPES = {"gate", "run_flow", "flow_input", "flow_output", "python_script", "mood_emoji"}


def test_corpus_is_large_named_uniquely_and_covers_the_native_types(notebook_corpus):
    names = [name for name, _ in notebook_corpus]
    assert len(names) >= 25
    assert len(set(names)) == len(names)
    assert names[-1] == "demo"
    assert all(graph.nodes for _, graph in notebook_corpus)
    types = {node.node_type for _, graph in notebook_corpus for node in graph.nodes}
    assert NATIVE_TYPES <= types
    assert {"catalog_reader", "catalog_writer", "sql_query", "union", "explore_data", "train_model"} <= types


def test_building_the_corpus_started_no_kernel_manager(notebook_corpus):
    from tests.notebook.conftest import KERNEL_CALLS_DURING_CORPUS

    assert KERNEL_CALLS_DURING_CORPUS == []


def test_demo_graph_holds_the_whole_showcase(notebook_corpus):
    demo = dict(notebook_corpus)["demo"]
    types = [node.node_type for node in demo.nodes]
    assert types.count("catalog_writer") == 6
    gate = next(node for node in demo.nodes if node.node_type == "gate")
    assert gate.setting_input.else_output
    script = next(node for node in demo.nodes if node.node_type == "python_script")
    assert script.setting_input.python_script_input.cells


def test_python_script_flow_declares_its_output_schema(notebook_corpus):
    script = dict(notebook_corpus)["python_script_cells"].get_node(2)
    assert [f.name for f in script.setting_input.output_schemas["main"]] == ["amount", "double"]
    assert [c.name for c in script.schema] == ["amount", "double"]


def test_placeholder_manifest_names_only_corpus_flows_and_nodes(notebook_corpus):
    graphs = dict(notebook_corpus)
    manifest = load_expected_placeholders()
    assert set(manifest) == set(graphs)
    for name, node_ids in manifest.items():
        assert set(node_ids) <= {node.node_id for node in graphs[name].nodes}, name
