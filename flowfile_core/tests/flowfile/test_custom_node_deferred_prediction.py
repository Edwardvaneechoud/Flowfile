"""Deferred (gated) schema prediction.

Data-needing predictions (requires_data_for_prediction, default False; pivot among
built-ins opts in) behave by declaration: hooks marked True get real upstream data
by materializing kernel-free chains in-core, pivot-style, while an un-run kernel
node in the chain is never executed implicitly — prediction stops with a
user-facing warning instead, and the exec-tier fallback is suppressed so no kernel
spins on empty inputs. A hookless node marked True never predicts by executing at
all: it blocks with a "run it to resolve" warning until the node has actually run.
"""
import polars as pl
import pytest

from flowfile_core.configs import node_store
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput
from flowfile_core.schemas import input_schema, schemas, transform_schema


@pytest.fixture(autouse=True)
def store_snapshot():
    saved_store = dict(node_store.CUSTOM_NODE_STORE)
    saved_dict = dict(node_store.node_dict)
    saved_list = list(node_store.nodes_list)
    yield node_store
    node_store.CUSTOM_NODE_STORE.clear()
    node_store.CUSTOM_NODE_STORE.update(saved_store)
    node_store.node_dict.clear()
    node_store.node_dict.update(saved_dict)
    node_store.nodes_list[:] = saved_list


def create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="kernel_gate_flow", path=".", execution_mode="Development")
    )
    return handler.get_flow(flow_id)


def add_manual_source(flow: FlowGraph, node_id: int = 1) -> None:
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type="manual_input"))
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}, {"a": 2}])
        )
    )


def add_custom(flow: FlowGraph, node_class: type[CustomNodeBase], node_id: int, upstream_id: int, **settings_kwargs):
    node_store.add_to_custom_node_store(node_class)
    instance = node_class.from_settings({})
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type=instance.item))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(upstream_id, node_id))
    flow.add_user_defined_node(
        custom_node=instance,
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=1, node_id=node_id, settings={}, is_user_defined=True, **settings_kwargs
        ),
    )
    return flow.get_node(node_id)


def make_one_hot_node(process_calls: list) -> type[CustomNodeBase]:
    class OneHotHookNode(CustomNodeBase):
        node_name: str = "One Hot Hook Node"
        node_category: str = "Testing"
        requires_data_for_prediction: bool = True
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            process_calls.append("ran")
            return inputs[0]

        def predict_output_schema(self, *inputs):
            values = inputs[0].select(pl.col("a").unique().sort()).collect()["a"].to_list()
            if not values:
                return None
            return inputs[0].with_columns([pl.lit(0).alias(f"a_{v}") for v in values])

    return OneHotHookNode


class KernelSourceNode(CustomNodeBase):
    node_name: str = "Kernel Source Node"
    node_category: str = "Testing"
    environment: str = "kernel"
    settings_schema: NodeSettings = NodeSettings(
        main_section=Section(title="Config", note=TextInput(label="Note")),
    )

    def process(self, *inputs):
        return inputs[0].with_columns(pl.lit("x").alias("cat"))


def predicted_names(node) -> list[str]:
    return [c.column_name for c in (node.get_predicted_schema() or [])]


def test_kernel_free_chain_gives_hook_real_data_pre_run():
    """The motivating one-hot case: correct dummy columns before any run."""
    process_calls: list = []
    flow = create_graph()
    add_manual_source(flow)
    node = add_custom(flow, make_one_hot_node(process_calls), node_id=2, upstream_id=1)

    assert predicted_names(node) == ["a", "a_1", "a_2"]
    assert node._schema_prediction_blocked is None
    assert process_calls == [], "the node's own process must not run for prediction"


def test_unrun_kernel_upstream_warns_instead_of_executing(monkeypatch):
    def _never(*args, **kwargs):
        raise AssertionError("kernel execution must never be triggered by prediction")

    monkeypatch.setattr(FlowGraph, "_execute_on_kernel", _never)
    process_calls: list = []
    flow = create_graph()
    add_manual_source(flow)
    kernel_node = add_custom(flow, KernelSourceNode, node_id=2, upstream_id=1, kernel_id="test-kernel")
    hook_node = add_custom(flow, make_one_hot_node(process_calls), node_id=3, upstream_id=2)

    assert kernel_node._executes_on_kernel is True
    assert predicted_names(hook_node) == []
    assert hook_node._schema_prediction_blocked is not None
    assert "kernel node 'kernel_source_node' (id 2)" in hook_node._schema_prediction_blocked
    assert process_calls == []


def test_hookless_kernel_node_own_prediction_warns(monkeypatch):
    """The historical 0-record kernel spin: gated, warned, never executed."""

    def _never(*args, **kwargs):
        raise AssertionError("kernel execution must never be triggered by prediction")

    monkeypatch.setattr(FlowGraph, "_execute_on_kernel", _never)
    flow = create_graph()
    add_manual_source(flow)
    kernel_node = add_custom(flow, KernelSourceNode, node_id=2, upstream_id=1, kernel_id="test-kernel")

    assert predicted_names(kernel_node) == []
    assert kernel_node._schema_prediction_blocked is not None


def test_warning_propagates_to_all_downstream_nodes(monkeypatch):
    """a -> b(kernel) -> c(select) -> d(select): both c and d must warn."""
    monkeypatch.setattr(
        FlowGraph, "_execute_on_kernel", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no kernel"))
    )
    flow = create_graph()
    add_manual_source(flow)
    add_custom(flow, KernelSourceNode, node_id=2, upstream_id=1, kernel_id="test-kernel")
    for node_id, upstream_id in ((3, 2), (4, 3)):
        flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type="select"))
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(upstream_id, node_id))
        flow.add_select(input_schema.NodeSelect(flow_id=1, node_id=node_id, select_input=[], keep_missing=True))

    for node_id in (3, 4):
        node_data = flow.get_node(node_id).get_node_data(flow_id=1, include_output=False, include_inputs=True)
        assert node_data.prediction_warning is not None, f"node {node_id} must surface the kernel warning"
        assert "(id 2)" in node_data.prediction_warning


def test_declared_output_schema_clears_warning_downstream(monkeypatch):
    """Declaring c's columns (Schema Validator) resolves it — c and d stop warning."""
    monkeypatch.setattr(
        FlowGraph, "_execute_on_kernel", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no kernel"))
    )
    flow = create_graph()
    add_manual_source(flow)
    add_custom(flow, KernelSourceNode, node_id=2, upstream_id=1, kernel_id="test-kernel")
    # c declares its output columns; d is a plain passthrough downstream of c.
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=3, node_type="select"))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))
    flow.add_select(
        input_schema.NodeSelect(
            flow_id=1,
            node_id=3,
            select_input=[],
            keep_missing=True,
            output_field_config=input_schema.OutputFieldConfig(
                enabled=True,
                fields=[
                    input_schema.OutputFieldInfo(name="id", data_type="Int64"),
                    input_schema.OutputFieldInfo(name="score", data_type="Float64"),
                ],
            ),
        )
    )
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=4, node_type="select"))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(3, 4))
    flow.add_select(input_schema.NodeSelect(flow_id=1, node_id=4, select_input=[], keep_missing=True))

    for node_id in (3, 4):
        node_data = flow.get_node(node_id).get_node_data(flow_id=1, include_output=False, include_inputs=True)
        assert node_data.prediction_warning is None, f"node {node_id} must not warn once c's schema is declared"


def test_prediction_warning_reaches_node_data(monkeypatch):
    monkeypatch.setattr(
        FlowGraph, "_execute_on_kernel", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no kernel"))
    )
    flow = create_graph()
    add_manual_source(flow)
    add_custom(flow, KernelSourceNode, node_id=2, upstream_id=1, kernel_id="test-kernel")
    hook_node = add_custom(flow, make_one_hot_node([]), node_id=3, upstream_id=2)

    node_data = hook_node.get_node_data(flow_id=1, include_output=False, include_inputs=True)
    assert node_data.prediction_warning is not None
    assert "has not run" in node_data.prediction_warning


def test_unmarked_hook_gets_schema_only_frames():
    """requires_data_for_prediction=False: shipped behavior, no materialization."""
    hook_saw: list = []

    class PureHookNode(CustomNodeBase):
        node_name: str = "Pure Hook Node"
        node_category: str = "Testing"
        requires_data_for_prediction: bool = False
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            return inputs[0]

        def predict_output_schema(self, *inputs):
            hook_saw.append(inputs[0].collect().height)
            return inputs[0].with_columns(pl.lit(0.0).alias("score"))

    flow = create_graph()
    add_manual_source(flow)
    node = add_custom(flow, PureHookNode, node_id=2, upstream_id=1)

    assert predicted_names(node) == ["a", "score"]
    assert hook_saw == [0], "unmarked hooks must see schema-only empty frames pre-run"


def test_warning_clears_after_upstream_has_data(monkeypatch):
    """Once the kernel upstream has results, prediction proceeds with real data."""
    flow = create_graph()
    add_manual_source(flow)
    kernel_node = add_custom(flow, KernelSourceNode, node_id=2, upstream_id=1, kernel_id="test-kernel")
    hook_node = add_custom(flow, make_one_hot_node([]), node_id=3, upstream_id=2)

    assert predicted_names(hook_node) == []
    assert hook_node._schema_prediction_blocked is not None

    # Simulate the kernel node having run (unit stand-in for a real kernel run;
    # the kernel-marked integration test covers the real thing).
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

    kernel_node.results.resulting_data = FlowDataEngine(pl.LazyFrame({"a": [1, 2], "cat": ["x", "x"]}))
    kernel_node.node_stats.has_completed_last_run = True

    schema = hook_node.get_predicted_schema(force=True)
    assert [c.column_name for c in schema] == ["a", "cat", "a_1", "a_2"]
    assert hook_node._schema_prediction_blocked is None


def test_pivot_downstream_of_kernel_warns(monkeypatch):
    monkeypatch.setattr(
        FlowGraph, "_execute_on_kernel", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no kernel"))
    )
    flow = create_graph()
    add_manual_source(flow)
    add_custom(flow, KernelSourceNode, node_id=2, upstream_id=1, kernel_id="test-kernel")
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=3, node_type="pivot"))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))
    flow.add_pivot(
        input_schema.NodePivot(
            flow_id=1,
            node_id=3,
            depending_on_id=2,
            pivot_input=transform_schema.PivotInput(
                index_columns=["a"], pivot_column="cat", value_col="a", aggregations=["count"]
            ),
        )
    )
    pivot_node = flow.get_node(3)

    assert (pivot_node.get_predicted_schema() or []) == []
    assert pivot_node._schema_prediction_blocked is not None
    assert "kernel node 'kernel_source_node' (id 2)" in pivot_node._schema_prediction_blocked


def test_pivot_kernel_free_chain_still_materializes():
    flow = create_graph()
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(
                [{"a": 1, "cat": "x"}, {"a": 2, "cat": "y"}]
            ),
        )
    )
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="pivot"))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_pivot(
        input_schema.NodePivot(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            pivot_input=transform_schema.PivotInput(
                index_columns=["a"], pivot_column="cat", value_col="a", aggregations=["count"]
            ),
        )
    )
    pivot_node = flow.get_node(2)

    names = [c.column_name for c in (pivot_node.get_predicted_schema() or [])]
    assert names, "kernel-free pivot prediction must still resolve columns"
    assert pivot_node._schema_prediction_blocked is None


def make_hookless_data_node(process_calls: list) -> type[CustomNodeBase]:
    class HooklessDataNode(CustomNodeBase):
        node_name: str = "Hookless Data Node"
        node_category: str = "Testing"
        requires_data_for_prediction: bool = True
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            process_calls.append("ran")
            values = inputs[0].select(pl.col("a").unique().sort()).collect()["a"].to_list()
            return inputs[0].with_columns([pl.lit(0).alias(f"a_{v}") for v in values])

    return HooklessDataNode


def test_hookless_requires_data_true_blocks_prediction():
    process_calls: list = []
    flow = create_graph()
    add_manual_source(flow)
    node = add_custom(flow, make_hookless_data_node(process_calls), node_id=2, upstream_id=1)

    assert predicted_names(node) == []
    assert node._schema_prediction_blocked is not None
    assert "(id 2)" in node._schema_prediction_blocked
    assert "Run it to resolve them." in node._schema_prediction_blocked
    assert node.results.warnings == node._schema_prediction_blocked
    assert process_calls == [], "blocked prediction must never execute process()"


def test_hookless_blocked_warning_propagates_downstream():
    process_calls: list = []
    flow = create_graph()
    add_manual_source(flow)
    add_custom(flow, make_hookless_data_node(process_calls), node_id=2, upstream_id=1)
    for node_id, upstream_id in ((3, 2), (4, 3)):
        flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=node_id, node_type="select"))
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(upstream_id, node_id))
        flow.add_select(input_schema.NodeSelect(flow_id=1, node_id=node_id, select_input=[], keep_missing=True))

    for node_id in (3, 4):
        node_data = flow.get_node(node_id).get_node_data(flow_id=1, include_output=False, include_inputs=True)
        assert node_data.prediction_warning is not None, f"node {node_id} must surface the blocked warning"
        assert "(id 2)" in node_data.prediction_warning
    assert process_calls == []


def test_hookless_blocked_clears_after_run():
    process_calls: list = []
    flow = create_graph()
    add_manual_source(flow)
    node = add_custom(flow, make_hookless_data_node(process_calls), node_id=2, upstream_id=1)

    assert predicted_names(node) == []
    flow.run_graph()
    assert node.node_stats.has_completed_last_run
    assert process_calls == ["ran"], "the explicit run is the only execution"

    schema = node.get_predicted_schema(force=True)
    assert [c.column_name for c in schema] == ["a", "a_1", "a_2"]
    assert node._schema_prediction_blocked is None
    node_data = node.get_node_data(flow_id=1, include_output=False, include_inputs=True)
    assert node_data.prediction_warning is None


def test_hookless_blocked_reblocks_after_settings_change():
    process_calls: list = []
    flow = create_graph()
    add_manual_source(flow)
    node_class = make_hookless_data_node(process_calls)
    node = add_custom(flow, node_class, node_id=2, upstream_id=1)
    flow.run_graph()
    assert node.get_predicted_schema(force=True), "post-run schema must resolve"

    flow.add_user_defined_node(
        custom_node=node_class.from_settings({"main_section": {"note": "changed"}}),
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=1, node_id=2, settings={"main_section": {"note": "changed"}}, is_user_defined=True
        ),
    )
    node = flow.get_node(2)
    assert predicted_names(node) == []
    assert node._schema_prediction_blocked is not None


def test_zero_input_hookless_blocked_never_runs_at_placement():
    """The eager is_start prefetch must hit the blocking callback, not process()."""
    import time

    process_calls: list = []

    class SourceDataNode(CustomNodeBase):
        node_name: str = "Source Data Node"
        node_category: str = "Testing"
        number_of_inputs: int = 0
        requires_data_for_prediction: bool = True
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            process_calls.append("ran")
            return pl.LazyFrame({"generated": [1]})

    node_store.add_to_custom_node_store(SourceDataNode)
    flow = create_graph()
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="source_data_node"))
    flow.add_user_defined_node(
        custom_node=SourceDataNode.from_settings({}),
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=1, node_id=1, settings={}, is_user_defined=True
        ),
    )
    node = flow.get_node(1)
    assert predicted_names(node) == []
    assert node._schema_prediction_blocked is not None
    time.sleep(0.3)  # let any orphaned background prefetch surface
    assert process_calls == [], "placement of a 0-input blocked node must never run process()"


def test_hookless_default_predicts_via_empty_frame_execution():
    """The flipped default (False): hookless prediction still runs process() on
    empty schema-only frames, exactly the pre-existing exec-tier mechanics."""
    heights: list = []

    class PlainAddColumnNode(CustomNodeBase):
        node_name: str = "Plain Add Column Node"
        node_category: str = "Testing"
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            heights.append(inputs[0].collect().height)
            return inputs[0].with_columns(pl.lit(0.0).alias("added"))

    flow = create_graph()
    add_manual_source(flow)
    node = add_custom(flow, PlainAddColumnNode, node_id=2, upstream_id=1)

    assert predicted_names(node) == ["a", "added"]
    assert heights == [0], "exec-tier prediction feeds schema-only empty frames"
    assert node._schema_prediction_blocked is None


def test_hookless_kernel_flag_true_keeps_kernel_message(monkeypatch):
    monkeypatch.setattr(
        FlowGraph, "_execute_on_kernel", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no kernel"))
    )

    class KernelDataNode(CustomNodeBase):
        node_name: str = "Kernel Data Node"
        node_category: str = "Testing"
        environment: str = "kernel"
        requires_data_for_prediction: bool = True
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            return inputs[0]

    flow = create_graph()
    add_manual_source(flow)
    node = add_custom(flow, KernelDataNode, node_id=2, upstream_id=1, kernel_id="test-kernel")

    assert predicted_names(node) == []
    assert node._schema_prediction_blocked is not None
    assert "kernel node" in node._schema_prediction_blocked


def _marker_node_source(marker_value: bool) -> str:
    return f'''import polars as pl

from flowfile import node_designer as nd


class PureSchemaNode(nd.CustomNodeBase):
    node_name: str = "Pure Schema Node"
    node_category: str = "Testing"
    requires_data_for_prediction: bool = {marker_value}

    settings_schema: nd.NodeSettings = nd.NodeSettings(
        main_section=nd.Section(
            title="Configuration",
            note=nd.TextInput(label="Note"),
        ),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''


def test_designer_roundtrip_preserves_marker():
    """The flag is a structured designer attribute: lifted (not class_extra),
    emitted once when True, and normalized away at the False default."""
    from flowfile_core.flowfile.node_designer.codegen import generate_source
    from flowfile_core.flowfile.node_designer.parsing import parse_source

    parsed = parse_source(_marker_node_source(True))
    assert parsed.designer_state.requires_data_for_prediction is True
    assert parsed.designer_state.class_extra == []
    regenerated = generate_source(parsed.designer_state)
    assert regenerated.count("requires_data_for_prediction: bool = True") == 1
    assert generate_source(parse_source(regenerated).designer_state) == regenerated

    parsed_false = parse_source(_marker_node_source(False))
    assert parsed_false.designer_state.requires_data_for_prediction is False
    regenerated_false = generate_source(parsed_false.designer_state)
    assert "requires_data_for_prediction" not in regenerated_false
    assert parse_source(regenerated_false).designer_state.requires_data_for_prediction is False
    assert generate_source(parse_source(regenerated_false).designer_state) == regenerated_false
