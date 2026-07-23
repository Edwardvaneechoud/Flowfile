"""Exec-free schema prediction for custom nodes.

Covers the `predict_output_schema` SDK hook (declared schema without running
process — no kernel/worker), its multi-output dict form, fallback to the
execution-based tier on raise/None, the reset-cascade-safe wiring, concurrent
prediction dedupe, and designer round-trip + security-scan preservation of the
hook method.
"""
import threading

import polars as pl
import pytest

from flowfile_core.configs import node_store
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput
from flowfile_core.schemas import input_schema, schemas


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
        schemas.FlowSettings(flow_id=flow_id, name="schema_prediction_flow", path=".", execution_mode="Development")
    )
    return handler.get_flow(flow_id)


def build_flow_with_custom_node(node_class: type[CustomNodeBase], settings: dict | None = None) -> FlowGraph:
    node_store.add_to_custom_node_store(node_class)
    flow = create_graph()
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}, {"a": 2}])
        )
    )
    instance = node_class.from_settings(settings or {})
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type=instance.item))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_user_defined_node(
        custom_node=instance,
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=1, node_id=2, settings=settings or {}, is_user_defined=True
        ),
    )
    return flow


def test_declared_schema_predicts_without_execution():
    process_calls: list[str] = []

    class DeclaredSchemaNode(CustomNodeBase):
        node_name: str = "Declared Schema Node"
        node_category: str = "Testing"
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", result_column=TextInput(label="Result column")),
        )

        def process(self, *inputs):
            process_calls.append("ran")
            return inputs[0]

        def predict_output_schema(self, *inputs):
            result_column = self.settings_schema.main_section.result_column.value
            return inputs[0].with_columns(pl.lit(0.0).alias(result_column))

    flow = build_flow_with_custom_node(
        DeclaredSchemaNode, settings={"main_section": {"result_column": "score"}}
    )
    node = flow.get_node(2)

    assert node.user_provided_schema_callback is not None, "hook wiring must survive reset cascades"
    predicted = node.get_predicted_schema()
    assert [c.column_name for c in predicted] == ["a", "score"]
    assert process_calls == [], "declared schema must not execute process()"


def test_hook_can_reuse_process_on_schema_frames():
    """Pure-polars nodes can predict via `return self.process(*inputs)`."""

    class ProcessReuseNode(CustomNodeBase):
        node_name: str = "Process Reuse Node"
        node_category: str = "Testing"
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            return inputs[0].with_columns((pl.col("a") * 1.5).alias("scaled"))

        def predict_output_schema(self, *inputs):
            return self.process(*inputs)

    flow = build_flow_with_custom_node(ProcessReuseNode)
    node = flow.get_node(2)
    predicted = node.get_predicted_schema()
    assert [c.column_name for c in predicted] == ["a", "scaled"]


def test_dict_return_fills_per_handle_schemas():
    class MultiOutNode(CustomNodeBase):
        node_name: str = "Multi Out Declared"
        node_category: str = "Testing"
        number_of_outputs: int = 2
        output_names: list[str] = ["main", "stats"]
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            raise AssertionError("must not run for declared schemas")

        def predict_output_schema(self, *inputs):
            return {
                "main": inputs[0],
                "stats": pl.LazyFrame(schema={"metric": pl.String(), "value": pl.Float64()}),
            }

    flow = build_flow_with_custom_node(MultiOutNode)
    node = flow.get_node(2)

    predicted = node.get_predicted_schema()
    assert [c.column_name for c in predicted] == ["a"], "default handle carries the 'main' schema"
    assert set(node._named_schemas) == {"output-0", "output-1"}
    stats_schema = node.get_predicted_resulting_data("output-1").schema
    assert [c.column_name for c in stats_schema] == ["metric", "value"]


def test_raising_hook_falls_back_to_execution():
    process_calls: list[str] = []

    class RaisingHookNode(CustomNodeBase):
        node_name: str = "Raising Hook Node"
        node_category: str = "Testing"
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            process_calls.append("ran")
            return inputs[0]

        def predict_output_schema(self, *inputs):
            raise ValueError("cannot predict")

    flow = build_flow_with_custom_node(RaisingHookNode)
    node = flow.get_node(2)

    predicted = node.get_predicted_schema()
    assert [c.column_name for c in predicted] == ["a"], "fallback still predicts via execution"
    assert process_calls, "the execution-based tier must have run"


def test_node_without_hook_gets_no_callback():
    class PlainNode(CustomNodeBase):
        node_name: str = "Plain Node"
        node_category: str = "Testing"
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            return inputs[0]

    flow = build_flow_with_custom_node(PlainNode)
    node = flow.get_node(2)
    assert node.user_provided_schema_callback is None


def test_concurrent_prediction_executes_once():
    """Two racing predictions of a callback-less custom node run process() once."""
    process_calls: list[str] = []

    class CountingNode(CustomNodeBase):
        node_name: str = "Counting Node"
        node_category: str = "Testing"
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            process_calls.append("ran")
            return inputs[0]

    flow = build_flow_with_custom_node(CountingNode)
    node = flow.get_node(2)

    results: list = []
    barrier = threading.Barrier(2)

    def predict():
        barrier.wait()
        results.append(node.get_predicted_schema())

    threads = [threading.Thread(target=predict) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(process_calls) == 1, "concurrent predictions must not execute the node twice"
    assert [c.column_name for c in results[0]] == ["a"]
    assert results[0] == results[1]


def test_data_dependent_hook_sees_real_upstream_data_after_run():
    """Hooks receive the upstream's real lazy data once it has run (one-hot style)."""

    class OneHotStyleNode(CustomNodeBase):
        node_name: str = "One Hot Style Node"
        node_category: str = "Testing"
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            return inputs[0]

        def predict_output_schema(self, *inputs):
            values = inputs[0].select(pl.col("a").unique().sort()).collect()["a"].to_list()
            if not values:
                return None  # pre-run: no data yet, fall back
            return inputs[0].with_columns([pl.lit(0).alias(f"a_{v}") for v in values])

    flow = build_flow_with_custom_node(OneHotStyleNode)
    node = flow.get_node(2)

    # Before the upstream ran the frame is schema-only: the hook opts out and
    # the exec tier supplies the passthrough schema.
    assert [c.column_name for c in node.get_predicted_schema()] == ["a"]

    flow.run_graph()
    predicted = node.get_predicted_schema(force=True)
    assert [c.column_name for c in predicted] == ["a", "a_1", "a_2"], (
        "post-run prediction must see the upstream's real values"
    )


def test_none_hook_downstream_sees_exec_predicted_schema():
    """A hook that opts out (returns None) must not starve downstream predictions."""
    process_calls: list[str] = []

    class OptOutHookNode(CustomNodeBase):
        node_name: str = "Opt Out Hook Node"
        node_category: str = "Testing"
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            process_calls.append("ran")
            return inputs[0]

        def predict_output_schema(self, *inputs):
            return None

    class DownstreamPlain(CustomNodeBase):
        node_name: str = "Downstream Plain"
        node_category: str = "Testing"
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            return inputs[0]

    flow = build_flow_with_custom_node(OptOutHookNode)
    node_store.add_to_custom_node_store(DownstreamPlain)
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=3, node_type="downstream_plain"))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))
    flow.add_user_defined_node(
        custom_node=DownstreamPlain.from_settings({}),
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=1, node_id=3, settings={}, is_user_defined=True
        ),
    )

    hooked = flow.get_node(2)
    assert [c.column_name for c in hooked.get_predicted_resulting_data().schema] == ["a"]
    downstream = flow.get_node(3)
    assert [c.column_name for c in downstream.get_predicted_schema()] == ["a"]
    assert process_calls, "the exec tier must have supplied the schema"


def test_zero_input_hook_node_never_executes_at_placement():
    """The eager is_start schema prefetch must use the hook, not the real function."""
    import time

    process_calls: list[str] = []

    class SourceHookNode(CustomNodeBase):
        node_name: str = "Source Hook Node"
        node_category: str = "Testing"
        number_of_inputs: int = 0
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            process_calls.append("ran")
            return pl.LazyFrame({"generated": [1]})

        def predict_output_schema(self, *inputs):
            return pl.LazyFrame(schema={"generated": pl.Int64()})

    node_store.add_to_custom_node_store(SourceHookNode)
    flow = create_graph()
    flow.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="source_hook_node"))
    flow.add_user_defined_node(
        custom_node=SourceHookNode.from_settings({}),
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=1, node_id=1, settings={}, is_user_defined=True
        ),
    )
    node = flow.get_node(1)
    predicted = node.get_predicted_schema()
    assert [c.column_name for c in predicted] == ["generated"]
    time.sleep(0.3)  # let any orphaned background prefetch surface
    assert process_calls == [], "placement of a 0-input hook node must never run process()"


def test_multi_output_single_schema_falls_back_to_execution():
    """A single frame from a multi-output hook is ambiguous — exec tier must fill handles."""

    class MultiOutSingleSchema(CustomNodeBase):
        node_name: str = "Multi Out Single Schema"
        node_category: str = "Testing"
        number_of_outputs: int = 2
        output_names: list[str] = ["main", "stats"]
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(title="Config", note=TextInput(label="Note")),
        )

        def process(self, *inputs):
            return {
                "main": inputs[0],
                "stats": pl.LazyFrame({"metric": ["rows"], "value": [1.0]}),
            }

        def predict_output_schema(self, *inputs):
            return inputs[0]

    flow = build_flow_with_custom_node(MultiOutSingleSchema)
    node = flow.get_node(2)
    # Cold non-default-handle prediction first: the exec fallback must fill the
    # per-handle caches and serve output-1's real schema, not clone output-0.
    stats_schema = node.get_predicted_resulting_data("output-1").schema
    assert [c.column_name for c in stats_schema] == ["metric", "value"]
    assert [c.column_name for c in node.get_predicted_schema()] == ["a"]


HOOK_NODE_SOURCE = '''import polars as pl

from flowfile import node_designer as nd


class AddScoreColumn(nd.CustomNodeBase):
    node_name: str = "Add Score Column"
    node_category: str = "Testing"

    settings_schema: nd.NodeSettings = nd.NodeSettings(
        main_section=nd.Section(
            title="Configuration",
            result_column=nd.TextInput(label="Result column"),
        ),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.lit(1.0).alias(self.settings_schema.main_section.result_column.value))

    def predict_output_schema(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return self.process(*inputs)
'''


def test_designer_roundtrip_preserves_hook_method():
    from flowfile_core.flowfile.node_designer.codegen import generate_source
    from flowfile_core.flowfile.node_designer.parsing import parse_source

    parsed = parse_source(HOOK_NODE_SOURCE)
    # The hook lifts into its dedicated field (editable in the designer), not class_extra.
    assert "def predict_output_schema" in parsed.designer_state.predict_schema_code
    assert not any("predict_output_schema" in block for block in parsed.designer_state.class_extra)

    regenerated = generate_source(parsed.designer_state)
    assert "def predict_output_schema" in regenerated
    assert "return self.process(*inputs)" in regenerated

    # Byte-stable fixed point: re-saving from the designer never mutates the hook.
    assert generate_source(parse_source(regenerated).designer_state) == regenerated

    # Clearing the field removes the method from the generated source.
    parsed.designer_state.predict_schema_code = ""
    without_hook = generate_source(parsed.designer_state)
    assert "predict_output_schema" not in without_hook
    assert parse_source(without_hook).designer_state.predict_schema_code == ""


def test_security_scan_accepts_hook_method():
    from flowfile_core.flowfile.community_nodes.security_scan import scan_source

    report = scan_source(HOOK_NODE_SOURCE)
    assert report.passed, f"pure schema hook must pass the scanner: {report.findings}"
    assert report.deny_count == 0
