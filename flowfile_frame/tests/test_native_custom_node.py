"""``ff.CustomNode`` and the ``ff.custom_node(...)`` factory: place user-defined nodes from Python."""

import inspect
from types import SimpleNamespace

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.configs import node_store
from flowfile_core.flowfile.user_defined.registry import KernelRequiredError
from shared.node_designer import (
    CustomNodeBase,
    NodeSettings,
    NumericInput,
    SecretSelector,
    Section,
    TextInput,
    ToggleSwitch,
)

from .native_helpers import results_by_id, round_trip

DATA = {"name": [" ann ", "bob ", " cy"], "amount": [1, 2, 3]}


@pytest.fixture(autouse=True)
def store_snapshot():
    """Session-registered classes and their templates are process-global; restore them per test."""
    saved_overrides = dict(node_store.CUSTOM_NODE_STORE._overrides)
    saved_dict = dict(node_store.node_dict)
    saved_list = list(node_store.nodes_list)
    yield
    node_store.CUSTOM_NODE_STORE.clear()
    node_store.CUSTOM_NODE_STORE.update(saved_overrides)
    node_store.node_dict.clear()
    node_store.node_dict.update(saved_dict)
    node_store.nodes_list[:] = saved_list


class NativeCleaner(CustomNodeBase):
    node_name: str = "Native Test Cleaner"
    node_category: str = "Testing"
    settings_schema: NodeSettings = NodeSettings(
        options=Section(
            title="Options",
            trim=ToggleSwitch(label="Trim"),
            column=TextInput(label="Column", default="name"),
        ),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        options = self.settings_schema.options
        frame = inputs[0]
        if options.trim.value:
            frame = frame.with_columns(pl.col(options.column.value).str.strip_chars())
        return frame


class NativeSplitter(CustomNodeBase):
    node_name: str = "Native Test Splitter"
    node_category: str = "Testing"
    number_of_outputs: int = 2
    output_names: list[str] = ["kept", "dropped"]
    settings_schema: NodeSettings = NodeSettings(
        main=Section(title="Main", threshold=TextInput(label="Threshold", default="2")),
    )

    def process(self, *inputs: pl.LazyFrame) -> dict[str, pl.LazyFrame]:
        threshold = int(self.settings_schema.main.threshold.value)
        return {
            "kept": inputs[0].filter(pl.col("amount") >= threshold),
            "dropped": inputs[0].filter(pl.col("amount") < threshold),
        }


class NativeGenerator(CustomNodeBase):
    node_name: str = "Native Test Generator"
    node_category: str = "Testing"
    number_of_inputs: int = 0

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return pl.LazyFrame({"generated": [1, 2]})


class NativeKernelScorer(CustomNodeBase):
    node_name: str = "Native Test Kernel Scorer"
    node_category: str = "Testing"
    environment: str = "kernel"
    number_of_outputs: int = 2
    output_names: list[str] = ["main", "stats"]

    def process(self, *inputs: pl.LazyFrame) -> dict[str, pl.LazyFrame]:
        raise AssertionError("a kernel node never runs at build time")

    def predict_output_schema(self, *inputs: pl.LazyFrame) -> dict[str, pl.LazyFrame]:
        return {
            "main": inputs[0].with_columns(pl.lit(0.0).alias("score")),
            "stats": pl.LazyFrame(schema={"metric": pl.String(), "value": pl.Float64()}),
        }


class NativeTwoSections(CustomNodeBase):
    node_name: str = "Native Test Two Sections"
    node_category: str = "Testing"
    settings_schema: NodeSettings = NodeSettings(
        first=Section(title="First", label=TextInput(label="Label", default="a"), unique=TextInput(label="U")),
        second=Section(
            title="Second",
            label=TextInput(label="Label", default="b"),
            kernel=TextInput(label="Kernel", default="d"),
        ),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]


def _frame() -> ff.FlowFrame:
    return ff.from_dict(DATA)


def _canonical(cls: type[CustomNodeBase], settings: dict) -> dict:
    return cls.from_settings(settings)._extract_settings_values()


# class / instance / name


def test_class_places_a_user_defined_node_with_canonical_settings():
    source = _frame()
    node = ff.CustomNode(NativeCleaner, source, settings={"options": {"trim": True}}, description="clean names")

    core = node.node
    settings = core.setting_input
    assert core.node_type == "native_test_cleaner"
    assert settings.is_user_defined is True
    assert settings.settings == _canonical(NativeCleaner, {"options": {"trim": True}})
    assert settings.settings == {"options": {"trim": True, "column": "name"}}
    assert node.settings == settings.settings
    assert settings.kernel_id is None
    assert settings.output_names == ["main"]
    assert settings.depending_on_ids == [source.node_id]
    assert settings.description == "clean names"
    assert node.outputs == ["main"] and node["main"] is node.output
    assert node.output._deferred is False
    assert core.node_inputs.main_inputs[0].node_id == source.node_id


def test_instance_contributes_its_configured_values():
    instance = NativeCleaner.from_settings({"options": {"trim": True}})
    node = ff.CustomNode(instance, _frame(), settings={"options": {"column": "amount"}})
    assert node.node.setting_input.settings == {"options": {"trim": True, "column": "amount"}}
    assert node.node_class is NativeCleaner


def test_instance_overriding_a_non_settings_field_is_refused():
    with pytest.raises(ff.NativeNodeError, match=r"overrides \['intro'\]"):
        ff.CustomNode(NativeCleaner(intro="changed"), _frame())


def test_name_resolves_through_the_store():
    ff.CustomNode(NativeCleaner, _frame())
    node = ff.CustomNode("Native Test Cleaner", _frame(), settings={"options": {"trim": True}})
    assert node.node.node_type == "native_test_cleaner"
    assert node.node_class is NativeCleaner


def test_unknown_name_raises_not_installed():
    with pytest.raises(ff.NativeNodeError, match="'no_such_node' is not installed"):
        ff.CustomNode("no such node", _frame())


def test_built_in_name_points_at_node():
    with pytest.raises(ff.NativeNodeError, match=r"fl.Node\('filter'"):
        ff.CustomNode("filter", _frame())


def test_class_with_a_built_in_key_is_refused():
    class Filter(CustomNodeBase):
        node_name: str = "Filter"

        def process(self, *inputs):
            return inputs[0]

    with pytest.raises(ff.NativeNodeError, match="built-in node"):
        ff.CustomNode(Filter, _frame())
    assert not node_store.node_dict["filter"].custom_node


def test_redefined_class_refreshes_its_template():
    ff.CustomNode(NativeCleaner, _frame())

    class NativeCleanerV2(NativeCleaner):
        node_name: str = "Native Test Cleaner"
        number_of_inputs: int = 2

        def process(self, *inputs):
            return pl.concat([inputs[0], inputs[1]])

    node = ff.CustomNode(NativeCleanerV2, _frame(), _frame())
    assert node_store.node_dict["native_test_cleaner"].input == 2
    assert node_store.CUSTOM_NODE_STORE["native_test_cleaner"] is NativeCleanerV2
    assert node.output.collect().height == 6


# settings validation


@pytest.mark.parametrize(
    "settings, match",
    [
        ({"option": {"trim": True}}, r"Unknown settings .*\['option'\]"),
        ({"options": {"trimm": True}}, r"Unknown settings .*\['options.trimm'\]"),
        ({"trim": True}, r"Unknown settings .*\['trim'\].*nested"),
        ({"Options": {"trim": True}}, r"Unknown settings .*\['Options'\]"),
        ({"options": True}, r"settings\['options'\] .* must be a dict"),
        ({"options": {"trim": {1, 2}}}, "JSON-serialisable"),
    ],
)
def test_invalid_settings_raise_and_leave_no_node(settings, match):
    source = _frame()
    before = {n.node_id for n in source.flow_graph.nodes}
    with pytest.raises(ff.NativeNodeError, match=match):
        ff.CustomNode(NativeCleaner, source, settings=settings)
    assert {n.node_id for n in source.flow_graph.nodes} == before


@pytest.mark.parametrize("count", [0, 2])
def test_wrong_number_of_inputs_raises(count):
    frames = [_frame() for _ in range(count)]
    with pytest.raises(ff.NativeNodeError, match=f"takes 1 input frame\\(s\\), got {count}"):
        ff.CustomNode(NativeCleaner, *frames)


def test_more_than_three_inputs_raise():
    class NativeFourInputs(CustomNodeBase):
        node_name: str = "Native Test Four Inputs"
        number_of_inputs: int = 4

        def process(self, *inputs):
            return inputs[0]

    with pytest.raises(ff.NativeNodeError, match="at most three input frames"):
        ff.CustomNode(NativeFourInputs, *[_frame() for _ in range(4)])


def test_kernel_on_a_local_node_is_refused():
    with pytest.raises(ff.NativeNodeError, match="runs locally"):
        ff.CustomNode(NativeCleaner, _frame(), kernel="ml-kernel")


def test_kernel_takes_an_id_or_an_object_with_an_id_like_python_script():
    by_object = ff.CustomNode(NativeKernelScorer, _frame(), kernel=SimpleNamespace(id="ml-kernel"))
    assert by_object.kernel == "ml-kernel" == by_object.node.setting_input.kernel_id
    with pytest.raises(ff.NativeNodeError, match=r"kernel= takes a kernel id or an object with an \.id, got int"):
        ff.CustomNode(NativeKernelScorer, _frame(), kernel=42)
    with pytest.raises(ff.NativeNodeError, match="runs locally"):
        ff.CustomNode(NativeCleaner, _frame(), kernel=SimpleNamespace(id="ml-kernel"))


def test_kernel_node_without_a_kernel_raises_and_leaves_no_node():
    source = _frame()
    with pytest.raises(ff.NativeNodeError, match="select a kernel first") as info:
        ff.CustomNode(NativeKernelScorer, source)
    assert isinstance(info.value.__cause__, KernelRequiredError)
    assert [n.node_id for n in source.flow_graph.nodes] == [source.node_id]


# execution


def test_local_node_collects_what_process_returns():
    settings = {"options": {"trim": True}}
    out = ff.CustomNode(NativeCleaner, _frame(), settings=settings).output
    expected = NativeCleaner.from_settings(settings).process(pl.LazyFrame(DATA)).collect()
    assert_frame_equal(out.collect(), expected)


def test_multi_output_node_indexes_by_name():
    node = ff.CustomNode(NativeSplitter, _frame(), settings={"main": {"threshold": "2"}})
    assert node.outputs == ["kept", "dropped"]
    assert node.node.setting_input.output_names == ["kept", "dropped"]
    assert (node["kept"].output_handle, node["dropped"].output_handle) == ("output-0", "output-1")
    assert node["kept"].collect()["amount"].to_list() == [2, 3]
    assert node["dropped"].collect()["amount"].to_list() == [1]
    with pytest.raises(ff.NativeNodeError, match=r"outputs \['kept', 'dropped'\]"):
        _ = node.output


def test_zero_input_hookless_local_node_runs_eagerly():
    flow = ff.create_flow_graph()
    node = ff.CustomNode(NativeGenerator, flow_graph=flow)
    assert node.flow_graph is flow
    assert node.output._deferred is False
    assert node.node.deferred_until_run is False
    assert_frame_equal(node.output.collect(), pl.DataFrame({"generated": [1, 2]}))


def test_session_class_on_a_remote_graph_builds_eagerly_as_core_runs_it_in_process():
    """A class with no node file is never offloaded to the worker, so it builds like on a local graph."""
    calls: list[str] = []

    class NativeCountingGenerator(CustomNodeBase):
        node_name: str = "Native Test Counting Generator"
        number_of_inputs: int = 0

        def process(self, *inputs):
            calls.append("ran")
            return pl.LazyFrame({"generated": [1]})

    flow = ff.create_flow_graph()
    flow.flow_settings.execution_location = "remote"
    node = ff.CustomNode(NativeCountingGenerator, flow_graph=flow)
    assert node.output._deferred is False
    assert calls  # process() ran while building
    assert node.output.collect()["generated"].to_list() == [1]


def test_kernel_node_is_deferred_and_seeded_from_its_hook():
    source = _frame()
    node = ff.CustomNode(NativeKernelScorer, source, kernel="ml-kernel")
    core = node.node
    assert core.setting_input.kernel_id == "ml-kernel"
    assert core._executes_on_kernel is True
    assert core.deferred_until_run is True
    assert node["main"]._deferred and node["stats"]._deferred
    assert node["main"].data.collect_schema().names() == ["name", "amount", "score"]
    assert node["stats"].data.collect_schema() == pl.Schema({"metric": pl.String(), "value": pl.Float64()})
    assert node["main"].data.collect().height == 0
    chained = node["main"].select("score")
    assert chained._deferred is True
    assert core.deferred_until_run is True


def test_hookless_data_dependent_node_is_deferred():
    class NativeDataDependent(CustomNodeBase):
        node_name: str = "Native Test Data Dependent"
        requires_data_for_prediction: bool = True

        def process(self, *inputs):
            raise AssertionError("never runs at build time")

    node = ff.CustomNode(NativeDataDependent, _frame())
    assert node.output._deferred is True
    assert node.node.deferred_until_run is True


def test_data_needing_hook_behind_an_unrun_kernel_is_deferred():
    class NativeDataHook(CustomNodeBase):
        node_name: str = "Native Test Data Hook"
        requires_data_for_prediction: bool = True

        def process(self, *inputs):
            raise AssertionError("never runs at build time")

        def predict_output_schema(self, *inputs):
            return inputs[0]

    script = ff.PythonScript(_frame(), code="output = input", kernel="ml-kernel")
    node = ff.CustomNode(NativeDataHook, script.output)
    assert node.node._schema_prediction_blocked
    assert node.output._deferred is True
    assert node.node.deferred_until_run is True


# save / open


def test_round_trip_keeps_the_custom_node_and_its_settings():
    source = _frame()
    node = ff.CustomNode(NativeCleaner, source, settings={"options": {"trim": True}}, description="clean")
    out = node.output.select("name")
    expected = out.collect()

    reopened, _ = round_trip(out, "custom_roundtrip.yaml")
    reopened_node = reopened.get_node(node.node_id)
    assert reopened_node.node_type == "native_test_cleaner"
    assert reopened_node.setting_input.is_user_defined is True
    assert reopened_node.setting_input.settings == node.settings
    assert reopened_node.setting_input.description == "clean"
    assert reopened_node.node_inputs.main_inputs[0].node_id == source.node_id

    run_info = reopened.run_graph()
    assert run_info.success is True
    assert_frame_equal(reopened.get_node(out.node_id).get_resulting_data().collect(), expected)


# factory


def test_factory_builds_the_same_node_as_the_canonical_form():
    source = _frame()
    via_factory = ff.custom_node(NativeCleaner)(source, trim=True)
    canonical = ff.CustomNode(NativeCleaner, source, settings={"options": {"trim": True}}).output

    factory_node = via_factory.flow_graph.get_node(via_factory.node_id)
    canonical_node = canonical.flow_graph.get_node(canonical.node_id)
    assert factory_node.node_type == canonical_node.node_type == "native_test_cleaner"
    assert factory_node.setting_input.settings == canonical_node.setting_input.settings
    assert factory_node.setting_input.is_user_defined is True
    assert_frame_equal(via_factory.collect(), canonical.collect())


def test_factory_accepts_a_name_and_an_instance():
    ff.CustomNode(NativeCleaner, _frame())
    by_name = ff.custom_node("native_test_cleaner")
    assert by_name.node_class is NativeCleaner
    out = by_name(_frame(), trim=True)
    assert out.flow_graph.get_node(out.node_id).setting_input.settings["options"]["trim"] is True

    by_instance = ff.custom_node(NativeCleaner.from_settings({"options": {"column": "amount"}}))
    assert inspect.signature(by_instance).parameters["column"].default == "amount"
    out = by_instance(_frame(), trim=False)
    assert out.flow_graph.get_node(out.node_id).setting_input.settings == {
        "options": {"trim": False, "column": "amount"}
    }


def test_factory_signature_lists_the_components_with_defaults():
    parameters = inspect.signature(ff.custom_node(NativeCleaner)).parameters
    assert list(parameters) == [
        "inputs",
        "trim",
        "column",
        "kernel",
        "deferred",
        "schemas",
        "description",
        "settings",
        "flow_graph",
    ]
    assert parameters["inputs"].kind is inspect.Parameter.VAR_POSITIONAL
    assert parameters["trim"].kind is inspect.Parameter.KEYWORD_ONLY
    assert (parameters["trim"].default, parameters["column"].default) == (False, "name")
    assert parameters["kernel"].default is None


def test_factory_unknown_keyword_lists_the_valid_names():
    with pytest.raises(ff.NativeNodeError, match=r"Unknown settings \['trimm'\].*\['column', 'trim'\]"):
        ff.custom_node(NativeCleaner)(_frame(), trimm=True)


def test_component_in_two_sections_needs_the_section_prefix():
    factory = ff.custom_node(NativeTwoSections)
    assert set(factory.parameters) == {"first__label", "second__label", "unique", "second__kernel"}
    with pytest.raises(ff.NativeNodeError, match=r"one of \['first__label', 'second__label'\]"):
        factory(_frame(), label="x")

    out = factory(_frame(), first__label="x", unique="u", second__kernel="text")
    assert out.flow_graph.get_node(out.node_id).setting_input.settings == {
        "first": {"label": "x", "unique": "u"},
        "second": {"label": "b", "kernel": "text"},
    }


def test_factory_refuses_a_component_given_both_ways():
    with pytest.raises(ff.NativeNodeError, match="given twice"):
        ff.custom_node(NativeCleaner)(_frame(), trim=True, settings={"options": {"trim": False}})


def test_factory_merges_nested_settings_with_keywords():
    out = ff.custom_node(NativeCleaner)(_frame(), trim=True, settings={"options": {"column": "name"}})
    assert out.flow_graph.get_node(out.node_id).setting_input.settings == {"options": {"trim": True, "column": "name"}}


def test_factory_call_on_a_multi_output_node_points_at_node():
    factory = ff.custom_node(NativeSplitter)
    source = _frame()
    with pytest.raises(ff.NativeNodeError, match=r"\.node\(\.\.\.\)"):
        factory(source, threshold="3")
    assert [n.node_id for n in source.flow_graph.nodes] == [source.node_id]

    node = factory.node(source, threshold="3")
    assert isinstance(node, ff.CustomNode)
    assert node["kept"].collect()["amount"].to_list() == [3]


# side-effect nodes, deferred=, schemas=


SINK_CALLS: list[str] = []


class NativeSink(CustomNodeBase):
    """A custom output node whose palette group comes from its category, not from ``node_type``."""

    node_name: str = "Native Test Sink"
    node_category: str = "Testing"
    node_type: str = "output"
    settings_schema: NodeSettings = NodeSettings(main=Section(title="Main", label=TextInput(label="Label")))

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        SINK_CALLS.append(self.settings_schema.main.label.value)
        return inputs[0]


@pytest.fixture
def sink_calls():
    SINK_CALLS.clear()
    yield SINK_CALLS
    SINK_CALLS.clear()


def test_output_node_counts_as_a_side_effect_node_whatever_its_group():
    ff.CustomNode(NativeSink, _frame(), settings={"main": {"label": "plain"}})
    template = node_store.node_dict["native_test_sink"]
    assert template.node_group == "testing" and template.node_type == "output"
    assert ff.native.is_side_effect_node_type("native_test_sink") is True


def test_output_node_below_a_closed_gate_is_seeded_at_build_and_skipped_by_the_run(sink_calls):
    source = _frame()
    ff.add_flow_parameter(source, ff.Parameter("mode", default="quick"))
    gate = ff.Gate(source, parameter="mode", operator="equals", value="full")
    closed = ff.CustomNode(NativeSink, gate.then, settings={"main": {"label": "then"}})
    live = ff.CustomNode(NativeSink, gate.otherwise, settings={"main": {"label": "otherwise"}})
    assert closed.deferred is True and live.deferred is True
    assert sink_calls == []

    run = closed.flow_graph.run_graph()

    results = results_by_id(run)
    assert run.success is True
    assert results[closed.node_id].skipped is True
    assert results[live.node_id].skipped is False
    assert sink_calls == ["otherwise"]


def test_output_node_on_a_plain_frame_still_builds_eagerly(sink_calls):
    node = ff.CustomNode(NativeSink, _frame(), settings={"main": {"label": "plain"}})
    assert node.deferred is False
    assert sink_calls == ["plain"]


def test_output_node_below_a_deferred_frame_is_seeded(sink_calls):
    script = ff.PythonScript(_frame(), code="x = 1", kernel="ml-kernel")
    node = ff.CustomNode(NativeSink, script.output, settings={"main": {"label": "below script"}})
    assert node.deferred is True and node.output._deferred is True
    assert sink_calls == []
    with pytest.raises(ff.NativeNodeError, match="placeholder rows"):
        ff.CustomNode(NativeSink, script.output, deferred=False)


def test_deferred_true_builds_nothing_and_collect_runs_the_graph():
    calls: list[str] = []

    class NativeCountingCleaner(NativeCleaner):
        node_name: str = "Native Test Counting Cleaner"

        def process(self, *inputs):
            calls.append("ran")
            return super().process(*inputs)

    node = ff.CustomNode(NativeCountingCleaner, _frame(), settings={"options": {"trim": True}}, deferred=True)
    assert node.deferred is True and node.output._deferred is True
    assert node.node.deferred_until_run is True
    assert calls == []

    assert node.output.collect()["name"].to_list() == ["ann", "bob", "cy"]
    assert calls == ["ran"]


def test_factory_passes_deferred_and_schemas_through():
    held = ff.custom_node(NativeCleaner)(_frame(), trim=True, deferred=True)
    assert held._deferred is True
    assert held.collect()["name"].to_list() == ["ann", "bob", "cy"]


class NativeHooklessKernel(CustomNodeBase):
    node_name: str = "Native Test Hookless Kernel"
    node_category: str = "Testing"
    environment: str = "kernel"
    number_of_outputs: int = 2
    output_names: list[str] = ["main", "stats"]

    def process(self, *inputs: pl.LazyFrame) -> dict[str, pl.LazyFrame]:
        raise AssertionError("a kernel node never runs at build time")


def test_schemas_give_a_hookless_kernel_node_its_placeholder_columns():
    bare = ff.CustomNode(NativeHooklessKernel, _frame(), kernel="ml-kernel")
    assert bare["main"].columns == []

    node = ff.CustomNode(
        NativeHooklessKernel,
        _frame(),
        kernel="ml-kernel",
        schemas={"main": {"name": ff.String, "score": ff.Float64}},
    )
    assert node["main"].data.collect_schema() == pl.Schema({"name": pl.String(), "score": pl.Float64()})
    assert node["stats"].columns == []
    scores = node["main"].select("score")
    assert scores._deferred is True
    assert scores.columns == ["score"]

    via_factory = ff.custom_node(NativeHooklessKernel).node(
        _frame(), kernel="ml-kernel", schemas={"stats": {"metric": ff.String}}
    )
    assert via_factory["stats"].columns == ["metric"]


def test_schemas_are_checked():
    with pytest.raises(ff.NativeNodeError, match=r"schemas= declares \['other'\]"):
        ff.CustomNode(NativeHooklessKernel, _frame(), kernel="k", schemas={"other": {"x": ff.Int64}})
    with pytest.raises(ff.NativeNodeError, match="predict_output_schema"):
        ff.CustomNode(NativeKernelScorer, _frame(), kernel="k", schemas={"main": {"x": ff.Int64}})


# flow parameters


PARAM_SEEN: list[tuple] = []


class NativeParamScaler(CustomNodeBase):
    node_name: str = "Native Test Param Scaler"
    node_category: str = "Testing"
    settings_schema: NodeSettings = NodeSettings(
        main=Section(
            title="Main",
            column=TextInput(label="Column", default="name"),
            factor=NumericInput(label="Factor", default=1),
            upper=ToggleSwitch(label="Upper"),
        ),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        main = self.settings_schema.main
        PARAM_SEEN.append((main.column.value, main.factor.value, main.upper.value))
        text = pl.col(main.column.value)
        if main.upper.value is True:
            text = text.str.to_uppercase()
        return inputs[0].with_columns(text, (pl.col("amount") * main.factor.value).alias("scaled"))


def test_parameter_values_are_stored_as_references_and_reach_process_resolved_and_typed():
    PARAM_SEEN.clear()
    source = ff.from_dict(DATA)
    factor = ff.add_flow_parameter(source, ff.Parameter("factor", default=3))
    ff.add_flow_parameter(source, ff.Parameter("upper", default=True, type="boolean"))
    ff.add_flow_parameter(source, ff.Parameter("col", default="name"))

    node = ff.CustomNode(
        NativeParamScaler, source, settings={"main": {"factor": factor, "upper": "${upper}", "column": "${col}"}}
    )

    stored = node.node.setting_input.settings
    assert stored == {"main": {"column": "${col}", "factor": "${factor}", "upper": "${upper}"}}
    assert node.settings == stored
    assert PARAM_SEEN[-1] == ("name", 3, True)
    assert node.output.collect()["scaled"].to_list() == [3, 6, 9]

    flow = node.flow_graph
    ff.set_flow_parameter(flow, "factor", 5)
    ff.set_flow_parameter(flow, "upper", False)
    run = flow.run_graph()
    assert run.success is True
    assert PARAM_SEEN[-1] == ("name", 5, False)
    out = flow.get_node(node.node_id).get_resulting_data().collect()
    assert out["scaled"].to_list() == [5, 10, 15]
    assert out["name"].to_list() == DATA["name"]
    assert flow.get_node(node.node_id).setting_input.settings == stored


def test_factory_takes_a_parameter_as_a_keyword_value():
    source = ff.from_dict(DATA)
    factor = ff.add_flow_parameter(source, ff.Parameter("factor", default=2, type="integer"))
    out = ff.custom_node(NativeParamScaler)(source, factor=factor)
    assert out.flow_graph.get_node(out.node_id).setting_input.settings["main"]["factor"] == "${factor}"
    assert out.collect()["scaled"].to_list() == [2, 4, 6]


@pytest.mark.parametrize("value", [ff.Parameter("nope", default=1), "${nope}"])
def test_undeclared_parameter_is_refused_and_leaves_no_node(value):
    source = ff.from_dict(DATA)
    with pytest.raises(ff.NativeNodeError, match=r"references flow parameter\(s\) \['nope'\], which are not declared"):
        ff.CustomNode(NativeParamScaler, source, settings={"main": {"factor": value}})
    assert [n.node_id for n in source.flow_graph.nodes] == [source.node_id]


# secrets


class NativeSecretReader(CustomNodeBase):
    node_name: str = "Native Test Secret Reader"
    node_category: str = "Testing"
    settings_schema: NodeSettings = NodeSettings(auth=Section(title="Auth", token=SecretSelector(label="Token")))

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        token = self.settings_schema.auth.token.secret_value.get_secret_value()
        return inputs[0].with_columns(pl.lit(len(token)).alias("token_length"))


def test_missing_secret_at_build_names_deferred():
    missing = "native_test_secret_that_does_not_exist"
    with pytest.raises(ff.NativeNodeError, match=f"Secret '{missing}' not found.*pass deferred=True"):
        ff.CustomNode(NativeSecretReader, _frame(), settings={"auth": {"token": missing}})

    node = ff.CustomNode(NativeSecretReader, _frame(), settings={"auth": {"token": missing}}, deferred=True)
    assert node.deferred is True
    assert node.node.setting_input.settings == {"auth": {"token": missing}}
