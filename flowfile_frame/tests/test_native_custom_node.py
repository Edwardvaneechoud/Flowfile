"""``ff.CustomNode`` and the ``ff.custom_node(...)`` factory: place user-defined nodes from Python."""

import inspect
import time
from types import SimpleNamespace

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.configs import node_store
from flowfile_core.flowfile.user_defined.registry import KernelRequiredError
from shared.node_designer import CustomNodeBase, NodeSettings, Section, TextInput, ToggleSwitch

from .native_helpers import round_trip

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


def test_local_node_on_a_remote_graph_is_deferred_and_never_runs():
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
    time.sleep(0.2)  # let any background schema prefetch surface
    assert node.output._deferred is True
    assert node.node.deferred_until_run is True
    assert calls == []


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
    assert list(parameters) == ["inputs", "trim", "column", "kernel", "description", "settings", "flow_graph"]
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
