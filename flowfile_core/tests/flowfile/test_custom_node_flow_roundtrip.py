"""
Graceful-degradation contract for custom nodes in saved flows:

- a flow with a custom node saves, reopens, and runs with its values intact
- a flow always opens: a missing node type loads in an error state with its
  settings preserved verbatim and its connections rendered
- running such a flow fails that node cleanly; re-saving is lossless
- schema drift surfaces as a node warning while matching values still apply
"""
import tempfile
from pathlib import Path

import polars as pl
import pytest
import yaml

from flowfile_core.configs import node_store
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput
from flowfile_core.schemas import input_schema, schemas


class RoundtripFixedColumn(CustomNodeBase):
    node_name: str = "Roundtrip Fixed Column"
    node_category: str = "Testing"
    title: str = "Roundtrip Fixed Column"
    intro: str = "Adds a column with a fixed value."

    settings_schema: NodeSettings = NodeSettings(
        main_section=Section(
            title="Configuration",
            standard_input=TextInput(label="Fixed Value"),
            column_name=TextInput(label="New Column Name"),
        ),
    )

    def process(self, *inputs):
        input_df = inputs[0]
        fixed_value = self.settings_schema.main_section.standard_input.value
        new_col_name = self.settings_schema.main_section.column_name.value
        return input_df.with_columns(pl.lit(fixed_value).alias(new_col_name))


NODE_TYPE = "roundtrip_fixed_column"
SETTINGS = {"main_section": {"standard_input": "hello", "column_name": "custom_col"}}


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
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


def create_graph(flow_id: int) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="roundtrip_flow", path=".", execution_mode="Development")
    )
    return handler.get_flow(flow_id)


def build_flow_with_custom_node(flow_id: int, settings: dict | None = None) -> FlowGraph:
    flow = create_graph(flow_id)
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}, {"a": 2}])
        )
    )
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type=NODE_TYPE))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    node_settings = input_schema.UserDefinedNode(
        flow_id=flow_id, node_id=2, settings=settings if settings is not None else SETTINGS, is_user_defined=True
    )
    flow.add_user_defined_node(
        custom_node=RoundtripFixedColumn.from_settings(node_settings.settings),
        user_defined_node_settings=node_settings,
    )
    return flow


def test_save_open_run_roundtrip(temp_dir, store_snapshot):
    store_snapshot.add_to_custom_node_store(RoundtripFixedColumn)
    flow = build_flow_with_custom_node(6001)
    yaml_path = temp_dir / "roundtrip.yaml"
    flow.save_flow(str(yaml_path))

    loaded = open_flow(yaml_path)
    node = loaded.get_node(2)
    assert node.setting_input.settings == SETTINGS
    assert node.setting_input.is_user_defined is True
    assert node.setting_input.node_source_hash is None  # not a file-backed node

    result = loaded.run_graph()
    assert result.success, result
    expected = FlowDataEngine({"a": [1, 2], "custom_col": ["hello", "hello"]})
    expected.assert_equal(loaded.get_node(2).get_resulting_data())


def test_yaml_carries_user_defined_marker(temp_dir, store_snapshot):
    store_snapshot.add_to_custom_node_store(RoundtripFixedColumn)
    flow = build_flow_with_custom_node(6002)
    yaml_path = temp_dir / "marker.yaml"
    flow.save_flow(str(yaml_path))

    data = yaml.safe_load(yaml_path.read_text())
    custom_node = next(n for n in data["nodes"] if n["id"] == 2)
    assert custom_node["setting_input"]["is_user_defined"] is True
    assert custom_node["setting_input"]["settings"] == SETTINGS


class TestMissingNodeGracefulDegradation:
    @pytest.fixture
    def saved_flow_path(self, temp_dir, store_snapshot):
        store_snapshot.add_to_custom_node_store(RoundtripFixedColumn)
        flow = build_flow_with_custom_node(6003)
        yaml_path = temp_dir / "missing_node.yaml"
        flow.save_flow(str(yaml_path))
        # simulate the node not being installed on this machine
        store_snapshot.remove_from_custom_node_store(NODE_TYPE)
        assert NODE_TYPE not in node_store.CUSTOM_NODE_STORE
        return yaml_path

    def test_flow_opens_with_error_state_and_settings_preserved(self, saved_flow_path):
        loaded = open_flow(saved_flow_path)

        node = loaded.get_node(2)
        assert node is not None, "missing custom node must never be dropped"
        assert node.node_type == NODE_TYPE
        assert "not installed" in node.results.errors
        assert node.setting_input.settings == SETTINGS
        assert node.setting_input.is_user_defined is True
        # connections still render
        assert [n.node_id for n in node.all_inputs] == [1]

    def test_running_fails_the_node_cleanly(self, saved_flow_path):
        loaded = open_flow(saved_flow_path)
        result = loaded.run_graph()
        assert not result.success
        step = next(s for s in result.node_step_result if s.node_id == 2)
        assert not step.success
        assert "not installed" in str(step.error)

    def test_resave_is_lossless(self, saved_flow_path, temp_dir):
        loaded = open_flow(saved_flow_path)
        resaved_path = temp_dir / "resaved.yaml"
        loaded.save_flow(str(resaved_path))

        original = yaml.safe_load(saved_flow_path.read_text())
        resaved = yaml.safe_load(resaved_path.read_text())
        original_node = next(n for n in original["nodes"] if n["id"] == 2)
        resaved_node = next(n for n in resaved["nodes"] if n["id"] == 2)
        assert resaved_node["type"] == NODE_TYPE
        assert resaved_node["setting_input"]["settings"] == original_node["setting_input"]["settings"]
        assert resaved_node["setting_input"]["is_user_defined"] is True

    def test_reopen_after_reinstall_recovers(self, saved_flow_path, store_snapshot):
        store_snapshot.add_to_custom_node_store(RoundtripFixedColumn)
        loaded = open_flow(saved_flow_path)
        node = loaded.get_node(2)
        assert node.results.errors is None
        result = loaded.run_graph()
        assert result.success


class TestSchemaDrift:
    def test_unknown_keys_warn_and_matching_values_apply(self, store_snapshot):
        store_snapshot.add_to_custom_node_store(RoundtripFixedColumn)
        flow = create_graph(6004)
        flow.add_node_promise(input_schema.NodePromise(flow_id=6004, node_id=1, node_type="manual_input"))
        flow.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=6004, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
            )
        )
        flow.add_node_promise(input_schema.NodePromise(flow_id=6004, node_id=2, node_type=NODE_TYPE))
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        drifted_settings = {
            "main_section": {"standard_input": "kept", "column_name": "kept_col", "gone_component": 1},
            "gone_section": {"x": 1},
        }
        node_settings = input_schema.UserDefinedNode(
            flow_id=6004, node_id=2, settings=drifted_settings, is_user_defined=True
        )
        custom_node = RoundtripFixedColumn.from_settings(node_settings.settings)
        flow.add_user_defined_node(custom_node=custom_node, user_defined_node_settings=node_settings)

        node = flow.get_node(2)
        assert "gone_section" in node.results.warnings
        assert "main_section.gone_component" in node.results.warnings
        # values that still match the schema survive
        assert custom_node.settings_schema.main_section.standard_input.value == "kept"
        # the raw stored settings stay untouched (lossless)
        assert node.setting_input.settings == drifted_settings

    def test_drift_survives_yaml_roundtrip(self, temp_dir, store_snapshot):
        store_snapshot.add_to_custom_node_store(RoundtripFixedColumn)
        drifted = {
            "main_section": {"standard_input": "kept", "column_name": "kept_col"},
            "legacy_section": {"old": True},
        }
        flow = build_flow_with_custom_node(6005, settings=drifted)
        yaml_path = temp_dir / "drift.yaml"
        flow.save_flow(str(yaml_path))

        loaded = open_flow(yaml_path)
        node = loaded.get_node(2)
        assert "legacy_section" in node.results.warnings
        assert node.setting_input.settings == drifted

        result = loaded.run_graph()
        assert result.success
        expected = FlowDataEngine({"a": [1, 2], "kept_col": ["kept", "kept"]})
        expected.assert_equal(loaded.get_node(2).get_resulting_data())
