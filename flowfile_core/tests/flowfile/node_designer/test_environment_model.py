"""
Tests for the execution-environment model:

- legacy requires_kernel / kernel_id shims map onto environment
- environment + dependencies surface in the frontend schema
- kernel-required nodes raise KernelRequiredError (HTTP 422 KERNEL_REQUIRED)
- the run-time dependency pre-check (KernelDependencyError) blocks only provable mismatches
- UserDefinedNode v2 accepts legacy payload shapes unchanged
"""
from unittest.mock import MagicMock

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.configs import node_store
from flowfile_core.flowfile import flow_graph as flow_graph_module
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput
from flowfile_core.flowfile.user_defined.registry import KernelDependencyError, KernelRequiredError
from flowfile_core.kernel.models import ImageFlavour, KernelInfo
from flowfile_core.schemas import input_schema, schemas


class PlainNode(CustomNodeBase):
    node_name: str = "Env Plain Node"

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]


class LegacyKernelNode(CustomNodeBase):
    node_name: str = "Env Legacy Kernel Node"
    requires_kernel: bool = True

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]


class LegacyKernelIdNode(CustomNodeBase):
    node_name: str = "Env Legacy Kernel Id Node"
    kernel_id: str = "design-time-kernel"

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]


class KernelEnvNode(CustomNodeBase):
    node_name: str = "Env Kernel Node"
    environment: str = "kernel"
    dependencies: list = ["scikit-learn>=1.4"]

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]


class TestLegacyShims:
    def test_plain_node_defaults(self):
        node = PlainNode()
        assert node.environment == "local"
        assert node.dependencies == []
        assert node.uses_kernel is False

    def test_requires_kernel_maps_to_environment(self):
        node = LegacyKernelNode()
        assert node.environment == "kernel"
        assert node.requires_kernel is True  # field preserved for old readers
        assert node.uses_kernel is True

    def test_kernel_id_keeps_legacy_uses_kernel_semantics(self):
        node = LegacyKernelIdNode()
        assert node.environment == "local"
        assert node.uses_kernel is True
        assert node.kernel_id == "design-time-kernel"

    def test_explicit_environment_kernel(self):
        node = KernelEnvNode()
        assert node.environment == "kernel"
        assert node.uses_kernel is True


class TestFrontendSchema:
    def test_schema_includes_environment_and_dependencies(self):
        schema = KernelEnvNode().get_frontend_schema()
        assert schema["environment"] == "kernel"
        assert schema["dependencies"] == ["scikit-learn>=1.4"]
        # legacy keys stay for older frontends
        assert "requires_kernel" in schema
        assert "kernel_id" in schema

    def test_schema_round_trips_environment(self):
        schema = PlainNode().get_frontend_schema()
        assert schema["environment"] == "local"
        assert schema["dependencies"] == []


def _make_graph(flow_id: int = 4801) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name="env_test", path="."))
    return handler.get_flow(flow_id)


@pytest.fixture
def store_snapshot():
    """Register test node classes freely; restores every store view afterwards."""
    saved_store = dict(node_store.CUSTOM_NODE_STORE)
    saved_dict = dict(node_store.node_dict)
    saved_list = list(node_store.nodes_list)
    yield node_store
    node_store.CUSTOM_NODE_STORE.clear()
    node_store.CUSTOM_NODE_STORE.update(saved_store)
    node_store.node_dict.clear()
    node_store.node_dict.update(saved_dict)
    node_store.nodes_list[:] = saved_list


class TestKernelRequiredGate:
    def test_kernel_environment_without_kernel_raises(self):
        graph = _make_graph()
        settings = input_schema.UserDefinedNode(flow_id=graph.flow_id, node_id=1, settings={}, is_user_defined=True)
        with pytest.raises(KernelRequiredError) as exc_info:
            graph.add_user_defined_node(custom_node=KernelEnvNode(), user_defined_node_settings=settings)
        assert isinstance(exc_info.value, ValueError)  # backward compat for existing except clauses
        assert exc_info.value.node_type == "env_kernel_node"

    def test_legacy_requires_kernel_without_kernel_raises(self):
        graph = _make_graph(4802)
        settings = input_schema.UserDefinedNode(flow_id=graph.flow_id, node_id=1, settings={}, is_user_defined=True)
        with pytest.raises(KernelRequiredError):
            graph.add_user_defined_node(custom_node=LegacyKernelNode(), user_defined_node_settings=settings)

    def test_design_time_kernel_id_satisfies_the_gate(self, store_snapshot):
        graph = _make_graph(4803)

        class PinnedKernelNode(CustomNodeBase):
            node_name: str = "Env Pinned Kernel Node"
            requires_kernel: bool = True
            kernel_id: str = "pinned-kernel"

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]

        store_snapshot.add_to_custom_node_store(PinnedKernelNode)
        settings = input_schema.UserDefinedNode(flow_id=graph.flow_id, node_id=1, settings={}, is_user_defined=True)
        graph.add_user_defined_node(custom_node=PinnedKernelNode(), user_defined_node_settings=settings)
        assert graph.get_node(1) is not None

    def test_route_returns_422_kernel_required(self, store_snapshot):
        with TestClient(main.app) as c:
            token = c.post("/auth/token").json()["access_token"]
        client = TestClient(main.app)
        client.headers = {"Authorization": f"Bearer {token}"}

        store_snapshot.add_to_custom_node_store(KernelEnvNode)
        flow_id = 4804
        if flow_file_handler.get_flow(flow_id) is not None:
            flow_file_handler.delete_flow(flow_id)
        flow_file_handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name="env_422", path="."))
        graph = flow_file_handler.get_flow(flow_id)
        graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="env_kernel_node"))

        response = client.post(
            "/user_defined_components/update_user_defined_node",
            params={"node_type": "env_kernel_node"},
            json={"flow_id": flow_id, "node_id": 1, "settings": {}},
        )
        assert response.status_code == 422, response.text
        detail = response.json()["detail"]
        assert detail["error_code"] == "KERNEL_REQUIRED"
        assert detail["node_type"] == "env_kernel_node"


class TestKernelDependencyPrecheck:
    """The run-time gate in _execute_on_kernel: only provable mismatches block.

    Only the gate is under test — the mocked manager makes everything after it
    fail on unrelated types, which the pass-through cases deliberately swallow.
    """

    def _manager(self, kernel: KernelInfo | None) -> MagicMock:
        manager = MagicMock()
        manager.get_kernel_sync.return_value = kernel
        return manager

    def test_missing_dependency_blocks_before_execute(self, monkeypatch):
        graph = _make_graph(4806)
        bare = KernelInfo(id="k1", name="Bare kernel", image_flavour=ImageFlavour.BASE)
        manager = self._manager(bare)
        monkeypatch.setattr(flow_graph_module, "get_kernel_manager", lambda: manager)

        with pytest.raises(KernelDependencyError) as exc_info:
            graph._execute_on_kernel(
                node_id=1,
                kernel_id="k1",
                code="",
                output_names=["main"],
                flow_data_engine=(),
                required_dependencies=["scikit-learn>=1.4"],
                node_type="env_kernel_node",
            )
        message = str(exc_info.value)
        assert "is missing packages" in message
        assert "scikit-learn>=1.4" in message
        assert "Bare kernel" in message
        assert isinstance(exc_info.value, ValueError)  # existing except clauses keep working
        manager.execute_sync.assert_not_called()

    def test_unverified_version_does_not_block(self, monkeypatch):
        graph = _make_graph(4807)
        legacy = KernelInfo(id="k1", name="Legacy", packages=["scikit-learn"])  # no resolved versions
        manager = self._manager(legacy)
        monkeypatch.setattr(flow_graph_module, "get_kernel_manager", lambda: manager)

        try:
            graph._execute_on_kernel(
                node_id=1,
                kernel_id="k1",
                code="",
                output_names=["main"],
                flow_data_engine=(),
                required_dependencies=["scikit-learn>=1.4"],
                node_type="env_kernel_node",
            )
        except KernelDependencyError:
            pytest.fail("pre-check blocked an unverifiable dependency")
        except Exception:
            pass

    def test_unknown_kernel_does_not_block(self, monkeypatch):
        graph = _make_graph(4808)
        manager = self._manager(None)  # id not in the manager: execute_sync owns that error
        monkeypatch.setattr(flow_graph_module, "get_kernel_manager", lambda: manager)

        try:
            graph._execute_on_kernel(
                node_id=1,
                kernel_id="gone",
                code="",
                output_names=["main"],
                flow_data_engine=(),
                required_dependencies=["scikit-learn>=1.4"],
                node_type="env_kernel_node",
            )
        except KernelDependencyError:
            pytest.fail("pre-check must not block for an unknown kernel id")
        except Exception:
            pass

    def test_no_dependencies_skips_lookup(self, monkeypatch):
        graph = _make_graph(4809)
        manager = self._manager(None)
        monkeypatch.setattr(flow_graph_module, "get_kernel_manager", lambda: manager)

        try:
            graph._execute_on_kernel(
                node_id=1, kernel_id="k1", code="", output_names=["main"], flow_data_engine=()
            )
        except Exception:
            pass
        manager.get_kernel_sync.assert_not_called()


class TestUserDefinedNodeV2:
    def test_none_settings_coerced_to_empty_dict(self):
        node = input_schema.UserDefinedNode(flow_id=1, node_id=1, settings=None)
        assert node.settings == {}

    def test_missing_settings_defaults_to_empty_dict(self):
        node = input_schema.UserDefinedNode(flow_id=1, node_id=1)
        assert node.settings == {}

    def test_non_dict_settings_coerced(self):
        node = input_schema.UserDefinedNode(flow_id=1, node_id=1, settings="garbage")
        assert node.settings == {}

    def test_non_dict_section_value_preserved_best_effort(self):
        node = input_schema.UserDefinedNode(flow_id=1, node_id=1, settings={"main": "raw_value"})
        assert node.settings == {"main": {"value": "raw_value"}}

    def test_legacy_payload_validates_unchanged(self):
        legacy = {
            "flow_id": 1,
            "node_id": 2,
            "settings": {"main_section": {"standard_input": "hello", "column_name": "col"}},
            "kernel_id": None,
            "output_names": ["main"],
            "is_user_defined": True,
        }
        node = input_schema.UserDefinedNode.model_validate(legacy)
        assert node.settings == legacy["settings"]
        assert node.node_source_hash is None
        assert node.settings_format_version == 1

    def test_new_fields_serialize_harmlessly(self):
        node = input_schema.UserDefinedNode(
            flow_id=1, node_id=1, settings={"s": {"c": 1}}, node_source_hash="ab" * 32
        )
        dumped = node.model_dump()
        assert dumped["node_source_hash"] == "ab" * 32
        assert dumped["settings_format_version"] == 1
        assert input_schema.UserDefinedNode.model_validate(dumped).settings == {"s": {"c": 1}}


class TestCustomNodeInfoDependencies:
    def test_node_info_exposes_dependencies(self, tmp_path):
        from flowfile_core.flowfile.user_defined.registry import CustomNodeRegistry
        from flowfile_core.routes.user_defined_components import _node_info_from_entry

        nodes_dir = tmp_path / "nodes"
        nodes_dir.mkdir()
        (nodes_dir / "dep_node.py").write_text(
            "import polars as pl\n"
            "from flowfile_core.flowfile.node_designer import CustomNodeBase\n\n\n"
            "class DepNode(CustomNodeBase):\n"
            '    node_name: str = "Dep Node"\n'
            '    environment: str = "kernel"\n'
            '    dependencies: list = ["scikit-learn>=1.4", "numpy"]\n\n'
            "    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n"
            "        return inputs[0]\n"
        )
        registry = CustomNodeRegistry(directory=nodes_dir)
        entry = registry.load_file(nodes_dir / "dep_node.py")
        info = _node_info_from_entry(entry)
        assert info.environment == "kernel"
        assert info.dependencies == ["scikit-learn>=1.4", "numpy"]

    def test_local_node_defaults_to_no_dependencies(self, tmp_path):
        from flowfile_core.flowfile.user_defined.registry import CustomNodeRegistry
        from flowfile_core.routes.user_defined_components import _node_info_from_entry

        nodes_dir = tmp_path / "nodes"
        nodes_dir.mkdir()
        (nodes_dir / "plain_node.py").write_text(
            "import polars as pl\n"
            "from flowfile_core.flowfile.node_designer import CustomNodeBase\n\n\n"
            "class PlainDepNode(CustomNodeBase):\n"
            '    node_name: str = "Plain Dep Node"\n\n'
            "    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n"
            "        return inputs[0]\n"
        )
        registry = CustomNodeRegistry(directory=nodes_dir)
        entry = registry.load_file(nodes_dir / "plain_node.py")
        info = _node_info_from_entry(entry)
        assert info.dependencies == []


class TestNodeSourceHashStamp:
    def test_add_stamps_hash_from_registry(self, tmp_path, monkeypatch, store_snapshot):
        import sys

        from flowfile_core.flowfile.user_defined.registry import CustomNodeRegistry

        nodes_dir = tmp_path / "nodes"
        nodes_dir.mkdir()
        (nodes_dir / "stamp_node.py").write_text(
            "import polars as pl\n"
            "from flowfile_core.flowfile.node_designer import CustomNodeBase\n\n\n"
            "class StampNode(CustomNodeBase):\n"
            '    node_name: str = "Stamp Node"\n\n'
            "    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n"
            "        return inputs[0]\n"
        )
        local_registry = CustomNodeRegistry(directory=nodes_dir)
        entry = local_registry.load_file(nodes_dir / "stamp_node.py")
        local_registry.ensure_class(entry)
        assert entry.node_class is not None
        store_snapshot.add_to_custom_node_store(entry.node_class)

        flow_graph_module = sys.modules["flowfile_core.flowfile.flow_graph"]
        monkeypatch.setattr(flow_graph_module, "user_defined_registry", local_registry)

        graph = _make_graph(4805)
        settings = input_schema.UserDefinedNode(flow_id=graph.flow_id, node_id=1, settings={}, is_user_defined=True)
        graph.add_user_defined_node(custom_node=entry.node_class(), user_defined_node_settings=settings)

        assert settings.node_source_hash == entry.source_hash
