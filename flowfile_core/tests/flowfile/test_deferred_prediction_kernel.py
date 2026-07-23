"""Milestone integration test for kernel-gated schema prediction.

A one-hot-style hook directly downstream of an un-run kernel custom node:
  (a) drawer-path prediction triggers ZERO kernel executions and returns fast;
  (b) the user-facing warning names the un-run kernel node (drawer field set);
  (c) after a real run, prediction resolves the exact dummy columns and the
      warning clears.
"""
import time
from contextlib import contextmanager

import polars as pl
import pytest

from flowfile_core.configs import node_store
from flowfile_core.configs.settings import OFFLOAD_TO_WORKER
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput
from flowfile_core.schemas import input_schema, schemas

pytestmark = pytest.mark.kernel

EXPECTED_COLUMNS = ["cat", "f0", "cat_0", "cat_1", "cat_2"]


class CategorySourceKernelNode(CustomNodeBase):
    node_name: str = "Category Source Kernel Node"
    node_category: str = "Testing"
    environment: str = "kernel"
    settings_schema: NodeSettings = NodeSettings(
        main_section=Section(title="Config", note=TextInput(label="Note")),
    )

    def process(self, *inputs):
        import polars as _pl

        frame = _pl.select(_pl.int_range(0, 3000).alias("i")).with_columns(
            [
                (_pl.col("i") % 3).cast(_pl.String).alias("cat"),
                _pl.col("i").cast(_pl.Float64).alias("f0"),
            ]
        )
        return frame.drop("i")


class OneHotDownstreamNode(CustomNodeBase):
    node_name: str = "One Hot Downstream Node"
    node_category: str = "Testing"
    settings_schema: NodeSettings = NodeSettings(
        main_section=Section(title="Config", note=TextInput(label="Note")),
    )

    def process(self, *inputs):
        return inputs[0]

    def predict_output_schema(self, *inputs):
        values = inputs[0].select(pl.col("cat").unique().sort()).collect(engine="streaming")["cat"].to_list()
        if not values:
            return None
        return inputs[0].with_columns([pl.lit(0).alias(f"cat_{v}") for v in values])


@contextmanager
def _kernel_flow_env(manager):
    import flowfile_core.kernel as kernel_module

    saved_store = dict(node_store.CUSTOM_NODE_STORE)
    saved_dict = dict(node_store.node_dict)
    saved_list = list(node_store.nodes_list)
    previous_manager = kernel_module._manager
    previous_offload = bool(OFFLOAD_TO_WORKER)
    kernel_module._manager = manager
    OFFLOAD_TO_WORKER.set(False)
    try:
        yield
    finally:
        OFFLOAD_TO_WORKER.set(previous_offload)
        kernel_module._manager = previous_manager
        node_store.CUSTOM_NODE_STORE.clear()
        node_store.CUSTOM_NODE_STORE.update(saved_store)
        node_store.node_dict.clear()
        node_store.node_dict.update(saved_dict)
        node_store.nodes_list[:] = saved_list


def _build_flow(kernel_id: str, flow_id: int) -> FlowGraph:
    node_store.add_to_custom_node_store(CategorySourceKernelNode)
    node_store.add_to_custom_node_store(OneHotDownstreamNode)
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="kernel_gate_flow",
            path=".",
            execution_mode="Development",
            execution_location="local",
        )
    )
    flow = handler.get_flow(flow_id)
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"seed": 1}])
        )
    )
    kernel_instance = CategorySourceKernelNode.from_settings({})
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type=kernel_instance.item))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_user_defined_node(
        custom_node=kernel_instance,
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=flow_id, node_id=2, settings={}, is_user_defined=True, kernel_id=kernel_id
        ),
    )
    hook_instance = OneHotDownstreamNode.from_settings({})
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=3, node_type=hook_instance.item))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))
    flow.add_user_defined_node(
        custom_node=hook_instance,
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=flow_id, node_id=3, settings={}, is_user_defined=True
        ),
    )
    return flow


def test_milestone_kernel_gate_then_run(kernel_manager, monkeypatch):
    manager, kernel_id = kernel_manager
    with _kernel_flow_env(manager):
        flow = _build_flow(kernel_id, flow_id=51)
        kernel_node, hook_node = flow.get_node(2), flow.get_node(3)

        executions: list = []
        real_execute = manager.execute_sync

        def counting_execute(*args, **kwargs):
            executions.append(1)
            return real_execute(*args, **kwargs)

        monkeypatch.setattr(manager, "execute_sync", counting_execute)

        # (a) drawer-path prediction: fast, zero kernel executions.
        start = time.monotonic()
        schema = hook_node.get_predicted_schema()
        elapsed = time.monotonic() - start
        assert not schema
        assert elapsed < 1.5, f"prediction blocked the drawer path for {elapsed:.1f}s"
        assert executions == [], "prediction must never execute the kernel"

        # (b) the warning names the kernel node and reaches the drawer payload.
        assert hook_node._schema_prediction_blocked is not None
        assert "(id 2)" in hook_node._schema_prediction_blocked
        node_data = hook_node.get_node_data(flow_id=51, include_output=False, include_inputs=True)
        assert node_data.prediction_warning is not None

        # (c) a real run resolves everything and clears the warning.
        run_info = flow.run_graph()
        assert run_info is not None and run_info.success is not False
        assert len(executions) == 1, "the run executes the kernel exactly once"
        assert kernel_node.node_stats.has_completed_last_run

        schema = hook_node.get_predicted_schema(force=True)
        assert [c.column_name for c in schema] == EXPECTED_COLUMNS
        assert hook_node._schema_prediction_blocked is None
