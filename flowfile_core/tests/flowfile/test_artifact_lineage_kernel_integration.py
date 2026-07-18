"""
Kernel integration tests for artifact lineage enforcement.

Core ships each kernel node an ``available_artifacts`` allowlist (artifact name →
publishing node id) derived from the node's upstream lineage. When a node reads
an artifact that was published *outside* its lineage, the kernel emits a
deprecation warning ("not an upstream input"); reads from within the lineage
stay silent.

This exercises the end-to-end wire: a python_script node A publishes an
artifact, an UNRELATED node B (no edge from A) reads it and must be warned, and
a DOWNSTREAM node C (edge A→C) reads it and must NOT be warned. It also confirms
the rich published-artifact shape round-trips (Gap 3): A's recorded refs carry a
non-empty ``type_name``.

Requires Docker (kernel container). Skips cleanly when Docker is unavailable via
the ``kernel_manager_with_core`` fixture.
"""

import pytest

from flowfile_core import flow_file_handler
from flowfile_core.flowfile.flow_graph import FlowGraph, RunInformation, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.schemas import input_schema, schemas

pytestmark = pytest.mark.kernel


def _create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="artifact_lineage_test_flow",
            path=".",
            execution_mode="Development",
            execution_location="remote",
            # Node 2 (publisher) and node 3 (out-of-lineage reader) are stage
            # siblings; serialize execution so node 3 can't read before node 2
            # publishes.
            max_parallel_workers=1,
        )
    )
    return handler.get_flow(flow_id)


def _handle_run_info(run_info: RunInformation):
    if not run_info.success:
        errors = "errors:"
        for step in run_info.node_step_result:
            if not step.success:
                errors += f"\n  node_id:{step.node_id}, error: {step.error}"
        raise ValueError(f"Graph should run successfully:\n{errors}")


class TestArtifactLineageWarning:
    """A run where an out-of-lineage read is warned and an in-lineage read is not."""

    def test_out_of_lineage_read_warns_in_lineage_does_not(
        self, kernel_manager_with_core: tuple[KernelManager, str]
    ):
        manager, kernel_id = kernel_manager_with_core
        import flowfile_core.kernel as _kernel_mod

        _prev = _kernel_mod._manager
        _kernel_mod._manager = manager

        _prev_flow = flow_file_handler._flows.get(1)
        try:
            graph = _create_graph()
            # /raw_logs resolves flows via the module-singleton handler; the
            # kernel's log() callback lands there, so the graph must be
            # registered like a production flow.
            flow_file_handler._flows[1] = graph

            # node 1: shared manual-input root so A (node 2) and B (node 3) are
            # siblings — B is NOT downstream of A, so "model" is outside B's lineage.
            graph.add_node_promise(
                input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input")
            )
            graph.add_manual_input(
                input_schema.NodeManualInput(
                    flow_id=1,
                    node_id=1,
                    raw_data_format=input_schema.RawData.from_pylist([{"x": 1}]),
                )
            )

            # node 2 (A): publishes "model" (a dict → type_name "dict")
            graph.add_node_promise(
                input_schema.NodePromise(flow_id=1, node_id=2, node_type="python_script")
            )
            publish_code = """
df = flowfile_ctx.read_input()
flowfile_ctx.publish_artifact("model", {"weights": [1.0, 2.0, 3.0]})
flowfile_ctx.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1,
                    node_id=2,
                    depending_on_ids=[1],
                    python_script_input=input_schema.PythonScriptInput(code=publish_code, kernel_id=kernel_id),
                )
            )

            # node 3 (B): reads "model" from OUTSIDE its lineage (upstream = {1})
            graph.add_node_promise(
                input_schema.NodePromise(flow_id=1, node_id=3, node_type="python_script")
            )
            read_outside_code = """
df = flowfile_ctx.read_input()
model = flowfile_ctx.read_artifact("model")
print(f"B read model outside lineage: {model}")
flowfile_ctx.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1,
                    node_id=3,
                    depending_on_ids=[1],
                    python_script_input=input_schema.PythonScriptInput(
                        code=read_outside_code, kernel_id=kernel_id
                    ),
                )
            )

            # node 4 (C): reads "model" from WITHIN its lineage (edge A→C)
            graph.add_node_promise(
                input_schema.NodePromise(flow_id=1, node_id=4, node_type="python_script")
            )
            read_inside_code = """
df = flowfile_ctx.read_input()
model = flowfile_ctx.read_artifact("model")
print(f"C read model inside lineage: {model}")
flowfile_ctx.publish_output(df)
"""
            graph.add_python_script(
                input_schema.NodePythonScript(
                    flow_id=1,
                    node_id=4,
                    depending_on_ids=[2],
                    python_script_input=input_schema.PythonScriptInput(
                        code=read_inside_code, kernel_id=kernel_id
                    ),
                )
            )

            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 3))
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 4))

            run_info = graph.run_graph()
            _handle_run_info(run_info)

            # The flow log file captures both the node-logger stream and the
            # forwarded kernel stdout/stderr — either carries the warning.
            with open(graph.flow_logger.log_file_path) as fh:
                log_text = fh.read()

            warning_lines = [ln for ln in log_text.splitlines() if "not an upstream input" in ln]
            assert warning_lines, "Expected a lineage deprecation warning for the out-of-lineage read"
            assert any("model" in ln for ln in warning_lines)
            # Warning is attributed to node B (id 3), not node C (id 4).
            assert any(("node 3" in ln or "Node ID: 3" in ln) for ln in warning_lines)
            assert not any(("node 4" in ln or "Node ID: 4" in ln) for ln in warning_lines)

            # Gap 3 end-to-end: A's published refs carry the rich type metadata.
            published = graph.artifact_context.get_published_by_node(2)
            assert published, "Node A should have recorded a published artifact"
            assert published[0].name == "model"
            assert published[0].type_name, "Published ref should carry a non-empty type_name"

        finally:
            _kernel_mod._manager = _prev
            if _prev_flow is not None:
                flow_file_handler._flows[1] = _prev_flow
            else:
                flow_file_handler._flows.pop(1, None)
