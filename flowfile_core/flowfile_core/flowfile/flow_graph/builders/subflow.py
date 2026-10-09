"""Subflow interface nodes: flow input, flow output and run-flow."""

from typing import TYPE_CHECKING

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.flowfile.flow_node.input_handles import input_handle
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


class SubflowBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_flow_output(self, settings: input_schema.NodeFlowOutput) -> "FlowGraph":
        """Adds a named subflow-output sink (passthrough, always materialized).

        When this flow runs inside another flow via a run_flow node, the parent
        reads this node's result as one of the subflow's outputs. The name must be
        unique among the graph's flow_output nodes unless ``unique_subflow_port_names``
        is off.
        """
        if self.unique_subflow_port_names:
            for other in self.nodes:
                if (
                    other.node_type == "flow_output"
                    and other.node_id != settings.node_id
                    and isinstance(other.setting_input, input_schema.NodeFlowOutput)
                    and other.setting_input.output_name == settings.output_name
                ):
                    raise ValueError(
                        f"flow_output name '{settings.output_name}' is already used by node {other.node_id}"
                    )

        def _func(df: FlowDataEngine):
            return df

        def schema_callback():
            node: FlowNode = self.get_node(settings.node_id)
            if node.node_inputs.main_inputs:
                return node.node_inputs.main_inputs[0].schema
            return []

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            input_columns=[],
            node_type="flow_output",
            setting_input=settings,
            schema_callback=schema_callback,
            input_node_ids=[settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_run_flow(self, settings: input_schema.NodeRunFlow) -> "FlowGraph":
        """Adds a node that executes a catalog-registered flow as a subflow.

        Inputs are keyed: handle input-0 carries optional parameter data; handles
        input-1..input-N feed the subflow's flow_input nodes (input_slots order).
        Outputs mirror the subflow's flow_output nodes (output_slots order); a
        subflow without outputs yields one run-summary row per run.
        """
        from flowfile_core.flowfile import subflow

        subflow.stamp_flow_reference(settings)
        _graph = self

        def _func(*inputs: FlowDataEngine):
            param_input = inputs[0] if inputs else None
            return subflow.execute_run_flow_node(_graph, settings, param_input, tuple(inputs[1:]))

        def schema_callback():
            if not settings.output_slots:
                return subflow.predict_run_summary_schema(settings)
            node = _graph.get_node(settings.node_id)
            named = subflow.predict_run_flow_named_schemas(settings)
            if node is not None and named:
                node._named_schemas = named
            return named.get(DEFAULT_OUTPUT_HANDLE, [])

        existing = self.get_node(settings.node_id)
        old_slots: list[str] | None = None
        if existing is not None and isinstance(existing.setting_input, input_schema.NodeRunFlow):
            old_slots = list(existing.setting_input.input_slots)

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            input_columns=[],
            node_type="run_flow",
            setting_input=settings,
            schema_callback=schema_callback,
            input_node_ids=[],
        )

        if old_slots is not None and old_slots != settings.input_slots:
            node = self.get_node(settings.node_id)
            # Keyed edges follow their slot by NAME; vanished names drop their edge.
            mapping: dict[str, str | None] = {}
            for old_index, slot_name in enumerate(old_slots):
                old_handle = input_handle(old_index + 1)
                if slot_name in settings.input_slots:
                    mapping[old_handle] = input_handle(settings.input_slots.index(slot_name) + 1)
                else:
                    mapping[old_handle] = None
            result = node.remap_dynamic_inputs(mapping)
            if result["dropped"]:
                self.flow_logger.warning(
                    f"run_flow node {settings.node_id}: dropped connection(s) on {', '.join(result['dropped'])} "
                    "after the subflow interface changed"
                )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_flow_input(self, settings: input_schema.NodeFlowInput) -> "FlowGraph":
        """Adds a named subflow-input placeholder source.

        Standalone runs serve the optional sample data (empty frame otherwise);
        a parent run_flow node overwrites ``node.function`` with real data. The name
        must be unique among the graph's flow_input nodes unless
        ``unique_subflow_port_names`` is off.
        """
        if self.unique_subflow_port_names:
            for other in self.nodes:
                if (
                    other.node_type == "flow_input"
                    and other.node_id != settings.node_id
                    and isinstance(other.setting_input, input_schema.NodeFlowInput)
                    and other.setting_input.input_name == settings.input_name
                ):
                    raise ValueError(f"flow_input name '{settings.input_name}' is already used by node {other.node_id}")
        if settings.raw_data_format is not None and settings.raw_data_format.columns:
            input_data = FlowDataEngine(settings.raw_data_format)
        else:
            input_data = FlowDataEngine()
        node = self.get_node(settings.node_id)
        is_new = node is None
        if node:
            node.node_type = "flow_input"
            node.name = "flow_input"
            node.function = input_data
            node.setting_input = settings
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                settings.node_id,
                function=input_data,
                setting_input=settings,
                name="flow_input",
                node_type="flow_input",
                parent_uuid=self.uuid,
            )
            self._node_db[settings.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(settings.node_id)
        self._notify_node_observers(node, is_new=is_new)
        return self
