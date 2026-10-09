"""Control and inspection nodes: gate, wait-for and Explore Data."""

from typing import TYPE_CHECKING

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


class ControlBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_explore_data(self, node_analysis: input_schema.NodeExploreData):
        """Adds a specialized node for data exploration and visualization.

        Args:
            node_analysis: The settings for the data exploration node.
        """

        def analysis_preparation(flowfile_table: FlowDataEngine) -> FlowDataEngine:
            """Pass-through: Graphic Walker aggregates on the worker, not in the browser.

            Charts read the node's result plan through ``/analysis_data/compute``,
            so the run itself owes the explorer nothing.
            """
            return flowfile_table

        def schema_callback():
            node = self.get_node(node_analysis.node_id)
            if len(node.all_inputs) == 1:
                input_node = node.all_inputs[0]
                return input_node.schema
            else:
                return [FlowfileColumn.from_input("col_1", "na")]

        self.add_node_step(
            node_id=node_analysis.node_id,
            node_type="explore_data",
            function=analysis_preparation,
            setting_input=node_analysis,
            schema_callback=schema_callback,
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_gate(self, gate_settings: input_schema.NodeGate) -> "FlowGraph":
        """Adds a Gate node — a passthrough whose downstream only runs when its condition holds.

        Parameter-mode conditions are evaluated before the run starts (all
        parameters are known at plan time); the node function only re-validates
        them so a broken condition fails the gate visibly. Formula conditions
        are decided when the gate executes: the flowfile formula is applied as
        a row predicate to the optional control input (else the data input) —
        open iff at least one row matches (a bounded one-row collect) — and
        the stage loop classifies the gate's downstream as deliberately
        skipped. The data input always passes through unchanged — single-node
        fetch deliberately ignores gates.
        """

        def _func(main: FlowDataEngine, control: FlowDataEngine | None = None) -> FlowDataEngine:
            del control  # only read at routing time, from the control node's result
            node = self.get_node(gate_settings.node_id)
            gate_input = node.setting_input.gate_input
            if gate_input.condition_source == "formula":
                if not gate_input.formula.strip():
                    raise ValueError("Gate is set to route on a formula, but no formula is configured")
            else:
                parameters_by_name = {p.name: p for p in self.flow_settings.parameters}
                gate_input.evaluate(parameters_by_name)
            return main

        def schema_callback():
            node = self.get_node(gate_settings.node_id)
            if node.node_inputs.main_inputs:
                return node.node_inputs.main_inputs[0].schema
            return []

        self.add_node_step(
            node_id=gate_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="gate",
            setting_input=gate_settings,
            schema_callback=schema_callback,
            input_node_ids=[gate_settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_wait_for(self, settings: input_schema.NodeWaitFor) -> "FlowGraph":
        """Adds a Wait For node — passes the left input through and waits on the right.

        Two distinct input handles like Join: connect the data path to the left
        and the dependency node (e.g. Train Model) to the right. The right
        input's data is discarded; only its completion gates this node.
        """

        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            # *right* is intentionally unused — its only job is to make sure
            # the framework waits for the dependency node to finish.
            del right
            return main

        def schema_callback():
            node = self.get_node(settings.node_id)
            if node.node_inputs.main_inputs:
                return node.node_inputs.main_inputs[0].schema
            return []

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            input_columns=[],
            node_type="wait_for",
            setting_input=settings,
            schema_callback=schema_callback,
            input_node_ids=settings.depending_on_ids,
        )
        return self
