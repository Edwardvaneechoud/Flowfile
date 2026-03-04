"""SubflowNode — wraps a saved flow (.yaml/.json) as a reusable custom node.

When a flow defines ``flow_arguments``, it can be registered as a subflow
node so that other flows can use it as a single node whose settings panel
exposes those arguments.
"""

import logging
import threading
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import Field

from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase, NodeSettings, create_section
from flowfile_core.flowfile.node_designer.ui_components import (
    MultiSelect,
    NumericInput,
    SingleSelect,
    TextInput,
    ToggleSwitch,
)
from flowfile_core.schemas.flow_args import FlowArgument

logger = logging.getLogger(__name__)

# Thread-local stack for circular-reference detection.
_subflow_stack = threading.local()

MAX_SUBFLOW_DEPTH = 5


def _get_subflow_stack() -> set[str]:
    if not hasattr(_subflow_stack, "stack"):
        _subflow_stack.stack = set()
    return _subflow_stack.stack


def _build_ui_component(arg: FlowArgument):
    """Convert a FlowArgument into the appropriate UI component."""
    label = arg.description or arg.name.replace("_", " ").title()

    if arg.arg_type == "string":
        if arg.options:
            return SingleSelect(
                label=label,
                options=arg.options,
                default=arg.default,
                value=arg.default,
            )
        return TextInput(
            label=label,
            default=arg.default or "",
            placeholder=f"Enter {arg.name}",
            value=arg.default,
        )
    elif arg.arg_type == "number":
        return NumericInput(
            label=label,
            default=arg.default,
            value=arg.default,
        )
    elif arg.arg_type == "boolean":
        return ToggleSwitch(
            label=label,
            default=bool(arg.default) if arg.default is not None else False,
            value=bool(arg.default) if arg.default is not None else False,
        )
    elif arg.arg_type == "list":
        if arg.options:
            return MultiSelect(
                label=label,
                options=arg.options,
                default=arg.default or [],
                value=arg.default or [],
            )
        return TextInput(
            label=label,
            default=",".join(arg.default) if isinstance(arg.default, list) else (arg.default or ""),
            placeholder="Comma-separated values",
            value=",".join(arg.default) if isinstance(arg.default, list) else (arg.default or ""),
        )
    else:
        return TextInput(label=label, default=str(arg.default or ""), value=str(arg.default or ""))


def _build_settings_schema(args: list[FlowArgument]) -> NodeSettings | None:
    """Build a NodeSettings instance from flow arguments.

    Each argument becomes a UI component inside an "arguments" section.
    """
    if not args:
        return None

    components = {}
    for arg in args:
        components[arg.name] = _build_ui_component(arg)

    section = create_section(**components)
    section.title = "Flow Arguments"
    section.description = "Configure the arguments for this subflow."
    return NodeSettings(arguments=section)


class SubflowNode(CustomNodeBase):
    """A custom node that wraps a saved flow (.yaml/.json) as a reusable component.

    The flow's ``flow_arguments`` are exposed as the node's settings panel.
    When executed, it opens the inner flow, injects argument values and input
    DataFrames, runs it, and returns the output.
    """

    flow_path: str = ""
    flow_arguments: list[FlowArgument] = Field(default_factory=list)
    _flow_mtime: float | None = None  # Track file modification time for hot reload

    @classmethod
    def from_flow_path(cls, flow_path: str) -> "SubflowNode":
        """Create a SubflowNode by loading a flow file and extracting its arguments.

        Args:
            flow_path: Path to the .yaml/.json flow file.

        Returns:
            A configured SubflowNode instance.
        """
        from flowfile_core.flowfile.manage.io_flowfile import _load_flow_storage

        path = Path(flow_path).resolve()
        flow_info = _load_flow_storage(path)

        flow_args = flow_info.flow_settings.flow_arguments
        flow_name = flow_info.flow_name or path.stem

        # Count inputs: nodes that have no dependencies (start nodes)
        start_nodes = []
        output_nodes = []
        for node_info in flow_info.data.values():
            # Start nodes: no left/right/main inputs
            if not node_info.left_input_id and not node_info.right_input_id and not (node_info.input_ids or []):
                start_nodes.append(node_info)
            # Output nodes: nodes with no outputs
            if not (node_info.outputs or []):
                output_nodes.append(node_info)

        # For subflow-as-node, the number_of_inputs is how many external
        # DataFrames the subflow expects (typically 0 for file-based flows,
        # or N for data-passing flows).
        # For now, default to 0 (file-based) unless explicitly configured.
        num_inputs = 0
        num_outputs = 1

        settings_schema = _build_settings_schema(flow_args)

        node = cls(
            node_name=f"Subflow: {flow_name}",
            node_category="Subflow",
            node_icon="subflow-icon.png",
            settings_schema=settings_schema,
            number_of_inputs=num_inputs,
            number_of_outputs=num_outputs,
            node_group="subflow",
            title=f"Subflow: {flow_name}",
            intro=f"Runs the flow '{flow_name}' with configurable arguments.",
            node_type="process" if num_inputs > 0 else "input",
            transform_type="wide",
            flow_path=str(path),
            flow_arguments=flow_args,
        )
        node._flow_mtime = path.stat().st_mtime if path.exists() else None
        return node

    def _extract_argument_values(self) -> dict[str, Any]:
        """Read the current settings values and map them back to argument names."""
        values: dict[str, Any] = {}
        if not self.settings_schema or not self.flow_arguments:
            return values

        for arg in self.flow_arguments:
            val = self.settings_schema.get_value(arg.name)
            if val is not None:
                values[arg.name] = val
        return values

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame | None:
        """Execute the inner flow with resolved arguments.

        Args:
            *inputs: DataFrames passed from upstream nodes in the parent flow.

        Returns:
            The output DataFrame from the inner flow's terminal node(s).
        """
        from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
        from flowfile_core.flowfile.manage.io_flowfile import open_flow

        # Circular reference detection
        stack = _get_subflow_stack()
        resolved_path = str(Path(self.flow_path).resolve())
        if resolved_path in stack:
            raise RuntimeError(
                f"Circular subflow reference detected: '{self.flow_path}' is "
                f"already in the execution stack: {stack}"
            )
        if len(stack) >= MAX_SUBFLOW_DEPTH:
            raise RuntimeError(
                f"Maximum subflow nesting depth ({MAX_SUBFLOW_DEPTH}) exceeded."
            )

        stack.add(resolved_path)
        try:
            # Open the inner flow
            inner_flow = open_flow(Path(self.flow_path))

            # Resolve arguments from our settings
            arg_values = self._extract_argument_values()
            if arg_values:
                inner_flow.resolve_arguments(arg_values)

            # Inject input DataFrames into the inner flow's start nodes
            if inputs:
                start_nodes = [n for n in inner_flow.nodes if not n.has_input]
                for df, start_node in zip(inputs, start_nodes, strict=False):
                    fde = FlowDataEngine(df)

                    def _make_input_func(data):
                        def _func():
                            return data
                        return _func

                    start_node.function = _make_input_func(fde)

            # Run the inner flow
            result = inner_flow.run_graph()
            if result is None or not result.success:
                error_msg = "Subflow execution failed"
                if result and result.node_step_result:
                    for nr in result.node_step_result:
                        if not nr.success and nr.error:
                            error_msg = f"Subflow '{self.node_name}' > Node {nr.node_id}: {nr.error}"
                            break
                raise RuntimeError(error_msg)

            # Extract output from the last node
            terminal_nodes = [n for n in inner_flow.nodes if not n.leads_to_nodes]
            if terminal_nodes:
                last_node = terminal_nodes[-1]
                if last_node.node_data is not None:
                    return last_node.node_data.data_frame
            return None

        finally:
            stack.discard(resolved_path)

    def to_node_template(self):
        """Override to set subflow-specific template properties."""
        template = super().to_node_template()
        template.node_group = "subflow"
        return template
