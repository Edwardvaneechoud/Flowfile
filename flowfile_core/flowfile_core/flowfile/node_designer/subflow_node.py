"""SubflowNode — wraps a saved flow (.yaml/.json) as a reusable custom node.

When a flow defines ``flow_arguments``, it can be registered as a subflow
node so that other flows can use it as a single node whose settings panel
exposes those arguments.

A subflow can accept table inputs and produce table outputs, making it
behave like a regular process node in the parent flow.  The number of
inputs/outputs is auto-detected from the inner flow structure or can be
explicitly configured via ``num_table_inputs`` / ``num_table_outputs`` in
the flow settings.
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

# Node types in the inner flow that represent data sources (they read their
# own data and should NOT be counted as external table-input slots).
_DATA_SOURCE_NODE_TYPES: set[str] = {
    "read",
    "read_csv",
    "read_parquet",
    "read_excel",
    "read_json",
    "read_clipboard",
    "database_reader",
    "cloud_storage_reader",
    "external_source",
    "sql_source",
}


def _get_subflow_stack() -> set[str]:
    if not hasattr(_subflow_stack, "stack"):
        _subflow_stack.stack = set()
    return _subflow_stack.stack


def _count_table_io(flow_info) -> tuple[int, int]:
    """Analyse the inner flow to determine number of table inputs / outputs.

    Detection uses **explicit flags first**, then falls back to heuristics:

    **Input slots** – nodes with ``is_flow_input == True``.  If no node has
    the flag set, falls back to counting start nodes whose type is *not* a
    file/database reader (e.g. ``manual_input``).

    **Output slots** – nodes with ``is_flow_output == True``.  If no node
    has the flag set, falls back to counting terminal nodes (no downstream
    connections).

    Returns:
        (num_inputs, num_outputs)
    """
    explicit_inputs: list = []
    explicit_outputs: list = []
    heuristic_inputs: list = []
    heuristic_outputs: list = []

    for node_info in flow_info.data.values():
        # Check explicit flags (on FlowfileNode or setting_input)
        node_is_input = getattr(node_info, "is_flow_input", False) or False
        node_is_output = getattr(node_info, "is_flow_output", False) or False

        # Also check setting_input for flags (when loaded from FlowInformation)
        si = getattr(node_info, "setting_input", None)
        if si is not None:
            node_is_input = node_is_input or (getattr(si, "is_flow_input", False) or False)
            node_is_output = node_is_output or (getattr(si, "is_flow_output", False) or False)

        if node_is_input:
            explicit_inputs.append(node_info)
        if node_is_output:
            explicit_outputs.append(node_info)

        # Heuristic detection
        is_start = (
            not node_info.left_input_id
            and not node_info.right_input_id
            and not (node_info.input_ids or [])
        )
        is_terminal = not (node_info.outputs or [])

        if is_start and node_info.type not in _DATA_SOURCE_NODE_TYPES:
            heuristic_inputs.append(node_info)
        if is_terminal:
            heuristic_outputs.append(node_info)

    # Prefer explicit flags; fall back to heuristic
    num_inputs = len(explicit_inputs) if explicit_inputs else len(heuristic_inputs)
    num_outputs = len(explicit_outputs) if explicit_outputs else len(heuristic_outputs)

    return num_inputs, max(num_outputs, 1)


def _find_input_nodes(flow) -> list:
    """Return inner-flow nodes that should receive external DataFrames.

    Prefers nodes whose ``setting_input.is_flow_input`` is True.
    Falls back to non-reader start nodes (heuristic).
    """
    explicit = [
        n for n in flow.nodes
        if getattr(getattr(n, "setting_input", None), "is_flow_input", False)
    ]
    if explicit:
        return explicit
    return [
        n for n in flow.nodes
        if not n.has_input and n.node_type not in _DATA_SOURCE_NODE_TYPES
    ]


def _find_output_nodes(flow) -> list:
    """Return inner-flow nodes whose results should be emitted.

    Prefers nodes whose ``setting_input.is_flow_output`` is True.
    Falls back to terminal nodes (no downstream connections).
    """
    explicit = [
        n for n in flow.nodes
        if getattr(getattr(n, "setting_input", None), "is_flow_output", False)
    ]
    if explicit:
        return explicit
    return [n for n in flow.nodes if not n.leads_to_nodes]


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

    **Table I/O** – The node can accept DataFrames from upstream nodes in
    the parent flow and produce output DataFrames for downstream nodes.
    The number of inputs/outputs is auto-detected from the inner flow
    structure or can be explicitly set via ``num_table_inputs`` /
    ``num_table_outputs`` in the flow settings.
    """

    flow_path: str = ""
    flow_arguments: list[FlowArgument] = Field(default_factory=list)
    _flow_mtime: float | None = None  # Track file modification time for hot reload

    @classmethod
    def from_flow_path(cls, flow_path: str) -> "SubflowNode":
        """Create a SubflowNode by loading a flow file and extracting its arguments.

        The number of table inputs and outputs is determined by:
        1. Explicit ``num_table_inputs`` / ``num_table_outputs`` in flow settings (if set).
        2. Auto-detection: start nodes that are *not* file/DB readers become
           input slots; terminal nodes (no downstream) become output slots.

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

        # Determine table I/O counts — explicit config takes priority
        explicit_in = getattr(flow_info.flow_settings, "num_table_inputs", None)
        explicit_out = getattr(flow_info.flow_settings, "num_table_outputs", None)

        auto_in, auto_out = _count_table_io(flow_info)

        num_inputs = explicit_in if explicit_in is not None else auto_in
        num_outputs = explicit_out if explicit_out is not None else auto_out

        logger.info(
            "SubflowNode '%s': inputs=%d (explicit=%s, auto=%d), "
            "outputs=%d (explicit=%s, auto=%d)",
            flow_name, num_inputs, explicit_in, auto_in,
            num_outputs, explicit_out, auto_out,
        )

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

        When the subflow has table inputs (``number_of_inputs > 0``), the
        incoming DataFrames are injected into the inner flow's non-reader
        start nodes — in the same order they appear in the inner flow.

        Args:
            *inputs: DataFrames passed from upstream nodes in the parent flow.

        Returns:
            The output DataFrame from the inner flow's first terminal node.
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

            # Inject input DataFrames into the inner flow's input nodes.
            # Prefer nodes explicitly marked with is_flow_input; fall back
            # to heuristic (non-reader start nodes).
            if inputs:
                injectable_nodes = _find_input_nodes(inner_flow)
                for df, target_node in zip(inputs, injectable_nodes, strict=False):
                    fde = FlowDataEngine(df)

                    def _make_input_func(data):
                        def _func():
                            return data
                        return _func

                    target_node.function = _make_input_func(fde)

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

            # Extract output from output nodes.
            # Prefer nodes explicitly marked with is_flow_output; fall
            # back to terminal nodes (no downstream connections).
            output_nodes = _find_output_nodes(inner_flow)
            if output_nodes:
                first_output = output_nodes[0]
                if first_output.node_data is not None:
                    return first_output.node_data.data_frame
            return None

        finally:
            stack.discard(resolved_path)

    def to_node_template(self):
        """Override to set subflow-specific template properties."""
        template = super().to_node_template()
        template.node_group = "subflow"
        return template
