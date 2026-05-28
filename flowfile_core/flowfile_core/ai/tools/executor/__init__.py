"""Tool executor with prospective schema validation.

``execute_tool_call`` is the single entry point for the LLM's typed tool
calls. The executor:

1. Parses the MCP-shaped tool name (``flowfile.<domain>.<op>``).
2. Resolves the target ``FlowGraph`` via ``flow_file_handler.get_flow``.
3. Dispatches per-domain (``graph`` / ``schema`` / ``codegen`` / ``meta``).
4. For ``graph.add_<node_type>``: validates settings via the Pydantic class,
   refuses on network egress, resolves upstream schemas via the tier
   handler, validates column refs, predicts output schema (mirror for
   static/source/passthrough; kernel dry-run for dynamic), then either
   applies via ``getattr(flow, f"add_{node_type}")(settings)`` or stages
   the payload for the diff layer to compose into a GraphDiff.
5. Emits one :class:`AuditEvent` per call with secrets redacted.

The executor does NOT do its own ``pl.scan_*`` calls — per the project
rule "the collect of polars data only takes place in the worker — use
nodes already", all data-touching paths go through the existing
``add_<node_type>`` infrastructure (which is worker-aware) or through
``kernel_runtime``.

Coordination with the diff layer: ``mode="stage"`` returns
``staged_node_payload`` with the validated settings + predicted schema;
the diff layer composes a list of those into a ``GraphDiff`` and wires
the accept path through ``HistoryManager.capture_if_changed`` for a
single undo point. The executor does NOT call
``audit.update_diff_action`` — that's the diff layer's contract.

The module was split into a package for navigability. The public API
is preserved verbatim — every symbol the old ``executor.py`` exposed
(including the underscored helpers tests reach for directly) is
re-exported here so
``from flowfile_core.ai.tools.executor import ...`` continues to work
without churn.
"""

from __future__ import annotations

# Public types + constants
from ._internal import (
    _AUTO_LAYOUT_FALLBACK_X,
    _AUTO_LAYOUT_FALLBACK_Y,
    _AUTO_LAYOUT_X_SPACING,
    _AUTO_LAYOUT_Y_SPACING,
    _CODE_BEARING,
    _POLARS_CODE_IMPORT_REFUSAL,
    _TOOL_NAME_RE,
    ExecutionMode,
    InsertionContext,
    ResultStatus,
    ToolExecutionResult,
    _extract_code,
    _record_event,
    _reject_and_audit,
    _resolve_flow,
    _resolve_insertion_position,
    _resolve_output_names,
)
from .coercions import (
    _coerce_connection_id_to_flat,
    _coerce_to_int_list_or_self,
    _coerce_to_int_or_none,
    _coerce_to_int_or_self,
    _unwrap_json_string_values,
)
from .dispatch import _handle_graph, execute_tool_call
from .handlers.add import _apply_add_node, _handle_add_node, _validate_polars_code_body_or_reject
from .handlers.codegen import _handle_codegen
from .handlers.connections import (
    _build_node_connection_from_flat,
    _format_connection_validation_error,
    _handle_connect,
    _handle_delete_connection,
    _handle_delete_node,
)
from .handlers.meta import _handle_meta
from .handlers.schema import _handle_schema
from .handlers.update import _apply_update_node_settings, _handle_update_node_settings
from .manual_input import (
    _normalize_manual_input_args,
    _normalize_manual_input_columns,
    _normalize_manual_input_data,
)
from .refusals import (
    _detect_sink_upstreams,
    _example_from_payload,
    _expects_object,
    _format_settings_validation_refusal,
    _format_sink_upstream_refusal,
    _navigate_schema,
    _summarize_expected_shape,
    _synthesize_example_from_schema,
)

__all__ = [
    # Public API — same surface the original ``executor.py`` exported.
    "ExecutionMode",
    "InsertionContext",
    "ResultStatus",
    "ToolExecutionResult",
    "execute_tool_call",
    # Internals re-exported because tests / the diff module / the planner
    # reach for them directly via the package facade.
    "_AUTO_LAYOUT_FALLBACK_X",
    "_AUTO_LAYOUT_FALLBACK_Y",
    "_AUTO_LAYOUT_X_SPACING",
    "_AUTO_LAYOUT_Y_SPACING",
    "_CODE_BEARING",
    "_POLARS_CODE_IMPORT_REFUSAL",
    "_TOOL_NAME_RE",
    "_apply_add_node",
    "_apply_update_node_settings",
    "_build_node_connection_from_flat",
    "_coerce_connection_id_to_flat",
    "_coerce_to_int_list_or_self",
    "_coerce_to_int_or_none",
    "_coerce_to_int_or_self",
    "_detect_sink_upstreams",
    "_example_from_payload",
    "_expects_object",
    "_extract_code",
    "_format_connection_validation_error",
    "_format_settings_validation_refusal",
    "_format_sink_upstream_refusal",
    "_handle_add_node",
    "_handle_codegen",
    "_handle_connect",
    "_handle_delete_connection",
    "_handle_delete_node",
    "_handle_graph",
    "_handle_meta",
    "_handle_schema",
    "_handle_update_node_settings",
    "_navigate_schema",
    "_normalize_manual_input_args",
    "_normalize_manual_input_columns",
    "_normalize_manual_input_data",
    "_record_event",
    "_reject_and_audit",
    "_resolve_flow",
    "_resolve_insertion_position",
    "_resolve_output_names",
    "_summarize_expected_shape",
    "_synthesize_example_from_schema",
    "_unwrap_json_string_values",
    "_validate_polars_code_body_or_reject",
]
