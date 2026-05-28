"""Tool catalog + executor.

The catalog is generated from ``NODE_TYPE_TO_SETTINGS_CLASS`` (use
``get_settings_class_for_node_type()`` so user-defined nodes come
for free). The executor handles prospective schema validation, the
upstream-tier handler, and the 1-row kernel dry-run for code-bearing
nodes. Public re-exports below are the single import surface for
downstream callers (ghost nodes, Cmd+K palette, inline-diff
approval, multi-turn planner, GraphDiff staging).
"""

from flowfile_core.ai.tools.classification import (
    NodeClass,
    classify_node_type,
    is_predictable_via_mirror,
)
from flowfile_core.ai.tools.codegen_ops import CODEGEN_OPS_TOOLS
from flowfile_core.ai.tools.dry_run import DryRunCache
from flowfile_core.ai.tools.executor import (
    ExecutionMode,
    InsertionContext,
    ResultStatus,
    ToolExecutionResult,
    execute_tool_call,
)
from flowfile_core.ai.tools.graph_ops import GRAPH_OPS_TOOLS
from flowfile_core.ai.tools.meta_ops import META_OPS_TOOLS
from flowfile_core.ai.tools.registry import (
    JSON_SCHEMA_DIALECT,
    MCP_TOOL_NAMESPACE,
    SURFACE_PRESETS,
    SurfaceLiteral,
    build_tool_catalog,
    mcp_tool_name,
)
from flowfile_core.ai.tools.schema_ops import SCHEMA_OPS_TOOLS

__all__ = [
    # main entry points
    "build_tool_catalog",
    "execute_tool_call",
    # executor types
    "ToolExecutionResult",
    "InsertionContext",
    "ExecutionMode",
    "ResultStatus",
    "DryRunCache",
    # classification
    "NodeClass",
    "classify_node_type",
    "is_predictable_via_mirror",
    # tool surface lists
    "GRAPH_OPS_TOOLS",
    "SCHEMA_OPS_TOOLS",
    "CODEGEN_OPS_TOOLS",
    "META_OPS_TOOLS",
    # presets + naming
    "SURFACE_PRESETS",
    "MCP_TOOL_NAMESPACE",
    "JSON_SCHEMA_DIALECT",
    "mcp_tool_name",
    # type literals
    "SurfaceLiteral",
]
