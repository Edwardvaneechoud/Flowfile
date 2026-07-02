"""Node-type classification.

The executor routes tool execution along two axes that depend on
the node type:

* **target node bucket** ``static`` / ``dynamic`` / ``source`` /
  ``passthrough`` — decides whether to predict via the ephemeral
  mirror-graph (``static`` / ``source`` / ``passthrough``) or kernel
  dry-run (``dynamic``).

* **upstream tier** — when the upstream node's ``predicted_schema``
  is ``None``, we delegate to the existing ``schema_callback``
  registered by the production ``add_<node_type>`` method. The
  callbacks are worker-aware: they use
  :class:`FlowDataEngine.create_from_path` (which routes through the
  worker for non-trivial cases) or pure-Python derivers (e.g. GA's
  ``derive_schema``). The executor does NOT do its own ``pl.scan_*``
  calls — that would bypass the worker.

The 22+1 explicit ``static`` set covers everything in
``NODE_TYPE_TO_SETTINGS_CLASS`` whose schema is derivable from
``(settings, upstream_schema)`` without code execution. The 9-entry ``dynamic``
set names the nodes whose schema callback either reads the upstream lazy frame
(``pivot``, and ``dynamic_rename`` whose ``first_row`` mode promotes data
values to column names) or executes user code (``polars_code`` /
``python_script`` / ``sql_query``) or expands rows in non-deterministic ways
(``unpivot`` / ``text_to_rows`` / ``graph_solver``). ``user_defined`` is
dynamic because the underlying class is runtime-registered.
"""

from __future__ import annotations

from typing import Literal

NodeClass = Literal["static", "dynamic", "source", "passthrough"]

# Derived from the node registry (each NodeSpec carries its ai_classification);
# built lazily to keep this module import-light.
_node_class_map: dict[str, NodeClass] | None = None


def _get_node_class_map() -> dict[str, NodeClass]:
    global _node_class_map
    if _node_class_map is None:
        from flowfile_core.flowfile.node_registry import BUILTIN_REGISTRY

        _node_class_map = BUILTIN_REGISTRY.ai_classification_map()
    return _node_class_map


def __getattr__(name: str):
    if name == "_NODE_CLASS_MAP":
        return _get_node_class_map()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def classify_node_type(node_type: str) -> NodeClass:
    """Return the node-class bucket for ``node_type``.

    Unknown node types default to ``"dynamic"`` — safe because dynamic routes
    through the kernel dry-run path which fails closed (refusal on missing
    upstream sample) rather than producing a wrong schema.
    """
    return _get_node_class_map().get(node_type, "dynamic")


def is_predictable_via_mirror(node_type: str) -> bool:
    """Return ``True`` iff this node type can be predicted by the mirror-graph
    path (i.e. it's static / source / passthrough — anything but ``dynamic``).

    Dynamic nodes need the kernel dry-run instead.
    """
    return classify_node_type(node_type) in ("static", "source", "passthrough")


__all__ = [
    "NodeClass",
    "classify_node_type",
    "is_predictable_via_mirror",
]
