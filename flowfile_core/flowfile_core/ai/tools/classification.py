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
dynamic because the underlying class is runtime-registered, and ``run_flow``
is dynamic because its output schema only exists after the referenced subflow
is executed.
"""

from __future__ import annotations

from typing import Final, Literal

NodeClass = Literal["static", "dynamic", "source", "passthrough"]

_NODE_CLASS_MAP: Final[dict[str, NodeClass]] = {
    "manual_input": "source",
    "filter": "static",
    "formula": "static",
    "select": "static",
    "dynamic_rename": "dynamic",
    "sort": "static",
    "record_id": "static",
    "sample": "static",
    "random_split": "static",
    "unique": "static",
    "group_by": "static",
    "window_functions": "static",
    "pivot": "dynamic",
    "unpivot": "dynamic",
    "text_to_rows": "dynamic",
    "graph_solver": "dynamic",
    "python_script": "dynamic",
    "polars_code": "dynamic",
    # sql_query has a schema_callback (add_sql_query) that resolves the output
    # schema by running the query plan lazily over 0-row inputs, so the mirror
    # can predict it (fails safe to None for >2 inputs it cannot wire).
    "sql_query": "static",
    "join": "static",
    "cross_join": "static",
    "fuzzy_match": "static",
    "record_count": "static",
    "explore_data": "static",
    "union": "static",
    "output": "static",
    "api_response": "static",
    "read": "source",
    "database_reader": "source",
    "database_writer": "static",
    "cloud_storage_reader": "source",
    "cloud_storage_writer": "static",
    "catalog_reader": "source",
    "catalog_writer": "static",
    "kafka_source": "source",
    "google_analytics_reader": "source",
    "rest_api_reader": "source",
    "external_source": "source",
    "promise": "passthrough",
    "user_defined": "dynamic",
    "train_model": "static",
    "apply_model": "static",
    "evaluate_model": "static",
    "wait_for": "static",
    # gate is an identity passthrough of its data input — the condition only
    # decides whether downstream nodes run, never the output schema.
    "gate": "static",
    # Flow-in-flow authoring primitives. flow_input has no input port (source);
    # flow_output is a passthrough sink; run_flow's output schema only exists
    # after the referenced subflow is run, so it's dynamic.
    "flow_input": "source",
    "flow_output": "static",
    "run_flow": "dynamic",
}


def classify_node_type(node_type: str) -> NodeClass:
    """Return the node-class bucket for ``node_type``.

    Unknown node types default to ``"dynamic"`` — safe because dynamic routes
    through the kernel dry-run path which fails closed (refusal on missing
    upstream sample) rather than producing a wrong schema.
    """
    return _NODE_CLASS_MAP.get(node_type, "dynamic")


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
