"""Schema introspection tools — owned by W30.

Read-only surface for ``read_node_schema`` / ``read_node_preview`` so the
LLM can ground references in the actual schema before proposing edits.
Reuses ``NodeData.main_output.table_schema`` from
``GET /node?get_data=false`` (``routes.py:978``).

Stub until W30.
"""
