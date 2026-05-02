"""Disk-persisted ``AgentSession`` records.

Owned by W42. Will provide:

* ``AgentSession`` Pydantic model (messages, tool calls, pending GraphDiff,
  snapshot of the graph at agent-start per D006);
* sidecar storage at ``{user_data_directory}/ai_sessions/{flow_id}/``
  (mirrors the ``flows_directory`` pattern at ``shared/storage_config.py:91``);
* resume + abort hooks invoked from the ``/ai/agent/{session_id}/*`` routes.

Per D007, sessions are sidecar-by-default and never embedded in ``.flowfile``
unless the user opts in via the export toggle (W43).
"""
