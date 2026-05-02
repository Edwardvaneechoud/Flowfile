"""Level 3 — Planner (multi-turn, plan-then-execute). Owned by W40.

Per plan §2 / §6.4, the planner:

* opens a session via ``POST /ai/agent/start``;
* accumulates tool calls into a ``GraphDiff`` (W41) without mutating the
  live graph;
* survives disconnects via ``sessions.AgentSession`` and the
  ``POST /ai/agent/{session_id}/resume`` route (W42);
* applies the diff atomically when the user clicks Accept
  (``POST /ai/diff/{session_id}/accept``, W41 + W35).

Per D006, snapshots the graph at agent-start; if the user mutates the
canvas mid-run the agent warns-and-pauses on conflict.

System prompt: ``prompts/base.md`` + ``prompts/planner.md`` (D008).

Stub until W40.
"""
