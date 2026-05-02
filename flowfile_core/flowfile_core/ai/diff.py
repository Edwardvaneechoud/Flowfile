"""``GraphDiff`` — staged additions / modifications / deletions.

Owned by W41. Will provide:

* the Pydantic ``GraphDiff`` model used by the Level 3 agent to accumulate
  proposed graph mutations *before* user approval;
* atomic apply via ``HistoryManager.capture_history_snapshot`` so the diff
  becomes a single undo point (see ``flow_graph.py:891`` /
  ``capture_history_snapshot`` at ``flow_graph.py:927``);
* a revert path that drops the staged diff without touching the live graph.

Until W41 lands, ``diff`` is a namespace stub.
"""
