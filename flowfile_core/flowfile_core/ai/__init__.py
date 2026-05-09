"""AI integration for Flowfile.

Public interface:

* ``router`` — FastAPI router for ``/ai/*`` endpoints, mounted in ``main.py``.

Package layout:

* ``providers/``  — provider abstraction over litellm + BYOK key load.
* ``tools/``      — tool catalog generation + executor.
* ``context/``    — subgraph + schema + sample serialisation.
* ``agents/``     — chat / autocomplete / planner surface implementations.
* ``prompts/``    — layered system prompts.
* ``streaming``   — SSE keepalive + resumption.
* ``scheduler``   — rate-limit windows + backoff.
* ``sessions``    — disk-persisted ``AgentSession``.
* ``diff``        — ``GraphDiff`` model + apply/revert.
* ``safety``      — PII scrubber + audit hooks.
* ``metrics``     — counters + cost-per-flow tracking.
"""

from flowfile_core.ai.routes import router

__all__ = ["router"]
