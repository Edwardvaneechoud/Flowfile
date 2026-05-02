"""AI integration for Flowfile.

Public interface:

* ``router`` — FastAPI router for ``/ai/*`` endpoints, mounted in ``main.py``.

The package layout is laid down by W10 as a skeleton; live logic lands per
workstream:

* ``providers/``  — provider abstraction over litellm (W11), BYOK key load (W12).
* ``tools/``      — tool catalog generation (W30) + executor (W31).
* ``context/``    — subgraph + schema + sample serialisation (W22, W24).
* ``agents/``     — Level 1 / 2 / 3 surface implementations.
* ``prompts/``    — layered system prompts per D008 (W22, W40).
* ``streaming``   — SSE keepalive + resumption (W13).
* ``scheduler``   — rate-limit windows + backoff (W14).
* ``sessions``    — disk-persisted ``AgentSession`` (W42).
* ``diff``        — ``GraphDiff`` model + apply/revert (W41).
* ``safety``      — PII scrubber + audit hooks (W25, W15).
* ``metrics``     — counters + cost-per-flow tracking (W11, W15).
"""

from flowfile_core.ai.routes import router

__all__ = ["router"]
