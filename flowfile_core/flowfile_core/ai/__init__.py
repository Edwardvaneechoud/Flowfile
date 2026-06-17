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

import os

# litellm reads model_prices_and_context_window_backup.json during `import litellm`.
# Force the bundled local copy and skip the raw.githubusercontent.com fetch of the
# model cost map: Flowfile only calls litellm.acompletion + reads litellm.exceptions,
# never its cost/model-info APIs, so the frozen map is functionally irrelevant. This
# avoids an outbound call (and offline failures) on first AI use. setdefault respects
# an explicit override (e.g. Docker). All litellm imports are lazy (inside functions),
# so this module-level set always runs before the first `import litellm`.
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

from flowfile_core.ai.routes import router  # noqa: E402

__all__ = ["router"]
