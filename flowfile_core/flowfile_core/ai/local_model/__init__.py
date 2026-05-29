"""On-demand local LLM runtime (llama.cpp ``llama-server`` + a small GGUF).

See :mod:`flowfile_core.ai.local_model.manager` for install / lifecycle.
Nothing downloads or runs until the user opts in via the
``/ai/local-model/*`` routes.
"""

from flowfile_core.ai.local_model import manager

__all__ = ["manager"]
