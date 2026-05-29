"""Local model adapter — on-demand llama.cpp server, fully offline.

The Flowfile-managed ``llama-server`` (see
:mod:`flowfile_core.ai.local_model.manager`) exposes an OpenAI-compatible
API on ``http://127.0.0.1:<port>/v1``, so litellm's ``openai/`` route
drives it with a per-call ``api_base``. No BYOK, no tools — this provider
backs the non-agentic chat + one-shot generation surfaces only, where a
1.5B model is reliable.

Instantiated directly by the local-model routes (NOT registered in the
BYOK ``PROVIDERS`` map) because its base URL is the manager's live port,
not a user-saved credential.
"""

from typing import ClassVar

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider


class LocalProvider(LiteLLMProvider):
    name: ClassVar[str] = "local"
    default_model: ClassVar[str] = "qwen2.5-coder-1.5b"
    # litellm routes ``openai/<name>`` + ``api_base`` to any OpenAI-compatible
    # server; llama-server speaks exactly that shape.
    model_prefix: ClassVar[str] = "openai/"
    supports_tools: ClassVar[bool] = False
    supports_streaming: ClassVar[bool] = True
    # Resolved at call time from the running server's port; never a fixed default.
    default_api_base: ClassVar[str | None] = None
