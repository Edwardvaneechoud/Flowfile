"""Provider abstraction over LLM backends (litellm-backed).

The ``Provider`` Protocol + per-vendor adapters live here; BYOK
keys are plugged via the existing Fernet pipeline — see
:mod:`flowfile_core.ai.byok` for the user-aware seam and
:mod:`flowfile_core.ai.credentials` for the storage layer. Public
re-exports below are the single import surface for downstream
callers.
"""

from flowfile_core.ai.providers._litellm_base import LiteLLMProvider
from flowfile_core.ai.providers.anthropic import AnthropicProvider
from flowfile_core.ai.providers.base import (
    ChatResponse,
    Message,
    Provider,
    Role,
    StreamChunk,
    ToolCall,
    ToolSpec,
    Usage,
)
from flowfile_core.ai.providers.google import GoogleProvider
from flowfile_core.ai.providers.groq import GroqProvider
from flowfile_core.ai.providers.ollama import OllamaProvider
from flowfile_core.ai.providers.openai import OpenAIProvider
from flowfile_core.ai.providers.openrouter import OpenRouterProvider
from flowfile_core.ai.providers.registry import (
    PROVIDERS,
    UnknownProviderError,
    list_supported_providers,
    provider_factory,
)

__all__ = [
    # Protocol + types
    "Provider",
    "Message",
    "Role",
    "ToolSpec",
    "ToolCall",
    "ChatResponse",
    "StreamChunk",
    "Usage",
    # Concrete adapters
    "LiteLLMProvider",
    "AnthropicProvider",
    "OpenAIProvider",
    "GoogleProvider",
    "GroqProvider",
    "OpenRouterProvider",
    "OllamaProvider",
    # Factory
    "provider_factory",
    "list_supported_providers",
    "UnknownProviderError",
    "PROVIDERS",
]
