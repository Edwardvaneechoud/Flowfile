"""Live gate for the notebook LSP bridge (mirrors ``ai/feature_flag.py``).

Reads ``settings.FLOWFILE_LSP_ENABLED`` (a ``MutableBool``) on every call so an
in-process flip from the admin endpoint takes effect immediately. Unlike the AI
gate, callers must NOT 503 when off — the editor degrades silently — so this
module exposes only a reader, not a raising dependency.
"""

from flowfile_core.configs import settings


def is_lsp_enabled() -> bool:
    return bool(settings.FLOWFILE_LSP_ENABLED)
