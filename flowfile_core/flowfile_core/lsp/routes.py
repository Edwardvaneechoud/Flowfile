"""The global `/lsp/capabilities` feature probe.

Cheap, kernel-independent, and returns 200 always: the frontend caches this to
decide whether to register the LSP CodeMirror sources at all. Per-cell analysis
goes through the owner-checked `/kernels/{id}/lsp/*` bridge in ``kernel/routes.py``.
"""

from fastapi import APIRouter

from flowfile_core.lsp.feature_flag import is_lsp_enabled
from flowfile_core.lsp.models import LspCapabilities

router = APIRouter()

_FEATURES = ["complete", "hover", "signature", "diagnostics"]


@router.get("/lsp/capabilities", response_model=LspCapabilities)
async def lsp_capabilities() -> LspCapabilities:
    enabled = is_lsp_enabled()
    return LspCapabilities(enabled=enabled, version="", features=_FEATURES if enabled else [])
