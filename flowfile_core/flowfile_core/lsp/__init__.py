"""Core-side LSP surface: the feature gate, the `/lsp/capabilities` probe, and the
Pydantic shapes the owner-checked `/kernels/{id}/lsp/*` bridge (in ``kernel/routes.py``)
forwards to the kernel's Jedi engine.

Core never runs Jedi itself — the engine lives in ``kernel_runtime`` — so these are
thin mirrors of ``kernel_runtime.lsp.models`` (core can't import ``kernel_runtime``).
"""
