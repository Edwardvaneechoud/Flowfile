"""Jedi-backed code intelligence for notebook cells, served in-process by the kernel.

Runs ``jedi.Interpreter`` over the live per-flow namespace plus the real installed
polars / flowfile_client, so completions/hover/signatures reflect both the actual
environment and any variables already executed into the cell's session.
"""

from kernel_runtime.lsp.models import (
    CompleteResponse,
    CompletionItem,
    Diagnostic,
    DiagnosticsResponse,
    HoverResponse,
    LspCapabilities,
    LspRequest,
    SignatureInfo,
    SignatureResponse,
)

__all__ = [
    "LspRequest",
    "CompletionItem",
    "CompleteResponse",
    "HoverResponse",
    "SignatureInfo",
    "SignatureResponse",
    "Diagnostic",
    "DiagnosticsResponse",
    "LspCapabilities",
]
