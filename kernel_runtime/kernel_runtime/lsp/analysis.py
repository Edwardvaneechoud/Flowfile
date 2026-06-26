"""Jedi engine: completions, hover, signatures, syntax diagnostics for one cell.

Functions are pure (no FastAPI, no global state): the route in ``main.py`` passes
a read-only snapshot of the cell's live namespace, and we layer the real installed
``polars`` + ``flowfile_client`` on top so the common globals resolve even before
any cell has run. Every Jedi call is wrapped — a stray completion request must
degrade to an empty result, never raise.
"""

import logging
from collections.abc import Callable
from typing import Any

import jedi

from kernel_runtime.lsp.context import to_jedi_position
from kernel_runtime.lsp.models import (
    CompleteResponse,
    CompletionItem,
    Diagnostic,
    DiagnosticsResponse,
    HoverResponse,
    LspCapabilities,
    SignatureInfo,
    SignatureResponse,
)

logger = logging.getLogger(__name__)

_MAX_COMPLETIONS = 100
_MAX_DOC_CHARS = 800
_FEATURES = ["complete", "hover", "signature", "diagnostics"]


def _safe(fn: Callable[[], Any], default: Any = None) -> Any:
    """Call a Jedi accessor that may raise on edge-case objects; swallow failures."""
    try:
        return fn()
    except Exception:  # noqa: BLE001 — jedi can raise a wide range introspecting objects
        return default


def _truncate(text: str | None) -> str:
    if not text:
        return ""
    return text if len(text) <= _MAX_DOC_CHARS else text[:_MAX_DOC_CHARS] + "…"


def _seed_namespace(live: dict | None) -> dict:
    """Build the interpreter namespace: real polars + flowfile_ctx, then live vars.

    Seeding the real installed modules means ``pl.`` and ``flowfile_ctx.`` complete
    from the first keystroke; live vars (executed cells) override the seeds.
    """
    ns: dict[str, Any] = {}
    try:
        import polars as pl

        ns["pl"] = pl
    except Exception:  # noqa: BLE001
        pass
    try:
        from kernel_runtime import flowfile_client

        ns["flowfile_ctx"] = flowfile_client
        ns["flowfile"] = flowfile_client
    except Exception:  # noqa: BLE001
        pass
    if live:
        ns.update(live)
    return ns


def _interpreter(code: str, live: dict | None) -> jedi.Interpreter:
    return jedi.Interpreter(code, namespaces=[_seed_namespace(live)])


def capabilities(version: str = "") -> LspCapabilities:
    return LspCapabilities(enabled=True, version=version, features=list(_FEATURES))


def complete(code: str, line: int, column: int, live: dict | None) -> CompleteResponse:
    jline, jcol = to_jedi_position(code, line, column)
    items: list[CompletionItem] = []
    try:
        completions = _interpreter(code, live).complete(jline, jcol)
    except Exception as exc:  # noqa: BLE001
        logger.debug("jedi.complete failed: %s", exc)
        return CompleteResponse(items=items)
    for comp in completions[:_MAX_COMPLETIONS]:
        suffix = _safe(lambda c=comp: c.complete)
        items.append(
            CompletionItem(
                label=comp.name,
                type=_safe(lambda c=comp: c.type) or "",
                detail=_safe(lambda c=comp: c.description) or "",
                documentation=_truncate(_safe(lambda c=comp: c.docstring(raw=True))),
                insert_text=suffix or None,
            )
        )
    return CompleteResponse(items=items)


def hover(code: str, line: int, column: int, live: dict | None) -> HoverResponse:
    jline, jcol = to_jedi_position(code, line, column)
    try:
        names = _interpreter(code, live).help(jline, jcol)
    except Exception as exc:  # noqa: BLE001
        logger.debug("jedi.help failed: %s", exc)
        return HoverResponse(contents=None)
    if not names:
        return HoverResponse(contents=None)
    name = names[0]
    header = _safe(lambda: name.full_name) or _safe(lambda: name.name) or ""
    sig = ""
    sigs = _safe(lambda: name.get_signatures(), []) or []
    if sigs:
        sig = _safe(lambda: sigs[0].to_string()) or ""
    doc = _truncate(_safe(lambda: name.docstring(raw=True)))
    parts = [p for p in (f"`{sig}`" if sig else header, doc) if p]
    return HoverResponse(contents="\n\n".join(parts) or None)


def signature(code: str, line: int, column: int, live: dict | None) -> SignatureResponse:
    jline, jcol = to_jedi_position(code, line, column)
    try:
        sigs = _interpreter(code, live).get_signatures(jline, jcol)
    except Exception as exc:  # noqa: BLE001
        logger.debug("jedi.get_signatures failed: %s", exc)
        return SignatureResponse(signatures=[], active_signature=0)
    out: list[SignatureInfo] = []
    for sig in sigs:
        params = _safe(lambda s=sig: [p.to_string() for p in s.params], []) or []
        idx = _safe(lambda s=sig: s.index)
        out.append(
            SignatureInfo(
                label=_safe(lambda s=sig: s.to_string()) or "",
                parameters=params,
                active_parameter=idx if isinstance(idx, int) else 0,
                documentation=_truncate(_safe(lambda s=sig: s.docstring(raw=True))),
            )
        )
    return SignatureResponse(signatures=out, active_signature=0)


def diagnostics(code: str) -> DiagnosticsResponse:
    """Syntax errors only (v1). Pyflakes (unused imports, undefined names) is P2.

    Uses ``jedi.Script`` (no namespace needed for parse errors).
    """
    out: list[Diagnostic] = []
    try:
        errors = jedi.Script(code).get_syntax_errors()
    except Exception as exc:  # noqa: BLE001
        logger.debug("jedi.get_syntax_errors failed: %s", exc)
        return DiagnosticsResponse(diagnostics=out)
    for err in errors:
        out.append(
            Diagnostic(
                line=_safe(lambda e=err: e.line) or 1,
                column=_safe(lambda e=err: e.column) or 0,
                end_line=_safe(lambda e=err: e.until_line) or (_safe(lambda e=err: e.line) or 1),
                end_column=_safe(lambda e=err: e.until_column) or 0,
                message=_safe(lambda e=err: e.get_message()) or "syntax error",
                severity="error",
                source="jedi",
            )
        )
    return DiagnosticsResponse(diagnostics=out)
