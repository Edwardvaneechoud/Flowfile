"""Jedi engine: completions, hover, signatures, syntax diagnostics for one cell.

Functions are pure (no FastAPI, no global state): the route in ``main.py`` passes
a read-only snapshot of the cell's live namespace, and we layer the real installed
``polars`` + ``flowfile_client`` on top so the common globals resolve even before
any cell has run. Every Jedi call is wrapped — a stray completion request must
degrade to an empty result, never raise.
"""

import inspect
import logging
import re
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
_MAX_COMPLETION_DOC_CHARS = 400
_FEATURES = ["complete", "hover", "signature", "diagnostics"]


def _safe(fn: Callable[[], Any], default: Any = None) -> Any:
    """Call a Jedi accessor that may raise on edge-case objects; swallow failures."""
    try:
        return fn()
    except Exception:  # noqa: BLE001 — jedi can raise a wide range introspecting objects
        return default


_RST_DIRECTIVE = re.compile(r"^\s*\.\.\s+[\w-]+::")
_SECTION_UNDERLINE = re.compile(r"^\s*[-=~^]{3,}\s*$")
_RST_ROLE = re.compile(r":[\w:+-]+:(`+)")


def _clean_doc(text: str) -> str:
    """Dedent and strip the reStructuredText Jedi hands back verbatim.

    Removes ``.. directive::`` blocks together with their indented bodies, the numpydoc
    section underlines, ``:role:`x``` markup and the double-backtick spelling, so the
    tooltip reads as prose. Section titles survive; the client styles them.
    """
    body = _RST_ROLE.sub(r"\1", inspect.cleandoc(text)).replace("``", "`")
    lines: list[str] = []
    skip_indent: int | None = None
    for raw in body.splitlines():
        line = raw.rstrip()
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        if skip_indent is not None:
            if not stripped or indent > skip_indent:
                continue
            skip_indent = None
        if _RST_DIRECTIVE.match(line):
            skip_indent = indent
            continue
        if _SECTION_UNDERLINE.match(line) and lines and lines[-1].strip():
            continue
        if not stripped and (not lines or not lines[-1].strip()):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _truncate(text: str | None, limit: int | None = None) -> str:
    """Clean a docstring, capping it only when *limit* is given."""
    if not text:
        return ""
    cleaned = _clean_doc(text)
    if limit is None or len(cleaned) <= limit:
        return cleaned
    return cleaned[:limit].rstrip() + "…"


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
    seen: set[tuple[str, str]] = set()
    for comp in completions[:_MAX_COMPLETIONS]:
        comp_type = _safe(lambda c=comp: c.type) or ""
        # jedi.Interpreter can emit a name twice (static parse + live namespace)
        if (comp.name, comp_type) in seen:
            continue
        seen.add((comp.name, comp_type))
        items.append(
            CompletionItem(
                label=comp.name,
                type=comp_type,
                detail=_safe(lambda c=comp: c.description) or "",
                documentation=_truncate(_safe(lambda c=comp: c.docstring(raw=True)), _MAX_COMPLETION_DOC_CHARS),
            )
        )
    return CompleteResponse(items=items)


_KIND_LABELS = {"instance": "variable", "statement": "variable", "param": "parameter", "path": "path"}


def _display_kind(name: Any) -> str:
    """Map a Jedi name's type onto the label shown in the hover title.

    Jedi reports methods as plain ``function``; a class parent promotes them.
    """
    kind = _safe(lambda: name.type) or ""
    if kind == "function" and _safe(lambda: name.parent().type) == "class":
        return "method"
    return _KIND_LABELS.get(kind, kind)


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
    sig = ""
    sigs = _safe(lambda: name.get_signatures(), []) or []
    if sigs:
        sig = _safe(lambda: sigs[0].to_string()) or ""
    doc = _truncate(_safe(lambda: name.docstring(raw=True)))
    return HoverResponse(
        contents=doc or None,
        kind=_display_kind(name),
        name=_safe(lambda: name.name) or _safe(lambda: name.full_name) or "",
        signature=sig,
    )


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
