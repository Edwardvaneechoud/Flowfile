"""Tolerant parsing of a model's JSON answer, shared by the JSON-mode AI surfaces.

Must not import ``litellm``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

_FENCE = "```"


def _loads(text: str) -> tuple[bool, Any]:
    try:
        return True, json.loads(text)
    except (json.JSONDecodeError, RecursionError):
        return False, None


def parse_json_payload(
    content: str | None,
    *,
    extract_object: bool = False,
    on_extracted: Callable[[], None] | None = None,
) -> tuple[Any, str | None]:
    """``(payload, None)``, or ``(None, "empty_response" | "parse_error")``.

    Strict JSON first, then the body of a Markdown fence around the whole answer. With
    ``extract_object`` the text from the first ``{`` to the last ``}`` is tried last, for an
    object with prose around it; ``on_extracted`` is called when a fence or that cut was needed.
    An answer nested too deeply to parse is a ``parse_error``.
    """
    if content is None or not content.strip():
        return None, "empty_response"
    text = content.strip()
    ok, parsed = _loads(text)
    if ok:
        return parsed, None
    candidates: list[str] = []
    if text.startswith(_FENCE):
        body = text.split("\n", 1)[-1]
        if body.rstrip().endswith(_FENCE):
            body = body.rstrip()[: -len(_FENCE)].rstrip()
        candidates.append(body)
    if extract_object:
        start, end = text.find("{"), text.rfind("}")
        if 0 <= start < end:
            candidates.append(text[start : end + 1])
    for candidate in candidates:
        ok, parsed = _loads(candidate)
        if ok:
            if on_extracted is not None:
                on_extracted()
            return parsed, None
    return None, "parse_error"


__all__ = ["parse_json_payload"]
