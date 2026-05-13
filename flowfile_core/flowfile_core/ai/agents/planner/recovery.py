"""Textual tool-call recovery for small open-weights models.

Llama-3.3-70b / -8b sometimes emit the function call as text content
rather than via the function-calling API:

* **Function-call shape** — ``flowfile.graph.add_sort({"sort_input": ...})``
* **JSON-object shape** — ``{"name": "...", "parameters": {...}}``

These helpers walk the assistant content for either shape and surface a
synthetic :class:`ToolCall` so the loop can dispatch as if the model
had used the API correctly.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from flowfile_core.ai.providers.base import ToolCall


def _walk_balanced_braces(content: str, start_idx: int) -> int:
    """Return the index just after the closing ``}`` that pairs with
    the ``{`` at ``start_idx``, or ``-1`` if no balanced pair exists.

    String-context-aware: open/close braces inside JSON strings are
    ignored so the depth counter doesn't get fooled by a value like
    ``"text {with} braces"``. Used by the JSON-object-shape recovery
    path to find a candidate object's bounds before passing the
    substring to ``json.loads``.
    """
    if start_idx >= len(content) or content[start_idx] != "{":
        return -1
    depth = 1
    in_str = False
    esc = False
    for i in range(start_idx + 1, len(content)):
        c = content[i]
        if esc:
            esc = False
            continue
        if in_str and c == "\\":
            esc = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i + 1
    return -1


def _try_parse_function_call_shape(
    content: str, expected_tool_names: set[str]
) -> list[tuple[str, dict[str, Any]]]:
    """Find ``<tool_name>(<json>)`` invocations in text content.

    Walks balanced parens (string-aware) for each expected tool name.
    Returns a list of ``(name, args)`` matches. The caller decides
    whether to dispatch (single match) or decline (multiple distinct
    names → summary).
    """
    found: list[tuple[str, dict[str, Any]]] = []
    for tool_name in expected_tool_names:
        marker = f"{tool_name}("
        idx = content.find(marker)
        if idx < 0:
            continue
        start = idx + len(marker)
        depth = 1
        end = -1
        in_str = False
        esc = False
        for i in range(start, len(content)):
            c = content[i]
            if esc:
                esc = False
                continue
            if in_str and c == "\\":
                esc = True
                continue
            if c == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end < 0:
            continue
        try:
            args = json.loads(content[start:end])
        except (TypeError, ValueError):
            continue
        if isinstance(args, dict):
            found.append((tool_name, args))
    return found


def _try_parse_json_object_shape(
    content: str, expected_tool_names: set[str]
) -> list[tuple[str, dict[str, Any]]]:
    """Find ``{"name": "...", "parameters": {...}}`` or the
    ``"arguments"`` alias as text content.

    Llama-3.3-8b (and other small open-weights models) often emit the
    OpenAI-style function-call envelope as text content rather than via
    the function-calling API. This helper walks the content for
    balanced ``{...}`` blocks, attempts ``json.loads`` on each, and
    accepts when the parsed object has ``name``/``tool`` plus
    ``parameters``/``arguments`` AND the name is in
    ``expected_tool_names``.
    """
    found: list[tuple[str, dict[str, Any]]] = []
    i = 0
    while i < len(content):
        if content[i] != "{":
            i += 1
            continue
        end = _walk_balanced_braces(content, i)
        if end < 0:
            break
        candidate = content[i:end]
        try:
            obj = json.loads(candidate)
        except (TypeError, ValueError):
            obj = None
        i = end
        if not isinstance(obj, dict):
            continue
        name = obj.get("name") or obj.get("tool")
        args = obj.get("parameters")
        if args is None:
            args = obj.get("arguments")
        if not isinstance(name, str) or name not in expected_tool_names or not isinstance(args, dict):
            continue
        found.append((name, args))
    return found


def _recover_textual_tool_call(
    content: str,
    expected_tool_names: set[str],
) -> ToolCall | None:
    """Last-resort parse for LLMs that emit a function-call invocation
    as **text content** rather than via the function-calling API.

    Two LLM-emitted shapes are recognised:

    * **Function-call shape** — ``flowfile.graph.add_sort({"sort_input": ...})``.
      Llama-3.3-70b's typical fallback when it almost-but-not-quite
      uses the function-calling API.
    * **JSON-object shape** — ``{"name": "flowfile.meta.classify_intent",
      "parameters": {"op_kind": "add", ...}}`` (OpenAI-envelope form).
      Llama-3.3-8b's typical fallback even at the simplest stage 0.
      The ``"arguments"`` alias is also accepted because some models
      use that key name instead of ``"parameters"``.

    Robustness rules:

    * Only matches against ``expected_tool_names`` (the catalog the
      current stage exposed). Defends against the model emitting an
      unrelated tool name in its prose.
    * Returns ``None`` when **multiple distinct** expected names
      match across both shapes — that's almost always a "summary of
      past calls" style message after a successful add. Re-firing
      those would double-stage. The loop's normal "no tool calls →
      terminate" path handles the summary case correctly.
    """
    if not content or not expected_tool_names:
        return None

    found = _try_parse_function_call_shape(content, expected_tool_names)
    found.extend(_try_parse_json_object_shape(content, expected_tool_names))

    # Dedupe by tool name — a single call can match both shapes if
    # the model emits both forms; that's still one logical call.
    distinct_names = {name for name, _ in found}
    if len(distinct_names) != 1 or not found:
        return None
    tool_name = next(iter(distinct_names))
    # Pick the args from the first match for that name.
    args = next(args for name, args in found if name == tool_name)

    # Synthetic id for the tool message correlation. Use sha256 (not
    # ``hash()``, which CPython salts per-process via PYTHONHASHSEED) so
    # the id is deterministic across runs — the planner's ``role="tool"``
    # reply uses this id so the LLM can correlate on the next turn, and
    # tests assert on the literal value.
    key_src = f"{tool_name}|{json.dumps(args, sort_keys=True)}".encode()
    sha = hashlib.sha256(key_src).hexdigest()[:8]
    return ToolCall(
        id=f"recovered_{tool_name.replace('.', '_')}_{sha}",
        name=tool_name,
        arguments=args,
    )
