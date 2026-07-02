"""Turn ``${name}`` flow-parameter references into function arguments in generated code.

Mechanism: before conversion, every node's settings get ``${name}`` refs
substituted with a unique sentinel string (``__FF_PARAM_name__``). Handlers emit
settings values as they always do; a token-level post-pass on the generated
module then rewrites the sentinels:

- a string literal that IS exactly one sentinel becomes a bare identifier
  (typed: ``.head('__FF_PARAM_limit__')`` -> ``.head(limit)``),
- a string literal containing sentinels becomes an f-string
  (``'a __FF_PARAM_x__ b'`` -> ``f'a {x} b'``),
- a sentinel in identifier position (inline code emission) becomes the name.

Anything the post-pass cannot rewrite safely (raw/byte/f-strings, multi-line
strings) degrades back to the literal ``${name}`` form, reported to the caller.
"""

import io
import re
import tokenize
from typing import Any

from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.flowfile.parameter_resolver import _apply_recursive, restore_parameters

SENTINEL_PREFIX = "__FF_PARAM_"
_SENTINEL_RE = re.compile(r"__FF_PARAM_([a-zA-Z_][a-zA-Z0-9_]*)__")
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_STRING_TOKEN_RE = re.compile(r"^([a-zA-Z]*)('''|\"\"\"|'|\")(.*)\2$", re.DOTALL)


def param_sentinel(name: str) -> str:
    return f"{SENTINEL_PREFIX}{name}__"


def codegen_parameters(parameters: list[FlowParameter]) -> list[FlowParameter]:
    """Parameters usable as function arguments (name must be a valid identifier)."""
    return [p for p in parameters if _IDENTIFIER_RE.match(p.name)]


def apply_param_sentinels(nodes_settings: list[Any], parameters: list[FlowParameter]) -> list:
    """Substitute ``${name}`` refs in every settings object with sentinels, in place.

    Returns the restorations list for ``restore_param_sentinels``. Refs to
    unknown parameters are left untouched (they stay literal in the output,
    matching the previous behavior).
    """
    sentinels = {p.name: param_sentinel(p.name) for p in parameters}
    restorations: list = []
    if not sentinels:
        return restorations
    for settings in nodes_settings:
        if settings is not None:
            # render_expressions=False: insert the sentinel verbatim (the token-level
            # post-pass rewrites it). Expression-literal rendering would quote the
            # sentinel and break the rewrite.
            _apply_recursive(settings, sentinels, restorations, render_expressions=False)
    return restorations


def restore_param_sentinels(restorations: list) -> None:
    restore_parameters(restorations)


def restore_sentinels_to_refs(text: str) -> str:
    """Sentinels back to literal ``${name}`` (for verbatim-embedded code like notebooks)."""
    return _SENTINEL_RE.sub(lambda m: "${" + m.group(1) + "}", text)


def parameter_default_repr(parameter: FlowParameter) -> str:
    """The Python literal used as the function-argument default."""
    return repr(parameter.typed_default())


# ParamType -> Python annotation for generated function signatures.
PARAM_TYPE_TO_ANNOTATION = {
    "string": "str",
    "enum": "str",
    "integer": "int",
    "float": "float",
    "boolean": "bool",
}


def param_arg(parameter: FlowParameter) -> str:
    """``name: type = default`` for a generated function signature."""
    annotation = PARAM_TYPE_TO_ANNOTATION.get(parameter.type, "str")
    return f"{parameter.name}: {annotation} = {parameter_default_repr(parameter)}"


def _rewrite_string_token(token_text: str, param_names: set[str]) -> str | None:
    match = _STRING_TOKEN_RE.match(token_text)
    if match is None:
        return None
    prefix, quote, inner = match.groups()
    lowered = prefix.lower()
    if "b" in lowered or "f" in lowered or "r" in lowered:
        return None
    if len(quote) == 3 or "\n" in inner:
        return None
    whole = _SENTINEL_RE.fullmatch(inner)
    if whole is not None and whole.group(1) in param_names:
        return whole.group(1)
    if not any(param_sentinel(name) in inner for name in param_names):
        return None
    escaped = inner.replace("{", "{{").replace("}", "}}")
    escaped = _SENTINEL_RE.sub(
        lambda m: "{" + m.group(1) + "}" if m.group(1) in param_names else m.group(0),
        escaped,
    )
    return f"f{prefix}{quote}{escaped}{quote}"


def resolve_param_sentinels(code: str, param_names: set[str]) -> tuple[str, set[str]]:
    """Rewrite sentinels in *code* into references to same-named function arguments.

    Returns ``(rewritten_code, leaked_names)`` where leaked names could not be
    rewritten safely and were degraded back to literal ``${name}`` text.
    """
    if SENTINEL_PREFIX not in code:
        return code, set()

    edits: dict[int, list[tuple[int, int, str]]] = {}
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(code).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError, ValueError):
        tokens = []

    for tok in tokens:
        if tok.start[0] != tok.end[0]:
            continue  # multi-line strings degrade via the cleanup pass
        replacement: str | None = None
        if tok.type == tokenize.NAME:
            sentinel_match = _SENTINEL_RE.fullmatch(tok.string)
            if sentinel_match is not None and sentinel_match.group(1) in param_names:
                replacement = sentinel_match.group(1)
        elif tok.type == tokenize.STRING and SENTINEL_PREFIX in tok.string:
            replacement = _rewrite_string_token(tok.string, param_names)
        if replacement is not None:
            edits.setdefault(tok.start[0] - 1, []).append((tok.start[1], tok.end[1], replacement))

    lines = code.split("\n")
    for row, row_edits in edits.items():
        line = lines[row]
        for start, end, replacement in sorted(row_edits, reverse=True):
            line = line[:start] + replacement + line[end:]
        lines[row] = line
    rewritten = "\n".join(lines)

    leaked = {m.group(1) for m in _SENTINEL_RE.finditer(rewritten) if m.group(1) in param_names}
    if leaked:
        rewritten = _SENTINEL_RE.sub(
            lambda m: "${" + m.group(1) + "}" if m.group(1) in param_names else m.group(0),
            rewritten,
        )
    return rewritten, leaked
