"""Settings autocomplete logic — owned by W34.

Two pure-async functions feed the formula and join-keys autocomplete
endpoints:

* :func:`suggest_formula_completions` — schema-aware completions for a
  ``formula`` settings expression. The LLM emits a JSON document; we
  extract literal column refs from each suggestion and drop those that
  reference columns not in the upstream's predicted schema. Suggestions
  with constructs the regex can't reason about (`pl.col(variable)`) are
  marked ``verified=False`` so the frontend renders an "unverified" badge
  rather than silently filtering them.

* :func:`suggest_join_keys` — proposes ``(left_col, right_col)`` pairs for
  a join settings panel given the two upstream column lists. Pairs that
  cite a column not in the corresponding upstream are dropped
  unconditionally — both schemas are knowable when joins are configured,
  so there's no "unverified" middle ground.

Both functions:

* Use the W14 :class:`RateLimitScheduler` for RPM tracking but with
  ``RetryPolicy(max_retries=0)`` semantics (no retries — autocomplete is
  fail-fast on a 3-second hard timeout).
* Pass ``response_format={"type":"json_object"}`` to the W11
  ``LiteLLMProvider.chat`` so the model is guided into JSON-mode where
  the provider supports it.
* Degrade gracefully when an upstream node has no
  ``predicted_schema`` (D011: cold flow → ``degraded=True`` rather than
  auto-fetching, which would blow the latency budget).
* Emit one :func:`metrics.record_autocomplete_call` event per call (NOT
  per keystroke).

The lazy-litellm contract from W11 / W12 / W13 is preserved — this module
must not import ``litellm`` at module load time. Provider calls are made
through the ``Provider`` Protocol seam.

The settings autocomplete surface (``settings_autocomplete``) is part of
W30's ``SurfaceLiteral`` and W22's ``SURFACE_TO_LEVEL`` (mapped to the
``copilot`` level). The strict-JSON instruction lives inline in this
module's per-call system prompts rather than in a fourth ``prompts/<level>.md``
file, since the JSON shape is autocomplete-specific and the level vocabulary
stays anchored to the three D008 depth levels.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from flowfile_core.ai.metrics import record_autocomplete_call
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import (
        FlowfileColumn,
    )
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

logger = logging.getLogger(__name__)


SURFACE: str = "settings_autocomplete"
"""The single ``SurfaceLiteral`` value W34 emits — kept as a constant so call
sites and test assertions stay in lock-step."""

DEFAULT_TIMEOUT_SECONDS: float = 3.0
"""Hard timeout per call. Bypasses the W14 retry budget — autocomplete is
fail-fast by design (frontend falls back to static suggestions on timeout)."""

MAX_FORMULA_SUGGESTIONS: int = 5
MAX_JOIN_KEY_PAIRS: int = 5


# --------------------------------------------------------------------------- #
# Wire types                                                                   #
# --------------------------------------------------------------------------- #


class FormulaSuggestion(BaseModel):
    """A single formula-completion candidate.

    ``verified`` reflects column-grounding: ``True`` means the suggestion's
    literal column refs all resolved against the upstream schema; ``False``
    means we couldn't statically extract refs (complex expression) and the
    frontend should render a "?" badge so the user sees it's AI-but-unverified.
    Suggestions with extractable-but-missing refs are dropped before
    serialisation, so a suggestion in the response is *never* known to be
    invalid.
    """

    model_config = ConfigDict(extra="forbid")

    insert_text: str
    label: str
    description: str | None = None
    verified: bool = False


class JoinKeyPair(BaseModel):
    """A single proposed ``(left_col, right_col)`` pair.

    ``confidence`` is the model's self-rating of the match quality (range
    ``[0, 1]`` by convention; we don't enforce upper-bound at the wire layer
    because providers occasionally drift). ``rationale`` is optional —
    cheaper providers tend to skip it.
    """

    model_config = ConfigDict(extra="forbid")

    left_col: str
    right_col: str
    confidence: float = 0.5
    rationale: str | None = None


class FormulaSuggestionsResponse(BaseModel):
    suggestions: list[FormulaSuggestion] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None


class JoinKeySuggestionsResponse(BaseModel):
    key_pairs: list[JoinKeyPair] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None


# Internal model: the JSON shape the LLM must emit. Separate from the public
# response model so we can keep the public surface clean (e.g. ``degraded``
# is set by the server, never by the LLM).
class _FormulaLLMOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    suggestions: list[FormulaSuggestion] = Field(default_factory=list)


class _JoinKeyLLMOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    key_pairs: list[JoinKeyPair] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Column reference extraction                                                  #
# --------------------------------------------------------------------------- #


# Literal patterns we can statically extract column refs from:
#   * pl.col("name")    or pl.col('name')
#   * [name]            (bare-bracketed identifier — flowfile's `formula`
#                        legacy syntax)
# Everything else (e.g. `pl.col(variable)`, dynamic generation) returns
# ``extraction_complete=False`` so the suggestion is forwarded with
# ``verified=False`` rather than silently filtered.
_COL_LITERAL_RE: re.Pattern[str] = re.compile(
    r"""
    pl\.col\(\s*(?P<q>['"])(?P<pl_name>[^'"]*)\1\s*\)   # pl.col("X")
    |
    \[(?P<bracket_name>[A-Za-z_][\w\-]*)\]              # [name]
    """,
    re.VERBOSE,
)

# Detect constructs we can't reason about:
#   * pl.col(<non-literal>)
#   * cs.matches(...) / cs.starts_with(...) / cs.string()  (column-selectors)
#   * f-string-style dynamic names
_OPAQUE_REF_RE: re.Pattern[str] = re.compile(
    r"""
    pl\.col\(\s*(?!['"])              # pl.col(  not followed by a quote
    |
    \bcs\.[a-z_]+\s*\(                # cs.matches(...) etc
    |
    f['"]                             # f"..." / f'...'
    """,
    re.VERBOSE,
)


def _extract_column_refs(text: str) -> tuple[set[str], bool]:
    """Pull static column references out of a formula expression.

    Returns ``(refs, extraction_complete)`` where ``extraction_complete``
    is ``False`` when the expression contains constructs the regex can't
    statically reason about — those suggestions can still be useful, but
    the caller should mark them ``verified=False``.
    """
    if not text:
        return set(), True

    refs: set[str] = set()
    for match in _COL_LITERAL_RE.finditer(text):
        name = match.group("pl_name") or match.group("bracket_name")
        if name:
            refs.add(name)

    extraction_complete = _OPAQUE_REF_RE.search(text) is None
    return refs, extraction_complete


# --------------------------------------------------------------------------- #
# Schema lookup                                                                #
# --------------------------------------------------------------------------- #


def _column_names_for_node(node: FlowNode) -> list[str] | None:
    """Return the predicted upstream column names, or ``None`` if unknown.

    W22's ``snapshot_node`` does the same dance for prompt rendering; we
    inline the schema-only path here because we don't need W22's full
    snapshot machinery (no settings, no edges, no samples) for autocomplete.
    """
    schema: list[FlowfileColumn] | None = node.node_schema.predicted_schema
    if not schema:
        return None
    return [col.column_name for col in schema]


def _get_main_upstream(graph: FlowGraph, node_id: int | str) -> FlowNode | None:
    """Resolve the immediate upstream node of ``node_id`` whose schema feeds
    the formula expression.

    Formula nodes have one input. We walk ``all_inputs`` rather than
    ``main_input`` because UDFs occasionally use the multi-port plumbing,
    and ``all_inputs`` is the union view.
    """
    target = graph.get_node(node_id)
    if target is None:
        return None
    inputs = target.all_inputs
    if not inputs:
        return None
    return inputs[0]


# --------------------------------------------------------------------------- #
# Prompt construction                                                          #
# --------------------------------------------------------------------------- #


_FORMULA_SYSTEM_PROMPT = """\
You suggest Polars formula expressions for Flowfile's `formula` node.

Output a single JSON object — no prose, no code blocks. Shape:

  {"suggestions": [
     {"insert_text": "<expression>",
      "label": "<short label>",
      "description": "<one-line, optional>"}
  ]}

Hard rules:

* `insert_text` must be a syntactically-valid Polars expression. Prefer
  `pl.col("X")` over `[X]` for new code, but match existing style if the
  user has already started typing `[`.
* You MAY only reference columns from the upstream schema you're given.
  Do not invent column names. Do not assume capitalisation.
* Keep suggestions short (<80 chars). Return at most {{MAX_SUGGESTIONS}}
  suggestions, ordered by relevance to the user's partial text and
  intent.
* If the upstream schema is empty, return `{"suggestions": []}`.
"""


_JOIN_KEYS_SYSTEM_PROMPT = """\
You propose join-key pairs for Flowfile's `join` node.

Output a single JSON object — no prose, no code blocks. Shape:

  {"key_pairs": [
     {"left_col": "<col from left>",
      "right_col": "<col from right>",
      "confidence": 0.0-1.0,
      "rationale": "<one-line, optional>"}
  ]}

Hard rules:

* `left_col` must be a column from the LEFT schema; `right_col` must be
  a column from the RIGHT schema. Do not swap. Do not invent names.
* Match by semantic similarity (`customer_id` ↔ `cust_id`,
  `userId` ↔ `user_id`), not just exact name equality. Exact-name matches
  are also valid and should usually rank highest.
* `confidence` reflects your match quality — 1.0 for exact-name + identical
  type, 0.7-0.9 for semantic match, ≤0.5 for "plausible but unsure".
* Return at most {{MAX_PAIRS}} pairs ordered by confidence descending.
* If either schema is empty or there's no plausible match, return
  `{"key_pairs": []}`.
* Respect the join `how={{HOW}}` — for `cross` joins, return `{"key_pairs": []}`
  (no keys needed).
"""


def _format_columns_for_prompt(columns: list[str]) -> str:
    if not columns:
        return "(empty)"
    return ", ".join(columns)


def _build_formula_messages(
    *,
    upstream_columns: list[str],
    partial_text: str,
    intent: str | None,
    max_suggestions: int,
) -> list[Message]:
    user_lines = [
        f"Upstream schema columns: {_format_columns_for_prompt(upstream_columns)}",
        f"Partial expression: {partial_text!r}",
    ]
    if intent:
        user_lines.append(f"User intent: {intent}")
    user_lines.append(f"Return at most {max_suggestions} suggestions.")
    # We use ``.replace`` rather than ``.format`` so the JSON shape examples in
    # the prompt — which legitimately use ``{`` / ``}`` — don't have to be
    # double-escaped.
    system = _FORMULA_SYSTEM_PROMPT.replace("{{MAX_SUGGESTIONS}}", str(max_suggestions))
    return [
        Message(role="system", content=system),
        Message(role="user", content="\n".join(user_lines)),
    ]


def _build_join_keys_messages(
    *,
    left_columns: list[str],
    right_columns: list[str],
    how: str,
    max_pairs: int,
) -> list[Message]:
    user_lines = [
        f"Left schema columns: {_format_columns_for_prompt(left_columns)}",
        f"Right schema columns: {_format_columns_for_prompt(right_columns)}",
        f"Join type: {how}",
        f"Return at most {max_pairs} pairs ordered by confidence descending.",
    ]
    system = _JOIN_KEYS_SYSTEM_PROMPT.replace("{{MAX_PAIRS}}", str(max_pairs)).replace("{{HOW}}", how)
    return [
        Message(role="system", content=system),
        Message(role="user", content="\n".join(user_lines)),
    ]


# --------------------------------------------------------------------------- #
# Provider call wrapper                                                        #
# --------------------------------------------------------------------------- #


async def _call_provider_for_json(
    *,
    provider: Provider,
    messages: list[Message],
    timeout: float,
    scheduler: RateLimitScheduler | None,
    max_tokens: int,
) -> tuple[str | None, str | None]:
    """Invoke ``provider.chat`` with JSON-mode and a hard timeout.

    Returns ``(content, error_reason)``. ``content`` is the LLM's raw
    response string when successful; ``error_reason`` is non-None when the
    call timed out or the provider raised. Retry/backoff is intentionally
    bypassed — the W14 scheduler is consulted only for RPM tracking via
    ``acquire``.
    """
    sched = scheduler or default_scheduler()

    async def _do_call() -> str | None:
        async with sched.acquire(provider.name, surface=SURFACE):
            response = await provider.chat(
                messages=messages,
                tools=None,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
        return response.content

    try:
        content = await asyncio.wait_for(_do_call(), timeout=timeout)
    except asyncio.TimeoutError:
        return None, "timeout"
    except Exception as exc:  # noqa: BLE001 — we deliberately collapse to a single reason
        logger.warning("autocomplete provider call failed: %s", exc, exc_info=False)
        return None, "provider_error"
    return content, None


def _parse_json_payload(content: str | None) -> tuple[Any, str | None]:
    """Strict JSON parse with a single non-error fallback (Markdown code-fence).

    Some providers wrap JSON in ```json fences even with ``response_format``
    set. We strip the fence and try once more. Any other deviation returns
    ``(None, "parse_error")``.
    """
    if content is None or not content.strip():
        return None, "empty_response"
    try:
        return json.loads(content), None
    except json.JSONDecodeError:
        pass
    # Fallback: strip ``` fences (` ```json ... ``` ` or ` ``` ... ``` `).
    stripped = content.strip()
    if stripped.startswith("```"):
        # Drop the opening fence line and trailing fence.
        without_open = stripped.split("\n", 1)[-1]
        if without_open.rstrip().endswith("```"):
            without_open = without_open.rstrip()[: -len("```")].rstrip()
        try:
            return json.loads(without_open), None
        except json.JSONDecodeError:
            pass
    return None, "parse_error"


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #


async def suggest_formula_completions(
    graph: FlowGraph,
    node_id: int | str,
    partial_text: str,
    intent: str | None = None,
    *,
    provider: Provider,
    max_suggestions: int = MAX_FORMULA_SUGGESTIONS,
    scheduler: RateLimitScheduler | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> FormulaSuggestionsResponse:
    """Schema-aware completions for a formula expression.

    See module docstring for invariants. The function never raises — every
    failure mode (cold upstream, timeout, parse error, validation error)
    becomes a ``degraded=True`` response with a stable ``reason`` string.
    """
    started = time.monotonic()
    upstream = _get_main_upstream(graph, node_id)
    upstream_columns: list[str] | None = _column_names_for_node(upstream) if upstream else None

    if upstream is None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="missing_upstream",
        )
        return FormulaSuggestionsResponse(
            suggestions=[],
            degraded=True,
            reason="No upstream node connected — connect a source first.",
        )

    if upstream_columns is None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="upstream_schema_unknown",
        )
        return FormulaSuggestionsResponse(
            suggestions=[],
            degraded=True,
            reason="Upstream schema unknown — run the upstream node first.",
        )

    messages = _build_formula_messages(
        upstream_columns=upstream_columns,
        partial_text=partial_text,
        intent=intent,
        max_suggestions=max_suggestions,
    )

    content, err = await _call_provider_for_json(
        provider=provider,
        messages=messages,
        timeout=timeout,
        scheduler=scheduler,
        max_tokens=512,
    )
    if err is not None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason=err,
        )
        return FormulaSuggestionsResponse(suggestions=[], degraded=True, reason=err)

    payload, parse_err = _parse_json_payload(content)
    if parse_err is not None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason=parse_err,
        )
        return FormulaSuggestionsResponse(suggestions=[], degraded=True, reason=parse_err)

    try:
        parsed = _FormulaLLMOutput.model_validate(payload)
    except ValidationError as exc:
        logger.debug("formula autocomplete validation error: %s", exc)
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="validation_error",
        )
        return FormulaSuggestionsResponse(
            suggestions=[],
            degraded=True,
            reason="validation_error",
        )

    available = set(upstream_columns)
    filtered: list[FormulaSuggestion] = []
    for suggestion in parsed.suggestions[:max_suggestions]:
        refs, extraction_complete = _extract_column_refs(suggestion.insert_text)
        if extraction_complete:
            missing = refs - available
            if missing:
                logger.debug(
                    "formula autocomplete dropped suggestion citing missing columns: %s",
                    sorted(missing),
                )
                continue
            verified = True
        else:
            # Constructs we can't statically validate — forward with the
            # unverified flag so the frontend renders a "?" badge.
            verified = False
        filtered.append(
            FormulaSuggestion(
                insert_text=suggestion.insert_text,
                label=suggestion.label,
                description=suggestion.description,
                verified=verified,
            )
        )

    latency_ms = int((time.monotonic() - started) * 1000)
    record_autocomplete_call(
        surface=SURFACE,
        provider=provider.name,
        latency_ms=latency_ms,
        suggestion_count=len(filtered),
        degraded_reason=None,
    )
    return FormulaSuggestionsResponse(suggestions=filtered, degraded=False)


async def suggest_join_keys(
    graph: FlowGraph,
    left_node_id: int | str,
    right_node_id: int | str,
    how: str = "inner",
    *,
    provider: Provider,
    max_pairs: int = MAX_JOIN_KEY_PAIRS,
    scheduler: RateLimitScheduler | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> JoinKeySuggestionsResponse:
    """Propose ``(left_col, right_col)`` pairs for a join settings panel.

    Schema grounding is strict: pairs whose ``left_col`` ∉ left schema or
    ``right_col`` ∉ right schema are dropped — both schemas are knowable
    when joins are configured (D011 doesn't apply here in the unverified
    sense; it applies as a "degraded if either side missing" policy).
    """
    started = time.monotonic()
    left_node = graph.get_node(left_node_id)
    right_node = graph.get_node(right_node_id)

    if left_node is None or right_node is None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="missing_node",
        )
        return JoinKeySuggestionsResponse(
            key_pairs=[],
            degraded=True,
            reason="Left or right node not found in flow.",
        )

    left_columns = _column_names_for_node(left_node)
    right_columns = _column_names_for_node(right_node)

    if left_columns is None or right_columns is None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="upstream_schema_unknown",
        )
        return JoinKeySuggestionsResponse(
            key_pairs=[],
            degraded=True,
            reason="Upstream schema unknown — run the upstream nodes first.",
        )

    messages = _build_join_keys_messages(
        left_columns=left_columns,
        right_columns=right_columns,
        how=how,
        max_pairs=max_pairs,
    )

    content, err = await _call_provider_for_json(
        provider=provider,
        messages=messages,
        timeout=timeout,
        scheduler=scheduler,
        max_tokens=512,
    )
    if err is not None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason=err,
        )
        return JoinKeySuggestionsResponse(key_pairs=[], degraded=True, reason=err)

    payload, parse_err = _parse_json_payload(content)
    if parse_err is not None:
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason=parse_err,
        )
        return JoinKeySuggestionsResponse(key_pairs=[], degraded=True, reason=parse_err)

    try:
        parsed = _JoinKeyLLMOutput.model_validate(payload)
    except ValidationError as exc:
        logger.debug("join keys autocomplete validation error: %s", exc)
        latency_ms = int((time.monotonic() - started) * 1000)
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=latency_ms,
            suggestion_count=0,
            degraded_reason="validation_error",
        )
        return JoinKeySuggestionsResponse(
            key_pairs=[],
            degraded=True,
            reason="validation_error",
        )

    left_set = set(left_columns)
    right_set = set(right_columns)
    valid: list[JoinKeyPair] = []
    for pair in parsed.key_pairs:
        if pair.left_col not in left_set:
            logger.debug(
                "join keys autocomplete dropped pair: left_col=%s not in left schema",
                pair.left_col,
            )
            continue
        if pair.right_col not in right_set:
            logger.debug(
                "join keys autocomplete dropped pair: right_col=%s not in right schema",
                pair.right_col,
            )
            continue
        valid.append(pair)

    valid.sort(key=lambda p: p.confidence, reverse=True)
    valid = valid[:max_pairs]

    latency_ms = int((time.monotonic() - started) * 1000)
    record_autocomplete_call(
        surface=SURFACE,
        provider=provider.name,
        latency_ms=latency_ms,
        suggestion_count=len(valid),
        degraded_reason=None,
    )
    return JoinKeySuggestionsResponse(key_pairs=valid, degraded=False)


__all__ = [
    "SURFACE",
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_FORMULA_SUGGESTIONS",
    "MAX_JOIN_KEY_PAIRS",
    "FormulaSuggestion",
    "JoinKeyPair",
    "FormulaSuggestionsResponse",
    "JoinKeySuggestionsResponse",
    "suggest_formula_completions",
    "suggest_join_keys",
]
