"""Settings autocomplete logic.

One pure-async function feeds the join-keys autocomplete endpoint:

* :func:`suggest_join_keys` — proposes ``(left_col, right_col)`` pairs
  for a join settings panel given the two upstream column lists. Pairs
  that cite a column not in the corresponding upstream are dropped
  unconditionally — both schemas are knowable when joins are
  configured, so there's no "unverified" middle ground.

It:

* Uses the :class:`RateLimitScheduler` for RPM tracking but with no
  retries — autocomplete is fail-fast on a 3-second hard timeout.
* Passes ``response_format={"type":"json_object"}`` to the provider so
  the model is guided into JSON-mode where the provider supports it.
* Degrades gracefully when an upstream node has no
  ``predicted_schema`` (cold flow → ``degraded=True`` rather than
  auto-fetching, which would blow the latency budget).
* Emits one :func:`metrics.record_autocomplete_call` event per call
  (NOT per keystroke).

The lazy-litellm contract is preserved — this module must not import
``litellm`` at module load time. Provider calls are made through the
``Provider`` Protocol seam.

The settings autocomplete surface (``settings_autocomplete``) is part
of the ``SurfaceLiteral`` and ``SURFACE_TO_LEVEL`` mapping (mapped to
the ``copilot`` level). The strict-JSON instruction lives inline in
this module's per-call system prompts rather than in a fourth
``prompts/<level>.md`` file since the JSON shape is
autocomplete-specific and the level vocabulary stays anchored to the
three depth levels.
"""

from __future__ import annotations

import asyncio
import json
import logging
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

DEFAULT_TIMEOUT_SECONDS: float = 3.0

MAX_JOIN_KEY_PAIRS: int = 5


# Wire types


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


class JoinKeySuggestionsResponse(BaseModel):
    key_pairs: list[JoinKeyPair] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None


# Internal model: the JSON shape the LLM must emit. Separate from the public
# response model so we can keep the public surface clean (e.g. ``degraded``
# is set by the server, never by the LLM).
class _JoinKeyLLMOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    key_pairs: list[JoinKeyPair] = Field(default_factory=list)


# Schema lookup


def _column_names_for_node(node: FlowNode) -> list[str] | None:
    """Return the predicted upstream column names, or ``None`` if the schema is missing/empty."""
    schema: list[FlowfileColumn] | None = node.node_schema.predicted_schema
    if not schema:
        return None
    return [col.column_name for col in schema]


# Prompt construction


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
    """Comma-join column names for the prompt, or ``"(empty)"`` when there are none."""
    if not columns:
        return "(empty)"
    return ", ".join(columns)


def _build_join_keys_messages(
    *,
    left_columns: list[str],
    right_columns: list[str],
    how: str,
    max_pairs: int,
) -> list[Message]:
    """Build the ``[system, user]`` messages for the join-key suggestion LLM."""
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


# Provider call wrapper


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
    response string when successful; ``error_reason`` is non-None
    when the call timed out or the provider raised. Retry/backoff is
    intentionally bypassed — the scheduler is consulted only for RPM
    tracking via ``acquire``.
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


# Public API


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

    Pairs whose ``left_col`` ∉ left schema or ``right_col`` ∉ right schema
    are dropped — both schemas are knowable when joins are configured. Returns
    a ``degraded`` response when either upstream schema is unavailable or a
    node is missing from the flow.
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
    "MAX_JOIN_KEY_PAIRS",
    "JoinKeyPair",
    "JoinKeySuggestionsResponse",
    "suggest_join_keys",
]
