"""Chat → Agent auto-promotion classifier.

Sits in front of the chat surface: when the user types a build /
modify request in the chat drawer (e.g. *"add a group_by node
grouping by status"* or a follow-up *"can you implement?"* after the
chat explained how) without flipping the Agent toggle, the
:func:`classify_intent` helper detects build intent and the route
layer promotes the dispatch to ``/ai/agent/start`` instead of
``/ai/chat/stream``. Read-only chat is the safe fallback whenever
the classifier is uncertain.

Strategy: **one LLM call, with conversation history**. We
deliberately do not maintain a regex / verb-list / noun-list ladder
— that approach is brittle (the chat *"how do I X"* → *"can you
implement?"* pattern reveals how much classification depends on
prior turns, which a regex can't see) and the cost of a small / fast
model (Haiku-class via the ``intent_classifier`` surface bucket) is
well under a second. The LLM is asked for strict JSON; the prompt
explicitly tells it to weight the conversation context.

Conservative failure modes — :func:`classify_intent` never raises.
Any provider error / timeout / parse failure surfaces as
``IntentClassification(kind="chat", confidence=0.0, reason=…)`` so
the chat-drawer dispatch keeps working when the classifier is
unavailable. The route layer maps ``kind="ambiguous"`` (regardless
of confidence) and low-confidence ``build`` to a chat verdict for
the same reason — auto-promoting on uncertainty breaks user trust.

Lazy-litellm contract preserved: this module does **not** import
``litellm`` at load time. Provider calls flow through the
:class:`~flowfile_core.ai.providers.base.Provider` Protocol seam.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Literal

from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Regex fast-path                                                              #
# --------------------------------------------------------------------------- #
# Bypass the LLM classifier when the user's message is an unambiguous short
# imperative AND the prior assistant turn proposed concrete build steps. The
# LLM-based classifier was empirically unreliable on this exact case (small
# Haiku-class models would return ``ambiguous`` or low-confidence ``build``
# below the 0.6 threshold, sending the user back into chat). The fast-path
# is a no-LLM, deterministic decision that catches the canonical case before
# the LLM ever sees the message.
#
# Conservative by design: matches only when BOTH conditions hold. If the
# user's message is anything richer than a bare imperative, OR the prior
# turn doesn't look like a build suggestion, the fast-path returns ``None``
# and the LLM classifier runs as the fallback. So the fast-path is purely
# additive — it can't *worsen* the classifier's behaviour, only add hits.

_FAST_PATH_IMPERATIVES: frozenset[str] = frozenset({
    "do",
    "do it",
    "do this",
    "do that",
    "just do it",
    "implement",
    "implement it",
    "implement that",
    "implement this",
    "apply",
    "apply it",
    "apply this",
    "yes",
    "yes please",
    "yes do it",
    "go",
    "go ahead",
    "execute",
    "make it so",
    "make it happen",
    "run it",
})
"""Lowercased, punctuation-stripped imperatives that count as bare
confirmation of a prior suggestion. ``in`` lookup against
:func:`_normalize_user_message`."""


_BUILD_SHAPE_PATTERNS: tuple[re.Pattern[str], ...] = (
    # tool-call-shaped pseudocode (any add_* call written as text)
    re.compile(r"\badd_\w+\s*\(", re.IGNORECASE),
    # JSON-shaped node settings
    re.compile(r'"type"\s*:\s*"\w+"', re.IGNORECASE),
    # First-person action prose the chat agent is told NOT to use but
    # sometimes still does
    re.compile(r"\bI'?ll add\b", re.IGNORECASE),
    re.compile(r"\bLet me add\b", re.IGNORECASE),
    re.compile(r"\bI'?ll place\b", re.IGNORECASE),
    re.compile(r"\bAdding the node\b", re.IGNORECASE),
    # Palette-style instructions (the assist.md "drag X from sidebar" pattern)
    re.compile(r"\bdrag\s+\*?\*?\w[\w ]*\*?\*?\s+from\b", re.IGNORECASE),
    # The Step 6 footer itself — when the chat agent says "say 'do it' to
    # switch to agent mode", the chat agent has explicitly flagged this
    # turn as actionable. If user replies with the magic word, definitely
    # promote.
    re.compile(r"\bsay ['\"]?do it['\"]?\b", re.IGNORECASE),
    re.compile(r"\bsay ['\"]?implement['\"]?\b", re.IGNORECASE),
    re.compile(r"\bswitch to agent mode\b", re.IGNORECASE),
)
"""Regexes that detect build-shaped content in a prior assistant turn.
ANY match flips the precondition for the fast-path. New patterns can be
added freely — false positives just give us a build-classification on
the next imperative, which is the safer side."""


def _normalize_user_message(text: str) -> str:
    """Lowercase, strip whitespace and trailing punctuation.

    The fast-path needs to match *"do it"*, *"DO IT"*, *"do it."*,
    *"do it!"*, etc. Strip everything that doesn't change the imperative
    intent.
    """
    # Use a single regex strip to avoid B005 (rstrip with multi-char string
    # is misleading because each char is treated independently — fine here
    # but the linter complains; regex makes intent explicit).
    lowered = text.strip().lower()
    return re.sub(r"[\s.!?,;:'\"]+$", "", lowered)


def _regex_fast_path_classify(
    user_message: str,
    history: list[Message] | None,
) -> IntentClassification | None:
    """Bypass the LLM classifier for unambiguous imperative confirmations.

    Returns:
        ``IntentClassification(kind="build", confidence=0.95, ...)`` when
        the user's message is a bare imperative AND the most recent
        non-empty assistant turn matches a build-shape pattern.

        ``None`` otherwise — the LLM classifier should run as the
        fallback.

    The function is fail-quiet: any malformed input returns ``None`` so
    the LLM path takes over.
    """
    if not user_message:
        return None
    normalized = _normalize_user_message(user_message)
    if normalized not in _FAST_PATH_IMPERATIVES:
        return None
    if not history:
        return None
    # Find the most recent non-empty assistant message (oldest first list,
    # so iterate reversed)
    last_assistant: str | None = None
    for msg in reversed(history):
        if msg.role == "assistant" and (msg.content or "").strip():
            last_assistant = msg.content
            break
    if last_assistant is None:
        return None
    if not any(p.search(last_assistant) for p in _BUILD_SHAPE_PATTERNS):
        return None
    return IntentClassification(
        kind="build",
        confidence=0.95,
        reason=(
            f"regex fast-path: imperative {normalized!r} after "
            "build-shaped assistant turn"
        ),
    )


SURFACE: str = "intent_classifier"
"""Surface bucket. The classifier routes to a Haiku-class model on
every provider that maps this surface (see
``provider.surface_models``). Owns its own surface key (rather than
borrowing ``settings_autocomplete``) so audit-log filtering and
future per-surface model tuning don't conflate the autocomplete tier
with the chat-auto-promotion classifier."""


DEFAULT_TIMEOUT_SECONDS: float = 5.0
"""Hard timeout per :meth:`Provider.chat` call. The fallback on
timeout is ``kind="chat"`` so a slow classifier degrades to "send
the message as chat" rather than blocking the user's send."""


DEFAULT_MAX_TOKENS: int = 96
"""Cap the classifier's reply at ~96 tokens. Strict JSON shapes well under
that; bigger replies almost always indicate the model ignored the format
contract and we'll parse-fail back to ``chat`` anyway."""


PROMOTION_CONFIDENCE_THRESHOLD: float = 0.6
"""Minimum classifier confidence required to actually flip the route
verdict to ``"agent"``. Used by :func:`verdict_for` (and the
``intent_router_routes`` route layer). Below this — even on
``kind="build"`` — the verdict stays ``"chat"`` for safety."""


DEFAULT_HISTORY_TURNS: int = 4
"""Last N user/assistant turns the classifier inspects. Four turns covers
the canonical *"how do I X?"* → *"here's how"* → *"can you implement?"*
pattern without bloating the prompt."""


DEFAULT_HISTORY_CHARS_PER_TURN: int = 1_000
"""Per-turn character cap. Long assistant responses (markdown explanations,
schema dumps) get truncated with an ellipsis so the classifier sees the
shape of the conversation, not the full content."""


_KIND_VALUES = ("build", "chat", "ambiguous")
ClassifiedKind = Literal["build", "chat", "ambiguous"]
RouteVerdict = Literal["chat", "agent"]


@dataclass(slots=True, frozen=True)
class IntentClassification:
    """Classifier output.

    ``kind`` is the LLM's bucket; ``confidence`` lets the route layer
    apply :data:`PROMOTION_CONFIDENCE_THRESHOLD`. ``reason`` is a
    one-sentence rationale that ends up in the frontend promotion banner
    ("Switched to Agent mode — *<reason>*. Click here to keep this as
    chat instead.") so users can see *why* their message was routed.
    """

    kind: ClassifiedKind
    confidence: float
    reason: str


_LLM_SYSTEM_PROMPT = (
    "You are an intent classifier for the Flowfile chat drawer. Given the "
    "conversation so far (if any), classify the user's MOST RECENT message "
    "as exactly one of:\n"
    "- 'build' — the user wants to add, modify, configure, or remove nodes "
    "or steps in their flow. Two patterns count as build:\n"
    "  (1) DIRECT REQUEST — the message itself names an action verb AND a "
    "target. These are build regardless of prior context:\n"
    "    * Bare imperatives: 'add a sort node', 'implement a left join', "
    "'build the pipeline', 'create a filter on active users', 'remove "
    "that node'.\n"
    "    * Polite imperatives wrapped in a question form: 'can you add a "
    "sort node?', 'could you implement a join', 'would you set up a "
    "group_by'.\n"
    "    * Directive phrasing: 'I want you to add a filter', 'I need you "
    "to remove the join', 'please create a group_by'.\n"
    "    The 'can you' / 'I want you to' wrapper is courtesy, not a real "
    "question — the action verb + target is what classifies the message.\n"
    "  (2) BARE CONFIRMATION of a prior suggestion — short messages with "
    "NO target of their own (e.g. 'yes do it', 'go ahead', 'apply', "
    "'implement that', 'can you implement?'). These are build only when "
    "a prior assistant turn has build-shaped content; otherwise ambiguous "
    "(see below).\n"
    "- 'chat' — the user wants explanation, schema info, lineage, debugging "
    "help, or any other read-only answer. Questions starting with 'how', "
    "'what', 'why', 'is', 'does' are typically chat.\n"
    "- 'ambiguous' — the message could go either way and there isn't enough "
    "context to choose.\n\n"
    "Conversation context is decisive ONLY for the bare-confirmation case "
    "(pattern 2 above). A short message like 'yes please', 'go ahead', "
    "'apply', or 'can you implement?' (no target of its own) is build "
    "intent when it follows an assistant turn suggesting concrete nodes / "
    "steps — the user is asking the assistant to execute the suggestion. "
    "The same words with no prior build-shaped suggestion are ambiguous. "
    "Direct requests (pattern 1) that name their own action verb + target "
    "are build regardless of prior context.\n\n"
    "Short-imperative rule. When the user's MOST RECENT message is one of:\n"
    "  'do', 'do it', 'do this', 'just do it', 'implement', 'implement it',\n"
    "  'implement that', 'apply', 'apply it', 'yes', 'yes please',\n"
    "  'yes do it', 'go', 'go ahead', 'execute', 'make it so'\n"
    "AND the prior assistant turn contains build-shaped content — any of:\n"
    "  - tool-call-shaped pseudocode like `add_<node_type>(...)`\n"
    "  - JSON node settings like `{\"id\": ..., \"type\": ..., \"settings\": {...}}`\n"
    "  - first-person action prose like \"I'll add\", \"Let me add\",\n"
    "    \"Here's the new node:\", \"I'll place it ...\"\n"
    "THEN return {\"kind\": \"build\", \"confidence\": 0.9, ...}. Do NOT return\n"
    "'ambiguous' for these — the imperative is the user confirming the prior\n"
    "build-shaped suggestion.\n\n"
    "Examples:\n\n"
    "A: \"I'll add a manual_input. add_manual_input(node_id=5, ...)\"\n"
    "user: \"do it\"\n"
    "→ {\"kind\": \"build\", \"confidence\": 0.9,\n"
    "   \"reason\": \"imperative confirms prior add_manual_input suggestion\"}\n\n"
    "A: \"You'd add a Sort node from the sidebar.\"\n"
    "user: \"do it\"\n"
    "→ {\"kind\": \"build\", \"confidence\": 0.85,\n"
    "   \"reason\": \"imperative escalates explanation to actual build\"}\n\n"
    "A: \"What columns does node 3 produce?\" (no build-shaped content)\n"
    "user: \"do it\"\n"
    "→ {\"kind\": \"ambiguous\", \"confidence\": 0.4,\n"
    "   \"reason\": \"imperative without prior build-shaped suggestion\"}\n\n"
    "(no prior conversation)\n"
    "user: \"can you add a sort node on the customer column?\"\n"
    "→ {\"kind\": \"build\", \"confidence\": 0.9,\n"
    "   \"reason\": \"direct request — 'can you add' carries action verb 'add' with target 'sort node'\"}\n\n"
    "(no prior conversation)\n"
    "user: \"I want you to remove the filter\"\n"
    "→ {\"kind\": \"build\", \"confidence\": 0.9,\n"
    "   \"reason\": \"directive 'I want you to' with action 'remove' on target 'filter'\"}\n\n"
    "(no prior conversation)\n"
    "user: \"implement a left join between users and orders\"\n"
    "→ {\"kind\": \"build\", \"confidence\": 0.9,\n"
    "   \"reason\": \"bare imperative with action verb + target — build is unambiguous\"}\n\n"
    "Reply with strict JSON only: "
    '{"kind": "build" | "chat" | "ambiguous", '
    '"confidence": <float in [0, 1]>, '
    '"reason": "<one-sentence rationale, plain text, no quoting issues>"}\n'
    "No prose outside the JSON. Be conservative — when truly unsure, prefer "
    "'chat' or 'ambiguous'."
)


def _clip_message(msg: Message, max_chars: int) -> Message:
    """Return ``msg`` with content trimmed to ``max_chars`` (ellipsis on overflow).

    Preserves ``role``; never raises. Used to bound the per-turn payload
    the classifier sees so a multi-thousand-char assistant explanation
    doesn't consume the entire prompt budget.
    """
    content = msg.content
    if content is None or len(content) <= max_chars:
        return msg
    if max_chars <= 3:
        return Message(role=msg.role, content=content[:max_chars])
    return Message(role=msg.role, content=content[: max_chars - 3].rstrip() + "...")


def _bound_history(
    history: list[Message] | None,
    *,
    turns: int,
    chars_per_turn: int,
) -> list[Message]:
    """Last ``turns`` non-empty user/assistant messages, each clipped.

    System messages are dropped — the classifier injects its own system
    prompt and a stale chat-side system message would just compete with it.
    """
    if not history:
        return []
    filtered = [m for m in history if m.role in ("user", "assistant") and (m.content or "").strip()]
    return [_clip_message(m, chars_per_turn) for m in filtered[-turns:]]


def _coerce_kind(raw: object) -> ClassifiedKind | None:
    if not isinstance(raw, str):
        return None
    value = raw.strip().lower()
    return value if value in _KIND_VALUES else None  # type: ignore[return-value]


def _coerce_confidence(raw: object) -> float | None:
    if isinstance(raw, bool):
        # Python's ``bool`` is a subclass of ``int``; treat True/False as
        # malformed rather than 1.0 / 0.0.
        return None
    if isinstance(raw, int | float):
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        if 0.0 <= value <= 1.0:
            return value
    return None


def _parse_llm_payload(content: str | None) -> IntentClassification | None:
    """Strict JSON parse with a single Markdown-fence retry.

    Returns ``None`` on any deviation from the contract — caller falls
    back to a chat verdict. Mirrors the parse-tolerance posture in
    :mod:`flowfile_core.ai.autocomplete` for the same reason: the
    classifier should fail-quiet, not 5xx.
    """
    if content is None or not content.strip():
        return None

    candidates = [content]
    stripped = content.strip()
    if stripped.startswith("```"):
        # Drop opening fence line and trailing fence.
        without_open = stripped.split("\n", 1)[-1]
        if without_open.rstrip().endswith("```"):
            without_open = without_open.rstrip()[: -len("```")].rstrip()
        candidates.append(without_open)

    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        kind = _coerce_kind(payload.get("kind"))
        if kind is None:
            continue
        confidence = _coerce_confidence(payload.get("confidence"))
        if confidence is None:
            # Default to a low-but-non-zero confidence so the verdict layer
            # still folds this into "chat" — better than treating as a hard
            # parse error and losing the kind signal in the audit log.
            confidence = 0.3
        reason_raw = payload.get("reason")
        reason = (
            reason_raw.strip()
            if isinstance(reason_raw, str) and reason_raw.strip()
            else "classifier returned no reason"
        )
        # Keep reasons short — they end up in a UI banner.
        if len(reason) > 200:
            reason = reason[:197].rstrip() + "..."
        return IntentClassification(kind=kind, confidence=confidence, reason=reason)

    return None


async def classify_intent(
    message: str,
    *,
    history: list[Message] | None = None,
    provider: Provider | None = None,
    scheduler: RateLimitScheduler | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    history_turns: int = DEFAULT_HISTORY_TURNS,
    history_chars_per_turn: int = DEFAULT_HISTORY_CHARS_PER_TURN,
) -> IntentClassification:
    """Classify ``message`` as ``build`` / ``chat`` / ``ambiguous``.

    The contract is *fail-quiet*: any provider failure / timeout / parse
    error returns ``IntentClassification(kind="chat", confidence=0.0,
    reason=…)`` so the route layer can always fall back to chat dispatch.
    Callers that want to act on the kind (e.g. promote to agent mode)
    should also check ``confidence >= PROMOTION_CONFIDENCE_THRESHOLD``
    via :func:`verdict_for`.

    ``history`` carries the recent chat turns (oldest first, capped at
    :data:`DEFAULT_HISTORY_TURNS`). Conversation context is decisive for
    short follow-ups like *"can you implement?"* — the LLM's verdict
    flips with the prior assistant turn.

    When ``provider`` is ``None`` the LLM is skipped entirely and the
    caller gets a chat fallback so the chat drawer keeps working when
    no provider has been configured yet.
    """
    if not message.strip():
        return IntentClassification(kind="chat", confidence=1.0, reason="empty message")

    # Regex fast-path: skip the LLM call for unambiguous short
    # imperatives following a build-shaped assistant turn. The LLM
    # classifier was empirically unreliable on this case (small
    # Haiku-class models would return ``ambiguous`` or low-confidence
    # ``build`` below the 0.6 threshold). The fast-path is conservative:
    # it only fires when BOTH the imperative AND the build-shape match,
    # so it can only add hits, never worsen the LLM's behaviour.
    fast = _regex_fast_path_classify(message, history)
    if fast is not None:
        return fast

    if provider is None:
        return IntentClassification(
            kind="chat",
            confidence=0.0,
            reason="no provider available for classifier; defaulting to chat",
        )

    sched = scheduler or default_scheduler()
    bounded_history = _bound_history(
        history,
        turns=history_turns,
        chars_per_turn=history_chars_per_turn,
    )
    llm_messages: list[Message] = [
        Message(role="system", content=_LLM_SYSTEM_PROMPT),
        *bounded_history,
        Message(role="user", content=message),
    ]

    async def _do_call() -> str | None:
        async with sched.acquire(provider.name, surface=SURFACE):
            response = await provider.chat(
                messages=llm_messages,
                tools=None,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
        return response.content

    try:
        content = await asyncio.wait_for(_do_call(), timeout=timeout)
    except asyncio.TimeoutError:
        logger.info("intent classifier timed out after %.1fs", timeout)
        return IntentClassification(
            kind="chat",
            confidence=0.0,
            reason=f"classifier timed out after {timeout:.0f}s",
        )
    except Exception as exc:  # noqa: BLE001 — collapse all transients to a single chat fallback
        logger.warning("intent classifier call failed: %s", exc, exc_info=False)
        return IntentClassification(
            kind="chat",
            confidence=0.0,
            reason="classifier call failed",
        )

    parsed = _parse_llm_payload(content)
    if parsed is None:
        logger.info("intent classifier returned malformed JSON; defaulting to chat")
        return IntentClassification(
            kind="chat",
            confidence=0.0,
            reason="classifier returned malformed JSON",
        )
    return parsed


def verdict_for(classification: IntentClassification) -> RouteVerdict:
    """Map a classification to the route verdict the frontend dispatches on.

    Only ``kind="build"`` with ``confidence >= PROMOTION_CONFIDENCE_THRESHOLD``
    promotes to agent. Everything else (``chat``, ``ambiguous``, or
    low-confidence ``build``) stays as chat — the safe default.
    """
    if classification.kind == "build" and classification.confidence >= PROMOTION_CONFIDENCE_THRESHOLD:
        return "agent"
    return "chat"


def message_preview(message: str, *, max_chars: int = 200) -> str:
    """Truncate ``message`` for audit-log storage.

    The audit row keeps a *preview* of the user's message — never the
    whole thing — so chat content never lands in the audit table at full
    fidelity. Mirrors the audit module's :data:`MAX_ARGS_BYTES`
    posture for the same reason.
    """
    norm = message.strip()
    if len(norm) <= max_chars:
        return norm
    return norm[: max_chars - 3].rstrip() + "..."


def now_ms() -> int:
    """Monotonic millisecond timestamp helper for ``latency_ms`` capture.

    Exposed as a module-level helper so tests can patch it without poking
    at ``time.monotonic_ns`` globally.
    """
    return int(time.monotonic() * 1000)


__all__ = [
    "ClassifiedKind",
    "DEFAULT_HISTORY_CHARS_PER_TURN",
    "DEFAULT_HISTORY_TURNS",
    "DEFAULT_MAX_TOKENS",
    "DEFAULT_TIMEOUT_SECONDS",
    "IntentClassification",
    "PROMOTION_CONFIDENCE_THRESHOLD",
    "RouteVerdict",
    "SURFACE",
    "classify_intent",
    "message_preview",
    "now_ms",
    "verdict_for",
]
