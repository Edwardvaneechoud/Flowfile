"""PII scrubbing, sample-row policy, secret redaction, refusal helpers.

Owned by W25 (this file's body) and W15 (the audit re-exports at the bottom).

The single import surface for the AI subsystem's safety layer:

* **Sample-row pipeline (D009 / §9.1).** Three modes — ``off`` (default for every
  new flow), ``regex`` (emails / phones / Luhn-validated cards masked at zero
  added latency), ``presidio`` (ML-backed; lazy-imported, opt-in dependency).
  Callers materialise rows however they like; this module is the choke point
  before they leave the box.

* **Secret redaction (§9.1 close).** ``redact_secrets`` walks an arbitrary
  nested settings payload — works on raw dicts post-``model_dump`` and on live
  ``pydantic.SecretStr`` instances — replacing secret-bearing values with
  ``<<secret:redacted>>`` and ``SecretRef``-shape fields with
  ``<<secret:{name}>>`` placeholders so the LLM can refer to a credential by
  name without ever seeing its value.

* **Refusal helpers (§9.6).** ``validate_column_references`` and
  ``detect_network_egress`` give downstream tool-execution paths (W31) a single
  vocabulary for refusing tool calls referencing unknown columns or for
  flagging ``python_script`` / ``polars_code`` payloads that try to phone home.

* **Audit re-exports (W15).** The audit log surface stays importable here so
  callers have one place to import from for everything privacy / safety
  related; W15 documented this contract first and we honour it additively.

Design notes:

* The Presidio path is **strictly lazy** — importing this module must not
  import ``presidio_analyzer``. ``mode='presidio'`` triggers the import on
  first use and raises :class:`PresidioNotAvailableError` with an install hint
  if the dependency is absent. A test in ``tests/ai/test_safety.py`` enforces
  the lazy contract via ``sys.modules``.

* Field-name heuristics in ``redact_secrets`` are intentionally
  **conservative** — a known token (``password``, ``api_key``, ``client_secret``,
  ``private_key``, ``access_token``, ``refresh_token``, ``session_token``,
  ``id_token``, ``auth_token``) anywhere in the field name triggers redaction.
  False positives matter less than false negatives here: the LLM seeing a
  redacted value is recoverable; a leaked credential is not. ``SecretStr``
  detection by Pydantic type is the primary defence; the field-name pass is
  the belt-and-braces.

* Phone regex covers NANP-shape with explicit separators or a ``+CC`` country
  code prefix; bare 10-digit runs are intentionally **not** matched to avoid
  redacting order IDs, timestamps, and similar non-phone digit strings. Cards
  pass a Luhn check before being masked. International phones outside the
  NANP shape leak through the regex tier; users who care about those opt into
  ``mode='presidio'``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from flowfile_core.ai.audit import (
    MAX_ARGS_BYTES,
    AuditEvent,
    DiffAction,
    ResultStatus,
    query_events,
    record_event,
    update_diff_action,
)

# ---------------------------------------------------------------------------
# Sample-row policy (D009)
# ---------------------------------------------------------------------------

SampleMode = Literal["off", "regex", "presidio"]

#: Default sample mode for every new flow.
#:
#: DEV DEFAULT: ``"regex"`` (samples on with regex PII scrubbing) so the
#: agent + chat surfaces have actual data to ground on while we test.
#: Plan §9.1 / D009 specs the production ship-default as ``"off"`` (schema
#: only, no row data leaves the box). Flip back before going live, or set
#: per-flow via :class:`FlowSafetyConfig.sample_mode`.
DEFAULT_SAMPLE_MODE: SampleMode = "regex"

#: Default sample-row count when the user opts into ``regex`` or ``presidio``.
#: Configurable per-flow on :class:`FlowSafetyConfig`.
DEFAULT_SAMPLE_ROW_COUNT: int = 5

#: Inclusive upper bound for ``sample_row_count``. Keeps a misconfigured flow
#: from streaming a megabyte of rows to a provider on every call.
MAX_SAMPLE_ROW_COUNT: int = 100


class FlowSafetyConfig(BaseModel):
    """Per-flow privacy / safety knobs (§9.5 project-level consent shape).

    Persistence is owned by W22 (sidecar next to ``ai_sessions/{flow_id}/``)
    and the consent-dialog UI is Phase 1 frontend work; W25 only defines the
    wire shape and the transforms that consume it. ``consented_at`` /
    ``consented_by_user_id`` / ``provider_acknowledged`` capture the §9.5
    "one-time consent" decision so callers can prove the consent dialog was
    shown for a given flow before sample rows ever leave.
    """

    model_config = ConfigDict(frozen=False, extra="forbid")

    flow_id: int | None = None
    sample_mode: SampleMode = DEFAULT_SAMPLE_MODE
    sample_row_count: int = Field(
        default=DEFAULT_SAMPLE_ROW_COUNT,
        ge=0,
        le=MAX_SAMPLE_ROW_COUNT,
    )
    consented_at: datetime | None = None
    consented_by_user_id: int | None = None
    provider_acknowledged: str | None = None


# ---------------------------------------------------------------------------
# Regex scrubber
# ---------------------------------------------------------------------------

EMAIL_PLACEHOLDER = "<<email>>"
PHONE_PLACEHOLDER = "<<phone>>"
CARD_PLACEHOLDER = "<<card>>"

_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")

#: NANP-shape with explicit separator or ``+CC`` country code. Bare digit runs
#: are not matched on purpose — see module docstring. Uses lookbehind /
#: lookahead instead of ``\b`` because ``\b`` would fail at the boundary
#: between a space and a ``(`` (both non-word characters).
_PHONE_RE = re.compile(r"(?<!\d)(?:\+\d{1,3}[\s.\-]?)?(?:\(\d{3}\)\s*|\d{3}[\s.\-])\d{3}[\s.\-]\d{4}(?!\d)")

#: Card-shape: 13–19 digits, optional space or hyphen separators between
#: groups. Matches are Luhn-validated before being masked.
_CARD_RAW_RE = re.compile(r"\b(?:\d[ \-]?){12,18}\d\b")


def _luhn_valid(digits: str) -> bool:
    total = 0
    for i, d in enumerate(reversed(digits)):
        n = ord(d) - 48
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def _mask_cards(text: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        raw = match.group(0)
        digits = "".join(c for c in raw if c.isdigit())
        if 13 <= len(digits) <= 19 and _luhn_valid(digits):
            return CARD_PLACEHOLDER
        return raw

    return _CARD_RAW_RE.sub(_replace, text)


def scrub_text_regex(text: str) -> str:
    """Mask emails, phones, and Luhn-valid cards in a single string.

    Order matters mildly: emails first (they contain ``.`` and digits that
    other regexes might otherwise touch), then phones, then cards.
    """
    text = _EMAIL_RE.sub(EMAIL_PLACEHOLDER, text)
    text = _PHONE_RE.sub(PHONE_PLACEHOLDER, text)
    text = _mask_cards(text)
    return text


def scrub_value_regex(value: Any) -> Any:
    """Recursively apply :func:`scrub_text_regex` to strings inside a payload.

    Walks dicts, lists, and tuples; preserves non-string scalars (ints, bools,
    None, dates, etc.) unchanged.
    """
    if isinstance(value, str):
        return scrub_text_regex(value)
    if isinstance(value, Mapping):
        return {k: scrub_value_regex(v) for k, v in value.items()}
    if isinstance(value, list):
        return [scrub_value_regex(v) for v in value]
    if isinstance(value, tuple):
        return tuple(scrub_value_regex(v) for v in value)
    return value


# ---------------------------------------------------------------------------
# Presidio adapter (lazy)
# ---------------------------------------------------------------------------

_PRESIDIO_INSTALL_HINT = (
    "Presidio is not installed. Add `presidio-analyzer` and " "`presidio-anonymizer` to enable sample_mode='presidio'."
)


class PresidioNotAvailableError(RuntimeError):
    """Raised when ``mode='presidio'`` is requested but Presidio is missing.

    Carries a stable install-hint message so the route layer can surface it
    verbatim to the caller (frontend will match on the literal string).
    """


def _load_presidio_analyzer() -> Any:
    try:
        from presidio_analyzer import AnalyzerEngine
    except ImportError as exc:
        raise PresidioNotAvailableError(_PRESIDIO_INSTALL_HINT) from exc
    return AnalyzerEngine()


def _load_presidio_anonymizer() -> Any:
    try:
        from presidio_anonymizer import AnonymizerEngine
    except ImportError as exc:
        raise PresidioNotAvailableError(_PRESIDIO_INSTALL_HINT) from exc
    return AnonymizerEngine()


def _scrub_text_presidio(text: str, *, analyzer: Any, anonymizer: Any) -> str:
    results = analyzer.analyze(text=text, language="en")
    if not results:
        return text
    return anonymizer.anonymize(text=text, analyzer_results=results).text


def _scrub_value_presidio(value: Any, *, analyzer: Any, anonymizer: Any) -> Any:
    if isinstance(value, str):
        return _scrub_text_presidio(value, analyzer=analyzer, anonymizer=anonymizer)
    if isinstance(value, Mapping):
        return {k: _scrub_value_presidio(v, analyzer=analyzer, anonymizer=anonymizer) for k, v in value.items()}
    if isinstance(value, list):
        return [_scrub_value_presidio(v, analyzer=analyzer, anonymizer=anonymizer) for v in value]
    if isinstance(value, tuple):
        return tuple(_scrub_value_presidio(v, analyzer=analyzer, anonymizer=anonymizer) for v in value)
    return value


# ---------------------------------------------------------------------------
# Sample orchestrator
# ---------------------------------------------------------------------------


def prepare_samples(
    rows: Iterable[Mapping[str, Any]],
    config: FlowSafetyConfig,
) -> list[dict[str, Any]]:
    """Apply the per-flow sample policy: row-count limit + selected scrubber.

    Returns ``[]`` whenever ``config.sample_mode == 'off'`` regardless of input
    — the D009 default. Callers (W22 context builder) can short-circuit before
    materialising rows when samples won't ship anyway.

    Raises :class:`PresidioNotAvailableError` when ``mode='presidio'`` and the
    optional dependency isn't installed; never imports Presidio in the
    ``off`` / ``regex`` paths.
    """
    if config.sample_mode == "off":
        return []

    limited: list[dict[str, Any]] = []
    for row in rows:
        if len(limited) >= config.sample_row_count:
            break
        limited.append(dict(row))

    if config.sample_mode == "regex":
        return [scrub_value_regex(row) for row in limited]

    if config.sample_mode == "presidio":
        analyzer = _load_presidio_analyzer()
        anonymizer = _load_presidio_anonymizer()
        return [_scrub_value_presidio(row, analyzer=analyzer, anonymizer=anonymizer) for row in limited]

    return limited  # pragma: no cover — Pydantic constrains the Literal


# ---------------------------------------------------------------------------
# Secret redaction
# ---------------------------------------------------------------------------

SECRET_PLACEHOLDER_TEMPLATE = "<<secret:{name}>>"
SECRET_REDACTED = "<<secret:redacted>>"

#: Substring tokens whose presence in a field name flags the value as a
#: secret. Compound tokens only — bare ``key`` / ``token`` would over-trigger
#: on common settings fields like ``aws_access_key_id`` or ``tokenizer``.
_SECRET_VALUE_SUBSTRINGS: tuple[str, ...] = (
    "password",
    "passwd",
    "api_key",
    "apikey",
    "secret_key",
    "client_secret",
    "private_key",
    "access_token",
    "refresh_token",
    "session_token",
    "id_token",
    "auth_token",
)

#: Field names that match exactly (after lowercase + dash→underscore
#: normalisation). Reserved for short, unambiguous secret-bearing fields.
SECRET_FIELD_NAMES: frozenset[str] = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "session_token",
        "id_token",
        "auth_token",
        "private_key",
        "client_secret",
        "encryption_key",
        "encryption_master_key",
        "sas_token",
    }
)


def _normalise_field_name(name: str) -> str:
    return name.lower().replace("-", "_")


def _is_secret_field(name: str, *, extra_tokens: frozenset[str]) -> bool:
    norm = _normalise_field_name(name)
    if norm in SECRET_FIELD_NAMES or norm in extra_tokens:
        return True
    return any(token in norm for token in _SECRET_VALUE_SUBSTRINGS)


def _is_secret_ref_field(name: str) -> bool:
    """``SecretRef`` shape — ``input_schema.py`` convention is the ``_ref`` suffix.

    Fields ending in ``_ref`` carry a string *name* referencing a stored
    secret; we emit ``<<secret:{name}>>`` so the LLM can refer to the credential
    without ever seeing its value.
    """
    return _normalise_field_name(name).endswith("_ref")


def redact_secrets(
    payload: Any,
    *,
    extra_secret_field_names: Iterable[str] | None = None,
) -> Any:
    """Walk ``payload`` and replace secret-bearing values with placeholders.

    Detection is twofold:

    1. **Type-driven.** Any :class:`pydantic.SecretStr` instance is replaced
       with :data:`SECRET_REDACTED` regardless of its field name. This
       protects against new settings classes adding secret fields without
       remembering to update the field-name list.
    2. **Field-name-driven.** Strings under a field whose name matches the
       conservative substring list (``password``, ``api_key``,
       ``client_secret``, ``private_key``, ``*_token``, …) are replaced with
       :data:`SECRET_REDACTED`. Strings under a ``*_ref``-suffix field are
       replaced with ``<<secret:{value}>>`` so the LLM can refer by name.

    ``extra_secret_field_names`` lets callers expand the heuristic for
    project-specific field names without monkey-patching module state. Names
    are matched after the same normalisation (lowercase + dash→underscore).

    The function is structure-preserving: dicts become dicts, lists stay
    lists, tuples stay tuples; non-string scalars (int, bool, None, datetime)
    are left untouched.
    """
    extra = (
        frozenset(_normalise_field_name(n) for n in extra_secret_field_names)
        if extra_secret_field_names
        else frozenset()
    )
    return _redact(payload, key_hint=None, extra=extra)


def _redact(value: Any, *, key_hint: str | None, extra: frozenset[str]) -> Any:
    if isinstance(value, SecretStr):
        return SECRET_REDACTED

    if isinstance(value, Mapping):
        return {k: _redact(v, key_hint=k, extra=extra) for k, v in value.items()}

    if isinstance(value, list):
        return [_redact(v, key_hint=key_hint, extra=extra) for v in value]

    if isinstance(value, tuple):
        return tuple(_redact(v, key_hint=key_hint, extra=extra) for v in value)

    if key_hint is not None and isinstance(value, str):
        # Idempotency: a value already wrapped in a placeholder must not be
        # re-wrapped. Without this, applying ``redact_secrets`` twice would
        # produce ``<<secret:<<secret:foo>>>>`` for ``*_ref`` fields.
        if value.startswith("<<secret:"):
            return value
        if _is_secret_ref_field(key_hint):
            return SECRET_PLACEHOLDER_TEMPLATE.format(name=value)
        if _is_secret_field(key_hint, extra_tokens=extra):
            return SECRET_REDACTED

    return value


# ---------------------------------------------------------------------------
# §9.6 refusal helpers
# ---------------------------------------------------------------------------

RefusalReason = Literal["unknown_columns", "network_egress", "missing_diff"]


def validate_column_references(
    refs: Iterable[str],
    available_columns: Iterable[str],
) -> list[str]:
    """Return refs not present in ``available_columns``, deduped, order-preserving.

    Backs §9.6's "Refuse tool calls referencing unknown columns" gate. The
    decision to *refuse* vs. *warn* is W31's; this function only reports.
    """
    available = set(available_columns)
    missing: list[str] = []
    seen: set[str] = set()
    for ref in refs:
        if ref in available or ref in seen:
            continue
        missing.append(ref)
        seen.add(ref)
    return missing


#: Patterns that suggest network egress in user / AI-authored Python or SQL
#: code. Each entry is ``(label, compiled_pattern)``; ``detect_network_egress``
#: returns the labels that matched, deduped and order-preserving, so callers
#: can include them in error messages ("blocked: requests, socket").
_EGRESS_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "requests",
        re.compile(r"\brequests\.(?:get|post|put|patch|delete|head|options|request|Session)\b"),
    ),
    (
        "httpx",
        re.compile(r"\bhttpx\.(?:get|post|put|patch|delete|head|options|request|Client|AsyncClient|stream)\b"),
    ),
    (
        "aiohttp",
        re.compile(r"\baiohttp\.(?:ClientSession|request|TCPConnector)\b"),
    ),
    (
        "urllib",
        re.compile(r"\burllib\.request\.(?:urlopen|Request|build_opener)\b"),
    ),
    (
        "urllib3",
        re.compile(r"\burllib3\.(?:PoolManager|HTTPConnectionPool|HTTPSConnectionPool|request|connection_from_url)\b"),
    ),
    (
        "socket",
        re.compile(r"\bsocket\.(?:socket|connect|create_connection|getaddrinfo)\b"),
    ),
    ("smtplib", re.compile(r"\bsmtplib\.(?:SMTP|SMTP_SSL|LMTP)\b")),
    ("ftplib", re.compile(r"\bftplib\.(?:FTP|FTP_TLS)\b")),
    ("paramiko", re.compile(r"\bparamiko\.(?:SSHClient|Transport|connect)\b")),
    ("imaplib", re.compile(r"\bimaplib\.(?:IMAP4|IMAP4_SSL)\b")),
)


def detect_network_egress(code: str) -> list[str]:
    """Return labels of network-egress patterns matched in ``code``.

    Detection is **syntactic, not behavioural** — a string literal like
    ``"requests.get"`` in an unrelated comment will match. That's the right
    trade-off for §9.6: false positives are recoverable (the user can opt in
    via "Network in AI code"); false negatives risk silent exfiltration.
    """
    if not code:
        return []
    matched: list[str] = []
    seen: set[str] = set()
    for label, pattern in _EGRESS_PATTERNS:
        if label in seen:
            continue
        if pattern.search(code):
            matched.append(label)
            seen.add(label)
    return matched


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------

__all__ = [
    # Sample-row pipeline (D009)
    "DEFAULT_SAMPLE_MODE",
    "DEFAULT_SAMPLE_ROW_COUNT",
    "MAX_SAMPLE_ROW_COUNT",
    "FlowSafetyConfig",
    "PresidioNotAvailableError",
    "SampleMode",
    "prepare_samples",
    "scrub_text_regex",
    "scrub_value_regex",
    # Regex placeholders
    "EMAIL_PLACEHOLDER",
    "PHONE_PLACEHOLDER",
    "CARD_PLACEHOLDER",
    # Secret redaction
    "SECRET_FIELD_NAMES",
    "SECRET_PLACEHOLDER_TEMPLATE",
    "SECRET_REDACTED",
    "redact_secrets",
    # §9.6 refusal helpers
    "RefusalReason",
    "detect_network_egress",
    "validate_column_references",
    # W15 audit re-exports — kept stable since W15 (do not remove without a contract bump)
    "MAX_ARGS_BYTES",
    "AuditEvent",
    "DiffAction",
    "ResultStatus",
    "query_events",
    "record_event",
    "update_diff_action",
]
