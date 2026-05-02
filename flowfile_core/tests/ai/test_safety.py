"""W25 — PII scrubber + safety layer tests.

Cases:

* ``test_default_sample_mode_is_off`` — D009 default-off invariant.
* ``test_flow_safety_config_defaults`` — :class:`FlowSafetyConfig` defaults
  match D009 (mode off, row count 5, no consent stamps).
* ``test_flow_safety_config_rejects_negative_row_count`` — Pydantic rejects
  ``sample_row_count=-1`` and ``sample_row_count=101``.
* ``test_flow_safety_config_rejects_unknown_mode`` — Literal narrowing.
* ``test_flow_safety_config_forbids_extra_fields`` — typo guard.
* ``test_prepare_samples_off_returns_empty`` — no rows leave the box in the
  default mode.
* ``test_prepare_samples_off_does_not_consume_iterator`` — short-circuit
  before materialising rows.
* ``test_prepare_samples_regex_truncates_to_row_count`` — count limit.
* ``test_prepare_samples_regex_masks_email`` — email pass.
* ``test_prepare_samples_regex_masks_phone`` — phone pass (NANP-shape with
  separators or ``+CC`` prefix).
* ``test_prepare_samples_regex_does_not_mask_bare_digits`` — 10-digit run
  without separators passes through (order IDs etc.).
* ``test_prepare_samples_regex_masks_luhn_valid_card`` — card pass with Luhn.
* ``test_prepare_samples_regex_skips_non_luhn_card_shape`` — 16-digit non-Luhn
  string left alone.
* ``test_prepare_samples_regex_recurses_into_nested`` — dicts within dicts /
  lists scrubbed too.
* ``test_prepare_samples_regex_preserves_non_string_scalars`` — ints, bools,
  None, datetimes pass through untouched.
* ``test_prepare_samples_presidio_raises_when_missing`` — clean install-hint
  error when the optional dep isn't there.
* ``test_prepare_samples_presidio_uses_analyzer_when_stubbed`` — wires through
  to a fake analyzer/anonymizer pair without importing real Presidio.
* ``test_lazy_presidio_contract`` — ``import flowfile_core.ai.safety`` does
  not import ``presidio_analyzer`` at module-load time.
* ``test_redact_secrets_replaces_secretstr`` — type-driven detection.
* ``test_redact_secrets_replaces_field_name_match`` — substring heuristic.
* ``test_redact_secrets_handles_nested_secret_field`` — recursion.
* ``test_redact_secrets_replaces_secret_ref_with_named_placeholder`` —
  ``*_ref`` → ``<<secret:{name}>>``.
* ``test_redact_secrets_preserves_normal_field`` — false-positive guard.
* ``test_redact_secrets_walks_lists_and_tuples`` — collection types preserved.
* ``test_redact_secrets_extra_field_names`` — caller-supplied tokens picked up.
* ``test_redact_secrets_does_not_match_unrelated_id_field`` —
  ``aws_access_key_id`` (plaintext per the codebase) is left alone; only
  ``_ref`` and substring tokens trigger.
* ``test_redact_secrets_handles_top_level_list`` — list payload, not dict.
* ``test_redact_secrets_idempotent`` — already-redacted payload stays stable.
* ``test_validate_column_references_all_present`` — empty list when all OK.
* ``test_validate_column_references_returns_missing`` — listed in input order.
* ``test_validate_column_references_dedupes_missing`` — same missing ref
  twice yields one entry.
* ``test_validate_column_references_empty_input`` — vacuously OK.
* ``test_detect_network_egress_requests`` — ``requests.post(...)``.
* ``test_detect_network_egress_socket`` — ``socket.connect(...)``.
* ``test_detect_network_egress_urllib`` — ``urllib.request.urlopen(...)``.
* ``test_detect_network_egress_aiohttp_httpx`` — async HTTP libs.
* ``test_detect_network_egress_smtplib_ftplib`` — non-HTTP egress.
* ``test_detect_network_egress_returns_empty_for_local_code`` — no false
  positives on ``polars.read_csv('local.csv')``.
* ``test_detect_network_egress_returns_empty_for_empty_string`` — guard.
* ``test_detect_network_egress_dedupes_labels`` — multiple ``requests.*``
  calls yield one ``"requests"`` label.
* ``test_w15_audit_reexports_intact`` — :mod:`flowfile_core.ai.safety` still
  re-exports the W15 audit surface.
"""

from __future__ import annotations

import sys
from datetime import datetime
from typing import Any

import pytest
from pydantic import BaseModel, SecretStr, ValidationError

from flowfile_core.ai import safety
from flowfile_core.ai.safety import (
    CARD_PLACEHOLDER,
    DEFAULT_SAMPLE_MODE,
    DEFAULT_SAMPLE_ROW_COUNT,
    EMAIL_PLACEHOLDER,
    MAX_ARGS_BYTES,
    PHONE_PLACEHOLDER,
    SECRET_PLACEHOLDER_TEMPLATE,
    SECRET_REDACTED,
    AuditEvent,
    DiffAction,
    FlowSafetyConfig,
    PresidioNotAvailableError,
    ResultStatus,
    detect_network_egress,
    prepare_samples,
    query_events,
    record_event,
    redact_secrets,
    scrub_text_regex,
    scrub_value_regex,
    update_diff_action,
    validate_column_references,
)

# ---------------------------------------------------------------------------
# D009 defaults
# ---------------------------------------------------------------------------


def test_default_sample_mode_is_off() -> None:
    assert DEFAULT_SAMPLE_MODE == "off"


def test_flow_safety_config_defaults() -> None:
    cfg = FlowSafetyConfig()
    assert cfg.sample_mode == "off"
    assert cfg.sample_row_count == DEFAULT_SAMPLE_ROW_COUNT == 5
    assert cfg.consented_at is None
    assert cfg.consented_by_user_id is None
    assert cfg.provider_acknowledged is None
    assert cfg.flow_id is None


def test_flow_safety_config_rejects_negative_row_count() -> None:
    with pytest.raises(ValidationError):
        FlowSafetyConfig(sample_row_count=-1)
    with pytest.raises(ValidationError):
        FlowSafetyConfig(sample_row_count=101)


def test_flow_safety_config_rejects_unknown_mode() -> None:
    with pytest.raises(ValidationError):
        FlowSafetyConfig(sample_mode="not-a-mode")  # type: ignore[arg-type]


def test_flow_safety_config_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        FlowSafetyConfig(extra_attr="oops")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# prepare_samples — D009 / §9.1 modes
# ---------------------------------------------------------------------------


def test_prepare_samples_off_returns_empty() -> None:
    rows = [{"name": "Alice", "email": "alice@example.com"}]
    assert prepare_samples(rows, FlowSafetyConfig()) == []


def test_prepare_samples_off_does_not_consume_iterator() -> None:
    consumed = 0

    def gen() -> Any:
        nonlocal consumed
        for i in range(10):
            consumed += 1
            yield {"i": i}

    out = prepare_samples(gen(), FlowSafetyConfig())
    assert out == []
    assert consumed == 0  # short-circuited before materialising


def test_prepare_samples_regex_truncates_to_row_count() -> None:
    rows = [{"i": i} for i in range(20)]
    cfg = FlowSafetyConfig(sample_mode="regex", sample_row_count=3)
    assert prepare_samples(rows, cfg) == [{"i": 0}, {"i": 1}, {"i": 2}]


def test_prepare_samples_regex_masks_email() -> None:
    rows = [{"contact": "Reach me at alice@example.com please"}]
    cfg = FlowSafetyConfig(sample_mode="regex")
    out = prepare_samples(rows, cfg)
    assert out == [{"contact": f"Reach me at {EMAIL_PLACEHOLDER} please"}]


def test_prepare_samples_regex_masks_phone() -> None:
    cases = [
        ("Call (555) 123-4567 today", f"Call {PHONE_PLACEHOLDER} today"),
        ("ph: 555-123-4567", f"ph: {PHONE_PLACEHOLDER}"),
        ("ph: 555.123.4567", f"ph: {PHONE_PLACEHOLDER}"),
        ("intl +1 555-123-4567 ok", f"intl {PHONE_PLACEHOLDER} ok"),
    ]
    cfg = FlowSafetyConfig(sample_mode="regex")
    for raw, expected in cases:
        assert prepare_samples([{"v": raw}], cfg) == [{"v": expected}]


def test_prepare_samples_regex_does_not_mask_bare_digits() -> None:
    """10 consecutive digits without separators are NOT masked (order IDs etc.)."""
    rows = [{"order_id": "5551234567"}]
    cfg = FlowSafetyConfig(sample_mode="regex")
    assert prepare_samples(rows, cfg) == [{"order_id": "5551234567"}]


def test_prepare_samples_regex_masks_luhn_valid_card() -> None:
    # 4111 1111 1111 1111 is a well-known Luhn-valid Visa test card.
    rows = [{"payment": "card 4111-1111-1111-1111 charged"}]
    cfg = FlowSafetyConfig(sample_mode="regex")
    out = prepare_samples(rows, cfg)
    assert out == [{"payment": f"card {CARD_PLACEHOLDER} charged"}]


def test_prepare_samples_regex_skips_non_luhn_card_shape() -> None:
    # 16 digits that don't pass Luhn: leave alone.
    rows = [{"id": "1234-5678-9012-3456"}]
    cfg = FlowSafetyConfig(sample_mode="regex")
    out = prepare_samples(rows, cfg)
    assert out == [{"id": "1234-5678-9012-3456"}]


def test_prepare_samples_regex_recurses_into_nested() -> None:
    rows = [
        {
            "user": {"email": "x@y.com", "phones": ["555-111-2222", "555-333-4444"]},
            "log": ["sent to a@b.co"],
        }
    ]
    cfg = FlowSafetyConfig(sample_mode="regex")
    out = prepare_samples(rows, cfg)
    assert out == [
        {
            "user": {
                "email": EMAIL_PLACEHOLDER,
                "phones": [PHONE_PLACEHOLDER, PHONE_PLACEHOLDER],
            },
            "log": [f"sent to {EMAIL_PLACEHOLDER}"],
        }
    ]


def test_prepare_samples_regex_preserves_non_string_scalars() -> None:
    rows = [{"qty": 3, "active": True, "missing": None, "ts": datetime(2026, 5, 2)}]
    cfg = FlowSafetyConfig(sample_mode="regex")
    out = prepare_samples(rows, cfg)
    assert out == [{"qty": 3, "active": True, "missing": None, "ts": datetime(2026, 5, 2)}]


# ---------------------------------------------------------------------------
# Presidio path
# ---------------------------------------------------------------------------


def test_prepare_samples_presidio_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """If presidio_analyzer can't import, raise a clean install-hint error."""
    # Make sure presidio_analyzer is not in sys.modules and ImportError fires.
    monkeypatch.setitem(sys.modules, "presidio_analyzer", None)

    cfg = FlowSafetyConfig(sample_mode="presidio")
    with pytest.raises(PresidioNotAvailableError) as exc:
        prepare_samples([{"v": "x"}], cfg)
    assert "Presidio is not installed" in str(exc.value)


def test_prepare_samples_presidio_uses_analyzer_when_stubbed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inject fake presidio_analyzer + presidio_anonymizer via sys.modules."""
    import types

    # Build a fake analyzer module + class.
    fake_result = types.SimpleNamespace(start=0, end=4, entity_type="PERSON")

    class FakeAnalyzer:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def analyze(self, *, text: str, language: str) -> list[Any]:
            assert language == "en"
            return [fake_result] if "Alice" in text else []

    class FakeAnonymizerResult:
        def __init__(self, text: str) -> None:
            self.text = text

    class FakeAnonymizer:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def anonymize(self, *, text: str, analyzer_results: list[Any]) -> FakeAnonymizerResult:
            return FakeAnonymizerResult(text.replace("Alice", "<PERSON>"))

    fake_analyzer_mod = types.ModuleType("presidio_analyzer")
    fake_analyzer_mod.AnalyzerEngine = FakeAnalyzer  # type: ignore[attr-defined]
    fake_anonymizer_mod = types.ModuleType("presidio_anonymizer")
    fake_anonymizer_mod.AnonymizerEngine = FakeAnonymizer  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "presidio_analyzer", fake_analyzer_mod)
    monkeypatch.setitem(sys.modules, "presidio_anonymizer", fake_anonymizer_mod)

    rows = [{"name": "Alice", "city": "Amsterdam"}]
    cfg = FlowSafetyConfig(sample_mode="presidio")
    out = prepare_samples(rows, cfg)
    assert out == [{"name": "<PERSON>", "city": "Amsterdam"}]


def test_lazy_presidio_contract() -> None:
    """``import flowfile_core.ai.safety`` must not pull in presidio_analyzer.

    Presidio is an opt-in dependency; pulling it on every import would make
    the AI subsystem fail-loud in environments that don't need it.
    """
    sys.modules.pop("presidio_analyzer", None)
    sys.modules.pop("presidio_anonymizer", None)
    sys.modules.pop("flowfile_core.ai.safety", None)

    import flowfile_core.ai.safety as _safety  # noqa: F401

    assert "presidio_analyzer" not in sys.modules
    assert "presidio_anonymizer" not in sys.modules


# ---------------------------------------------------------------------------
# Secret redaction
# ---------------------------------------------------------------------------


def test_redact_secrets_replaces_secretstr() -> None:
    out = redact_secrets({"creds": SecretStr("super-secret-value")})
    assert out == {"creds": SECRET_REDACTED}


def test_redact_secrets_replaces_field_name_match() -> None:
    out = redact_secrets({"password": "abc", "api_key": "xyz", "client_secret": "qqq"})
    assert out == {
        "password": SECRET_REDACTED,
        "api_key": SECRET_REDACTED,
        "client_secret": SECRET_REDACTED,
    }


def test_redact_secrets_handles_nested_secret_field() -> None:
    payload = {"db": {"host": "localhost", "password": "p"}}
    out = redact_secrets(payload)
    assert out == {"db": {"host": "localhost", "password": SECRET_REDACTED}}


def test_redact_secrets_replaces_secret_ref_with_named_placeholder() -> None:
    out = redact_secrets({"password_ref": "my-stored-key", "api_key_ref": "openai-prod"})
    assert out == {
        "password_ref": SECRET_PLACEHOLDER_TEMPLATE.format(name="my-stored-key"),
        "api_key_ref": SECRET_PLACEHOLDER_TEMPLATE.format(name="openai-prod"),
    }


def test_redact_secrets_preserves_normal_field() -> None:
    payload = {"name": "Alice", "host": "db.example.com", "port": 5432, "active": True}
    assert redact_secrets(payload) == payload


def test_redact_secrets_walks_lists_and_tuples() -> None:
    payload = {
        "creds": [{"password": "a"}, {"password": "b"}],
        "tuple_field": ({"token": "t"},),
    }
    out = redact_secrets(payload)
    assert out == {
        "creds": [{"password": SECRET_REDACTED}, {"password": SECRET_REDACTED}],
        "tuple_field": ({"token": SECRET_REDACTED},),
    }
    assert isinstance(out["tuple_field"], tuple)


def test_redact_secrets_extra_field_names() -> None:
    out = redact_secrets({"my_custom_thing": "leak"}, extra_secret_field_names=("my_custom_thing",))
    assert out == {"my_custom_thing": SECRET_REDACTED}


def test_redact_secrets_does_not_match_unrelated_id_field() -> None:
    # `aws_access_key_id` is plaintext per the codebase (database/models.py).
    # Our heuristic tokens (api_key, client_secret, *_token, ...) don't include
    # `aws_access_key_id`; only `_ref` suffix or those substrings should fire.
    payload = {
        "aws_access_key_id": "AKIA...",
        "user_id": 1,
        "flow_id": 42,
        "node_id": "filter-1",
    }
    assert redact_secrets(payload) == payload


def test_redact_secrets_handles_top_level_list() -> None:
    out = redact_secrets([{"password": "a"}, {"password": "b"}])
    assert out == [{"password": SECRET_REDACTED}, {"password": SECRET_REDACTED}]


def test_redact_secrets_idempotent() -> None:
    payload = {"password": "abc", "password_ref": "name"}
    once = redact_secrets(payload)
    twice = redact_secrets(once)
    assert once == twice


def test_redact_secrets_with_pydantic_model_dump() -> None:
    """SecretStr survives model_dump() as a SecretStr instance — caught by the type pass."""

    class Cfg(BaseModel):
        password: SecretStr
        host: str

    cfg = Cfg(password="abc", host="db.example.com")
    dumped = cfg.model_dump()
    out = redact_secrets(dumped)
    # password becomes redacted; host preserved
    assert out["host"] == "db.example.com"
    assert out["password"] == SECRET_REDACTED


# ---------------------------------------------------------------------------
# §9.6 refusal helpers
# ---------------------------------------------------------------------------


def test_validate_column_references_all_present() -> None:
    assert validate_column_references(["a", "b"], ["a", "b", "c"]) == []


def test_validate_column_references_returns_missing() -> None:
    assert validate_column_references(["a", "x", "b", "y"], ["a", "b"]) == ["x", "y"]


def test_validate_column_references_dedupes_missing() -> None:
    assert validate_column_references(["x", "x", "y", "x"], ["a"]) == ["x", "y"]


def test_validate_column_references_empty_input() -> None:
    assert validate_column_references([], ["a", "b"]) == []
    assert validate_column_references(["a"], []) == ["a"]


def test_detect_network_egress_requests() -> None:
    assert detect_network_egress("import requests; requests.post('http://x')") == ["requests"]


def test_detect_network_egress_socket() -> None:
    code = "import socket\ns = socket.socket(); s.connect(('x', 80))"
    matched = detect_network_egress(code)
    assert "socket" in matched


def test_detect_network_egress_urllib() -> None:
    code = "import urllib.request\nurllib.request.urlopen('http://x')"
    assert detect_network_egress(code) == ["urllib"]


def test_detect_network_egress_aiohttp_httpx() -> None:
    code_a = "async with aiohttp.ClientSession() as s: ..."
    code_h = "httpx.get('http://x')"
    assert detect_network_egress(code_a) == ["aiohttp"]
    assert detect_network_egress(code_h) == ["httpx"]


def test_detect_network_egress_smtplib_ftplib() -> None:
    assert detect_network_egress("smtplib.SMTP('mail.example.com')") == ["smtplib"]
    assert detect_network_egress("ftplib.FTP('ftp.example.com')") == ["ftplib"]


def test_detect_network_egress_returns_empty_for_local_code() -> None:
    code = "import polars as pl\npl.read_csv('/local/path/data.csv')"
    assert detect_network_egress(code) == []


def test_detect_network_egress_returns_empty_for_empty_string() -> None:
    assert detect_network_egress("") == []


def test_detect_network_egress_dedupes_labels() -> None:
    code = "requests.get('http://a')\n" "requests.post('http://b')\n" "socket.connect(('h', 1))\n"
    matched = detect_network_egress(code)
    # requests appears twice but should be deduped, ordered by first occurrence
    assert matched == ["requests", "socket"]


# ---------------------------------------------------------------------------
# Public surface helpers
# ---------------------------------------------------------------------------


def test_scrub_text_regex_handles_clean_text() -> None:
    """A string with no PII passes through unchanged."""
    text = "Plain text without any contact info or numbers."
    assert scrub_text_regex(text) == text


def test_scrub_value_regex_handles_scalar() -> None:
    """Top-level scalars work — not just dict / list payloads."""
    assert scrub_value_regex("alice@example.com") == EMAIL_PLACEHOLDER
    assert scrub_value_regex(42) == 42
    assert scrub_value_regex(None) is None


# ---------------------------------------------------------------------------
# W15 audit re-export contract (additive constraint)
# ---------------------------------------------------------------------------


def test_w15_audit_reexports_intact() -> None:
    """W15 documented these names as importable from safety; W25 must keep them."""
    assert MAX_ARGS_BYTES > 0
    assert AuditEvent is safety.AuditEvent
    assert record_event is safety.record_event
    assert query_events is safety.query_events
    assert update_diff_action is safety.update_diff_action
    # Literals are imported to make sure the names resolve at import time.
    assert ResultStatus is not None
    assert DiffAction is not None
