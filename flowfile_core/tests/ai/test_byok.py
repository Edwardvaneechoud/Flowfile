"""W12 — BYOK key storage tests.

Cases:

* ``test_credentials_table_exists_after_alembic`` — Alembic migration 012
  lands the ``ai_provider_credentials`` table on a fresh DB; W29 widens the
  expected column set with ``models``.
* ``test_alembic_head_is_singular`` — ``alembic heads`` resolves to exactly
  one revision (guards against a branched migration history).
* ``test_upsert_creates_new_credential_with_secret`` — happy path round trip;
  ``Secret.encrypted_value`` decrypts back to the input api_key.
* ``test_upsert_rotates_existing_secret_in_place`` — second upsert with a
  new key reuses the original ``Secret.id`` (no orphan rows).
* ``test_upsert_keeps_key_when_api_key_is_none`` — partial update preserves
  the existing secret.
* ``test_upsert_clears_key_when_clear_api_key_true`` — the FK is nulled and
  the ``Secret`` row is gone.
* ``test_upsert_rejects_api_key_and_clear_together`` — Pydantic validator
  raises 422.
* ``test_delete_removes_credential_and_secret_atomically`` — both rows gone.
* ``test_get_configured_provider_uses_stored_key`` — ``provider_factory`` is
  called with the decrypted api_key and stored api_base.
* ``test_get_configured_provider_falls_back_to_env`` — no row + env var set
  → factory called with ``api_key=None`` (litellm picks up env).
* ``test_get_configured_provider_resolution_order`` — explicit model wins
  over stored default; stored default wins over surface map.
* ``test_get_configured_provider_ollama_no_key`` — Ollama works with only
  ``api_base`` and no key.
* ``test_get_configured_provider_raises_when_unconfigured`` — neither row
  nor env var nor Ollama → ``ProviderNotConfiguredError``.
* ``test_routes_list_providers_enriches_status`` — three states across
  providers: ``configured``, ``env_fallback``, ``unconfigured``.
* ``test_routes_post_provider`` — happy path; response is
  ``ProviderCredentialPublic`` with ``has_key=True``.
* ``test_routes_delete_provider_404_when_missing`` — and 204 when present.
* ``test_routes_test_provider_records_status`` — ``last_test_status="ok"``
  persisted; mocked provider's ``chat`` is awaited.
* ``test_routes_test_provider_records_error`` — ``last_test_status="error"``;
  exception message captured (truncated to 512 chars).
* ``test_lazy_litellm_import_for_credentials`` — ``import
  flowfile_core.ai.credentials`` does not pull ``litellm`` into ``sys.modules``.
* ``test_credentials_per_user_unique`` — a second insert for the same
  ``(user_id, provider)`` raises ``IntegrityError``.

W29 cases:

* ``test_upsert_stores_models_verbatim`` — ``models=[a, b, c]`` round-trips
  through the public projection in the same order.
* ``test_upsert_clears_models_via_flag_and_via_empty_list`` — both
  ``clear_models=True`` and ``models=[]`` collapse to ``models=None``.
* ``test_upsert_keeps_models_when_payload_models_is_none`` — partial update
  doesn't overwrite an existing curated list.
* ``test_upsert_rejects_models_and_clear_models_together`` — Pydantic
  validator raises (mirrors the api_key / clear_api_key rule).
* ``test_get_configured_provider_picks_first_model_when_no_surface_match`` —
  step 4 of W29's resolver precedence.
* ``test_get_configured_provider_prefers_surface_model_when_in_curated`` —
  step 3 wins over step 4 when the routed model is curated.
* ``test_get_configured_provider_models_loses_to_stored_default`` —
  ``default_model`` (step 2) still beats the curated list (steps 3+4).
* ``test_alembic_013_upgrade_downgrade_round_trip`` — fresh sqlite; upgrade
  to 013 adds the column, downgrade to 012 drops it; existing 012 rows
  survive the upgrade.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

from flowfile_core import main
from flowfile_core.ai import byok as byok_module
from flowfile_core.ai.byok import (
    ProviderNotConfiguredError,
    detect_env_fallback,
    get_configured_provider,
)
from flowfile_core.ai.credentials import (
    ProviderCredentialInput,
    decrypt_api_key,
    delete_provider_credential,
    get_provider_credential,
    list_provider_credentials,
    update_test_status,
    upsert_provider_credential,
)
from flowfile_core.ai.providers import Message
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.database.connection import engine, get_db_context
from flowfile_core.database.models import AiProviderCredential, Secret, User


# ---------- shared fixtures ----------


@pytest.fixture
def local_user_id() -> int:
    with get_db_context() as db:
        user = db.query(User).filter_by(username="local_user").first()
        assert user is not None, "Tests rely on conftest's setup_test_db local_user"
        return user.id


@pytest.fixture(autouse=True)
def _cleanup_credential_rows() -> Iterator[None]:
    """Each test starts with empty credential + their owned secret rows."""
    with get_db_context() as db:
        # Wipe any test-leftover ai_* secrets first to avoid orphan rows.
        creds = db.query(AiProviderCredential).all()
        secret_ids = [c.api_key_secret_id for c in creds if c.api_key_secret_id is not None]
        db.query(AiProviderCredential).delete()
        if secret_ids:
            db.query(Secret).filter(Secret.id.in_(secret_ids)).delete(synchronize_session=False)
        db.commit()
    yield
    with get_db_context() as db:
        creds = db.query(AiProviderCredential).all()
        secret_ids = [c.api_key_secret_id for c in creds if c.api_key_secret_id is not None]
        db.query(AiProviderCredential).delete()
        if secret_ids:
            db.query(Secret).filter(Secret.id.in_(secret_ids)).delete(synchronize_session=False)
        db.commit()


@pytest.fixture
def authed_client(local_user_id: int) -> Iterator[TestClient]:
    """TestClient with ``get_current_active_user`` overridden to local_user.

    Skips the ``/auth/token`` round-trip — that path requires
    ``FLOWFILE_MODE=electron`` to issue a token without form data, which the
    pytest harness doesn't set. Dependency override is symmetric: the
    teardown pops it so other tests aren't affected.
    """
    fake_user = PydanticUser(id=local_user_id, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def _input(api_key: str | None = "sk-test", **kwargs: Any) -> ProviderCredentialInput:
    """Build a ``ProviderCredentialInput`` with sensible defaults."""
    return ProviderCredentialInput(api_key=api_key, **kwargs)


# ---------- schema / Alembic ----------


def test_credentials_table_exists_after_alembic() -> None:
    inspector = inspect(engine)
    assert "ai_provider_credentials" in inspector.get_table_names()
    columns = {c["name"] for c in inspector.get_columns("ai_provider_credentials")}
    expected = {
        "id",
        "user_id",
        "provider",
        "api_key_secret_id",
        "api_base",
        "default_model",
        "models",  # W29
        "last_tested_at",
        "last_test_status",
        "last_test_error",
        "created_at",
        "updated_at",
    }
    assert expected.issubset(columns), f"missing: {expected - columns}"


def test_alembic_head_is_singular() -> None:
    """A branched alembic head silently breaks `alembic upgrade head`."""
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    cfg = Config("flowfile_core/flowfile_core/alembic.ini")
    cfg.set_main_option("script_location", "flowfile_core/flowfile_core/alembic")
    script = ScriptDirectory.from_config(cfg)
    heads = script.get_heads()
    assert len(heads) == 1, f"expected single head, got: {heads}"


# ---------- DB CRUD ----------


def test_upsert_creates_new_credential_with_secret(local_user_id: int) -> None:
    with get_db_context() as db:
        cred = upsert_provider_credential(
            db,
            local_user_id,
            "anthropic",
            _input(api_key="sk-ant-fresh", api_base="https://api.example/v1", default_model="claude-haiku-4-5"),
        )
        assert cred.id is not None
        assert cred.api_base == "https://api.example/v1"
        assert cred.default_model == "claude-haiku-4-5"
        assert cred.api_key_secret_id is not None
        assert decrypt_api_key(db, cred) == "sk-ant-fresh"


def test_upsert_rotates_existing_secret_in_place(local_user_id: int) -> None:
    with get_db_context() as db:
        first = upsert_provider_credential(db, local_user_id, "openai", _input(api_key="sk-v1"))
        original_secret_id = first.api_key_secret_id
        assert original_secret_id is not None

        second = upsert_provider_credential(db, local_user_id, "openai", _input(api_key="sk-v2"))
        assert second.id == first.id
        assert second.api_key_secret_id == original_secret_id, "secret id should remain stable"
        assert decrypt_api_key(db, second) == "sk-v2"

        secret = db.query(Secret).filter(Secret.id == original_secret_id).first()
        assert secret is not None
        # Secret name follows the convention.
        assert secret.name.startswith(f"ai:openai:api_key:{local_user_id}:")


def test_upsert_keeps_key_when_api_key_is_none(local_user_id: int) -> None:
    with get_db_context() as db:
        upsert_provider_credential(db, local_user_id, "groq", _input(api_key="gsk-keep"))
        # Partial update: only change default_model.
        upsert_provider_credential(
            db, local_user_id, "groq", _input(api_key=None, default_model="llama-3.1-70b-versatile")
        )
        cred = get_provider_credential(db, local_user_id, "groq")
        assert cred is not None
        assert cred.default_model == "llama-3.1-70b-versatile"
        assert decrypt_api_key(db, cred) == "gsk-keep"


def test_upsert_clears_key_when_clear_api_key_true(local_user_id: int) -> None:
    with get_db_context() as db:
        cred = upsert_provider_credential(db, local_user_id, "openrouter", _input(api_key="or-keep"))
        secret_id = cred.api_key_secret_id

        upsert_provider_credential(db, local_user_id, "openrouter", _input(api_key=None, clear_api_key=True))
        refreshed = get_provider_credential(db, local_user_id, "openrouter")
        assert refreshed is not None
        assert refreshed.api_key_secret_id is None
        assert db.query(Secret).filter(Secret.id == secret_id).first() is None


def test_upsert_rejects_api_key_and_clear_together() -> None:
    """Mutual exclusion is enforced by the Pydantic validator."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        ProviderCredentialInput(api_key="x", clear_api_key=True)


def test_delete_removes_credential_and_secret_atomically(local_user_id: int) -> None:
    with get_db_context() as db:
        cred = upsert_provider_credential(db, local_user_id, "google", _input(api_key="gem-1"))
        secret_id = cred.api_key_secret_id

        delete_provider_credential(db, local_user_id, "google")

        assert get_provider_credential(db, local_user_id, "google") is None
        assert db.query(Secret).filter(Secret.id == secret_id).first() is None


def test_credentials_per_user_unique(local_user_id: int) -> None:
    """A second raw insert for the same (user, provider) must raise."""
    with get_db_context() as db:
        first = AiProviderCredential(user_id=local_user_id, provider="anthropic")
        db.add(first)
        db.commit()

        dup = AiProviderCredential(user_id=local_user_id, provider="anthropic")
        db.add(dup)
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()


# ---------- get_configured_provider ----------


class _ProviderStub:
    """Drop-in for ``provider_factory`` returning a stub remembering kwargs."""

    last_kwargs: dict[str, Any] = {}

    def __init__(self, **kwargs: Any) -> None:
        type(self).last_kwargs = kwargs
        self.api_key = kwargs.get("api_key")
        self.api_base = kwargs.get("api_base")
        self.model = kwargs.get("model")
        self.surface = kwargs.get("surface")


def _patch_factory(monkeypatch: pytest.MonkeyPatch) -> type[_ProviderStub]:
    """Replace ``provider_factory`` with a stub-returning fake."""
    _ProviderStub.last_kwargs = {}

    def fake(name: str, **kwargs: Any) -> _ProviderStub:
        return _ProviderStub(name=name, **kwargs)

    monkeypatch.setattr(byok_module, "provider_factory", fake)
    return _ProviderStub


def test_get_configured_provider_uses_stored_key(local_user_id: int, monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _patch_factory(monkeypatch)
    with get_db_context() as db:
        upsert_provider_credential(
            db,
            local_user_id,
            "anthropic",
            _input(api_key="sk-stored", api_base="https://example/v1", default_model="claude-haiku-4-5"),
        )
        provider = get_configured_provider(db, local_user_id, "anthropic")
        assert provider.api_key == "sk-stored"
        assert provider.api_base == "https://example/v1"
        assert stub.last_kwargs["model"] == "claude-haiku-4-5"


def test_get_configured_provider_falls_back_to_env(local_user_id: int, monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _patch_factory(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "env-fallback-key")
    with get_db_context() as db:
        provider = get_configured_provider(db, local_user_id, "anthropic")
        # No row → no api_key passed; litellm reads env on its own.
        assert provider.api_key is None
        # Surface fallback should still resolve.
        assert stub.last_kwargs["api_key"] is None


def test_get_configured_provider_resolution_order(local_user_id: int, monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _patch_factory(monkeypatch)
    with get_db_context() as db:
        upsert_provider_credential(
            db, local_user_id, "anthropic", _input(api_key="k", default_model="claude-haiku-4-5")
        )
        # Stored default_model wins over surface map.
        get_configured_provider(db, local_user_id, "anthropic", surface="agent_complex")
        assert stub.last_kwargs["model"] == "claude-haiku-4-5"
        assert stub.last_kwargs["surface"] is None

        # Explicit model wins over both stored default and surface.
        get_configured_provider(db, local_user_id, "anthropic", surface="agent_complex", model="claude-opus-4-7")
        assert stub.last_kwargs["model"] == "claude-opus-4-7"


def test_get_configured_provider_ollama_no_key(local_user_id: int, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ollama needs api_base only — no env var, no key required."""
    monkeypatch.delenv("OLLAMA_API_KEY", raising=False)  # belt + braces
    stub = _patch_factory(monkeypatch)
    with get_db_context() as db:
        upsert_provider_credential(db, local_user_id, "ollama", _input(api_key=None, api_base="http://localhost:11434"))
        provider = get_configured_provider(db, local_user_id, "ollama")
        assert provider.api_key is None
        assert provider.api_base == "http://localhost:11434"
        assert stub.last_kwargs["api_base"] == "http://localhost:11434"


def test_get_configured_provider_raises_when_unconfigured(local_user_id: int, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_factory(monkeypatch)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with get_db_context() as db, pytest.raises(ProviderNotConfiguredError):
        get_configured_provider(db, local_user_id, "anthropic")


# ---------- env-fallback detection ----------


def test_detect_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert detect_env_fallback("anthropic") is False
    monkeypatch.setenv("ANTHROPIC_API_KEY", "x")
    assert detect_env_fallback("anthropic") is True
    # Ollama never reports env_fallback (no env var concept).
    assert detect_env_fallback("ollama") is False


# ---------- routes ----------


def test_routes_list_providers_enriches_status(
    authed_client: TestClient, local_user_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Clear all known env vars so we can shape statuses precisely.
    for env in (
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "GROQ_API_KEY",
        "OPENROUTER_API_KEY",
    ):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "env-only")

    with get_db_context() as db:
        upsert_provider_credential(db, local_user_id, "anthropic", _input(api_key="sk-1"))

    response = authed_client.get("/ai/providers")
    assert response.status_code == 200
    items = {item["provider"]: item for item in response.json()}
    assert items["anthropic"]["status"] == "configured"
    assert items["openai"]["status"] == "env_fallback"
    assert items["google"]["status"] == "unconfigured"
    # Class metadata is included.
    assert items["anthropic"]["supports_tools"] is True
    assert "cmd_k" in items["anthropic"]["surfaces"]


def test_routes_post_provider(authed_client: TestClient, local_user_id: int) -> None:
    response = authed_client.post(
        "/ai/providers/openai",
        json={"api_key": "sk-route", "default_model": "gpt-5"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["provider"] == "openai"
    assert body["has_key"] is True
    assert body["default_model"] == "gpt-5"

    with get_db_context() as db:
        cred = get_provider_credential(db, local_user_id, "openai")
        assert cred is not None
        assert decrypt_api_key(db, cred) == "sk-route"


def test_routes_delete_provider_404_when_missing(authed_client: TestClient, local_user_id: int) -> None:
    response = authed_client.delete("/ai/providers/groq")
    assert response.status_code == 404

    with get_db_context() as db:
        upsert_provider_credential(db, local_user_id, "groq", _input(api_key="gsk-x"))
    response = authed_client.delete("/ai/providers/groq")
    assert response.status_code == 204


def test_routes_post_unknown_provider_404(authed_client: TestClient) -> None:
    response = authed_client.post("/ai/providers/notreal", json={"api_key": "x"})
    assert response.status_code == 404


def test_routes_test_provider_records_status(
    authed_client: TestClient,
    local_user_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mock the provider's chat to succeed; expect last_test_status='ok'."""
    with get_db_context() as db:
        upsert_provider_credential(db, local_user_id, "anthropic", _input(api_key="sk"))

    fake_chat = AsyncMock(return_value=object())
    captured: dict[str, Any] = {}

    class _FakeProvider:
        api_key = "sk"
        api_base = None
        model = "anthropic/claude-sonnet-4-6"

        async def chat(self, *, messages, max_tokens=None):  # type: ignore[no-untyped-def]
            captured["messages"] = messages
            captured["max_tokens"] = max_tokens
            return await fake_chat()

    def fake_get(db, user_id, provider, **kwargs):  # type: ignore[no-untyped-def]
        return _FakeProvider()

    monkeypatch.setattr("flowfile_core.ai.byok_routes.get_configured_provider", fake_get)

    response = authed_client.post("/ai/providers/anthropic/test")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "error": None}
    assert isinstance(captured["messages"][0], Message)
    assert captured["max_tokens"] == 1

    with get_db_context() as db:
        cred = get_provider_credential(db, local_user_id, "anthropic")
        assert cred is not None
        assert cred.last_test_status == "ok"
        assert cred.last_test_error is None
        assert cred.last_tested_at is not None


def test_routes_test_provider_records_error(
    authed_client: TestClient,
    local_user_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider raises → status='error' with truncated message captured."""
    with get_db_context() as db:
        upsert_provider_credential(db, local_user_id, "openai", _input(api_key="sk"))

    long_msg = "boom! " * 200  # > 512 chars

    class _FailingProvider:
        async def chat(self, *, messages, max_tokens=None):  # type: ignore[no-untyped-def]
            raise RuntimeError(long_msg)

    monkeypatch.setattr(
        "flowfile_core.ai.byok_routes.get_configured_provider",
        lambda *args, **kwargs: _FailingProvider(),
    )

    response = authed_client.post("/ai/providers/openai/test")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert body["error"] is not None
    assert len(body["error"]) <= 512

    with get_db_context() as db:
        cred = get_provider_credential(db, local_user_id, "openai")
        assert cred is not None
        assert cred.last_test_status == "error"
        assert cred.last_test_error is not None
        assert len(cred.last_test_error) <= 512


# ---------- update_test_status helper ----------


def test_update_test_status_round_trip(local_user_id: int) -> None:
    with get_db_context() as db:
        cred = upsert_provider_credential(db, local_user_id, "groq", _input(api_key="x"))
        update_test_status(db, cred.id, ok=True)
        refreshed = get_provider_credential(db, local_user_id, "groq")
        assert refreshed is not None
        assert refreshed.last_test_status == "ok"
        assert refreshed.last_test_error is None

        update_test_status(db, cred.id, ok=False, error="rate-limited")
        refreshed = get_provider_credential(db, local_user_id, "groq")
        assert refreshed is not None
        assert refreshed.last_test_status == "error"
        assert refreshed.last_test_error == "rate-limited"


def test_update_test_status_missing_id_is_noop(local_user_id: int) -> None:
    with get_db_context() as db:
        # Should not raise.
        update_test_status(db, 999_999, ok=True)


# ---------- list_provider_credentials ----------


def test_list_provider_credentials_returns_all(local_user_id: int) -> None:
    with get_db_context() as db:
        upsert_provider_credential(db, local_user_id, "anthropic", _input(api_key="a"))
        upsert_provider_credential(db, local_user_id, "openai", _input(api_key="o"))
        rows = list_provider_credentials(db, local_user_id)
        names = {row.provider for row in rows}
        assert names == {"anthropic", "openai"}


# ---------- lazy import ----------


def test_lazy_litellm_import_for_credentials() -> None:
    """``flowfile_core.ai.credentials`` is provider-import-free.

    Mirrors the W11 / W15 lazy-import contract — importing this module must
    not pull in ``litellm`` (which would eagerly load every per-vendor SDK).
    Snapshot ``sys.modules``, drop the relevant entries, re-import, assert.
    """
    cleared: dict[str, Any] = {}
    for mod_name in list(sys.modules):
        if mod_name == "litellm" or mod_name.startswith("litellm.") or mod_name == "flowfile_core.ai.credentials":
            cleared[mod_name] = sys.modules.pop(mod_name)
    try:
        import flowfile_core.ai.credentials  # noqa: F401

        assert "litellm" not in sys.modules, "Importing flowfile_core.ai.credentials must not eagerly import litellm"
    finally:
        for mod_name, mod in cleared.items():
            sys.modules[mod_name] = mod


# ---------- W29 — per-credential curated model list ----------


def test_upsert_stores_models_verbatim(local_user_id: int) -> None:
    """Round-trip the curated list through the public projection in order."""
    from flowfile_core.ai.credentials import to_public

    with get_db_context() as db:
        cred = upsert_provider_credential(
            db,
            local_user_id,
            "openrouter",
            _input(
                api_key="or-x",
                models=[
                    "moonshotai/kimi-k2:free",
                    "deepseek/deepseek-chat-v3:free",
                    "meta-llama/llama-3.3-70b-instruct:free",
                ],
            ),
        )
        public = to_public(cred)
        assert public.models == [
            "moonshotai/kimi-k2:free",
            "deepseek/deepseek-chat-v3:free",
            "meta-llama/llama-3.3-70b-instruct:free",
        ]


def test_upsert_clears_models_via_flag_and_via_empty_list(local_user_id: int) -> None:
    """Both ``clear_models=True`` and ``models=[]`` collapse to NULL."""
    from flowfile_core.ai.credentials import to_public

    with get_db_context() as db:
        upsert_provider_credential(
            db, local_user_id, "openrouter", _input(api_key="or-x", models=["a", "b"])
        )

        # Clear via flag.
        upsert_provider_credential(
            db, local_user_id, "openrouter", _input(api_key=None, clear_models=True)
        )
        cred = get_provider_credential(db, local_user_id, "openrouter")
        assert cred is not None
        assert to_public(cred).models is None

        # Re-set, then clear via empty list.
        upsert_provider_credential(
            db, local_user_id, "openrouter", _input(api_key=None, models=["a"])
        )
        upsert_provider_credential(
            db, local_user_id, "openrouter", _input(api_key=None, models=[])
        )
        cred = get_provider_credential(db, local_user_id, "openrouter")
        assert cred is not None
        assert to_public(cred).models is None


def test_upsert_keeps_models_when_payload_models_is_none(local_user_id: int) -> None:
    """Partial update with ``models=None`` preserves the existing list."""
    from flowfile_core.ai.credentials import to_public

    with get_db_context() as db:
        upsert_provider_credential(
            db, local_user_id, "openrouter", _input(api_key="or-x", models=["a", "b"])
        )
        # Update only default_model — models should survive.
        upsert_provider_credential(
            db,
            local_user_id,
            "openrouter",
            _input(api_key=None, models=None, default_model="a"),
        )
        cred = get_provider_credential(db, local_user_id, "openrouter")
        assert cred is not None
        assert to_public(cred).models == ["a", "b"]
        assert cred.default_model == "a"


def test_upsert_rejects_models_and_clear_models_together() -> None:
    """Mutual exclusion mirrors the api_key / clear_api_key rule."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        ProviderCredentialInput(models=["a"], clear_models=True)


def test_get_configured_provider_picks_first_model_when_no_surface_match(
    local_user_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Step 4: curated list, no surface routing match → first entry wins."""
    stub = _patch_factory(monkeypatch)
    with get_db_context() as db:
        # default_model deliberately omitted so step 2 doesn't short-circuit.
        upsert_provider_credential(
            db,
            local_user_id,
            "openrouter",
            _input(
                api_key="or-x",
                default_model=None,
                models=["custom/model-x:free", "custom/model-y:free"],
            ),
        )
        # OpenRouter's class-level surface_models["cmd_k"] is
        # "anthropic/claude-haiku-4.5" — not in the curated list, so step 4
        # picks the first entry.
        get_configured_provider(db, local_user_id, "openrouter", surface="cmd_k")
        assert stub.last_kwargs["model"] == "custom/model-x:free"
        # Surface should not be passed once we've resolved a model — otherwise
        # the factory would re-resolve via surface_models and overwrite.
        assert stub.last_kwargs["surface"] is None


def test_get_configured_provider_prefers_surface_model_when_in_curated(
    local_user_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Step 3: curated list contains the surface routing target → it wins."""
    stub = _patch_factory(monkeypatch)
    surface_route = "anthropic/claude-haiku-4.5"  # OpenRouter surface_models["cmd_k"]
    with get_db_context() as db:
        upsert_provider_credential(
            db,
            local_user_id,
            "openrouter",
            _input(
                api_key="or-x",
                default_model=None,
                models=["custom/model-x:free", surface_route],
            ),
        )
        get_configured_provider(db, local_user_id, "openrouter", surface="cmd_k")
        assert stub.last_kwargs["model"] == surface_route


def test_get_configured_provider_models_loses_to_stored_default(
    local_user_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Step 2 still beats steps 3+4: stored ``default_model`` wins outright."""
    stub = _patch_factory(monkeypatch)
    with get_db_context() as db:
        upsert_provider_credential(
            db,
            local_user_id,
            "openrouter",
            _input(
                api_key="or-x",
                default_model="default-stays",
                models=["custom/model-x:free", "anthropic/claude-haiku-4.5"],
            ),
        )
        get_configured_provider(db, local_user_id, "openrouter", surface="cmd_k")
        assert stub.last_kwargs["model"] == "default-stays"


def test_alembic_013_upgrade_downgrade_round_trip(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Upgrade fresh DB to 012, insert a row, upgrade to 013, downgrade back.

    Confirms the W29 column add is reversible and that pre-existing 012 rows
    survive the upgrade. Uses an isolated sqlite file routed through the
    same ``get_database_url`` seam the alembic ``env.py`` reads, so the
    URL we set is the one the migrations actually run against.
    """
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import create_engine, text

    db_file = tmp_path / "alembic_013.sqlite"
    db_url = f"sqlite:///{db_file}"

    # env.py reads ``shared.storage_config.get_database_url()`` to populate
    # ``sqlalchemy.url`` regardless of what's in alembic.ini, so we patch the
    # source rather than the config.
    import shared.storage_config as storage_config

    monkeypatch.setattr(storage_config, "get_database_url", lambda: db_url)

    cfg = Config("flowfile_core/flowfile_core/alembic.ini")
    cfg.set_main_option("script_location", "flowfile_core/flowfile_core/alembic")

    # Stamp through 012 first so we can prove pre-existing rows survive.
    command.upgrade(cfg, "012")
    engine = create_engine(db_url)
    fake_user_id = 9_999_999  # SQLite FK enforcement is off by default; safe.
    with engine.begin() as conn:
        # Confirm pre-013 schema doesn't have the column.
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(ai_provider_credentials)"))}
        assert "models" not in cols

        conn.execute(
            text(
                "INSERT INTO ai_provider_credentials (user_id, provider) VALUES (:uid, :prov)"
            ),
            {"uid": fake_user_id, "prov": "openrouter"},
        )

    # Upgrade to 013 — column appears, original row survives.
    command.upgrade(cfg, "013")
    with engine.begin() as conn:
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(ai_provider_credentials)"))}
        assert "models" in cols
        survived = conn.execute(
            text(
                "SELECT provider, models FROM ai_provider_credentials"
                " WHERE user_id=:uid"
            ),
            {"uid": fake_user_id},
        ).first()
        assert survived is not None
        assert survived[0] == "openrouter"
        assert survived[1] is None  # nullable, never set

    # Downgrade back to 012 — column gone.
    command.downgrade(cfg, "012")
    with engine.begin() as conn:
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(ai_provider_credentials)"))}
        assert "models" not in cols
