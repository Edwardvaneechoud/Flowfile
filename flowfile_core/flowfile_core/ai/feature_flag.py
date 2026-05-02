"""W17 — `FEATURE_FLAG_AI` gating for the `/ai/*` router.

Single process-wide kill switch for the AI subsystem, sourced from
``flowfile_core.configs.settings.FEATURE_FLAG_AI`` — a ``MutableBool``
initialised from the ``FEATURE_FLAG_AI`` env var at process start. Off by
default through Phase 0 per ``_feature_llm_integration.md`` §10.

The ``MutableBool`` is the single source of truth. Any Python call site can
flip it at runtime via ``settings.FEATURE_FLAG_AI.set(...)`` — the admin
endpoint (W18), test fixtures (``flag_on`` / ``flag_off``), or anywhere
else. ``is_ai_enabled()`` reads the live value on every call, so toggles
take effect immediately without process restart or module reload. The env
var remains the boot-time default + persistence-across-restart mechanism.

Two surfaces:

* :func:`is_ai_enabled` — boolean read for non-route call sites (agents,
  schedulers) that want to short-circuit before doing provider I/O.
* :func:`require_ai_enabled` — FastAPI dependency wired into the ``ai_router``
  constructor; raises ``HTTPException(503)`` on every AI request when the flag
  is off so the frontend gets a single, machine-readable signal.

Auth gating (``Depends(get_current_active_user)``) is orthogonal — added
per-route as W11/W12 land authenticated AI endpoints.
"""

from __future__ import annotations

from fastapi import HTTPException, status

from flowfile_core.configs import settings as _settings

DISABLED_DETAIL = "AI features are disabled. Set FEATURE_FLAG_AI=true to enable."


def is_ai_enabled() -> bool:
    """Return True iff the AI subsystem is enabled for this process.

    Reads ``settings.FEATURE_FLAG_AI`` (a ``MutableBool``) on every call so
    in-process flips via ``.set(True)`` from the admin endpoint (W18) or
    test fixtures take effect immediately. The MutableBool is initialised
    from the ``FEATURE_FLAG_AI`` env var at process start, so the env var
    remains the boot-time default + persistence-across-restart mechanism.
    """
    return bool(_settings.FEATURE_FLAG_AI)


def require_ai_enabled() -> None:
    """FastAPI dependency that gates the entire ``/ai/*`` router.

    Wired via ``APIRouter(dependencies=[Depends(require_ai_enabled)])`` in
    ``flowfile_core.ai.routes`` so every endpoint inherits the gate without
    per-route boilerplate (matches the pattern at
    ``flowfile_core/kernel/routes.py:37`` and
    ``flowfile_core/routes/secrets.py:21``).

    Raises
    ------
    fastapi.HTTPException
        ``503 Service Unavailable`` when ``FEATURE_FLAG_AI`` is off, with a
        detail string the frontend can match on without parsing prose.
    """
    if not is_ai_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=DISABLED_DETAIL,
        )
