"""Natural-language → cron generation, backing ``POST /ai/generate_cron``.

The user describes a schedule in plain English; the LLM returns a 5-field cron
string. JSON-mode, fail-fast on a timeout, never raises (soft failures return
``degraded=True`` + a ``reason``). The result is checked with the same
``validate_cron_expression`` the schedule-create path uses, so we never return a
cron the backend would reject. Must not import ``litellm`` at module load.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError

from flowfile_core.ai.metrics import record_autocomplete_call
from flowfile_core.ai.providers.base import Message, Provider
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler
from flowfile_core.catalog.validators import validate_cron_expression

logger = logging.getLogger(__name__)

SURFACE: str = "cron"
DEFAULT_TIMEOUT_SECONDS: float = 6.0  # deliberate button press, not per-keystroke
MAX_DESCRIPTION_LEN: int = 500


class CronGenerationResponse(BaseModel):
    """``cron_expression`` is a validated 5-field string on success, else ``None``
    with ``degraded`` + a ``reason`` code."""

    model_config = ConfigDict(extra="forbid")

    cron_expression: str | None = None
    explanation: str | None = None
    degraded: bool = False
    reason: str | None = None


class _CronLLMOutput(BaseModel):
    """The JSON shape the LLM must emit."""

    model_config = ConfigDict(extra="ignore")

    cron_expression: str
    explanation: str | None = None


# Plain string (not ``.format``-ed) so the braces in the JSON examples stay literal.
_CRON_SYSTEM_PROMPT = """\
You convert a plain-English schedule description into a standard 5-field cron
expression for Flowfile's scheduler.

Output a single JSON object — no prose, no code blocks. Shape:

  {"cron_expression": "<5-field cron>", "explanation": "<short plain-English read-back>"}

The five space-separated fields are, in order:

  minute(0-59)  hour(0-23, 24-hour clock)  day-of-month(1-31)  month(1-12)  day-of-week(0-6, 0=Sunday)

Use `*` for "every", `,` for lists, `-` for ranges, and `*/n` for steps.

Hard rules:

* Emit EXACTLY 5 space-separated fields. Never use 6-field (with-seconds) cron
  and never use named macros like `@daily` or `@hourly`.
* Interpret clock times as local wall-clock time. Do NOT apply any timezone
  offset — the scheduler applies the schedule's timezone separately.
* "weekday" / "business day" means Mon-Fri (`1-5`); "weekend" means Sat+Sun (`0,6`).
* A bare hour with no am/pm (e.g. "at 6") means the morning hour (06:00).
* If the text does NOT describe a recurring schedule, or is too vague to pin a
  concrete time, return {"cron_expression": "", "explanation": "<why>"} rather
  than guessing.

Examples (input -> output):

  "every weekday at 9am" -> {"cron_expression": "0 9 * * 1-5", "explanation": "09:00, Mon-Fri"}
  "every 15 minutes" -> {"cron_expression": "*/15 * * * *", "explanation": "Every 15 minutes"}
  "daily at 2:30pm" -> {"cron_expression": "30 14 * * *", "explanation": "14:30 daily"}
  "Mondays and Thursdays at 6am" -> {"cron_expression": "0 6 * * 1,4", "explanation": "06:00 Mon and Thu"}
  "first day of every month at midnight" -> {"cron_expression": "0 0 1 * *", "explanation": "00:00 on the 1st"}
  "every hour on the hour" -> {"cron_expression": "0 * * * *", "explanation": "Top of every hour"}
  "every 6 hours" -> {"cron_expression": "0 */6 * * *", "explanation": "Every 6 hours"}
  "hello there" -> {"cron_expression": "", "explanation": "Not a schedule"}
"""


def _build_messages(description: str) -> list[Message]:
    return [
        Message(role="system", content=_CRON_SYSTEM_PROMPT),
        Message(role="user", content=f"Schedule description: {description!r}"),
    ]


async def _call_provider_for_json(
    *,
    provider: Provider,
    messages: list[Message],
    timeout: float,
    scheduler: RateLimitScheduler | None,
    max_tokens: int,
) -> tuple[str | None, str | None]:
    """Call ``provider.chat`` in JSON-mode with a hard timeout; no retries.
    Returns ``(content, error_reason)``."""
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
    except Exception as exc:  # noqa: BLE001
        logger.warning("cron generation provider call failed: %s", exc, exc_info=False)
        return None, "provider_error"
    return content, None


def _parse_json_payload(content: str | None) -> tuple[Any, str | None]:
    """Parse JSON, tolerating a ```json code fence."""
    if content is None or not content.strip():
        return None, "empty_response"
    try:
        return json.loads(content), None
    except json.JSONDecodeError:
        pass
    stripped = content.strip()
    if stripped.startswith("```"):
        without_open = stripped.split("\n", 1)[-1]
        if without_open.rstrip().endswith("```"):
            without_open = without_open.rstrip()[: -len("```")].rstrip()
        try:
            return json.loads(without_open), None
        except json.JSONDecodeError:
            pass
    return None, "parse_error"


def _record(provider: Provider, started: float, reason: str | None, *, success: bool = False) -> None:
    record_autocomplete_call(
        surface=SURFACE,
        provider=provider.name,
        latency_ms=int((time.monotonic() - started) * 1000),
        suggestion_count=1 if success else 0,
        degraded_reason=reason,
    )


async def generate_cron_expression(
    description: str,
    *,
    provider: Provider,
    scheduler: RateLimitScheduler | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> CronGenerationResponse:
    """Turn a plain-English description into a validated cron string. Never raises;
    soft failures return ``degraded=True`` with a stable ``reason``."""
    started = time.monotonic()
    desc = (description or "").strip()
    if not desc:
        _record(provider, started, "empty_description")
        return CronGenerationResponse(degraded=True, reason="empty_description")

    content, err = await _call_provider_for_json(
        provider=provider,
        messages=_build_messages(desc),
        timeout=timeout,
        scheduler=scheduler,
        max_tokens=256,
    )
    if err is not None:
        _record(provider, started, err)
        return CronGenerationResponse(degraded=True, reason=err)

    payload, parse_err = _parse_json_payload(content)
    if parse_err is not None:
        _record(provider, started, parse_err)
        return CronGenerationResponse(degraded=True, reason=parse_err)

    try:
        parsed = _CronLLMOutput.model_validate(payload)
    except ValidationError:
        _record(provider, started, "validation_error")
        return CronGenerationResponse(degraded=True, reason="validation_error")

    expr = parsed.cron_expression.strip()
    if not expr:
        # model declined (not a schedule / too vague) — forward its explanation
        _record(provider, started, "no_expression")
        return CronGenerationResponse(degraded=True, reason="no_expression", explanation=parsed.explanation)

    try:
        validate_cron_expression(expr)  # same check create_schedule runs
    except ValueError:
        _record(provider, started, "invalid_cron")
        return CronGenerationResponse(degraded=True, reason="invalid_cron")

    _record(provider, started, None, success=True)
    return CronGenerationResponse(cron_expression=expr, explanation=parsed.explanation, degraded=False)


__all__ = [
    "SURFACE",
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_DESCRIPTION_LEN",
    "CronGenerationResponse",
    "generate_cron_expression",
]
