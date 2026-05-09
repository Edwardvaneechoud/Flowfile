"""Rate-limit-aware scheduler for provider calls.

Sits between the ``Provider`` Protocol and any caller that wants to
issue chat / stream requests. Provides:

* per-provider sliding-window RPM / RPD enforcement (operator-tunable
  via ``FLOWFILE_AI_<PROVIDER>_RPM`` / ``RPD`` env vars; unset → no
  enforcement);
* ``Retry-After`` honor on 429 regardless of configured limits — the
  next ``acquire`` blocks until the hint expires;
* exponential-backoff retry on transient errors (``2s, 4s, 8s, 16s``
  max 4 retries, ±25 % jitter; server ``Retry-After`` always wins
  when longer than the local backoff);
* a ``RateLimitHint`` callback the caller can pipe into the SSE
  stream for the "rate-limited, retrying in Ns" toast.

The litellm import is lazy. Tests verify
``flowfile_core.ai.scheduler`` import does not pull ``litellm`` into
``sys.modules``.

Boundary discipline:

* No SSE encoding here — surface ``RateLimitHint`` only; the chat /
  agent pipelines compose the SSE event.
* No audit writes — the executor records audit events on success;
  retries are deliberately invisible to the user-facing quota
  counter ("failure is free").
* ``byok.get_configured_provider`` is **not** auto-wrapped — opt-in
  primitive. Callers compose ``with_provider_retry(provider, ...)``.
* No persistence across restarts; in-memory deques only.
* Per-provider granularity (not per-(provider, model)) — surface →
  model fanout is a known limitation.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import ClassVar, TypeVar

from flowfile_core.ai.providers.base import Provider, StreamChunk

logger = logging.getLogger(__name__)

T = TypeVar("T")

__all__ = [
    "DEFAULT_RETRY_POLICY",
    "ProviderCallFailure",
    "RateLimitHint",
    "RateLimitScheduler",
    "RetryPolicy",
    "default_scheduler",
    "stream_with_provider_retry",
    "with_provider_retry",
]


# ---------- types ----------


@dataclass(frozen=True)
class RateLimitHint:
    """Surfaced when the scheduler decides to wait.

    Emitted on:

    * a configured RPM / RPD bucket being full at ``acquire()`` time;
    * a 429 ``Retry-After`` setting ``next_allowed_at``;
    * each retry attempt inside ``with_provider_retry`` /
      ``stream_with_provider_retry``.

    ``attempt`` is 0 for the pre-call throttle (caller hasn't issued any
    request yet) and 1+ for retries after a transient failure.
    """

    provider: str
    retry_after_seconds: float
    attempt: int = 0


@dataclass(frozen=True)
class RetryPolicy:
    """Controls the exponential-backoff schedule of ``with_provider_retry``.

    Defaults match plan §5.1: ``2s, 4s, 8s, 16s`` (4 retries on top of the
    initial attempt) with ±25 % jitter. ``max_delay`` clamps the deterministic
    factor; jitter is then applied within ``[0, max_delay]``.
    """

    max_retries: int = 4
    base_delay: float = 2.0
    factor: float = 2.0
    max_delay: float = 16.0
    jitter: float = 0.25

    def delay_for(
        self,
        attempt: int,
        *,
        retry_after_hint: float | None = None,
        rng: random.Random | None = None,
    ) -> float:
        """Return seconds to sleep before retry ``attempt + 1`` (0-indexed).

        ``retry_after_hint`` from a server-supplied ``Retry-After`` header is
        honored when it's longer than the local backoff. The local backoff
        is ``base_delay * factor**attempt`` clamped at ``max_delay``, then
        jittered uniformly in ``[1 - jitter, 1 + jitter]`` clamped to
        ``[0, max_delay]``.
        """
        deterministic = min(self.base_delay * (self.factor**attempt), self.max_delay)
        if self.jitter > 0:
            r = rng or random
            multiplier = r.uniform(1 - self.jitter, 1 + self.jitter)
            local = max(0.0, min(deterministic * multiplier, self.max_delay))
        else:
            local = max(0.0, deterministic)
        if retry_after_hint is not None and retry_after_hint > local:
            return retry_after_hint
        return local


DEFAULT_RETRY_POLICY = RetryPolicy()


class ProviderCallFailure(RuntimeError):
    """Raised by ``with_provider_retry`` after all attempts are exhausted.

    The original transient exception is set as ``__cause__`` via the
    ``raise ... from`` site, so callers can inspect ``exc.__cause__`` for
    diagnostics.
    """

    def __init__(self, provider: str, attempts: int) -> None:
        super().__init__(f"Provider {provider!r} call failed after {attempts} attempt(s).")
        self.provider = provider
        self.attempts = attempts


# ---------- retryable-types resolution (lazy) ----------


_RETRYABLE_TYPES: tuple[type[BaseException], ...] | None = None


def _retryable_exception_types() -> tuple[type[BaseException], ...]:
    """Resolve the litellm exception classes we treat as retryable.

    Lazy on first call. ``flowfile_core.ai.scheduler`` must not import
    ``litellm`` at module level — keeps the lazy contract intact.
    """
    global _RETRYABLE_TYPES
    if _RETRYABLE_TYPES is not None:
        return _RETRYABLE_TYPES
    candidates: list[type[BaseException]] = [TimeoutError, ConnectionError]
    try:
        from litellm import exceptions as lle  # lazy
    except Exception as exc:  # pragma: no cover - litellm is a hard dep
        logger.warning(
            "Could not import litellm.exceptions; provider retry will only "
            "trigger on bare TimeoutError / ConnectionError. (%s)",
            exc,
        )
    else:
        for attr in (
            "RateLimitError",
            "Timeout",
            "APIConnectionError",
            "InternalServerError",
            "ServiceUnavailableError",
        ):
            cls = getattr(lle, attr, None)
            if isinstance(cls, type) and issubclass(cls, BaseException):
                candidates.append(cls)
    _RETRYABLE_TYPES = tuple(dict.fromkeys(candidates))
    return _RETRYABLE_TYPES


def _extract_retry_after(exc: BaseException) -> float | None:
    """Best-effort extraction of ``Retry-After`` from an exception.

    Handles two shapes commonly seen on litellm-wrapped 429s:

    * direct ``retry_after`` attribute (some libs);
    * ``response.headers["retry-after"]`` (httpx-style).
    """
    direct = getattr(exc, "retry_after", None)
    if direct is not None:
        try:
            value = float(direct)
        except (TypeError, ValueError):
            value = -1.0
        if value > 0:
            return value

    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    try:
        retry_after = headers.get("retry-after") or headers.get("Retry-After")
    except AttributeError:
        return None
    if retry_after is None:
        return None
    try:
        value = float(retry_after)
    except (TypeError, ValueError):
        return None
    if value > 0:
        return value
    return None


# ---------- per-provider window tracker ----------


@dataclass
class _ProviderState:
    rpm_limit: int | None = None
    rpd_limit: int | None = None
    rpm_window: deque[float] = field(default_factory=deque)
    rpd_window: deque[float] = field(default_factory=deque)
    next_allowed_at: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


TimeFn = Callable[[], float]
SleepFn = Callable[[float], Awaitable[None]]


def _read_int_env(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return None
    try:
        value = int(raw.strip())
    except ValueError:
        logger.warning(
            "Ignoring %s=%r (not an integer); rate-limit enforcement disabled " "for this provider.",
            name,
            raw,
        )
        return None
    if value <= 0:
        logger.warning(
            "Ignoring %s=%d (must be > 0); rate-limit enforcement disabled " "for this provider.",
            name,
            value,
        )
        return None
    return value


# Provider names that are local — no remote rate-limiting applies.
_UNLIMITED_PROVIDERS: frozenset[str] = frozenset({"ollama"})


def _default_time() -> float:
    """Default monotonic-ish clock for production usage.

    Uses ``time.monotonic`` so retry-after arithmetic isn't surprised by
    wall-clock jumps. Tests inject their own ``time_source``.
    """
    return time.monotonic()


class RateLimitScheduler:
    """In-memory per-provider RPM / RPD tracker.

    Construction reads the env vars once on first ``_state`` lookup per
    provider; tests construct fresh instances after ``monkeypatch.setenv``.
    The default ``default_scheduler()`` lazily instantiates a process-wide
    singleton.

    Public methods:

    * :meth:`acquire` — async context manager that throttles the next call.
    * :meth:`note_response` — feedback hook for 429 ``Retry-After`` hints.
    * :meth:`limits_for` — read-only inspection helper for tests / debug.
    * :meth:`now` / :meth:`sleep` — exposed for retry helpers and tests.
    """

    _RPM_TEMPLATE: ClassVar[str] = "FLOWFILE_AI_{provider}_RPM"
    _RPD_TEMPLATE: ClassVar[str] = "FLOWFILE_AI_{provider}_RPD"

    def __init__(
        self,
        *,
        time_source: TimeFn | None = None,
        sleep: SleepFn | None = None,
    ) -> None:
        self._time_source: TimeFn = time_source or _default_time
        self._sleep_fn: SleepFn = sleep or asyncio.sleep
        self._states: dict[str, _ProviderState] = {}

    def now(self) -> float:
        return self._time_source()

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        await self._sleep_fn(seconds)

    def _state(self, provider: str) -> _ProviderState:
        state = self._states.get(provider)
        if state is None:
            if provider in _UNLIMITED_PROVIDERS:
                rpm = None
                rpd = None
            else:
                key = provider.upper()
                rpm = _read_int_env(self._RPM_TEMPLATE.format(provider=key))
                rpd = _read_int_env(self._RPD_TEMPLATE.format(provider=key))
            state = _ProviderState(rpm_limit=rpm, rpd_limit=rpd)
            self._states[provider] = state
        return state

    def limits_for(self, provider: str) -> tuple[int | None, int | None]:
        """Effective ``(rpm, rpd)`` after env-var resolution. ``None`` is unset."""
        state = self._state(provider)
        return (state.rpm_limit, state.rpd_limit)

    @asynccontextmanager
    async def acquire(
        self,
        provider: str,
        *,
        surface: str | None = None,
        on_rate_limit_hint: Callable[[RateLimitHint], None] | None = None,
    ) -> AsyncIterator[None]:
        """Wait until the next call to ``provider`` is permitted, then record it.

        ``surface`` is accepted for forward-compat with per-(provider, surface)
        tracking but is currently unused — provider-level granularity only.
        """
        del surface  # reserved for future per-surface throttling
        state = self._state(provider)
        while True:
            wait = await self._compute_and_record(state, self.now())
            if wait <= 0:
                break
            if on_rate_limit_hint is not None:
                on_rate_limit_hint(RateLimitHint(provider=provider, retry_after_seconds=wait, attempt=0))
            await self.sleep(wait)
        try:
            yield
        finally:
            pass

    async def _compute_and_record(self, state: _ProviderState, now: float) -> float:
        """Atomically check budget; record the slot if available; otherwise return wait.

        Holding the per-provider lock across the check-and-record prevents
        the classic race where two coroutines simultaneously observe an
        empty bucket.
        """
        async with state.lock:
            wait = 0.0

            if state.next_allowed_at > now:
                wait = max(wait, state.next_allowed_at - now)

            if state.rpm_limit is not None:
                _drop_older_than(state.rpm_window, now - 60.0)
                if len(state.rpm_window) >= state.rpm_limit:
                    oldest = state.rpm_window[0]
                    wait = max(wait, oldest + 60.0 - now)

            if state.rpd_limit is not None:
                _drop_older_than(state.rpd_window, now - 86_400.0)
                if len(state.rpd_window) >= state.rpd_limit:
                    oldest = state.rpd_window[0]
                    wait = max(wait, oldest + 86_400.0 - now)

            if wait <= 0:
                if state.rpm_limit is not None:
                    state.rpm_window.append(now)
                if state.rpd_limit is not None:
                    state.rpd_window.append(now)
                return 0.0
            return wait

    def note_response(
        self,
        provider: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        """Feedback hook for the post-call site.

        Caller invokes when it receives a 429 with a ``Retry-After`` hint.
        Subsequent ``acquire`` calls block until the hint expires.
        """
        if retry_after_seconds is None or retry_after_seconds <= 0:
            return
        state = self._state(provider)
        candidate = self.now() + retry_after_seconds
        if candidate > state.next_allowed_at:
            state.next_allowed_at = candidate


def _drop_older_than(window: deque[float], cutoff: float) -> None:
    while window and window[0] < cutoff:
        window.popleft()


_GLOBAL_SCHEDULER: RateLimitScheduler | None = None


def default_scheduler() -> RateLimitScheduler:
    """Process-wide ``RateLimitScheduler`` singleton (lazy)."""
    global _GLOBAL_SCHEDULER
    if _GLOBAL_SCHEDULER is None:
        _GLOBAL_SCHEDULER = RateLimitScheduler()
    return _GLOBAL_SCHEDULER


# ---------- chat retry wrapper ----------


async def with_provider_retry(
    provider: Provider,
    coro_factory: Callable[[], Awaitable[T]],
    *,
    scheduler: RateLimitScheduler | None = None,
    policy: RetryPolicy = DEFAULT_RETRY_POLICY,
    surface: str | None = None,
    on_rate_limit_hint: Callable[[RateLimitHint], None] | None = None,
    rng: random.Random | None = None,
) -> T:
    """Run ``coro_factory()`` with throttling + exponential-backoff retry.

    Each attempt re-acquires a slot from the scheduler before invoking the
    coroutine factory (so retries don't bypass throttling). Retryable
    exceptions trigger backoff; non-retryable propagate immediately.
    """
    sched = scheduler or default_scheduler()
    retryable = _retryable_exception_types()
    last_exc: BaseException | None = None
    for attempt in range(policy.max_retries + 1):
        delay: float | None = None
        async with sched.acquire(
            provider.name,
            surface=surface,
            on_rate_limit_hint=on_rate_limit_hint,
        ):
            try:
                return await coro_factory()
            except retryable as exc:
                last_exc = exc
                retry_after = _extract_retry_after(exc)
                sched.note_response(provider.name, retry_after_seconds=retry_after)
                if attempt >= policy.max_retries:
                    break
                delay = policy.delay_for(attempt, retry_after_hint=retry_after, rng=rng)
                if on_rate_limit_hint is not None:
                    on_rate_limit_hint(
                        RateLimitHint(
                            provider=provider.name,
                            retry_after_seconds=delay,
                            attempt=attempt + 1,
                        )
                    )
        if delay is not None:
            await sched.sleep(delay)
    assert last_exc is not None  # loop only exits via break on retryable
    raise ProviderCallFailure(provider.name, attempts=policy.max_retries + 1) from last_exc


# ---------- streaming retry wrapper ----------


async def stream_with_provider_retry(
    provider: Provider,
    stream_factory: Callable[[], AsyncIterator[StreamChunk]],
    *,
    scheduler: RateLimitScheduler | None = None,
    policy: RetryPolicy = DEFAULT_RETRY_POLICY,
    surface: str | None = None,
    on_rate_limit_hint: Callable[[RateLimitHint], None] | None = None,
    rng: random.Random | None = None,
) -> AsyncIterator[StreamChunk]:
    """Stream variant of :func:`with_provider_retry`.

    Retries the call only until the first chunk is yielded. After
    that any error from the upstream generator propagates — mid-stream
    resumption belongs to the replay-buffer / session layer.
    """
    sched = scheduler or default_scheduler()
    retryable = _retryable_exception_types()
    last_exc: BaseException | None = None
    for attempt in range(policy.max_retries + 1):
        delay: float | None = None
        async with sched.acquire(
            provider.name,
            surface=surface,
            on_rate_limit_hint=on_rate_limit_hint,
        ):
            try:
                upstream = stream_factory()
                first = await upstream.__anext__()
            except StopAsyncIteration:
                return
            except retryable as exc:
                last_exc = exc
                retry_after = _extract_retry_after(exc)
                sched.note_response(provider.name, retry_after_seconds=retry_after)
                if attempt >= policy.max_retries:
                    break
                delay = policy.delay_for(attempt, retry_after_hint=retry_after, rng=rng)
                if on_rate_limit_hint is not None:
                    on_rate_limit_hint(
                        RateLimitHint(
                            provider=provider.name,
                            retry_after_seconds=delay,
                            attempt=attempt + 1,
                        )
                    )
            else:
                yield first
                async for chunk in upstream:
                    yield chunk
                return
        if delay is not None:
            await sched.sleep(delay)
    assert last_exc is not None
    raise ProviderCallFailure(provider.name, attempts=policy.max_retries + 1) from last_exc
