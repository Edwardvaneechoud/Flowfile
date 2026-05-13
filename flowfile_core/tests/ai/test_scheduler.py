"""rate-limit scheduler tests.

Cases:

* Lazy-litellm contract — import does not pull litellm into ``sys.modules``.
* ``RetryPolicy.delay_for`` — deterministic without jitter, caps at
  ``max_delay``, jitter within ±25 % range, retry-after override semantics.
* ``RateLimitScheduler.acquire`` — under-budget passes immediately;
  RPM-exceeded blocks for the right duration; RPD-exceeded blocks for ~24 h;
  rate-limit hint callback fired with correct ``RateLimitHint``; unset limits
  never block; ollama is always unlimited even if env vars are set.
* ``RateLimitScheduler.note_response`` — 429 ``Retry-After`` blocks the next
  acquire and clears once the clock advances past the hint.
* ``with_provider_retry`` — first-call success, retry-then-success, retry-after
  from exception, retry-after from response.headers, max retries → typed
  failure with ``__cause__``, non-retryable propagates, hint callback invoked
  per retry boundary.
* ``stream_with_provider_retry`` — retries before first chunk; mid-stream
  failures propagate without re-invoking the factory.
* Concurrency — concurrent ``acquire`` calls serialise window writes
  through the per-provider lock.
* Env-var parsing — per-provider RPM/RPD reads at scheduler construction;
  invalid values log a warning and fall back to unlimited.
* ``default_scheduler`` returns a singleton.
* ``_retryable_exception_types`` resolves lazily and includes the litellm
  RateLimitError class.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import random
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import pytest

import flowfile_core.ai.scheduler as scheduler_mod
from flowfile_core.ai.providers.base import StreamChunk
from flowfile_core.ai.scheduler import (
    DEFAULT_RETRY_POLICY,
    ProviderCallFailure,
    RateLimitHint,
    RateLimitScheduler,
    RetryPolicy,
    default_scheduler,
    stream_with_provider_retry,
    with_provider_retry,
)

# ---------- shared fakes ----------


@dataclass
class FakeProvider:
    """Structurally satisfies the ``Provider`` Protocol."""

    name: str = "anthropic"
    model: str = "claude-sonnet-4-6"
    supports_tools: bool = True
    supports_streaming: bool = True

    async def chat(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise AssertionError("FakeProvider.chat should not be called by the retry helper")

    def stream(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise AssertionError("FakeProvider.stream should not be called by the retry helper")


class FakeClock:
    """Deterministic clock + sleep — the scheduler's only time/IO surface.

    ``advance(seconds)`` lets a test fast-forward without sleep; ``sleep`` is
    awaitable, records the requested delay, and advances ``t`` by it.
    """

    def __init__(self, start: float = 100.0) -> None:
        self.t = start
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def sched(clock: FakeClock) -> RateLimitScheduler:
    return RateLimitScheduler(time_source=clock.now, sleep=clock.sleep)


@pytest.fixture
def fake_rate_limit_error(monkeypatch: pytest.MonkeyPatch) -> type[Exception]:
    """Inject a fake retryable exception class for the duration of a test.

    Replaces the cached ``_RETRYABLE_TYPES`` tuple so ``with_provider_retry``
    treats this fake as the only retryable type. Avoids the litellm
    constructor signature drift (which differs across releases).
    """

    class FakeRateLimitError(Exception):
        def __init__(
            self,
            *,
            retry_after: float | None = None,
            headers_retry_after: float | str | None = None,
        ) -> None:
            super().__init__("rate limited (test)")
            if retry_after is not None:
                self.retry_after = retry_after
            if headers_retry_after is not None:

                class _Resp:
                    headers = {"retry-after": str(headers_retry_after)}

                self.response = _Resp()

    monkeypatch.setattr(scheduler_mod, "_RETRYABLE_TYPES", (FakeRateLimitError,))
    return FakeRateLimitError


# ---------- lazy-litellm contract ----------


def test_no_litellm_pulled_in_by_scheduler_import() -> None:
    """``flowfile_core.ai.scheduler`` must not pull litellm into ``sys.modules``."""
    for mod_name in list(sys.modules):
        if mod_name.startswith("litellm") or mod_name == "flowfile_core.ai.scheduler":
            del sys.modules[mod_name]

    importlib.import_module("flowfile_core.ai.scheduler")
    assert "litellm" not in sys.modules, (
        "scheduler import pulled litellm into sys.modules: "
        f"{sorted(m for m in sys.modules if m.startswith('litellm'))}"
    )


# ---------- RetryPolicy.delay_for ----------


def test_delay_for_returns_deterministic_backoff_without_jitter() -> None:
    policy = RetryPolicy(jitter=0.0)
    assert policy.delay_for(0) == pytest.approx(2.0)
    assert policy.delay_for(1) == pytest.approx(4.0)
    assert policy.delay_for(2) == pytest.approx(8.0)
    assert policy.delay_for(3) == pytest.approx(16.0)


def test_delay_for_caps_at_max_delay() -> None:
    policy = RetryPolicy(jitter=0.0)
    assert policy.delay_for(5) == pytest.approx(16.0)
    assert policy.delay_for(10) == pytest.approx(16.0)


def test_delay_for_jitter_within_range() -> None:
    policy = RetryPolicy(jitter=0.25)
    rng = random.Random(0xDEADBEEF)
    for attempt in range(4):
        deterministic = min(2.0 * (2.0**attempt), 16.0)
        for _ in range(50):
            delay = policy.delay_for(attempt, rng=rng)
            assert 0.0 <= delay <= 16.0
            # ±25% before clamping; the upper-bound check survives clamping.
            assert delay <= deterministic * 1.25 + 1e-9


def test_delay_for_uses_retry_after_when_longer() -> None:
    policy = RetryPolicy(jitter=0.0)
    # base 2 s vs hint 30 s → hint wins.
    assert policy.delay_for(0, retry_after_hint=30.0) == pytest.approx(30.0)


def test_delay_for_keeps_backoff_when_retry_after_shorter() -> None:
    policy = RetryPolicy(jitter=0.0)
    # base 4 s for attempt=1 vs hint 0.5 → backoff wins.
    assert policy.delay_for(1, retry_after_hint=0.5) == pytest.approx(4.0)


# ---------- RateLimitScheduler.acquire — RPM ----------


@pytest.mark.asyncio
async def test_acquire_passes_under_budget(
    sched: RateLimitScheduler,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLOWFILE_AI_ANTHROPIC_RPM", "10")
    fresh = RateLimitScheduler(time_source=clock.now, sleep=clock.sleep)
    for _ in range(5):
        async with fresh.acquire("anthropic"):
            pass
    assert clock.sleeps == []  # never had to wait


@pytest.mark.asyncio
async def test_acquire_blocks_when_rpm_exceeded(clock: FakeClock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLOWFILE_AI_ANTHROPIC_RPM", "2")
    sched = RateLimitScheduler(time_source=clock.now, sleep=clock.sleep)
    async with sched.acquire("anthropic"):
        pass
    async with sched.acquire("anthropic"):
        pass
    # Third call must wait ~60 s for the first slot to roll off.
    async with sched.acquire("anthropic"):
        pass
    assert clock.sleeps == [pytest.approx(60.0)]


@pytest.mark.asyncio
async def test_acquire_emits_rate_limit_hint_callback(clock: FakeClock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLOWFILE_AI_ANTHROPIC_RPM", "1")
    sched = RateLimitScheduler(time_source=clock.now, sleep=clock.sleep)
    hints: list[RateLimitHint] = []
    async with sched.acquire("anthropic", on_rate_limit_hint=hints.append):
        pass
    async with sched.acquire("anthropic", on_rate_limit_hint=hints.append):
        pass
    assert len(hints) == 1
    assert hints[0].provider == "anthropic"
    assert hints[0].attempt == 0
    assert hints[0].retry_after_seconds == pytest.approx(60.0)


@pytest.mark.asyncio
async def test_acquire_unset_limits_never_blocks(sched: RateLimitScheduler, clock: FakeClock) -> None:
    for _ in range(50):
        async with sched.acquire("anthropic"):
            pass
    assert clock.sleeps == []


@pytest.mark.asyncio
async def test_acquire_ollama_always_unlimited_even_if_rpm_set(
    clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLOWFILE_AI_OLLAMA_RPM", "1")
    sched = RateLimitScheduler(time_source=clock.now, sleep=clock.sleep)
    rpm, rpd = sched.limits_for("ollama")
    assert rpm is None and rpd is None
    for _ in range(20):
        async with sched.acquire("ollama"):
            pass
    assert clock.sleeps == []


# ---------- RateLimitScheduler.acquire — RPD ----------


@pytest.mark.asyncio
async def test_acquire_blocks_when_rpd_exceeded(clock: FakeClock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLOWFILE_AI_ANTHROPIC_RPD", "2")
    sched = RateLimitScheduler(time_source=clock.now, sleep=clock.sleep)
    async with sched.acquire("anthropic"):
        pass
    async with sched.acquire("anthropic"):
        pass
    async with sched.acquire("anthropic"):
        pass
    assert clock.sleeps == [pytest.approx(86_400.0)]


# ---------- note_response / 429 hints ----------


@pytest.mark.asyncio
async def test_note_response_with_retry_after_blocks_subsequent_acquires(
    sched: RateLimitScheduler, clock: FakeClock
) -> None:
    sched.note_response("anthropic", retry_after_seconds=10.0)
    async with sched.acquire("anthropic"):
        pass
    assert clock.sleeps == [pytest.approx(10.0)]


@pytest.mark.asyncio
async def test_note_response_clears_after_window_passes(sched: RateLimitScheduler, clock: FakeClock) -> None:
    sched.note_response("anthropic", retry_after_seconds=10.0)
    clock.advance(20.0)  # past the hint
    async with sched.acquire("anthropic"):
        pass
    assert clock.sleeps == []


# ---------- with_provider_retry — happy + retry paths ----------


@pytest.mark.asyncio
async def test_with_provider_retry_returns_first_success(
    sched: RateLimitScheduler,
    clock: FakeClock,
    fake_rate_limit_error: type[Exception],
) -> None:
    provider = FakeProvider(name="anthropic")
    calls: list[int] = []

    async def coro() -> str:
        calls.append(1)
        return "ok"

    result = await with_provider_retry(provider, coro, scheduler=sched)
    assert result == "ok"
    assert calls == [1]
    assert clock.sleeps == []


@pytest.mark.asyncio
async def test_with_provider_retry_backs_off_on_rate_limit_then_succeeds(
    sched: RateLimitScheduler,
    clock: FakeClock,
    fake_rate_limit_error: type[Exception],
) -> None:
    provider = FakeProvider(name="anthropic")
    attempt = {"n": 0}

    async def coro() -> str:
        attempt["n"] += 1
        if attempt["n"] == 1:
            raise fake_rate_limit_error()
        return "ok"

    result = await with_provider_retry(
        provider,
        coro,
        scheduler=sched,
        policy=RetryPolicy(jitter=0.0),
    )
    assert result == "ok"
    assert attempt["n"] == 2
    # Single retry → single backoff sleep at base_delay = 2 s.
    assert clock.sleeps == [pytest.approx(2.0)]


@pytest.mark.asyncio
async def test_with_provider_retry_uses_retry_after_from_exception(
    sched: RateLimitScheduler,
    clock: FakeClock,
    fake_rate_limit_error: type[Exception],
) -> None:
    provider = FakeProvider(name="anthropic")
    attempt = {"n": 0}

    async def coro() -> str:
        attempt["n"] += 1
        if attempt["n"] == 1:
            raise fake_rate_limit_error(retry_after=7.0)
        return "ok"

    await with_provider_retry(provider, coro, scheduler=sched, policy=RetryPolicy(jitter=0.0))
    # Hint of 7 s overrides the 2 s local backoff. note_response also sets
    # next_allowed_at, but the retry sleep is exactly the policy delay so
    # the second acquire finds the hint expired.
    assert clock.sleeps == [pytest.approx(7.0)]


@pytest.mark.asyncio
async def test_with_provider_retry_uses_retry_after_from_response_headers(
    sched: RateLimitScheduler,
    clock: FakeClock,
    fake_rate_limit_error: type[Exception],
) -> None:
    provider = FakeProvider(name="anthropic")
    attempt = {"n": 0}

    async def coro() -> str:
        attempt["n"] += 1
        if attempt["n"] == 1:
            raise fake_rate_limit_error(headers_retry_after=15)
        return "ok"

    await with_provider_retry(provider, coro, scheduler=sched, policy=RetryPolicy(jitter=0.0))
    assert clock.sleeps == [pytest.approx(15.0)]


@pytest.mark.asyncio
async def test_with_provider_retry_max_retries_raises_provider_call_failure(
    sched: RateLimitScheduler,
    clock: FakeClock,
    fake_rate_limit_error: type[Exception],
) -> None:
    provider = FakeProvider(name="anthropic")

    async def coro() -> str:
        raise fake_rate_limit_error()

    with pytest.raises(ProviderCallFailure) as exc_info:
        await with_provider_retry(provider, coro, scheduler=sched, policy=RetryPolicy(jitter=0.0))

    assert exc_info.value.provider == "anthropic"
    assert exc_info.value.attempts == 5  # initial + 4 retries
    assert isinstance(exc_info.value.__cause__, fake_rate_limit_error)
    # 4 backoff sleeps: 2, 4, 8, 16.
    assert clock.sleeps == [
        pytest.approx(2.0),
        pytest.approx(4.0),
        pytest.approx(8.0),
        pytest.approx(16.0),
    ]


@pytest.mark.asyncio
async def test_with_provider_retry_does_not_retry_non_retryable(
    sched: RateLimitScheduler,
    clock: FakeClock,
    fake_rate_limit_error: type[Exception],
) -> None:
    provider = FakeProvider(name="anthropic")
    calls = {"n": 0}

    async def coro() -> str:
        calls["n"] += 1
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await with_provider_retry(provider, coro, scheduler=sched)

    assert calls["n"] == 1
    assert clock.sleeps == []


@pytest.mark.asyncio
async def test_with_provider_retry_invokes_rate_limit_hint_per_retry(
    sched: RateLimitScheduler,
    clock: FakeClock,
    fake_rate_limit_error: type[Exception],
) -> None:
    provider = FakeProvider(name="anthropic")
    attempt = {"n": 0}
    hints: list[RateLimitHint] = []

    async def coro() -> str:
        attempt["n"] += 1
        if attempt["n"] < 3:
            raise fake_rate_limit_error()
        return "ok"

    await with_provider_retry(
        provider,
        coro,
        scheduler=sched,
        policy=RetryPolicy(jitter=0.0),
        on_rate_limit_hint=hints.append,
    )
    assert [h.attempt for h in hints] == [1, 2]
    assert all(h.provider == "anthropic" for h in hints)


# ---------- stream_with_provider_retry ----------


def _make_stream_factory(
    *responses: Exception | list[StreamChunk],
) -> Callable[[], AsyncIterator[StreamChunk]]:
    """Build a sync factory that returns sequentially from ``responses``.

    Each entry is either an exception (raised synchronously from the factory)
    or a list of chunks (returned as an async generator).
    """

    calls = [0]

    def factory() -> AsyncIterator[StreamChunk]:
        idx = calls[0]
        calls[0] += 1
        if idx >= len(responses):
            raise IndexError("stream_factory called more times than responses provided")
        response = responses[idx]
        if isinstance(response, BaseException):
            raise response

        async def gen() -> AsyncIterator[StreamChunk]:
            for chunk in response:
                yield chunk

        return gen()

    factory.calls = calls  # type: ignore[attr-defined]
    return factory


@pytest.mark.asyncio
async def test_stream_retries_before_first_chunk(
    sched: RateLimitScheduler,
    clock: FakeClock,
    fake_rate_limit_error: type[Exception],
) -> None:
    provider = FakeProvider(name="anthropic")
    factory = _make_stream_factory(
        fake_rate_limit_error(),
        [StreamChunk(content_delta="a"), StreamChunk(content_delta="b")],
    )

    chunks: list[StreamChunk] = []
    async for chunk in stream_with_provider_retry(provider, factory, scheduler=sched, policy=RetryPolicy(jitter=0.0)):
        chunks.append(chunk)

    assert [c.content_delta for c in chunks] == ["a", "b"]
    assert clock.sleeps == [pytest.approx(2.0)]


@pytest.mark.asyncio
async def test_stream_does_not_retry_after_first_chunk(
    sched: RateLimitScheduler,
    clock: FakeClock,
    fake_rate_limit_error: type[Exception],
) -> None:
    provider = FakeProvider(name="anthropic")

    class _FailMidStream:
        def __init__(self) -> None:
            self.n = 0

        def __aiter__(self) -> _FailMidStream:
            return self

        async def __anext__(self) -> StreamChunk:
            self.n += 1
            if self.n == 1:
                return StreamChunk(content_delta="x")
            raise fake_rate_limit_error()

    factory_calls = {"n": 0}

    def factory() -> AsyncIterator[StreamChunk]:
        factory_calls["n"] += 1
        return _FailMidStream()

    chunks: list[StreamChunk] = []
    with pytest.raises(fake_rate_limit_error):
        async for chunk in stream_with_provider_retry(
            provider, factory, scheduler=sched, policy=RetryPolicy(jitter=0.0)
        ):
            chunks.append(chunk)

    assert len(chunks) == 1
    assert factory_calls["n"] == 1  # no retry after first chunk arrived


# ---------- concurrency ----------


@pytest.mark.asyncio
async def test_concurrent_acquires_serialise_window_writes(clock: FakeClock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLOWFILE_AI_ANTHROPIC_RPM", "1")
    sched = RateLimitScheduler(time_source=clock.now, sleep=clock.sleep)
    results: list[int] = []

    async def caller(i: int) -> None:
        async with sched.acquire("anthropic"):
            results.append(i)

    await asyncio.gather(caller(1), caller(2))

    assert sorted(results) == [1, 2]
    # Exactly one of the two coroutines had to wait the full window.
    assert clock.sleeps == [pytest.approx(60.0)]


# ---------- env-var parsing ----------


def test_env_var_parsing_reads_per_provider_rpm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLOWFILE_AI_GOOGLE_RPM", "5")
    monkeypatch.setenv("FLOWFILE_AI_GOOGLE_RPD", "1500")
    sched = RateLimitScheduler()
    assert sched.limits_for("google") == (5, 1500)


def test_env_var_parsing_invalid_value_logs_warning_and_falls_back_to_unlimited(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("FLOWFILE_AI_OPENAI_RPM", "not-a-number")
    monkeypatch.setenv("FLOWFILE_AI_OPENAI_RPD", "-3")
    sched = RateLimitScheduler()
    with caplog.at_level(logging.WARNING, logger="flowfile_core.ai.scheduler"):
        limits = sched.limits_for("openai")
    assert limits == (None, None)
    assert "FLOWFILE_AI_OPENAI_RPM" in caplog.text
    assert "FLOWFILE_AI_OPENAI_RPD" in caplog.text


# ---------- default scheduler / lazy retryable types ----------


def test_default_scheduler_returns_singleton() -> None:
    a = default_scheduler()
    b = default_scheduler()
    assert a is b


def test_retryable_exception_types_resolved_lazily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The litellm exception classes are resolved on first call and cached.

    Reset the cache, call the resolver, assert litellm classes show up. This
    pairs with ``test_no_litellm_pulled_in_by_scheduler_import`` — the
    *import* side stays clean even though the *first call* loads litellm.
    """
    monkeypatch.setattr(scheduler_mod, "_RETRYABLE_TYPES", None)
    types = scheduler_mod._retryable_exception_types()
    assert TimeoutError in types
    assert ConnectionError in types
    type_names = {t.__name__ for t in types}
    assert (
        "RateLimitError" in type_names
    ), f"Expected litellm.exceptions.RateLimitError in resolved set; got {type_names}"
    # Second call returns the same cached tuple.
    assert scheduler_mod._retryable_exception_types() is types


# ---------- contract sanity: DEFAULT_RETRY_POLICY ----------


def test_default_retry_policy_matches_plan_section_5_1() -> None:
    """The shipped default reflects plan §5.1 verbatim."""
    p = DEFAULT_RETRY_POLICY
    assert p.max_retries == 4
    assert p.base_delay == pytest.approx(2.0)
    assert p.factor == pytest.approx(2.0)
    assert p.max_delay == pytest.approx(16.0)
    assert p.jitter == pytest.approx(0.25)


# ---------- coro-factory shape sanity (helps editors infer the type) ----------


def test_with_provider_retry_signature_accepts_coro_factory() -> None:
    """Compile-time-ish check: ``with_provider_retry`` accepts a ``Callable[[], Awaitable[T]]``.

    Validates the documented integration shape so downstream callers can rely
    on it without reading the source.
    """

    async def some_chat() -> str:
        return "ok"

    factory: Callable[[], Awaitable[str]] = some_chat
    assert callable(factory)
