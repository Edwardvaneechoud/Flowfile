"""Revoking consent must stop delivery that is already under way.

The entry gates are covered elsewhere; what these pin is the window between a
revoke and the buffers that outlive it — the in-memory queue a ``flush`` would
post, the daemon loop mid-tick, and a spool drain that already read its lines
off disk. Every one of them carries the install id the revoke just erased.
"""

from __future__ import annotations

import queue
import threading
import time
from collections.abc import Callable
from typing import Any

import pytest

from shared import telemetry
from shared import telemetry_spool as spool


@pytest.fixture
def no_background(monkeypatch):
    """Pin delivery to the call under test so nothing races the assertions."""
    monkeypatch.setattr(telemetry, "_ensure_worker", lambda: None)


def _wait_until(predicate: Callable[[], Any], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)


def _spool_envelopes(count: int) -> None:
    identifier = telemetry.install_id() or "install"
    lines = [spool.encode(telemetry._envelope("app_started", {}, identifier)) for _ in range(count)]
    telemetry._spool_file().write_text("".join(line + "\n" for line in lines), encoding="utf-8")


def _hook_post(monkeypatch, hook: Callable[[], None]):
    """Run *hook* right after each recorded post, from inside the sending thread."""
    inner = telemetry._post

    def _post(url: str, json: dict[str, Any], timeout: float | None = None):
        response = inner(url, json, timeout)
        hook()
        return response

    monkeypatch.setattr(telemetry, "_post", _post)


def test_flush_after_revoke_posts_nothing_and_drops_the_queue(enabled, no_background, posts):
    telemetry.emit("app_started")
    telemetry.emit("flow_created")
    assert telemetry._queue.qsize() == 2

    telemetry.set_consent(False)
    assert telemetry._queue.empty(), "revoking must discard what is buffered, not park it"

    telemetry.flush()

    assert posts.sent == [], "queued events carry the id that was just forgotten"


def test_send_refuses_a_batch_handed_to_it_after_a_revoke(enabled, no_background, posts):
    """``_send`` owns the last gate: every other guard can be bypassed by a caller holding a batch."""
    batch = [telemetry._envelope("app_started", {}, telemetry.install_id() or "install")]
    telemetry.set_consent(False)

    assert telemetry._send(batch) == telemetry.SEND_PERMANENT
    assert posts.sent == []
    assert not telemetry._spool_file().exists(), "a refused batch must not be parked for a later drain"


def test_the_daemon_loop_leaves_the_queue_alone_after_a_revoke(enabled, posts):
    """The loop's pre-take gate, isolated: without it the envelope is taken and silently dropped."""
    envelope = telemetry._envelope("app_started", {}, telemetry.install_id() or "install")
    pending: queue.Queue = queue.Queue()
    pending.put(envelope)
    telemetry.set_consent(False)

    wake, stop = threading.Event(), threading.Event()
    wake.set()
    thread = threading.Thread(target=telemetry._loop, args=(pending, wake, stop), daemon=True)
    thread.start()
    try:
        _wait_until(pending.empty, 0.5)
    finally:
        stop.set()
        wake.set()
        thread.join(2.0)

    assert posts.sent == []
    assert pending.qsize() == 1, "the loop must not dequeue what it is no longer allowed to send"


def test_a_revoke_mid_drain_stops_the_remaining_batches(enabled, no_background, posts, monkeypatch):
    _spool_envelopes(250)
    _hook_post(monkeypatch, lambda: telemetry.set_consent(False) if len(posts.sent) == 1 else None)

    telemetry._drain_spool()

    assert len(posts.sent) == 1, "a drain must stop at the batch boundary after consent is revoked"
    assert not telemetry._spool_file().exists()


def test_re_granting_consent_does_not_release_the_old_ids_mid_drain(enabled, no_background, posts, monkeypatch):
    _spool_envelopes(250)

    def _revoke_then_grant() -> None:
        if len(posts.sent) == 1:
            telemetry.set_consent(False)
            telemetry.set_consent(True)

    _hook_post(monkeypatch, _revoke_then_grant)

    telemetry._drain_spool()

    assert len(posts.sent) == 1, "the purge invalidated the lines this drain was holding"
    assert not telemetry._spool_file().exists()
