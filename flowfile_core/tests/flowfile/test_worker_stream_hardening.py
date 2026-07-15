"""Tests for the bounded worker WebSocket receive (wedged-worker hardening).

Core's ``_receive_raw_result`` must never wait on a silent worker forever:
each poll tick checks cancellation (``should_abort``) and total silence
against ``_WS_INACTIVITY_TIMEOUT`` (env ``FLOWFILE_WORKER_WS_TIMEOUT``).
A stall raises ``WorkerStreamStalled`` — and, critically, mid-receive
failures must NOT trigger the fetchers' REST fallback (which would
re-submit the task to a worker that is presumed wedged).
"""

import time

import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.subprocess_operations import (
    streaming as stream_mod,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import (
    subprocess_operations as subprocess_ops,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.streaming import (
    WorkerStreamInterrupted,
    WorkerStreamStalled,
    _receive_raw_result,
)


class _FakeWs:
    """Duck-typed stand-in for a websockets.sync client connection.

    The script is a list of items played back by ``recv``:
    - ``"timeout"``: sleep ``gap`` seconds, then raise ``TimeoutError``
      (simulating a quiet poll tick);
    - anything else: returned as the received message.
    An exhausted script keeps raising ``TimeoutError`` (permanent silence).
    """

    def __init__(self, script=(), gap: float = 0.0):
        self._script = list(script)
        self._gap = gap
        self.closed = False

    def recv(self, timeout=None):
        if not self._script:
            time.sleep(self._gap)
            raise TimeoutError
        item = self._script.pop(0)
        if item == "timeout":
            time.sleep(self._gap)
            raise TimeoutError
        return item

    def close(self):
        self.closed = True


def test_silent_worker_raises_stalled(monkeypatch):
    """A worker that never sends anything must surface as a stall, promptly."""
    monkeypatch.setattr(stream_mod, "_WS_INACTIVITY_TIMEOUT", 0.05)
    ws = _FakeWs(gap=0.01)

    start = time.monotonic()
    with pytest.raises(WorkerStreamStalled, match="FLOWFILE_WORKER_WS_TIMEOUT"):
        _receive_raw_result(ws, "task-silent")
    assert time.monotonic() - start < 5, "stall detection must be bounded"


def test_heartbeats_keep_stream_alive(monkeypatch):
    """Progress heartbeats (even with an unchanged value) reset the silence clock."""
    monkeypatch.setattr(stream_mod, "_WS_INACTIVITY_TIMEOUT", 0.2)
    ws = _FakeWs(
        script=[
            "timeout",
            '{"type": "progress", "progress": 50}',
            "timeout",
            '{"type": "progress", "progress": 50}',
            "timeout",
            '{"type": "complete", "has_result": false, "file_ref": "x"}',
        ],
        gap=0.05,
    )

    raw_result, status = _receive_raw_result(ws, "task-heartbeat")
    assert raw_result is None
    assert status is not None and status.progress == 100


def test_should_abort_interrupts_receive():
    """Cancellation must break the wait on the next poll tick."""
    ws = _FakeWs(gap=0.0)

    with pytest.raises(WorkerStreamInterrupted, match="Canceled"):
        _receive_raw_result(ws, "task-abort", should_abort=lambda: True)


def test_stalled_receive_does_not_fall_back_to_rest(monkeypatch):
    """A stall after a successful submit must not re-submit the task via REST."""
    monkeypatch.setattr(subprocess_ops, "streaming_start", lambda **kwargs: _FakeWs())

    def _stalled_receive(ws, task_id, should_abort=None):
        raise WorkerStreamStalled("worker presumed unresponsive")

    monkeypatch.setattr(subprocess_ops, "streaming_receive", _stalled_receive)
    monkeypatch.setattr(
        subprocess_ops,
        "trigger_df_operation",
        lambda **kwargs: pytest.fail("REST fallback must not run after a successful WS submit"),
    )

    with pytest.raises(WorkerStreamStalled):
        subprocess_ops.ExternalDfFetcher(
            flow_id=1, node_id=1, lf=pl.LazyFrame({"a": [1]}), file_ref="t-stall", wait_on_completion=True
        )


def test_stalled_sampler_does_not_fall_back_to_rest(monkeypatch):
    monkeypatch.setattr(subprocess_ops, "streaming_start", lambda **kwargs: _FakeWs())

    def _stalled_receive(ws, task_id, should_abort=None):
        raise WorkerStreamStalled("worker presumed unresponsive")

    monkeypatch.setattr(subprocess_ops, "streaming_receive", _stalled_receive)
    monkeypatch.setattr(
        subprocess_ops,
        "trigger_sample_operation",
        lambda **kwargs: pytest.fail("REST fallback must not run after a successful WS submit"),
    )

    with pytest.raises(WorkerStreamStalled):
        subprocess_ops.ExternalSampler(
            lf=pl.LazyFrame({"a": [1]}), node_id=1, flow_id=1, file_ref="t-sample-stall", wait_on_completion=True
        )


def test_generic_stream_error_still_falls_back_to_rest(monkeypatch):
    """Connection-phase failures keep the original REST fallback behavior."""

    def _no_ws(**kwargs):
        raise ConnectionRefusedError("worker has no WS endpoint")

    monkeypatch.setattr(subprocess_ops, "streaming_start", _no_ws)

    def _rest_called(**kwargs):
        raise RuntimeError("REST_CALLED")

    monkeypatch.setattr(subprocess_ops, "trigger_df_operation", _rest_called)

    with pytest.raises(RuntimeError, match="REST_CALLED"):
        subprocess_ops.ExternalDfFetcher(
            flow_id=1, node_id=1, lf=pl.LazyFrame({"a": [1]}), file_ref="t-fallback", wait_on_completion=True
        )


def test_receive_thread_marks_stall_as_worker_unresponsive(monkeypatch):
    """Non-blocking mode: a stall surfaces via get_result() with error_code -2.

    -1 is reserved for "worker child died" (nodes degrade gracefully and
    continue); -2 makes the node fail fast because the worker itself is gone.
    """
    monkeypatch.setattr(subprocess_ops, "streaming_start", lambda **kwargs: _FakeWs())

    def _stalled_receive(ws, task_id, should_abort=None):
        raise WorkerStreamStalled("worker presumed unresponsive")

    monkeypatch.setattr(subprocess_ops, "streaming_receive", _stalled_receive)

    fetcher = subprocess_ops.ExternalDfFetcher(
        flow_id=1, node_id=1, lf=pl.LazyFrame({"a": [1]}), file_ref="t-async-stall", wait_on_completion=False
    )
    with pytest.raises(Exception, match="unresponsive"):
        fetcher.get_result()
    assert fetcher.error_code == -2
