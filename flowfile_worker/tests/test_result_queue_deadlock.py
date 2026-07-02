"""Regression tests for the result-queue drain (multiprocessing deadlock).

A child that ``queue.put()`` a payload larger than the OS pipe buffer (~64 KB on
Linux) blocks in its background feeder thread until the parent reads it. The old
code called ``p.join()`` (and spun on ``while p.is_alive()``) *before* draining the
queue, so a large result deadlocked the worker forever with no timeout. The monitor
now breaks on the completion signal and drains the queue before joining.

``calculate_schema`` on a wide table is the realistic trigger: its ``schema_stats``
payload (per-column stat dicts) easily crosses the pipe buffer.
"""

import pickle
import threading
import time

import polars as pl
import pytest

from flowfile_worker import models, mp_context, status_dict, status_dict_lock
from flowfile_worker.funcs import calculate_schema
from flowfile_worker.spawner import drain_result_queue, handle_task, process_manager


def _wide_lf(n_cols: int = 2000, n_rows: int = 3) -> pl.LazyFrame:
    """A frame wide enough that calculate_schema's queue payload exceeds ~64 KB."""
    return pl.LazyFrame({f"col_{i}": list(range(n_rows)) for i in range(n_cols)})


def _spawn_calculate_schema():
    lf = _wide_lf()
    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    q = mp_context.Queue(maxsize=1)
    p = mp_context.Process(
        target=calculate_schema,
        kwargs=dict(
            polars_serializable_object=lf.serialize(),
            progress=progress,
            error_message=error_message,
            queue=q,
            flowfile_flow_id=-1,
            flowfile_node_id=-1,
        ),
    )
    p.start()
    return p, progress, error_message, q


@pytest.mark.worker
def test_drain_result_queue_large_payload_no_deadlock():
    """Draining before join lets a child with a >64 KB result exit cleanly."""
    p, progress, error_message, q = _spawn_calculate_schema()
    try:
        start = time.monotonic()
        result = drain_result_queue(q, p, timeout=30.0)
        p.join(timeout=30)
        elapsed = time.monotonic() - start
    finally:
        if p.is_alive():
            p.terminate()
            p.join()

    assert not p.is_alive(), "child never exited — result was not drained before join"
    assert progress.value == 100, error_message[:].decode(errors="replace")
    assert isinstance(result, list) and result
    # Guard the premise: the payload must actually exceed the pipe buffer, else the
    # test would pass even against the old join-before-drain code.
    assert len(pickle.dumps(result)) > 64 * 1024
    assert elapsed < 30


@pytest.mark.worker
def test_handle_task_completes_with_large_payload():
    """End-to-end: handle_task must reach Completed (with results) for a large payload,
    not hang on the ``while p.is_alive()`` spin."""
    task_id = "test-wide-schema-deadlock"
    p, progress, error_message, q = _spawn_calculate_schema()
    process_manager.add_process(task_id, p)
    with status_dict_lock:
        status_dict[task_id] = models.Status(
            background_task_id=task_id, status="Starting", file_ref="", result_type="other"
        )

    # Run in a thread so a regression can't hang the test suite (no pytest-timeout).
    done = threading.Event()

    def _run():
        try:
            handle_task(task_id, p, progress, error_message, q)
        finally:
            done.set()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    finished = done.wait(timeout=45)

    try:
        assert finished, "handle_task did not return — deadlocked on the result queue"
        with status_dict_lock:
            status = status_dict[task_id]
        assert status.status == "Completed", status.error_message
        assert status.results is not None and len(status.results) > 0
    finally:
        if p.is_alive():
            p.terminate()
            p.join()
        with status_dict_lock:
            status_dict.pop(task_id, None)
        process_manager.remove_process(task_id)
