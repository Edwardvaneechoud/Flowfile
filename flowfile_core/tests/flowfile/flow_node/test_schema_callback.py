import threading
import time

from flowfile_core.flowfile.flow_node.schema_callback import SingleExecutionFuture


def test_basic_call_returns_result():
    f = SingleExecutionFuture(lambda: 42)
    assert f() == 42
    assert f.is_completed()
    # Cached on subsequent calls.
    assert f() == 42


def test_reset_allows_recomputation():
    counter = {"n": 0}

    def func():
        counter["n"] += 1
        return counter["n"]

    f = SingleExecutionFuture(func)
    assert f() == 1
    f.reset()
    assert f() == 2


def test_in_flight_task_cannot_poison_state_after_reset():
    """A slow task whose executor was shut down via reset(wait=False) must not
    write its (now stale) result back into _result_value / _has_completed."""
    block = threading.Event()
    release = threading.Event()
    call_count = {"n": 0}

    def slow_func():
        call_count["n"] += 1
        my_call = call_count["n"]
        block.set()
        release.wait(timeout=5)
        return f"call_{my_call}"

    f = SingleExecutionFuture(slow_func)
    f.start()
    block.wait(timeout=5)  # First call is now blocked inside slow_func.

    f.reset()  # Bumps generation, shuts down executor wait=False.
    release.set()  # Let the stale task finish — it must not write back.
    time.sleep(0.1)  # Give the stale thread time to attempt the write.

    assert not f.is_completed(), "Stale task wrote _has_completed after reset"
    assert f.get_result() is None, "Stale task wrote _result_value after reset"

    # Fresh call must trigger a new execution, not return the stale value.
    block.clear()
    release.clear()
    release.set()  # Don't block this call.

    result = f()
    assert result == "call_2", f"Expected fresh execution, got {result}"


def test_reset_after_completion_clears_state():
    f = SingleExecutionFuture(lambda: "x")
    assert f() == "x"
    f.reset()
    assert not f.is_completed()
    assert f.get_result() is None
