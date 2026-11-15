"""Fail a test that writes ``os.environ`` from any thread but the main one.

A write calls ``setenv``/``unsetenv``, which may move the C environment block while another thread
(a request thread, a Polars or Rust thread) reads it with ``getenv``: glibc then crashes the whole
pytest process with a segfault, at a random test, long after the write that caused it. The guard
records every off-main-thread write with the stack that made it, lets the write go through as
before, and fails the test that was running when it happened, so the race shows up as one
readable failure instead of an occasional exit code 139.

Use it from a suite's ``conftest.py``::

    from test_utils.environ_thread_guard import fail_on_off_main_thread_environ_writes  # noqa: F401

``os.environ`` writes on the main thread (module level, fixtures, ``monkeypatch.setenv`` in a test
body) stay allowed. For code that must change a variable during a request, patch the function that
reads it instead (see ``as_server`` in ``flowfile_core/tests/flowfile_core_test_utils.py``).
"""

import os
import sys
import threading
import traceback
from collections import Counter

import pytest

_lock = threading.Lock()
_writes: list[str] = []
_installed = False
_STACK_DEPTH = 12
_SHOWN = 3


def _record(action: str, key) -> None:
    if threading.current_thread() is threading.main_thread():
        return
    stack = "".join(traceback.format_stack()[:-2][-_STACK_DEPTH:])
    main_frame = sys._current_frames().get(threading.main_thread().ident)
    main_stack = "".join(traceback.format_stack(main_frame)[-_STACK_DEPTH:]) if main_frame else "  (unknown)\n"
    with _lock:
        _writes.append(
            f"{action} {key!r} on thread {threading.current_thread().name!r}:\n{stack}"
            f"  ...while the main thread was at:\n{main_stack}"
        )


def install() -> None:
    """Wrap the writes of ``os.environ`` and ``os.environb``, which share the C environment; idempotent."""
    global _installed
    if _installed:
        return
    watched = (os.environ, getattr(os, "environb", None))
    environ_class = type(os.environ)
    real_set = environ_class.__setitem__
    real_del = environ_class.__delitem__

    def __setitem__(self, key, value):
        if any(self is environ for environ in watched):
            _record("set", key)
        real_set(self, key, value)

    def __delitem__(self, key):
        if any(self is environ for environ in watched):
            _record("delete", key)
        real_del(self, key)

    environ_class.__setitem__ = __setitem__
    environ_class.__delitem__ = __delitem__
    _installed = True


def take_writes() -> list[str]:
    """The off-main-thread writes recorded since the last call, which this clears."""
    with _lock:
        found = list(_writes)
        _writes.clear()
    return found


install()


def hold_gitpython_env() -> None:
    """Stop GitPython from writing ``os.environ`` around every git call; call it on the main thread.

    On Windows each git subprocess runs inside ``git.util.patch_env("NoDefaultCurrentDirectoryInExePath", "1")``,
    which sets and then deletes the variable, from whatever thread runs git (a request thread, in the route tests).
    This sets it once here and gives ``git.cmd`` a ``patch_env`` that writes nothing while the variable already
    holds the value asked for, so git sees the same environment and no thread writes it.
    """
    import contextlib

    import git.cmd

    original = git.cmd.patch_env

    @contextlib.contextmanager
    def patch_env(name, value):
        if os.environ.get(name) == value:
            yield
            return
        with original(name, value):
            yield

    os.environ["NoDefaultCurrentDirectoryInExePath"] = "1"
    git.cmd.patch_env = patch_env


@pytest.fixture
def allow_off_thread_environ_writes():
    """Drop the writes made during this test, for one that exercises a write production makes on its main thread.

    uvicorn runs the app's lifespan on the main thread, a ``TestClient`` on its portal thread, so a test of what the
    lifespan writes would otherwise fail on the test client's thread rather than on anything the server does.
    """
    yield
    take_writes()


@pytest.fixture(autouse=True)
def fail_on_off_main_thread_environ_writes():
    yield
    found = take_writes()
    if found:
        pytest.fail(_report(found), pytrace=False)


def _report(found: list[str]) -> str:
    distinct = Counter(found).most_common()
    shown = [f"[{count}x] {write}" for write, count in distinct[:_SHOWN]]
    if len(distinct) > _SHOWN:
        shown.append(f"... and {len(distinct) - _SHOWN} more distinct write site(s)")
    return (
        f"os.environ was written {len(found)} time(s) off the main thread; such writes race native getenv calls "
        "and segfault the run. Patch the function that reads the variable instead of the environment.\n\n"
        + "\n".join(shown)
    )
