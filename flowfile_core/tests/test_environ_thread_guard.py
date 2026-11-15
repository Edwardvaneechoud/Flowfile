import os
import threading
from unittest import mock

from test_utils import environ_thread_guard

KEY = "FLOWFILE_TEST_ENVIRON_GUARD"


def run_in_thread(fn) -> None:
    thread = threading.Thread(target=fn, name="guard-test")
    thread.start()
    thread.join()


def test_a_write_on_the_main_thread_is_allowed():
    os.environ[KEY] = "1"
    del os.environ[KEY]

    assert environ_thread_guard.take_writes() == []


def test_a_write_on_another_thread_is_recorded_with_its_stack():
    def write():
        os.environ[KEY] = "1"
        del os.environ[KEY]

    run_in_thread(write)

    found = environ_thread_guard.take_writes()
    assert len(found) == 2
    assert found[0].startswith(f"set {KEY!r} on thread 'guard-test'")
    assert found[1].startswith(f"delete {KEY!r} on thread 'guard-test'")
    assert "in write" in found[0]
    assert KEY not in os.environ


def test_the_patch_dict_that_segfaulted_ci_is_recorded():
    def in_mode():
        with mock.patch.dict(os.environ, {KEY: "docker"}):
            return os.environ[KEY]

    run_in_thread(in_mode)

    found = environ_thread_guard.take_writes()
    assert found, "mock.patch.dict(os.environ) on a request thread must be caught"
    assert all("guard-test" in entry for entry in found)
    assert KEY not in os.environ


def test_a_write_to_os_environb_on_another_thread_is_recorded():
    if not hasattr(os, "environb"):
        return

    def write():
        os.environb[KEY.encode()] = b"1"
        del os.environb[KEY.encode()]

    run_in_thread(write)

    assert len(environ_thread_guard.take_writes()) == 2


def test_a_recorded_write_names_where_the_main_thread_was():
    run_in_thread(lambda: os.environ.__setitem__(KEY, "1"))
    del os.environ[KEY]

    (found,) = environ_thread_guard.take_writes()
    main_part = found.split("...while the main thread was at:", 1)[1]
    assert "in run_in_thread" in main_part


def test_the_opt_out_fixture_drops_the_writes_of_its_test(allow_off_thread_environ_writes):
    run_in_thread(lambda: os.environ.__setitem__(KEY, "1"))
    del os.environ[KEY]
