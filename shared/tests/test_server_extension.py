"""A server's extension is the one module its configuration names: loaded on first use, never because it is installed,
and absent on every machine that names none."""

import multiprocessing
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

from shared import server_extension
from shared.server_extension import Identity
from shared.tests import toy_server_extension as toy

TOY = "shared.tests.toy_server_extension"


@pytest.fixture
def unloaded(monkeypatch):
    """Nothing is loaded and nothing named when the test starts; what the test loads is forgotten after it."""
    monkeypatch.delenv(server_extension.VARIABLE, raising=False)
    server_extension.reset()
    monkeypatch.setattr(toy, "calls", [])
    monkeypatch.setattr(toy, "answers", {})
    yield
    server_extension.reset()


def test_with_no_extension_named_every_call_answers_as_a_flowfile_on_its_own(unloaded):
    assert server_extension.load() is None
    assert server_extension.call("sign_in_method", default={"method": "password"}) == {"method": "password"}
    assert server_extension.call("routers", "core", default=()) == ()
    assert (server_extension.identity(), server_extension.joined()) == (None, None)
    assert server_extension.defines("open_sealed") is False
    assert server_extension.word("sealer", "issuer") == "issuer"


def test_a_process_that_names_no_extension_imports_none(tmp_path):
    code = (
        "import sys\n"
        "from shared import server_extension\n"
        "server_extension.before_import('core')\n"
        "server_extension.call('identity')\n"
        f"print({TOY!r} in sys.modules, server_extension.load())\n"
    )
    env = {name: value for name, value in os.environ.items() if name != server_extension.VARIABLE}

    done = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=60)

    assert done.returncode == 0, done.stderr
    assert done.stdout.split() == ["False", "None"]


def test_the_named_module_is_imported_on_the_first_call_and_asked_from_then_on(unloaded, monkeypatch):
    monkeypatch.setenv(server_extension.VARIABLE, TOY)
    toy.answers["bind"] = "a socket"

    assert server_extension.call("bind", "core", "127.0.0.1", 8080) == "a socket"
    assert server_extension.load() is toy
    assert toy.calls == [("bind", "core", "127.0.0.1", 8080)]
    assert server_extension.word("sealer", "issuer") == "toy"


def test_a_hook_the_extension_does_not_define_answers_the_default(unloaded, monkeypatch):
    monkeypatch.setenv(server_extension.VARIABLE, TOY)

    assert server_extension.defines("sign_out") is False
    assert server_extension.call("password_refusal") is None
    assert server_extension.call("sign_in_method", default={"method": "password"}) == {"method": "password"}


def test_a_name_that_is_no_hook_is_refused(unloaded):
    with pytest.raises(ValueError, match="no server extension hook"):
        server_extension.call("take_over")


def test_a_named_module_that_cannot_be_imported_stops_the_process(unloaded, monkeypatch):
    monkeypatch.setenv(server_extension.VARIABLE, "no_such_server_extension")

    with pytest.raises(SystemExit) as stopped:
        server_extension.call("identity")

    assert "FLOWFILE_SERVER_EXTENSION names no_such_server_extension" in str(stopped.value)


def test_before_import_runs_in_a_main_process_only(unloaded, monkeypatch):
    monkeypatch.setenv(server_extension.VARIABLE, TOY)
    monkeypatch.setattr(multiprocessing.current_process(), "name", "SpawnProcess-1")

    server_extension.before_import("worker")

    assert toy.calls == []
    assert server_extension._resolved is False

    monkeypatch.setattr(multiprocessing.current_process(), "name", "MainProcess")
    server_extension.before_import("worker")

    assert toy.calls == [("before_import", "worker")]


def test_a_server_is_joined_only_with_keys_of_its_own(monkeypatch):
    named = Identity(name="team-1", url="http://hub.example", issuer="http://hub.example")
    toy.use(monkeypatch, SimpleNamespace(identity=lambda: named))

    assert server_extension.identity() is named
    assert server_extension.joined() is None

    joined = Identity(name="team-1", url="http://hub.example", issuer="http://hub.example", node_id=3, joined=True)
    toy.use(monkeypatch, SimpleNamespace(identity=lambda: joined))

    assert server_extension.joined() is joined


@pytest.fixture
def storage_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(server_extension, "_marker", lambda: tmp_path / server_extension.MARKER_FILE)
    return tmp_path


def test_an_unmarked_storage_dir_is_served_with_or_without_an_extension(storage_dir, unloaded, monkeypatch):
    server_extension.require_marked()
    monkeypatch.setenv(server_extension.VARIABLE, TOY)
    server_extension.require_marked()


def test_a_marked_storage_dir_is_served_only_with_its_extension(storage_dir, unloaded, monkeypatch, caplog):
    server_extension.mark_storage_dir(TOY)
    server_extension.mark_storage_dir(TOY, storage_dir)

    with pytest.raises(SystemExit) as stopped:
        server_extension.require_marked()

    assert stopped.value.code == 2
    assert f"served only with the server extension {TOY}" in caplog.text
    assert f"{server_extension.VARIABLE}={TOY}" in caplog.text

    monkeypatch.setenv(server_extension.VARIABLE, "another_extension")
    with pytest.raises(SystemExit):
        server_extension.require_marked()

    monkeypatch.setenv(server_extension.VARIABLE, TOY)
    server_extension.require_marked()
