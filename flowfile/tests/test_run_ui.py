"""``flowfile run ui`` serves where ``--host`` and ``--port`` say, takes its server extension's flags, and serves only
the editor.

A host other machines reach is served only in a server mode, where everyone signs in; in the default mode it is
refused before anything is served. A server extension that binds the socket first has it handed to the server.

The command is parsed with ``start_server`` stubbed, and ``start_server`` runs with ``flowfile_core.main.run``
stubbed and a bare app in place of core's, so no port is bound and no browser opens; every setting it changes is
restored after the test.
"""

import os
import sys
from urllib.parse import urlsplit

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import flowfile.web as web
import flowfile_core.main as core_main
from flowfile.__main__ import main
from flowfile_core.configs import settings
from shared.tests import toy_server_extension as toy


def test_run_ui_lists_its_server_extension_s_flags_in_its_help(monkeypatch, capsys):
    toy.use(monkeypatch, toy)
    monkeypatch.setattr(sys, "argv", ["flowfile", "run", "ui", "--help"])

    with pytest.raises(SystemExit) as exited:
        main()

    assert exited.value.code == 0
    out = capsys.readouterr().out
    for flag in ("--toy", "--host", "--port"):
        assert flag in out


def test_run_ui_without_an_extension_refuses_a_flag_it_does_not_know(monkeypatch, capsys):
    toy.use(monkeypatch, None)
    monkeypatch.setattr(sys, "argv", ["flowfile", "run", "ui", "--toy", "t"])

    with pytest.raises(SystemExit) as exited:
        main()

    assert exited.value.code == 2
    assert "unrecognized arguments: --toy" in capsys.readouterr().err


def test_run_ui_hands_host_and_port_to_the_server_and_takes_its_extension_s_flags(monkeypatch):
    toy.use(monkeypatch, toy)
    started = []
    monkeypatch.setattr(web, "start_server", lambda **kwargs: started.append(kwargs))
    argv = ["flowfile", "run", "ui", "--host", "0.0.0.0", "--port", "8080", "--no-browser", "--toy", "t"]
    monkeypatch.setattr(sys, "argv", argv)

    main()

    assert started == [{"host": "0.0.0.0", "port": 8080, "open_browser": False}]
    assert settings.parse_args().host == "0.0.0.0"


def test_run_ui_without_flags_serves_this_machine_only(monkeypatch):
    started = []
    monkeypatch.setattr(web, "start_server", lambda **kwargs: started.append(kwargs))
    monkeypatch.setattr(sys, "argv", ["flowfile", "run", "ui", "--no-browser"])

    main()

    assert started == [{"host": "127.0.0.1", "port": 63578, "open_browser": False}]


@pytest.mark.parametrize(
    "flags,port", [([], 63578), (["--port", "8080"], 8080)], ids=["no port", "a port given"]
)
def test_run_core_serves_63578_unless_given_a_port(monkeypatch, flags, port):
    started = []
    monkeypatch.setattr(core_main, "run", lambda host, port: started.append((host, port)))
    monkeypatch.setattr(sys, "argv", ["flowfile", "run", "core", "--host", "127.0.0.1", *flags])

    main()

    assert started == [("127.0.0.1", port)]


@pytest.fixture
def editor_files(tmp_path, monkeypatch):
    """A built editor of two files, and a file beside its folder that it must never serve."""
    static = tmp_path / "static"
    (static / "assets").mkdir(parents=True)
    (static / "index.html").write_text("<html>editor</html>", encoding="utf-8")
    (static / "assets" / "app.js").write_text("console.log('editor')", encoding="utf-8")
    (tmp_path / "secret.txt").write_text("not the editor's", encoding="utf-8")
    monkeypatch.setattr(web, "static_dir", static)
    return tmp_path


@pytest.fixture
def served(monkeypatch) -> dict:
    """Runs ``start_server`` up to uvicorn: the app it extends and the host and port it hands core's ``run``."""
    seen: dict = {"app": FastAPI()}
    monkeypatch.setattr(core_main, "app", seen["app"])
    monkeypatch.setattr(core_main, "run", lambda host, port: seen.update(host=host, port=port))
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", settings.OFFLOAD_TO_WORKER.value)
    monkeypatch.setattr(settings.SINGLE_FILE_MODE, "value", settings.SINGLE_FILE_MODE.value)
    monkeypatch.setattr(settings, "WORKER_URL", settings.WORKER_URL)
    monkeypatch.setenv("FLOWFILE_MODE", os.environ.get("FLOWFILE_MODE", "electron"))
    monkeypatch.setenv("FLOWFILE_WORKER_LOCAL_FILES", "0")
    return seen


@pytest.mark.parametrize("mode", ["server", "docker"])
def test_a_server_mode_serves_on_any_host_and_port_and_marks_the_editor_and_worker_it_mounts(
    mode, served, editor_files, monkeypatch
):
    monkeypatch.setenv("FLOWFILE_MODE", mode)

    web.start_server(host="0.0.0.0", port=8080, open_browser=False)

    assert (served["host"], served["port"]) == ("0.0.0.0", 8080)
    assert settings.WORKER_URL == "http://127.0.0.1:8080/worker"
    assert (served["app"].state.editor_path, served["app"].state.worker_path) == ("/ui", "/worker")


def test_run_ui_on_a_host_other_machines_reach_is_refused_in_the_default_mode(served, monkeypatch, capsys):
    monkeypatch.delenv("FLOWFILE_MODE", raising=False)
    argv = ["flowfile", "run", "ui", "--host", "0.0.0.0", "--no-browser"]
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit) as exited:
        main()

    assert exited.value.code == 2
    assert "host" not in served
    assert not hasattr(served["app"].state, "worker_path")
    err = capsys.readouterr().err
    assert "Not serving on 0.0.0.0" in err
    assert "FLOWFILE_MODE=docker" in err


@pytest.mark.parametrize("mode", ["electron", "package"])
@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.168.1.10"])
def test_outside_a_server_mode_no_host_but_a_loopback_one_is_served(mode, host, served, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", mode)
    worker_url = settings.WORKER_URL

    with pytest.raises(SystemExit) as exited:
        web.start_server(host=host, port=8080, open_browser=False)

    assert exited.value.code == 2
    assert "host" not in served
    assert settings.WORKER_URL == worker_url
    assert not hasattr(served["app"].state, "worker_path")


@pytest.mark.parametrize("host", ["localhost", "127.0.0.2", "::1"])
def test_the_default_mode_serves_a_loopback_host(host, served, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")

    web.start_server(host=host, port=8080, open_browser=False)

    assert (served["host"], served["port"]) == (host, 8080)
    assert urlsplit(settings.WORKER_URL).port == 8080


def test_a_server_without_the_editor_s_files_marks_no_editor(served, tmp_path, monkeypatch):
    monkeypatch.setattr(web, "static_dir", tmp_path / "missing")

    web.start_server(open_browser=False)

    assert (served["host"], served["port"]) == ("127.0.0.1", 63578)
    assert not hasattr(served["app"].state, "editor_path")
    assert served["app"].state.worker_path == "/worker"


def test_the_editor_is_served_at_ui_slash_and_never_a_file_outside_its_folder(served, editor_files):
    web.start_server(open_browser=False)
    client = TestClient(served["app"])

    for path in ("/ui", "/ui/", "/ui/#token=t", "/ui/flows/1"):
        response = client.get(path, follow_redirects=False)
        assert (response.status_code, response.text) == (200, "<html>editor</html>"), path
    assert client.get("/ui/assets/app.js").text == "console.log('editor')"
    for path in (f"/ui/{editor_files / 'secret.txt'}", "/ui/..%2Fsecret.txt", "/ui/%2Fetc%2Fhosts"):
        response = client.get(path, follow_redirects=False)
        assert (response.status_code, response.text) == (200, "<html>editor</html>"), path
