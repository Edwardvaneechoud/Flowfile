import pytest
import requests

from flowfile_core.flowfile.execution.exceptions import (
    WorkerConnectionError,
    WorkerError,
    WorkerTaskError,
)
from flowfile_core.flowfile.execution.transport import WorkerTransport, get_default_transport


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text="", ok=None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text
        self.ok = ok if ok is not None else status_code < 400

    def json(self):
        return self._json_data


def _capture_requests(monkeypatch, response=None, exc=None):
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if exc is not None:
            raise exc
        return response or FakeResponse()

    monkeypatch.setattr(requests, "request", fake_request)
    return calls


def test_base_url_from_string():
    t = WorkerTransport(base_url="http://myworker:1234")
    assert t.base_url == "http://myworker:1234"


def test_base_url_from_callable_resolved_per_call():
    urls = iter(["http://a:1", "http://b:2"])
    t = WorkerTransport(base_url=lambda: next(urls))
    assert t.base_url == "http://a:1"
    assert t.base_url == "http://b:2"


def test_base_url_defaults_to_settings():
    from flowfile_core.configs.settings import WORKER_URL

    assert WorkerTransport().base_url == WORKER_URL


def test_ws_base_url_conversion():
    assert WorkerTransport(base_url="http://w:1").ws_base_url == "ws://w:1"
    assert WorkerTransport(base_url="https://w:1").ws_base_url == "wss://w:1"


def test_post_builds_url_and_passes_kwargs(monkeypatch):
    calls = _capture_requests(monkeypatch)
    t = WorkerTransport(base_url="http://w:1")
    t.post("/submit_query/", data=b"abc", headers={"X-Task-Id": "t1"})
    method, url, kwargs = calls[0]
    assert method == "post"
    assert url == "http://w:1/submit_query/"
    assert kwargs["data"] == b"abc"
    assert kwargs["headers"]["X-Task-Id"] == "t1"


def test_connection_error_raises_typed_error(monkeypatch):
    _capture_requests(monkeypatch, exc=requests.exceptions.ConnectionError("Connection refused"))
    t = WorkerTransport(base_url="http://w:1")
    with pytest.raises(WorkerConnectionError) as excinfo:
        t.get("/status/abc")
    assert "http://w:1/status/abc" in str(excinfo.value)


def test_worker_connection_error_is_requests_exception():
    # Legacy call sites catch requests.RequestException; the typed error must stay catchable there.
    err = WorkerConnectionError("nope")
    assert isinstance(err, requests.exceptions.ConnectionError)
    assert isinstance(err, requests.RequestException)
    assert isinstance(err, WorkerError)


def test_get_status_returns_status(monkeypatch):
    payload = {
        "background_task_id": "t1",
        "status": "Completed",
        "file_ref": "t1",
        "progress": 100,
        "results": None,
    }
    _capture_requests(monkeypatch, response=FakeResponse(200, payload))
    status = WorkerTransport(base_url="http://w:1").get_status("t1")
    assert status.status == "Completed"
    assert status.background_task_id == "t1"


def test_get_status_raises_on_error(monkeypatch):
    _capture_requests(monkeypatch, response=FakeResponse(404, text="not found"))
    with pytest.raises(WorkerTaskError):
        WorkerTransport(base_url="http://w:1").get_status("t1")


def test_results_exist_paths(monkeypatch):
    t = WorkerTransport(base_url="http://w:1")
    _capture_requests(monkeypatch, response=FakeResponse(200, {"status": "Completed"}))
    assert t.results_exist("t1") is True
    _capture_requests(monkeypatch, response=FakeResponse(200, {"status": "Processing"}))
    assert t.results_exist("t1") is False
    _capture_requests(monkeypatch, exc=requests.exceptions.ConnectionError("refused"))
    assert t.results_exist("t1") is False


def test_cancel_and_clear_task(monkeypatch):
    t = WorkerTransport(base_url="http://w:1")
    calls = _capture_requests(monkeypatch, response=FakeResponse(200))
    assert t.cancel_task("t1") is True
    assert t.clear_task("t1") is True
    assert [(m, u) for m, u, _ in calls] == [
        ("post", "http://w:1/cancel_task/t1"),
        ("delete", "http://w:1/clear_task/t1"),
    ]


def test_default_transport_is_singleton():
    assert get_default_transport() is get_default_transport()


def test_single_file_mode_url_gets_worker_prefix():
    from flowfile_core.configs import settings

    original = bool(settings.SINGLE_FILE_MODE)
    settings.SINGLE_FILE_MODE.set(True)
    try:
        url = settings.get_default_worker_url(worker_port=63578)
        assert url.endswith("/worker")
        t = WorkerTransport(base_url=url)
        assert t.base_url.endswith("/worker")
        assert t.ws_base_url.startswith("ws://") and t.ws_base_url.endswith("/worker")
    finally:
        settings.SINGLE_FILE_MODE.set(original)
