"""A process that cannot resolve the worker's internal token must degrade like one whose
worker is down: a cache probe raises ComputeUnavailable, instead of the
auth handler crashing every worker call with a bare ValueError (package-mode headless
runs, such as the WASM parity harness, mint no token and have no worker)."""

import pytest
import requests

from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops


@pytest.fixture
def no_internal_token(monkeypatch):
    import flowfile_core.auth.jwt as jwt_module

    monkeypatch.setenv("FLOWFILE_MODE", "package")
    monkeypatch.delenv("FLOWFILE_INTERNAL_TOKEN", raising=False)
    monkeypatch.setattr(jwt_module, "_internal_token", None)
    yield
    monkeypatch.setattr(jwt_module, "_internal_token", None)


def test_worker_call_raises_a_request_exception(no_internal_token):
    with pytest.raises(requests.RequestException) as excinfo:
        subops._worker_session.get("/status/abc", worker_url=WORKER_URL, timeout=1)
    assert isinstance(excinfo.value, subops.WorkerAuthUnavailable)
    assert "FLOWFILE_INTERNAL_TOKEN" in str(excinfo.value)


def test_results_exists_raises_compute_unavailable(no_internal_token):
    with pytest.raises(ComputeUnavailable):
        subops.results_exists("abc", worker_url=WORKER_URL)
