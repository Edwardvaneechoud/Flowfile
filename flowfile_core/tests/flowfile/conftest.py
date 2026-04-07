import pytest

from tests.conftest import is_worker_running


@pytest.fixture(params=["local", "remote"])
def execution_location(request):
    if request.param == "remote" and not is_worker_running():
        pytest.skip("Worker not running")
    return request.param
