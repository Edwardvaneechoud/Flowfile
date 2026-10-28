import logging
import os
import socket
import sys
import uuid

import pytest

os.environ["TEST_MODE"] = "1"
os.environ.setdefault("FLOWFILE_INTERNAL_TOKEN", "flowfile-test-internal-token")

INTERNAL_AUTH_HEADERS = {"X-Flowfile-Internal": os.environ["FLOWFILE_INTERNAL_TOKEN"]}

from tests.utils import is_docker_available

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, ResultRef, WorkerLocation
from test_utils.postgres import fixtures as pg_fixtures

V1_HEADERS = {**INTERNAL_AUTH_HEADERS, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)}


def ref_location(ref: ResultRef) -> WorkerLocation:
    """Where the worker holds the result *ref* names."""
    return WorkerLocation.model_validate_json(ref.location)


def is_port_in_use(port, host="localhost"):
    """Check if a port is in use on the specified host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((host, port))
            return True
        except ConnectionRefusedError:
            return False


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger("flowfile_fixture")


@pytest.fixture(scope="session", autouse=True)
def shutdown_worker_pool():
    """Retire the module-singleton warm pool's members at session end.

    Tests that don't patch pool.task_pool lease members from the env-configured
    singleton (on by default; CI sets FLOWFILE_WORKER_POOL_SIZE), and a
    bare pytest process never runs the app lifespan that would shut it down.
    """
    yield
    from flowfile_worker.pool import task_pool

    task_pool.shutdown()


@pytest.fixture(scope="session", autouse=True)
def postgres_db():
    """
    Pytest fixture that ensures PostgreSQL container is running for the test session.
    Automatically starts and stops a PostgreSQL container with sample data.
    """
    if is_port_in_use(5433) or pg_fixtures.can_connect_to_db():
        print("PostgreSQL is already running on port 5433, skipping container creation")
        yield
        return

    elif not is_docker_available():
        print("Docker is not available, skipping PostgreSQL container creation")
        yield
        return

    with pg_fixtures.managed_postgres() as db_info:
        if not db_info:
            pytest.fail("PostgreSQL container could not be started")
        yield db_info


@pytest.fixture
def held():
    """Fresh ids a test submits or runs an operation under.

    When the test ends each id is released with the outputs ``<id>_0`` to ``<id>_3`` an operation holds under it, as
    ``DELETE /v1/results/{id}`` releases them, and its job entry is dropped.
    """
    from flowfile_worker import service, status_dict, status_dict_lock

    ids: list[str] = []

    def new_id() -> str:
        ids.append(uuid.uuid4().hex)
        return ids[-1]

    yield new_id
    for held_id in ids:
        for result_id in (*(f"{held_id}_{i}" for i in range(4)), held_id):
            service.release(result_id)
        with status_dict_lock:
            status_dict.pop(held_id, None)


@pytest.fixture
def unloadable_plan(tmp_path, monkeypatch) -> bytes:
    """A serialized plan whose UDF module is gone before the worker loads it, as with a core-only package."""
    import importlib
    import shutil

    import polars as pl

    module_name = f"core_only_udf_{tmp_path.name.replace('-', '_')}"
    (tmp_path / f"{module_name}.py").write_text("def plus_one(s):\n    return s + 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module(module_name)
    plan = pl.LazyFrame({"a": [1, 2]}).select(pl.col("a").map_batches(module.plus_one, return_dtype=pl.Int64))
    payload = plan.serialize()
    (tmp_path / f"{module_name}.py").unlink()
    shutil.rmtree(tmp_path / "__pycache__", ignore_errors=True)
    sys.modules.pop(module_name, None)
    return payload
