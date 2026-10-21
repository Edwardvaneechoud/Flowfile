"""``GET /capabilities`` and ``GET /v1/capabilities``: what this worker process can run, behind the internal token.

Beside the versions and plugins the handshake compares, both answer with what the worker declares: the operation
kinds it runs, the URI schemes it reads and writes (object storage and ``https``, the presigned URLs a blob is copied
to and from, and ``file`` only with ``FLOWFILE_WORKER_LOCAL_FILES=1``) and ``credential_resolution``.
"""

import importlib.metadata
import importlib.util
import logging
import platform
import sys
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.plugins import _resolve_plugin_path
from polars_grouper import graph_solver

from flowfile_worker import main
from shared._version import get_version
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, Capabilities
from tests.conftest import INTERNAL_AUTH_HEADERS

client = TestClient(main.app, headers=INTERNAL_AUTH_HEADERS)
v1_client = TestClient(main.app, headers={**INTERNAL_AUTH_HEADERS, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)})

both_routes = pytest.mark.parametrize(
    ("route_client", "path"), [(client, "/capabilities"), (v1_client, "/v1/capabilities")], ids=["legacy", "v1"]
)


def declared(route_client: TestClient, path: str) -> Capabilities:
    response = route_client.get(path)
    assert response.status_code == 200
    return Capabilities.model_validate(response.json())


@pytest.mark.worker
def test_capabilities_requires_internal_token():
    assert TestClient(main.app).get("/capabilities").status_code == 401


@pytest.mark.worker
def test_capabilities_reports_this_process():
    response = client.get("/capabilities")
    assert response.status_code == 200
    caps = Capabilities.model_validate(response.json())
    assert caps.protocol_version == PROTOCOL_VERSION == 1
    assert caps.app_version == get_version()
    assert caps.polars_version == pl.__version__
    assert caps.python_version == platform.python_version()
    assert caps.platform == f"{sys.platform}-{platform.machine()}"


@pytest.mark.worker
@both_routes
def test_capabilities_report_the_index_width_of_the_loaded_polars_runtime(route_client, path):
    caps = declared(route_client, path)
    assert caps.polars_runtime == ("64" if pl.get_index_type() == pl.UInt64 else "32")


@pytest.mark.worker
def test_capabilities_plugin_set_and_versions():
    caps = Capabilities.model_validate(client.get("/capabilities").json())
    assert set(caps.plugins) == {"polars_grouper", "polars_simed", "polars_ds", "deltalake", "pl_fuzzy_frame_match"}
    for name, version in caps.plugins.items():
        assert version == importlib.metadata.version(name)


@pytest.mark.worker
def test_capabilities_plugin_libs_are_prefix_relative_and_match_plan_bytes():
    caps = Capabilities.model_validate(client.get("/capabilities").json())
    assert set(caps.plugin_libs) == {"polars_grouper", "polars_ds"}
    for name, lib in caps.plugin_libs.items():
        package_dir = Path(importlib.util.find_spec(name).origin).parent
        assert lib == str(_resolve_plugin_path(package_dir))
        assert not Path(lib).is_absolute()
        assert (Path(sys.prefix) / lib).is_file()
    # The path is the one a plan using the plugin carries, byte for byte.
    plan = pl.LazyFrame({"a": ["x"], "b": ["y"]}).select(graph_solver(pl.col("a"), pl.col("b"))).serialize()
    assert caps.plugin_libs["polars_grouper"].encode() in plan
    assert sys.prefix.encode() not in plan


@pytest.mark.worker
@both_routes
def test_capabilities_name_the_operations_this_worker_runs(route_client, path):
    assert declared(route_client, path).operations == {
        "database_read",
        "kafka_read",
        "google_analytics_read",
        "rest_api_read",
        "database_write",
        "cloud_write",
        "file_write",
        "file_read",
        "fuzzy_join",
        "custom_node",
        "random_split",
        "ml_train",
        "ml_apply",
        "delta_write",
        "delta_merge",
        "delta_scd2",
        "viz_session",
        "polars_code",
    }


@pytest.mark.worker
@both_routes
def test_every_v1_operation_route_is_among_the_operations(route_client, path):
    routed = {p.removeprefix("/v1/ops/") for p in main.app.openapi()["paths"] if p.startswith("/v1/ops/")}

    assert len(routed) == 11
    assert routed <= declared(route_client, path).operations


@pytest.mark.worker
@both_routes
@pytest.mark.parametrize("value", [None, "0", ""], ids=["unset", "0", "empty"])
def test_without_local_files_the_worker_reads_object_storage_and_urls_and_writes_object_storage_and_https(
    monkeypatch, route_client, path, value
):
    if value is None:
        monkeypatch.delenv("FLOWFILE_WORKER_LOCAL_FILES", raising=False)
    else:
        monkeypatch.setenv("FLOWFILE_WORKER_LOCAL_FILES", value)

    caps = declared(route_client, path)

    assert caps.read_schemes == {"s3", "az", "gs", "http", "https"}
    assert caps.write_schemes == {"s3", "az", "gs", "https"}


@pytest.mark.worker
@both_routes
def test_local_files_add_the_file_scheme_on_the_next_request(monkeypatch, route_client, path):
    monkeypatch.delenv("FLOWFILE_WORKER_LOCAL_FILES", raising=False)
    assert "file" not in declared(route_client, path).read_schemes

    monkeypatch.setenv("FLOWFILE_WORKER_LOCAL_FILES", "1")
    caps = declared(route_client, path)

    assert caps.read_schemes == {"file", "s3", "az", "gs", "http", "https"}
    assert caps.write_schemes == {"file", "s3", "az", "gs", "https"}


@pytest.mark.worker
@both_routes
def test_the_worker_resolves_credentials_with_its_master_key(route_client, path):
    assert declared(route_client, path).credential_resolution == "master_key"


@pytest.mark.worker
def test_legacy_route_warns_on_foreign_protocol_version(caplog):
    with caplog.at_level(logging.WARNING, logger="FlowfileWorker"):
        response = client.get("/status/does-not-exist", headers={PROTOCOL_VERSION_HEADER: "99"})
    assert response.status_code != 401
    warnings = [r.message for r in caplog.records if PROTOCOL_VERSION_HEADER in r.message]
    assert len(warnings) == 1
    assert "99" in warnings[0] and str(PROTOCOL_VERSION) in warnings[0] and "/status/does-not-exist" in warnings[0]


@pytest.mark.worker
def test_legacy_route_is_silent_on_its_own_protocol_version(caplog):
    with caplog.at_level(logging.WARNING, logger="FlowfileWorker"):
        client.get("/status/does-not-exist", headers={PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)})
        client.get("/status/does-not-exist")
    assert not [r for r in caplog.records if PROTOCOL_VERSION_HEADER in r.message]
