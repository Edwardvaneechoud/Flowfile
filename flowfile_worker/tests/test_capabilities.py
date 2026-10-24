"""``GET /capabilities``: what this worker process can run, behind the internal token."""

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
