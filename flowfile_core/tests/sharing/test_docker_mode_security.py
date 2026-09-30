"""Docker-mode security regressions: dtype strings are data, editor routes still look flows up without the
caller's session (strict xfails), and authoring custom-node source is admin-only.

The dtype probe is ``pl.Config.set_tbl_rows(7)``: harmless, but evaluating it writes
``POLARS_FMT_MAX_ROWS``, so an evaluated call is observable without running anything risky.
"""

import os
import uuid
from pathlib import Path

import polars as pl
import pytest

from flowfile_core import flow_file_handler
from flowfile_core.flowfile.code_generator.native_handlers import _dtype_expr
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import (
    cast_str_to_polars_type,
    safe_eval_pl_type,
)
from flowfile_core.flowfile.user_defined.registry import registry
from shared.storage_config import storage

_TMP_FLOW_DIR = Path.cwd() / "flowfile_core/tests/support_files/flows/tmp"
_PROBE = "pl.Config.set_tbl_rows(7)"
_PROBE_ENV = "POLARS_FMT_MAX_ROWS"
_EXEC_MARKER_ENV = "FLOWFILE_TEST_CUSTOM_NODE_EXEC"


@pytest.fixture
def probe_env(monkeypatch):
    monkeypatch.delenv(_PROBE_ENV, raising=False)
    yield
    os.environ.pop(_PROBE_ENV, None)


@pytest.fixture
def flow_session_cleanup():
    before = {f.flow_id for f in flow_file_handler.flowfile_flows}
    yield
    for f in list(flow_file_handler.flowfile_flows):
        if f.flow_id not in before:
            flow_file_handler.delete_flow(f.flow_id)


@pytest.fixture
def own_flow(flow_session_cleanup):
    _TMP_FLOW_DIR.mkdir(parents=True, exist_ok=True)
    created: list[Path] = []

    def _make(client) -> int:
        path = _TMP_FLOW_DIR / f"sec_{uuid.uuid4().hex[:10]}.yaml"
        created.append(path)
        resp = client.post("/editor/create_flow/", params={"flow_path": str(path), "register_in_catalog": False})
        assert resp.status_code == 200, resp.text
        return resp.json()

    yield _make
    for path in created:
        path.unlink(missing_ok=True)


def _manual_input(client, flow_id: int, columns: list[dict], data: list[list]):
    client.post(
        "/editor/add_node/",
        params={"flow_id": flow_id, "node_id": 1, "node_type": "manual_input", "pos_x": 0, "pos_y": 0},
    )
    return client.post(
        "/update_settings/",
        params={"node_type": "manual_input"},
        json={
            "flow_id": flow_id,
            "node_id": 1,
            "pos_x": 0,
            "pos_y": 0,
            "raw_data_format": {"columns": columns, "data": data},
        },
    )


@pytest.mark.parametrize(
    "dtype",
    [
        pl.Int64,
        pl.Datetime("us", "UTC"),
        pl.List(pl.Int64),
        pl.Struct({"a b": pl.Int64, "c": pl.List(pl.String)}),
        pl.Array(pl.Int64, (2, 3)),
        pl.Enum(["a", "b'c"]),
        pl.Decimal(10, 2),
        pl.Duration("ms"),
    ],
    ids=str,
)
def test_polars_dtype_strings_round_trip(dtype):
    assert safe_eval_pl_type(str(dtype)) == dtype


def test_pl_prefixed_dtype_strings_parse():
    assert safe_eval_pl_type("pl.List(pl.Int64)") == pl.List(pl.Int64)
    assert safe_eval_pl_type(" pl.Datetime(time_unit='us', time_zone='UTC')") == pl.Datetime("us", "UTC")
    with pytest.raises(ValueError):
        safe_eval_pl_type("pl.List(Int64)", bare_names=False)


@pytest.mark.parametrize(
    "text",
    [_PROBE, "pl", "pl.col", "Int64.__class__", "pl._utils", "List(pl.col('a'))", "Enum(categories=x)", "Field('a', Int64)"],
)
def test_non_dtype_strings_are_rejected(text, probe_env):
    with pytest.raises(ValueError, match="Failed to safely evaluate"):
        safe_eval_pl_type(text)
    assert _PROBE_ENV not in os.environ


def test_cast_str_to_polars_type_does_not_evaluate_calls(probe_env):
    assert cast_str_to_polars_type(_PROBE) == pl.String
    assert _PROBE_ENV not in os.environ, "a dtype string was evaluated as a Python call"


def test_codegen_dtype_expr_does_not_evaluate_calls(probe_env):
    assert _dtype_expr(_PROBE.removeprefix("pl.")) is None
    assert _PROBE_ENV not in os.environ, "a dtype string was evaluated as a Python call"


def test_codegen_dtype_expr_keeps_emittable_forms():
    assert _dtype_expr("Int64") == "fl.Int64"
    assert _dtype_expr("Datetime(time_unit='us', time_zone=None)") == "fl.Datetime(time_unit='us', time_zone=None)"
    assert _dtype_expr("List(Int64)") is None


def test_dtype_string_via_update_settings_is_not_evaluated(users, client_for, own_flow, probe_env):
    bob = client_for("bob")
    flow_id = own_flow(bob)
    _manual_input(bob, flow_id, [{"name": "a", "data_type": _PROBE}], [[1, 2]])
    bob.get("/node/data", params={"flow_id": flow_id, "node_id": 1})
    assert _PROBE_ENV not in os.environ, "a non-admin's node settings evaluated a dtype string as code in core"


_UNSCOPED = pytest.mark.xfail(strict=True, reason="editor routes look flows up by id without the caller's session")


@_UNSCOPED
def test_node_data_not_readable_by_other_user(users, client_for, own_flow):
    alice, bob = client_for("alice"), client_for("bob")
    flow_id = own_flow(alice)
    resp = _manual_input(alice, flow_id, [{"name": "secret", "data_type": "String"}], [["alice-only"]])
    assert resp.status_code == 200, resp.text
    assert alice.get("/node/data", params={"flow_id": flow_id, "node_id": 1}).status_code == 200

    resp = bob.get("/node/data", params={"flow_id": flow_id, "node_id": 1})
    assert resp.status_code == 404, f"bob read alice's node preview: {resp.status_code} {resp.text[:200]}"


@_UNSCOPED
def test_update_settings_not_writable_by_other_user(users, client_for, own_flow):
    alice, bob = client_for("alice"), client_for("bob")
    flow_id = own_flow(alice)
    _manual_input(alice, flow_id, [{"name": "a", "data_type": "String"}], [["x"]])

    resp = bob.post(
        "/update_settings/",
        params={"node_type": "manual_input"},
        json={
            "flow_id": flow_id,
            "node_id": 1,
            "raw_data_format": {"columns": [{"name": "a", "data_type": "String"}], "data": [["from-bob"]]},
        },
    )
    assert resp.status_code == 404, f"bob edited alice's flow: {resp.status_code} {resp.text[:200]}"


_MARKER_NODE = f'''
import os

import polars as pl

from flowfile import node_designer as nd

os.environ["{_EXEC_MARKER_ENV}"] = "1"


class SecMarkerNode{{suffix}}(nd.CustomNodeBase):
    node_name: str = "Sec Marker {{suffix}}"
    node_category: str = "Testing"
    settings_schema: nd.NodeSettings = nd.NodeSettings(
        main=nd.Section(title="Main", value=nd.TextInput(label="Value", default="x")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''


@pytest.fixture
def marker_node(monkeypatch):
    monkeypatch.delenv(_EXEC_MARKER_ENV, raising=False)
    storage.user_defined_nodes_directory.mkdir(parents=True, exist_ok=True)
    suffix = uuid.uuid4().hex[:8]
    stem = f"sec_marker_{suffix}"
    yield stem, _MARKER_NODE.format(suffix=suffix)
    registry.remove_file(f"{stem}.py")
    (storage.user_defined_nodes_directory / f"{stem}.py").unlink(missing_ok=True)
    os.environ.pop(_EXEC_MARKER_ENV, None)


def test_non_admin_cannot_save_custom_node(users, client_for, marker_node):
    stem, code = marker_node
    resp = client_for("bob").post("/user_defined_components/save-custom-node", json={"file_name": stem, "code": code})
    assert resp.status_code == 403, resp.text
    assert not (storage.user_defined_nodes_directory / f"{stem}.py").exists()
    assert registry.get(stem) is None


def test_non_admin_cannot_delete_custom_node(users, client_for, marker_node):
    stem, code = marker_node
    saved = client_for("admin").post("/user_defined_components/save-custom-node", json={"file_name": stem, "code": code})
    assert saved.status_code == 200, saved.text

    resp = client_for("bob").delete(f"/user_defined_components/delete-custom-node/{stem}.py")
    assert resp.status_code == 403, resp.text
    assert (storage.user_defined_nodes_directory / f"{stem}.py").exists()


def test_non_admin_cannot_dry_run_custom_node(users, client_for, marker_node):
    _, code = marker_node
    resp = client_for("bob").post("/user_defined_components/dry-run", json={"code": code})
    assert resp.status_code == 403, resp.text


def test_admin_can_save_and_delete_custom_node(users, client_for, marker_node):
    admin = client_for("admin")
    stem, code = marker_node
    resp = admin.post("/user_defined_components/save-custom-node", json={"file_name": stem, "code": code})
    assert resp.status_code == 200, resp.text
    assert os.environ.get(_EXEC_MARKER_ENV) is None, "saving must not exec the node module"

    resp = admin.delete(f"/user_defined_components/delete-custom-node/{stem}.py")
    assert resp.status_code == 200, resp.text
