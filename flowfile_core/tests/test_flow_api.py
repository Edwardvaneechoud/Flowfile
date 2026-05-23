"""Tests for hosting flows as HTTP data APIs.

Covers:
- the api_response sink node round-tripping and being executed,
- the synchronous runner (records + columns serialization, parameter filtering),
- typed parameter validation,
- API-key hashing + the verify_api_key auth dependency,
- the publish / key-management endpoints (called directly with an in-memory DB).
"""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from flowfile_core.auth import api_key as api_key_mod
from flowfile_core.database import models as db_models
from flowfile_core.flowfile import api_runner
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.routes import flow_api
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.flow_api_schema import (
    ApiEndpointCreate,
    ApiKeyCreate,
    ApiParamSpec,
)
from flowfile_core.schemas.schemas import FlowParameter

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _build_and_save_flow(path, orientation: str = "records") -> None:
    """manual_input -> polars filter on ${region} -> api_response, saved to *path*."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=1,
            name="api_flow",
            path=str(path),
            execution_mode="Development",
            execution_location="local",
        )
    )
    graph = handler.get_flow(1)
    graph.flow_settings.parameters = [FlowParameter(name="region", default_value="EU")]

    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"region": "EU", "v": 1}, {"region": "US", "v": 2}]),
        )
    )

    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=1,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.filter(pl.col('region') == '${region}')"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=3, node_type="api_response"))
    graph.add_api_response(
        input_schema.NodeApiResponse(flow_id=1, node_id=3, depending_on_id=2, orientation=orientation)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

    graph.save_flow(str(path))


@pytest.fixture
def db_session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    db_models.Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Runner: execution + serialization + parameter filtering
# ---------------------------------------------------------------------------


def test_run_flow_as_api_records_with_param(tmp_path):
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, "records")

    specs = [ApiParamSpec(name="region", type="string")]
    out = api_runner.run_flow_as_api(str(flow_path), owner_id=1, param_specs=specs, query={"region": "US"})

    assert out["orientation"] == "records"
    assert out["row_count"] == 1
    assert out["data"] == [{"region": "US", "v": 2}]


def test_run_flow_as_api_uses_flow_default_when_param_omitted(tmp_path):
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, "records")

    specs = [ApiParamSpec(name="region", type="string")]
    out = api_runner.run_flow_as_api(str(flow_path), owner_id=1, param_specs=specs, query={})

    assert out["data"] == [{"region": "EU", "v": 1}]


def test_run_flow_as_api_columns(tmp_path):
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, "columns")

    out = api_runner.run_flow_as_api(
        str(flow_path), owner_id=1, param_specs=[ApiParamSpec(name="region")], query={"region": "EU"}
    )

    assert out["orientation"] == "columns"
    assert out["data"] == {"region": ["EU"], "v": [1]}
    assert out["row_count"] == 1


# ---------------------------------------------------------------------------
# Typed parameter validation
# ---------------------------------------------------------------------------


def test_resolve_params_typed_coercion():
    specs = [
        ApiParamSpec(name="n", type="integer"),
        ApiParamSpec(name="r", type="enum", enum_values=["a", "b"]),
    ]
    assert api_runner.resolve_params(specs, {"n": "5", "r": "a"}) == {"n": "5", "r": "a"}

    with pytest.raises(api_runner.ApiParamError):
        api_runner.resolve_params(specs, {"n": "not-an-int", "r": "a"})
    with pytest.raises(api_runner.ApiParamError):
        api_runner.resolve_params(specs, {"n": "5", "r": "not-in-enum"})


def test_resolve_params_required_and_default():
    with pytest.raises(api_runner.ApiParamError):
        api_runner.resolve_params([ApiParamSpec(name="region", required=True)], {})

    assert api_runner.resolve_params([ApiParamSpec(name="region", default="EU")], {}) == {"region": "EU"}


def test_boolean_coercion():
    spec = ApiParamSpec(name="b", type="boolean")
    assert api_runner._coerce(spec, "YES") == "true"
    assert api_runner._coerce(spec, "0") == "false"
    with pytest.raises(api_runner.ApiParamError):
        api_runner._coerce(spec, "maybe")


# ---------------------------------------------------------------------------
# API keys + auth dependency
# ---------------------------------------------------------------------------


def test_api_key_hash_roundtrip():
    raw, key_hash, prefix = api_key_mod.generate_api_key()
    assert raw.startswith("ffk_")
    assert prefix == raw[:12]
    assert api_key_mod.hash_api_key(raw) == key_hash
    assert raw not in key_hash  # hashed, not reversible


def _make_endpoint_with_key(db_session, slug="sales", enabled=True, key_enabled=True, expires_at=None):
    ep = db_models.FlowApiEndpoint(registration_id=1, owner_id=1, slug=slug, enabled=enabled)
    db_session.add(ep)
    db_session.commit()
    db_session.refresh(ep)
    raw, key_hash, prefix = api_key_mod.generate_api_key()
    key = db_models.FlowApiKey(
        endpoint_id=ep.id,
        owner_id=1,
        name="k",
        key_hash=key_hash,
        key_prefix=prefix,
        enabled=key_enabled,
        expires_at=expires_at,
    )
    db_session.add(key)
    db_session.commit()
    return ep, raw


def test_verify_api_key_valid(db_session):
    ep, raw = _make_endpoint_with_key(db_session)
    resolved = api_key_mod.verify_api_key(slug="sales", x_api_key=raw, db=db_session)
    assert resolved.id == ep.id
    db_session.refresh(resolved)
    assert resolved is not None


@pytest.mark.parametrize("bad", ["missing", "wrong_token", "wrong_slug", "disabled_key", "disabled_endpoint"])
def test_verify_api_key_rejections(db_session, bad):
    import datetime

    expires = datetime.datetime(2000, 1, 1) if bad == "expired" else None
    ep, raw = _make_endpoint_with_key(
        db_session,
        enabled=(bad != "disabled_endpoint"),
        key_enabled=(bad != "disabled_key"),
        expires_at=expires,
    )
    token = None if bad == "missing" else ("ffk_definitely_wrong" if bad == "wrong_token" else raw)
    slug = "other" if bad == "wrong_slug" else "sales"

    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug=slug, x_api_key=token, db=db_session)
    assert exc.value.status_code == 401


def test_verify_api_key_expired(db_session):
    import datetime

    ep, raw = _make_endpoint_with_key(db_session, expires_at=datetime.datetime(2000, 1, 1))
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug="sales", x_api_key=raw, db=db_session)
    assert exc.value.status_code == 401


# ---------------------------------------------------------------------------
# Management endpoints (publish + keys)
# ---------------------------------------------------------------------------


def test_publish_endpoint_and_keys(db_session, tmp_path):
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, "records")
    reg = db_models.FlowRegistration(flow_uuid=str(uuid4()), name="api_flow", flow_path=str(flow_path), owner_id=1)
    db_session.add(reg)
    db_session.commit()
    db_session.refresh(reg)
    user = SimpleNamespace(id=1)

    out = flow_api.publish_endpoint(
        ApiEndpointCreate(
            registration_id=reg.id, slug="Sales", parameters=[ApiParamSpec(name="region", type="string")]
        ),
        current_user=user,
        db=db_session,
    )
    assert out.slug == "sales"  # normalized
    assert out.path == "/api/data/sales"
    assert out.response_node_id == 3
    assert [p.name for p in out.parameters] == ["region"]

    # one endpoint per flow
    with pytest.raises(HTTPException) as exc:
        flow_api.publish_endpoint(
            ApiEndpointCreate(registration_id=reg.id, slug="sales2"), current_user=user, db=db_session
        )
    assert exc.value.status_code == 409

    # key minting returns the raw token exactly once
    created = flow_api.create_key(out.id, ApiKeyCreate(name="prod"), current_user=user, db=db_session)
    assert created.api_key.startswith("ffk_")
    assert created.key_prefix == created.api_key[:12]

    # listing keys never exposes the raw token
    keys = flow_api.list_keys(out.id, current_user=user, db=db_session)
    assert len(keys) == 1
    assert not hasattr(keys[0], "api_key")


def test_publish_requires_api_response_node(db_session, tmp_path):
    """A flow without an api_response node cannot be published."""
    handler = FlowfileHandler()
    flow_path = tmp_path / "no_api.yaml"
    handler.register_flow(
        schemas.FlowSettings(flow_id=2, name="no_api", path=str(flow_path), execution_location="local")
    )
    graph = handler.get_flow(2)
    graph.add_node_promise(input_schema.NodePromise(flow_id=2, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=2, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]))
    )
    graph.save_flow(str(flow_path))

    reg = db_models.FlowRegistration(flow_uuid=str(uuid4()), name="no_api", flow_path=str(flow_path), owner_id=1)
    db_session.add(reg)
    db_session.commit()
    db_session.refresh(reg)

    with pytest.raises(HTTPException) as exc:
        flow_api.publish_endpoint(
            ApiEndpointCreate(registration_id=reg.id, slug="noapi"), current_user=SimpleNamespace(id=1), db=db_session
        )
    assert exc.value.status_code == 400
