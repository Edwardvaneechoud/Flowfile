"""Tests for hosting flows as HTTP data APIs.

Covers:
- the api_response sink node round-tripping and being executed,
- the synchronous runner (records + columns serialization, parameter filtering),
- typed parameter validation,
- API-key hashing + the verify_api_key auth dependency,
- the publish / key-management endpoints (called directly with an in-memory DB).
"""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import polars as pl
import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from flowfile_core.auth import api_key as api_key_mod
from flowfile_core.database import models as db_models
from flowfile_core.flowfile import api_consumer_manager, api_runner
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.routes import flow_api
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.flow_api_schema import (
    ApiEndpointCreate,
    ApiKeyCreate,
    ApiKeyUpdate,
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


@pytest.fixture(autouse=True)
def _force_local_api_execution(monkeypatch):
    """Keep API runs in-core for hermetic tests (no live worker).

    ``run_flow_as_api`` now defaults to the global, worker-aware execution location;
    the worker-offload path is exercised directly in the ``_serialize`` tests below.
    """
    monkeypatch.setattr(api_runner, "get_global_execution_location", lambda: "local")


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


def test_params_inherited_from_flow_without_config(tmp_path):
    """Flow ${name} params are accepted even when the endpoint declares no param specs."""
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, "records")

    # No param_specs at all — the runner must still inherit `region` from the flow.
    out = api_runner.run_flow_as_api(str(flow_path), owner_id=1, param_specs=[], query={"region": "US"})
    assert out["data"] == [{"region": "US", "v": 2}]


def test_run_flow_as_api_columns(tmp_path):
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, "columns")

    out = api_runner.run_flow_as_api(
        str(flow_path), owner_id=1, param_specs=[ApiParamSpec(name="region")], query={"region": "EU"}
    )

    assert out["orientation"] == "columns"
    assert out["data"] == {"region": ["EU"], "v": [1]}
    assert out["row_count"] == 1


def test_serialize_offloads_final_collect_to_worker_when_remote(monkeypatch):
    """With a worker available, the terminal collect is shipped to it (ExternalDfFetcher),
    not run in core."""
    df = pl.DataFrame({"region": ["US"], "v": [2]})
    used = {}

    class _FakeFetcher:
        has_error = False
        error_code = 0
        error_description = None

        def __init__(self, *, lf, **_kwargs):
            used["called"] = True
            self._lf = lf  # the (capped) lazy plan; stands in for the worker's IPC result

        def get_result(self):
            return self._lf

    monkeypatch.setattr(api_runner, "ExternalDfFetcher", _FakeFetcher)

    def _local_collect(n_records=None):
        used["local"] = True  # must NOT happen on the remote path
        return df

    data = SimpleNamespace(data_frame=df.lazy(), collect=_local_collect)
    flow = SimpleNamespace(flow_settings=SimpleNamespace(execution_location="remote"), flow_id=1)
    api_node = SimpleNamespace(hash="abc", node_id=3)
    settings = SimpleNamespace(orientation="records", max_rows=None)

    out = api_runner._serialize(data, settings, flow, api_node)
    assert used.get("called") is True  # offloaded to the worker
    assert used.get("local") is None  # core did not run the heavy collect
    assert out == {"data": [{"region": "US", "v": 2}], "row_count": 1, "orientation": "records"}


def test_serialize_falls_back_to_local_when_worker_unreachable(monkeypatch):
    """A worker that can't be reached degrades to an in-core collect, not a 500."""
    df = pl.DataFrame({"region": ["EU"], "v": [1]})

    def _unreachable(*_a, **_k):
        raise ConnectionError("worker down")

    monkeypatch.setattr(api_runner, "ExternalDfFetcher", _unreachable)
    captured = {}

    def _local_collect(n_records=None):
        captured["local"] = True
        return df

    data = SimpleNamespace(data_frame=df.lazy(), collect=_local_collect)
    flow = SimpleNamespace(flow_settings=SimpleNamespace(execution_location="remote"), flow_id=1)
    api_node = SimpleNamespace(hash="abc", node_id=3)
    settings = SimpleNamespace(orientation="records", max_rows=None)

    out = api_runner._serialize(data, settings, flow, api_node)
    assert captured.get("local") is True  # fell back to an in-core collect
    assert out == {"data": [{"region": "EU", "v": 1}], "row_count": 1, "orientation": "records"}


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


def _make_endpoint_with_key(
    db_session, slug="sales", enabled=True, key_enabled=True, expires_at=None, consumer_enabled=True
):
    """Create an endpoint + its implicit consumer + grant + a key bound to that consumer.

    Auth now resolves key -> consumer -> grant -> endpoint, so a bare endpoint+key is
    no longer authenticatable; the consumer + grant are what make the key valid.
    """
    ep = db_models.FlowApiEndpoint(registration_id=1, owner_id=1, slug=slug, enabled=enabled)
    db_session.add(ep)
    db_session.commit()
    db_session.refresh(ep)
    consumer = api_consumer_manager.get_or_create_implicit_consumer(db_session, ep)
    consumer.enabled = consumer_enabled
    raw, key_hash, prefix = api_key_mod.generate_api_key()
    key = db_models.FlowApiKey(
        consumer_id=consumer.id,
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


@pytest.mark.parametrize("bad", ["missing", "wrong_token", "wrong_slug", "disabled_key"])
def test_verify_api_key_rejections(db_session, bad):
    ep, raw = _make_endpoint_with_key(db_session, key_enabled=(bad != "disabled_key"))
    token = None if bad == "missing" else ("ffk_definitely_wrong" if bad == "wrong_token" else raw)
    slug = "other" if bad == "wrong_slug" else "sales"

    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug=slug, x_api_key=token, db=db_session)
    assert exc.value.status_code == 401


def test_verify_api_key_disabled_endpoint_is_403(db_session):
    """A valid key for a disabled endpoint gets a clear 403, not a misleading 401."""
    ep, raw = _make_endpoint_with_key(db_session, enabled=False)
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug="sales", x_api_key=raw, db=db_session)
    assert exc.value.status_code == 403
    assert "disabled" in exc.value.detail.lower()


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


@pytest.mark.asyncio
async def test_owner_test_run(db_session, tmp_path):
    from flowfile_core.schemas.flow_api_schema import ApiTestRequest

    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, "records")
    reg = db_models.FlowRegistration(
        flow_uuid=str(uuid4()), name="api_flow", flow_path=str(flow_path), owner_id=1
    )
    db_session.add(reg)
    db_session.commit()
    db_session.refresh(reg)
    user = SimpleNamespace(id=1)

    out = flow_api.publish_endpoint(
        ApiEndpointCreate(
            registration_id=reg.id, slug="sales", parameters=[ApiParamSpec(name="region", type="string")]
        ),
        current_user=user,
        db=db_session,
    )
    assert out.flow_name == "api_flow"

    result = await flow_api.test_endpoint(
        out.id, body=ApiTestRequest(params={"region": "US"}), current_user=user, db=db_session
    )
    assert result["row_count"] == 1
    assert result["data"] == [{"region": "US", "v": 2}]


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


# ---------------------------------------------------------------------------
# Public-endpoint hardening (error hiding, concurrency cap, key toggle, races)
# ---------------------------------------------------------------------------


def _publish_via_db(db_session, tmp_path, slug="sales"):
    """Build + save a real flow, register it, and publish it. Returns (reg, endpoint, user)."""
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, "records")
    reg = db_models.FlowRegistration(flow_uuid=str(uuid4()), name="api_flow", flow_path=str(flow_path), owner_id=1)
    db_session.add(reg)
    db_session.commit()
    db_session.refresh(reg)
    user = SimpleNamespace(id=1)
    out = flow_api.publish_endpoint(
        ApiEndpointCreate(registration_id=reg.id, slug=slug, parameters=[ApiParamSpec(name="region", type="string")]),
        current_user=user,
        db=db_session,
    )
    ep = db_session.get(db_models.FlowApiEndpoint, out.id)
    return reg, ep, user


@pytest.mark.asyncio
async def test_public_route_hides_internal_error_detail(db_session, tmp_path, monkeypatch):
    """#2: the public route must not echo node-level error text (paths, SQL, columns)."""
    _reg, ep, _user = _publish_via_db(db_session, tmp_path)
    secret = "node 3: failed reading /etc/passwd via SELECT * FROM secrets (column ssn)"

    def _boom(*_a, **_k):
        raise api_runner.ApiExecutionError(secret)

    monkeypatch.setattr(flow_api, "run_flow_as_api", _boom)
    request = SimpleNamespace(query_params={})

    with pytest.raises(HTTPException) as exc:
        await flow_api.run_published_flow(slug=ep.slug, request=request, endpoint=ep, db=db_session)
    assert exc.value.status_code == 500
    assert exc.value.detail == "Flow execution failed"
    assert secret not in str(exc.value.detail)


@pytest.mark.asyncio
async def test_owner_test_run_keeps_verbose_error_detail(db_session, tmp_path, monkeypatch):
    """#2 counterpart: the owner-only test path *does* surface the detailed error."""
    from flowfile_core.schemas.flow_api_schema import ApiTestRequest

    _reg, ep, user = _publish_via_db(db_session, tmp_path)
    secret = "node 3: detailed owner-facing failure"

    def _boom(*_a, **_k):
        raise api_runner.ApiExecutionError(secret)

    monkeypatch.setattr(flow_api, "run_flow_as_api", _boom)

    with pytest.raises(HTTPException) as exc:
        await flow_api.test_endpoint(ep.id, body=ApiTestRequest(params={}), current_user=user, db=db_session)
    assert exc.value.status_code == 500
    assert secret in str(exc.value.detail)


@pytest.mark.asyncio
async def test_public_route_503_when_saturated(db_session, tmp_path, monkeypatch):
    """#3: a fully saturated concurrency semaphore yields a fast 503, not a queued run."""
    _reg, ep, _user = _publish_via_db(db_session, tmp_path)
    # No free slots -> the guard must reject before running the graph.
    monkeypatch.setattr(flow_api, "_API_RUN_SEMAPHORE", asyncio.Semaphore(0))
    request = SimpleNamespace(query_params={})

    with pytest.raises(HTTPException) as exc:
        await flow_api.run_published_flow(slug=ep.slug, request=request, endpoint=ep, db=db_session)
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_public_route_releases_semaphore_slot(db_session, tmp_path):
    """The slot is released after a run so the endpoint isn't permanently saturated."""
    _reg, ep, _user = _publish_via_db(db_session, tmp_path)
    request = SimpleNamespace(query_params={"region": "US"})

    out = await flow_api.run_published_flow(slug=ep.slug, request=request, endpoint=ep, db=db_session)
    assert out["data"] == [{"region": "US", "v": 2}]
    # The module-level semaphore is back to its full count (nothing leaked).
    assert not flow_api._API_RUN_SEMAPHORE.locked()


def test_concurrent_publish_returns_409(db_session, tmp_path, monkeypatch):
    """#21: a UNIQUE-constraint race at commit surfaces as 409, not a raw 500."""
    flow_path = tmp_path / "flow.yaml"
    _build_and_save_flow(flow_path, "records")
    reg = db_models.FlowRegistration(flow_uuid=str(uuid4()), name="api_flow", flow_path=str(flow_path), owner_id=1)
    db_session.add(reg)
    db_session.commit()
    db_session.refresh(reg)
    user = SimpleNamespace(id=1)

    # Simulate a racing publisher that wins the slug between our pre-check and our
    # commit: the UNIQUE constraint then raises IntegrityError on commit.
    def _racing_commit():
        raise IntegrityError("INSERT", {}, Exception("UNIQUE constraint failed: flow_api_endpoints.slug"))

    monkeypatch.setattr(db_session, "commit", _racing_commit)

    with pytest.raises(HTTPException) as exc:
        flow_api.publish_endpoint(
            ApiEndpointCreate(registration_id=reg.id, slug="sales"), current_user=user, db=db_session
        )
    assert exc.value.status_code == 409


def test_key_enable_disable_toggle(db_session, tmp_path):
    """#9: PATCH toggles FlowApiKey.enabled, and a disabled key stops authenticating."""
    _reg, ep, user = _publish_via_db(db_session, tmp_path)
    created = flow_api.create_key(ep.id, ApiKeyCreate(name="prod"), current_user=user, db=db_session)
    raw = created.api_key

    # A fresh key authenticates.
    assert api_key_mod.verify_api_key(slug=ep.slug, x_api_key=raw, db=db_session).id == ep.id

    # Disable it in place (revoke without deleting).
    updated = flow_api.update_key(ep.id, created.id, ApiKeyUpdate(enabled=False), current_user=user, db=db_session)
    assert updated.enabled is False
    with pytest.raises(HTTPException) as exc:
        api_key_mod.verify_api_key(slug=ep.slug, x_api_key=raw, db=db_session)
    assert exc.value.status_code == 401  # disabled key is rejected

    # Re-enable it and it works again.
    updated = flow_api.update_key(ep.id, created.id, ApiKeyUpdate(enabled=True), current_user=user, db=db_session)
    assert updated.enabled is True
    assert api_key_mod.verify_api_key(slug=ep.slug, x_api_key=raw, db=db_session).id == ep.id


def test_update_key_rejects_foreign_key(db_session, tmp_path):
    """A key belonging to another endpoint can't be toggled via this endpoint's path."""
    _reg, ep, user = _publish_via_db(db_session, tmp_path)
    # A key row scoped to a different endpoint id.
    other = db_models.FlowApiKey(
        endpoint_id=ep.id + 999, owner_id=1, name="other", key_hash="x", key_prefix="ffk_x"
    )
    db_session.add(other)
    db_session.commit()
    db_session.refresh(other)
    with pytest.raises(HTTPException) as exc:
        flow_api.update_key(ep.id, other.id, ApiKeyUpdate(enabled=False), current_user=user, db=db_session)
    assert exc.value.status_code == 404


def test_get_endpoint_advertises_effective_specs(db_session, tmp_path):
    """#11: get_endpoint advertises the runtime-effective params, dropping stale overrides."""
    _reg, ep, user = _publish_via_db(db_session, tmp_path)
    # Inject a stale stored override for a param the flow no longer has ("ghost").
    ep.param_schema_json = flow_api._dump_param_schema(
        [ApiParamSpec(name="region", type="string"), ApiParamSpec(name="ghost", type="string")]
    )
    db_session.commit()

    out = flow_api.get_endpoint(ep.id, current_user=user, db=db_session)
    # Only the flow's real ${region} param is advertised; "ghost" is dropped, matching
    # what the runner enforces.
    assert [p.name for p in out.parameters] == ["region"]


def _reg(db_session, *, name, owner_id=1, is_api_compatible=True, flow_path="/tmp/x.yaml"):
    reg = db_models.FlowRegistration(
        flow_uuid=str(uuid4()),
        name=name,
        flow_path=flow_path,
        owner_id=owner_id,
        is_api_compatible=is_api_compatible,
    )
    db_session.add(reg)
    db_session.commit()
    db_session.refresh(reg)
    return reg


def test_list_publishable_flows(db_session, tmp_path):
    """The picker lists only api-compatible, unpublished flows owned by the user."""
    user = SimpleNamespace(id=1)

    real = tmp_path / "alpha.yaml"
    _build_and_save_flow(real, "records")
    a = _reg(db_session, name="alpha", flow_path=str(real))  # compatible, unpublished, file exists
    b = _reg(db_session, name="beta")  # compatible but will be published -> excluded
    _reg(db_session, name="gamma", is_api_compatible=False)  # not compatible -> excluded
    _reg(db_session, name="delta", owner_id=2)  # other owner -> excluded
    _reg(db_session, name="epsilon", flow_path=str(tmp_path / "missing.yaml"))  # file missing -> included

    # Publish beta so it drops out of the publishable list.
    db_session.add(db_models.FlowApiEndpoint(registration_id=b.id, owner_id=1, slug="beta"))
    db_session.commit()

    out = flow_api.list_publishable_flows(current_user=user, db=db_session)
    by_name = {f.name: f for f in out}

    assert [f.name for f in out] == ["alpha", "epsilon"]  # ordered by name; others excluded
    assert by_name["alpha"].registration_id == a.id
    assert by_name["alpha"].file_exists is True
    assert by_name["epsilon"].file_exists is False  # flagged, but still offered
