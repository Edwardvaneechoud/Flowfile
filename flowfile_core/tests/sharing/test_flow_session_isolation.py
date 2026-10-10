"""A user reaches only the flows open in their own editor session.

Flow ids are not secrets: a flow's file stores its id and reuses it on open. So every
logged-in route that names an in-memory flow must resolve it against the caller's session
(``routes.get_flow_or_404``). ``test_route_resolves_flow_for_caller`` is built from the live
app, so a new route that takes a flow id is checked without being added here, or has to be
listed in ``EXEMPT_ROUTES`` / ``EXEMPT_PREFIXES`` with a reason.
"""

import types
import typing
from enum import Enum
from types import SimpleNamespace

import pytest
from fastapi.routing import APIRoute, RouteContext, iter_route_contexts
from pydantic import BaseModel

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import chat_routes
from flowfile_core.ai import diff as ai_diff
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs.settings import FEATURE_FLAG_AI
from flowfile_core.notebook import gate as notebook_gate
from flowfile_core.routes.notebook import require_notebook_sync
from flowfile_core.schemas import input_schema, schemas

EXEMPT_ROUTES = {
    ("POST", "/raw_logs"): "worker/kernel log sink; the request carries no user to scope to",
    ("POST", "/flow/register/"): "creates a flow; see test_register_does_not_evict_another_users_flow",
    ("POST", "/artifacts/prepare-upload"): "source_flow_id is provenance metadata, never looked up",
    ("POST", "/notebook/session/node_result"): "kernel callback; a user is refused first, the flow is the kernel's",
    ("POST", "/notebook/session/node_run"): "kernel callback; a user is refused first, the flow is the kernel's",
    ("POST", "/notebook/session/lookup"): "kernel callback; a user is refused first, the flow is the kernel's",
}
EXEMPT_PREFIXES = {
    "/catalog/flows/{flow_id}": "a catalog registration id, authorized by the catalog AccessResolver",
    "/kernels/{kernel_id}/": "a namespace key inside a kernel the route has already owner-checked",
}

FIELD_VALUES = {"node_id": 1, "provider": "anthropic"}


def _raw_rest_api_body(flow_id: int) -> dict:
    return {**_sample_model(input_schema.NodeRestApiReader), "flow_id": flow_id}


RAW_BODIES = {
    ("POST", "/update_settings/"): lambda flow_id: {"flow_id": flow_id, "node_id": 1},
    ("POST", "/user_defined_components/update_user_defined_node"): lambda flow_id: {"flow_id": flow_id, "node_id": 1},
    ("POST", "/rest_api/sample"): _raw_rest_api_body,
}
PATH_VALUES = {
    "diff_id": lambda flow_id: ai_diff.register_diff(ai_diff.GraphDiff(session_id="isolation", flow_id=flow_id)),
}


def _sample(annotation, name: str = ""):
    """A minimal value FastAPI/pydantic accept for ``annotation``."""
    if name in FIELD_VALUES:
        return FIELD_VALUES[name]
    origin, args = typing.get_origin(annotation), typing.get_args(annotation)
    if origin in (typing.Union, types.UnionType):
        options = [arg for arg in args if arg is not type(None)]
        return _sample(options[0], name) if options else None
    if origin is typing.Literal:
        return args[0]
    if origin is typing.Annotated:
        return _sample(args[0], name)
    if origin is tuple and args and args[-1] is not Ellipsis:
        return [_sample(arg, name) for arg in args]
    if origin in (list, set, tuple, frozenset):
        return [_sample(args[0], name)] if args else []
    if annotation in (list, set, tuple, frozenset):
        return []
    if origin is dict or annotation is dict:
        return {}
    if isinstance(annotation, type):
        if issubclass(annotation, BaseModel):
            return _sample_model(annotation)
        if issubclass(annotation, Enum):
            return next(iter(annotation)).value
        for kind, value in ((bool, False), (int, 1), (float, 1.0), (str, "x")):
            if issubclass(annotation, kind):
                return value
    return None


def _sample_model(model: type[BaseModel]) -> dict:
    return {
        field.alias or name: _sample(field.annotation, name)
        for name, field in model.model_fields.items()
        if field.is_required()
    }


def _locations(dependant):
    for kind in ("path", "query", "body"):
        for param in getattr(dependant, f"{kind}_params"):
            yield kind, param
    for sub in dependant.dependencies:
        yield from _locations(sub)


def _flow_id_slots(method: str, route: RouteContext) -> list[tuple[str, str, str | None]]:
    """Every place a request can name a flow, as ``(location, param, model field)``."""
    slots = []
    for kind, param in _locations(route.dependant):
        annotation = param.field_info.annotation
        if kind != "body":
            if "flow_id" in param.name:
                slots.append((kind, param.alias, None))
        elif (method, route.path) in RAW_BODIES or annotation is dict or typing.get_origin(annotation) is dict:
            slots.append(("raw", param.alias, None))
        elif isinstance(annotation, type) and issubclass(annotation, BaseModel):
            slots += [("body", param.alias, f.alias or n) for n, f in annotation.model_fields.items() if "flow_id" in n]
        elif "flow_id" in param.name:
            slots.append(("body", param.alias, None))
    return slots


def _flow_id_routes() -> dict[tuple[str, str], RouteContext]:
    """Every live route that takes a flow id, keyed by its mounted path (routers stay nested in ``app.routes``)."""
    return {
        (method, route.path): route
        for route in iter_route_contexts(main.app.routes)
        if isinstance(route.original_route, APIRoute)
        for method in route.methods
        if _flow_id_slots(method, route)
    }


FLOW_ID_ROUTES = _flow_id_routes()
SCOPED_ROUTES = sorted(
    key
    for key in FLOW_ID_ROUTES
    if key not in EXEMPT_ROUTES and not any(key[1].startswith(prefix) for prefix in EXEMPT_PREFIXES)
)


def _build_request(method: str, route: RouteContext, targets: dict, token: str) -> dict:
    """Request kwargs for a TestClient: flow-id slots from ``targets``, everything else sampled."""
    path_params, query, body = {}, {}, {}
    for kind, param in _locations(route.dependant):
        name = param.alias
        if kind == "body":
            if ("raw", name, None) in targets:
                body[name] = RAW_BODIES[(method, route.path)](targets[("raw", name, None)])
                continue
            if ("body", name, None) in targets:
                body[name] = targets[("body", name, None)]
                continue
            annotation = param.field_info.annotation
            value = _sample(annotation, param.name)
            for (loc, owner, field), flow_id in targets.items():
                if loc == "body" and owner == name and field is not None:
                    value[field] = flow_id
            if isinstance(annotation, type) and issubclass(annotation, BaseModel):
                annotation.model_validate(value)  # a failure here means _sample needs a FIELD_VALUES entry
            body[name] = value
            continue
        slot = (kind, name, None)
        if slot in targets:
            value = targets[slot]
        elif name == "access_token":
            value = token
        elif name in PATH_VALUES:
            value = PATH_VALUES[name](next(iter(targets.values())))
        elif not param.field_info.is_required():
            continue
        else:
            value = _sample(param.field_info.annotation, param.name)
        (path_params if kind == "path" else query)[name] = value
    kwargs = {"url": route.path.format(**path_params), "params": query}
    if body:
        embed = getattr(route, "_embed_body_fields", len(body) > 1)
        kwargs["json"] = body if embed else next(iter(body.values()))
    return kwargs


def _open_flow(client) -> int:
    flow_id = client.post("/editor/create_flow/", params={"persist": False, "register_in_catalog": False}).json()
    assert client.post(
        "/editor/add_node/", params={"flow_id": flow_id, "node_id": 1, "node_type": "manual_input"}
    ).is_success
    manual_input = input_schema.NodeManualInput(
        flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"secret": 42}])
    )
    assert client.post("/transform/manual_input", json=manual_input.model_dump(mode="json")).is_success
    return flow_id


def _snapshot(flow) -> tuple[str, str]:
    return flow.get_flowfile_data().model_dump_json(), flow.flow_settings.model_dump_json()


@pytest.fixture
def flows(users, client_for):
    alice, bob = client_for("alice"), client_for("bob")
    alice_flow, bob_flow = _open_flow(alice), _open_flow(bob)
    yield SimpleNamespace(alice=alice, bob=bob, alice_flow=alice_flow, bob_flow=bob_flow)
    flow_file_handler.delete_flow(alice_flow, users["alice"].id)
    flow_file_handler.delete_flow(bob_flow, users["bob"].id)


@pytest.fixture
def lookups(monkeypatch) -> list[tuple[int, int | None]]:
    """Every ``(flow_id, user_id)`` the handler is asked to resolve; ``user_id=None`` is unscoped."""
    calls = []
    get_flow, user_has_flow = flow_file_handler.get_flow, flow_file_handler.user_has_flow

    def spy_get_flow(flow_id, user_id=None):
        calls.append((flow_id, user_id))
        return get_flow(flow_id, user_id)

    def spy_user_has_flow(user_id, flow_id):
        calls.append((flow_id, user_id))
        return user_has_flow(user_id, flow_id)

    monkeypatch.setattr(flow_file_handler, "get_flow", spy_get_flow)
    monkeypatch.setattr(flow_file_handler, "user_has_flow", spy_user_has_flow)
    return calls


@pytest.fixture
def ai_enabled(monkeypatch):
    original = FEATURE_FLAG_AI.value
    FEATURE_FLAG_AI.set(True)
    # Chat resolves its provider before the flow; the test only needs to get past it.
    monkeypatch.setattr(chat_routes, "get_configured_provider", lambda *args, **kwargs: object())
    yield
    FEATURE_FLAG_AI.set(original)


@pytest.fixture
def notebook_gates_open(monkeypatch):
    """Lift the notebook's mode gates (admin-only sync, desktop-only kernel sessions) so the lookups behind them run."""
    monkeypatch.setattr(notebook_gate, "kernel_sessions_allowed", lambda user: True)
    monkeypatch.setattr(notebook_gate, "is_loopback", lambda http: True)
    monkeypatch.setitem(main.app.dependency_overrides, require_notebook_sync, get_current_active_user)


@pytest.mark.parametrize(("method", "path"), SCOPED_ROUTES, ids=[f"{m} {p}" for m, p in SCOPED_ROUTES])
def test_route_resolves_flow_for_caller(method, path, users, flows, lookups, ai_enabled, notebook_gates_open):
    """Pointing any one flow-id slot at alice's flow (the rest at bob's own) never reaches alice's flow."""
    route = FLOW_ID_ROUTES[(method, path)]
    alice_graph = flow_file_handler.get_flow(flows.alice_flow)
    token = flows.bob.headers["Authorization"].removeprefix("Bearer ")
    slots = _flow_id_slots(method, route)
    for slot in slots:
        targets = {s: flows.bob_flow for s in slots} | {slot: flows.alice_flow}
        before = _snapshot(alice_graph)
        request = _build_request(method, route, targets, token)
        lookups.clear()

        response = flows.bob.request(method, **request)

        touched = [user_id for flow_id, user_id in lookups if flow_id == flows.alice_flow]
        assert touched, f"{slot} never reached a flow lookup ({response.status_code}: {response.text[:300]})"
        assert set(touched) == {users["bob"].id}, f"{slot} resolved alice's flow without scoping to bob"
        assert flow_file_handler.get_flow(flows.alice_flow, users["alice"].id) is alice_graph
        assert _snapshot(alice_graph) == before


def test_exemptions_name_live_routes():
    for key in EXEMPT_ROUTES:
        assert key in FLOW_ID_ROUTES, f"stale exemption {key}"
    for prefix in EXEMPT_PREFIXES:
        assert any(path.startswith(prefix) for _, path in FLOW_ID_ROUTES), f"stale exemption {prefix}"


def test_bob_cannot_preview_or_edit_alice_flow(flows):
    node = {"flow_id": flows.alice_flow, "node_id": 1}
    settings = input_schema.NodeManualInput(
        **node, raw_data_format=input_schema.RawData.from_pylist([{"secret": 0}])
    ).model_dump(mode="json")

    assert flows.bob.get("/node/data", params=node).status_code == 404
    assert flows.bob.post("/update_settings/", params={"node_type": "manual_input"}, json=settings).status_code == 404

    assert flows.alice.get("/node/data", params=node).status_code == 200
    assert flows.alice.post("/update_settings/", params={"node_type": "manual_input"}, json=settings).status_code == 200


def test_register_does_not_evict_another_users_flow(flows, users):
    alice_graph = flow_file_handler.get_flow(flows.alice_flow)
    settings = schemas.FlowSettings(flow_id=flows.alice_flow, name="taken", path=".")

    response = flows.bob.post("/flow/register/", json=settings.model_dump(mode="json"))

    assert response.status_code == 409
    assert flow_file_handler.get_flow(flows.alice_flow, users["alice"].id) is alice_graph
    assert flow_file_handler.get_flow(flows.alice_flow, users["bob"].id) is None


def test_opening_a_file_another_user_has_open_gets_a_separate_flow(users, client_for, tmp_path):
    alice, bob = client_for("alice"), client_for("bob")
    path = str(tmp_path / "shared_flow.yaml")
    alice_id = alice.post("/editor/create_flow/", params={"flow_path": path, "register_in_catalog": False}).json()
    alice_graph = flow_file_handler.get_flow(alice_id)
    bob_id = bob.get("/import_flow/", params={"flow_path": path}).json()
    try:
        assert bob_id != alice_id
        assert flow_file_handler.get_flow(alice_id, users["alice"].id) is alice_graph
        assert flow_file_handler.get_flow(alice_id, users["bob"].id) is None
        bob_graph = flow_file_handler.get_flow(bob_id, users["bob"].id)
        assert bob_graph is not alice_graph
        assert bob_graph.flow_logger.get_log_filepath() != alice_graph.flow_logger.get_log_filepath()
        # Re-opening your own file still reloads it in place.
        assert alice.get("/import_flow/", params={"flow_path": path}).json() == alice_id
    finally:
        flow_file_handler.delete_flow(alice_id, users["alice"].id)
        flow_file_handler.delete_flow(bob_id, users["bob"].id)
