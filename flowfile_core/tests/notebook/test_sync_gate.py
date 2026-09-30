"""Syncing notebook cells to the canvas (``POST /notebook/plan``, ``POST /editor/notebook/push/``) is admin-only in
the multi-user modes, before the flow lookup; rendering stays open to every user and electron is never gated.

The mode is flipped per test with ``monkeypatch.setenv``, as the sharing tests do: every gate reads
``FLOWFILE_MODE`` per call.
"""

import pytest
from cryptography.fernet import Fernet

from flowfile_core.notebook.render import code_fingerprint
from tests.notebook.conftest import NOTEBOOK_OWNER_ID
from tests.notebook.test_push import _body, _cell_of, _node_of_type, _raise_threshold

MEMBER_ID = 2
SYNC_ROUTES = ("/notebook/plan", "/editor/notebook/push/")


@pytest.fixture
def flowfile_mode(monkeypatch):
    def _set(mode: str) -> None:
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        monkeypatch.setenv("JWT_SECRET_KEY", "notebook-sync-gate-secret")
        monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())

    return _set


def _orders(open_as, user_id):
    import flowfile as fl

    orders = fl.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    return open_as(orders.filter(fl.col("amount") > 10).flow_graph, user_id=user_id)


def _edit_body(graph):
    return _body(graph, _raise_threshold, [_cell_of(graph, _node_of_type(graph, "filter").node_id)])


@pytest.mark.parametrize("mode", ["docker", "package"])
def test_a_member_cannot_sync_in_a_multi_user_mode(runner, open_as, client_as, flowfile_mode, mode):
    flowfile_mode(mode)
    graph = _orders(open_as, MEMBER_ID)
    fingerprint = code_fingerprint(graph)
    member = client_as(MEMBER_ID)

    for route in SYNC_ROUTES:
        response = member.post(route, json=_edit_body(graph))
        assert response.status_code == 403, response.text
        assert response.json() == {"detail": "Admin privileges required"}
        unknown = member.post(route, json={**_edit_body(graph), "flow_id": 987654})
        assert unknown.status_code == 403, unknown.text
    assert code_fingerprint(graph) == fingerprint

    rendered = member.get("/notebook/render", params={"flow_id": graph.flow_id})
    assert rendered.status_code == 200, rendered.text


@pytest.mark.parametrize("mode", ["docker", "package"])
def test_an_admin_can_sync_in_a_multi_user_mode(runner, open_as, client_as, flowfile_mode, mode):
    flowfile_mode(mode)
    graph = _orders(open_as, NOTEBOOK_OWNER_ID)
    admin = client_as(NOTEBOOK_OWNER_ID)

    plan = admin.post("/notebook/plan", json=_edit_body(graph))
    assert plan.status_code == 200, plan.text
    push = admin.post("/editor/notebook/push/", json=_edit_body(graph))
    assert push.status_code == 200, push.text
    assert "20" in _node_of_type(graph, "filter").setting_input.filter_input.advanced_filter


def test_electron_never_gates_a_sync(runner, open_as, client_as, flowfile_mode):
    flowfile_mode("electron")
    graph = _orders(open_as, MEMBER_ID)
    member = client_as(MEMBER_ID)

    plan = member.post("/notebook/plan", json=_edit_body(graph))
    assert plan.status_code == 200, plan.text
    push = member.post("/editor/notebook/push/", json=_edit_body(graph))
    assert push.status_code == 200, push.text
    assert "20" in _node_of_type(graph, "filter").setting_input.filter_input.advanced_filter
