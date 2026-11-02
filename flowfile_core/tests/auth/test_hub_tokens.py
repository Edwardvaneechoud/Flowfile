"""A core that is a node on a hub serves the hub's ``run`` tokens as their user and starts runs for ``dispatch`` tokens.

Every test runs the real verifier against a JWKS this process serves (``shared.tests.hub_jwks``). The core is in docker
mode with its own JWT secret, as a team node is; with ``FLOWFILE_HUB_URL`` unset the same tokens are just wrong tokens.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import invalidate_user_cache
from flowfile_core.auth.password import get_password_hash
from flowfile_core.catalog.services import runs as runs_service
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from shared.tests.hub_jwks import hub_signer  # noqa: F401

client = TestClient(main.app)


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def team_node(hub_signer, monkeypatch):
    """This core as a node on the hub: docker mode, its own JWT secret, hub tokens on."""
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-for-hub-token-tests")
    invalidate_user_cache()
    yield hub_signer
    invalidate_user_cache()


def _user(username: str, is_admin: bool):
    with get_db_context() as db:
        db.query(db_models.User).filter(db_models.User.username == username).delete()
        row = db_models.User(
            username=username,
            email=f"{username}@flowfile.app",
            hashed_password=get_password_hash("irrelevant-password"),
            disabled=False,
            is_admin=is_admin,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        user_id = row.id
    yield user_id
    with get_db_context() as db:
        db.query(db_models.User).filter(db_models.User.id == user_id).delete()
        db.commit()


@pytest.fixture
def user():
    yield from _user("hub_ana", is_admin=False)


@pytest.fixture
def admin():
    yield from _user("hub_admin", is_admin=True)


@pytest.fixture
def no_spawn(monkeypatch):
    """A run-now that creates its run row but starts no ``flowfile run flow`` process."""
    spawned = []

    def fake_spawn(flow_path, run_id, **kwargs):
        spawned.append((flow_path, run_id))
        return 4242

    monkeypatch.setattr(runs_service, "spawn_flow_subprocess", fake_spawn)
    yield spawned
    with get_db_context() as db:
        db.query(db_models.FlowRun).filter(db_models.FlowRun.pid == 4242).delete()
        db.commit()


def test_a_run_token_is_its_user_by_uid(team_node, user):
    response = client.get("/auth/users/me", headers=bearer(team_node.token(sub="hub_ana", uid=user)))

    assert response.status_code == 200, response.text
    assert response.json()["username"] == "hub_ana"
    assert response.json()["id"] == user


def test_a_run_token_without_a_uid_falls_back_to_its_subject(team_node, user):
    response = client.get("/auth/users/me", headers=bearer(team_node.token(sub="hub_ana", uid=None)))

    assert response.status_code == 200, response.text
    assert response.json()["id"] == user


def test_a_run_token_for_a_user_this_core_does_not_have_is_401(team_node):
    response = client.get("/auth/users/me", headers=bearer(team_node.token(sub="nobody", uid=987654)))

    assert response.status_code == 401
    assert response.json()["detail"] == "Could not validate credentials"


def test_a_dispatch_token_is_not_a_user(team_node, user):
    response = client.get("/auth/users/me", headers=bearer(team_node.token(scope="dispatch", sub="hub", uid=None)))

    assert response.status_code == 401
    assert response.json()["detail"] == "dispatch tokens are not user tokens"


def test_a_refused_hub_token_is_401_with_the_reason(team_node, user):
    token = team_node.token(sub="hub_ana", uid=user, ttl=-120)

    response = client.get("/auth/users/me", headers=bearer(token))

    assert response.status_code == 401
    assert "expired" in response.json()["detail"]
    assert token not in response.text


def test_an_admin_route_refuses_a_hub_token_even_for_an_administrator(team_node, admin):
    headers = bearer(team_node.token(sub="hub_admin", uid=admin))
    assert client.get("/auth/users/me", headers=headers).status_code == 200

    response = client.get("/auth/users", headers=headers)

    assert response.status_code == 403
    assert "hub token" in response.json()["detail"]


def test_an_admin_route_refuses_a_dispatch_token(team_node):
    response = client.get("/auth/users", headers=bearer(team_node.token(scope="dispatch", sub="hub", uid=None)))

    assert response.status_code == 403
    assert response.json()["detail"] == "Admin routes take no hub token"


def test_a_dispatch_token_starts_a_run_owned_by_the_service_identity(team_node, registration, no_spawn, monkeypatch):
    monkeypatch.setenv("FLOWFILE_INTERNAL_SERVICE_USER_ID", "1")
    _namespace_id, registration_id = registration

    response = client.post(
        f"/catalog/flows/{registration_id}/run",
        headers=bearer(team_node.token(scope="dispatch", sub="hub", uid=None)),
    )

    assert response.status_code == 200, response.text
    run_id = response.json()["id"]
    assert no_spawn == [(response.json()["flow_path"], run_id)]
    with get_db_context() as db:
        run = db.get(db_models.FlowRun, run_id)
        assert run.user_id == 1
        assert run.run_type == "manual"
        assert run.registration_id == registration_id


def test_a_dispatch_token_reaches_no_other_catalog_route(team_node, user):
    headers = bearer(team_node.token(scope="dispatch", sub="hub", uid=None))

    assert client.get("/catalog/namespaces", headers=headers).status_code == 401
    assert client.get("/catalog/tables", headers=headers).status_code == 401
    assert client.get("/catalog/flows", headers=headers).status_code == 401
    assert client.get("/catalog/active-runs", headers=headers).status_code == 401


def test_a_run_token_reads_the_catalog_as_its_user(team_node, user):
    response = client.get("/catalog/namespaces", headers=bearer(team_node.token(sub="hub_ana", uid=user)))

    assert response.status_code == 200, response.text


def test_a_run_token_cannot_start_a_run(team_node, user, registration, no_spawn):
    _namespace_id, registration_id = registration

    response = client.post(
        f"/catalog/flows/{registration_id}/run", headers=bearer(team_node.token(sub="hub_ana", uid=user))
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "run tokens cannot start a run"
    assert no_spawn == []


def test_an_expired_dispatch_token_starts_nothing(team_node, registration, no_spawn):
    _namespace_id, registration_id = registration

    response = client.post(
        f"/catalog/flows/{registration_id}/run",
        headers=bearer(team_node.token(scope="dispatch", sub="hub", uid=None, ttl=-120)),
    )

    assert response.status_code == 401
    assert "expired" in response.json()["detail"]
    assert no_spawn == []


def test_without_a_hub_url_every_hub_token_is_a_wrong_token(team_node, user, registration, no_spawn, monkeypatch):
    run_token = team_node.token(sub="hub_ana", uid=user)
    dispatch_token = team_node.token(scope="dispatch", sub="hub", uid=None)
    _namespace_id, registration_id = registration
    monkeypatch.delenv("FLOWFILE_HUB_URL")

    me = client.get("/auth/users/me", headers=bearer(run_token))
    run = client.post(f"/catalog/flows/{registration_id}/run", headers=bearer(dispatch_token))

    assert me.status_code == 401 and me.json()["detail"] == "Could not validate credentials"
    assert run.status_code == 401 and run.json()["detail"] == "Could not validate credentials"
    assert no_spawn == []
