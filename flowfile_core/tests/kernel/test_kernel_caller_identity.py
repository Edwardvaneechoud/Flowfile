"""A kernel's calls to core, signed with the kernel's own token, act for the kernel's owner and for nobody else.

No Docker: the kernel rows are written to the test database and the calls are plain HTTP requests to the app. The
calls of a kernel image from before a token per kernel, signed with the internal token and ``X-Kernel-Id``, are taken
on a desktop and refused on a multi-user server.
"""

import uuid

import pytest
from cryptography.fernet import Fernet
from fastapi import HTTPException
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth import jwt as core_jwt
from flowfile_core.auth.jwt import (
    create_kernel_token,
    get_internal_token,
    get_user_or_internal_service,
    internal_token_valid,
    kernel_caller,
)
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context

DESKTOP_MODES = ("electron", "package")
SERVER_MODES = ("docker", "server")


@pytest.fixture
def mode(monkeypatch):
    """Switch ``FLOWFILE_MODE`` for one test, with the keys every mode but the desktop reads from its environment."""

    def _set(name: str) -> None:
        monkeypatch.setenv("FLOWFILE_MODE", name)
        if name != "electron":
            monkeypatch.setenv("JWT_SECRET_KEY", "kernel-token-test-secret-key")
            monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())

    return _set


@pytest.fixture
def kernel_of():
    """Make a kernel row owned by a fresh person, started unless ``token_id`` is ``None``; teardown removes both."""
    made: list[tuple[str, int]] = []

    def _make(
        *, disabled: bool = False, delete_owner: bool = False, is_admin: bool = False, token_id: str | None = "t1"
    ) -> str:
        suffix = uuid.uuid4().hex[:8]
        with get_db_context() as db:
            owner = db_models.User(
                username=f"kernel_owner_{suffix}", email=f"{suffix}@flowfile.app", disabled=disabled, is_admin=is_admin
            )
            db.add(owner)
            db.commit()
            kernel_id = f"kernel-{suffix}"
            db.add(db_models.Kernel(id=kernel_id, name="k", user_id=owner.id, token_id=token_id))
            db.commit()
            made.append((kernel_id, owner.id))
            if delete_owner:
                db.delete(owner)
                db.commit()
            return kernel_id

    yield _make
    with get_db_context() as db:
        for kernel_id, owner_id in made:
            db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).delete()
            db.query(db_models.User).filter(db_models.User.id == owner_id).delete()
        db.commit()


def _owner_id(kernel_id: str) -> int:
    with get_db_context() as db:
        return db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).one().user_id


def _token(kernel_id: str, token_id: str = "t1") -> str:
    return create_kernel_token(_owner_id(kernel_id), kernel_id, token_id)


def _bearer(kernel_id: str, token_id: str = "t1") -> dict[str, str]:
    """Headers as the kernel runtime sends them: its token where a kernel has always put what core hands it, and
    its id for a core that predates the token."""
    return {"X-Internal-Token": _token(kernel_id, token_id), "X-Kernel-Id": kernel_id}


def _older_image(kernel_id: str | None) -> dict[str, str]:
    """Headers as a kernel image from before a token per kernel sends them."""
    headers = {"X-Internal-Token": get_internal_token()}
    if kernel_id is not None:
        headers["X-Kernel-Id"] = kernel_id
    return headers


def _status(headers: dict[str, str], route: str = "/catalog/namespaces") -> int:
    return TestClient(main.app).get(route, headers=headers).status_code


@pytest.mark.parametrize("name", [*DESKTOP_MODES, *SERVER_MODES])
def test_a_kernel_token_is_answered_as_the_kernels_owner_in_every_mode(kernel_of, mode, name):
    mode(name)
    kernel_id = kernel_of(is_admin=True)

    assert _status(_bearer(kernel_id)) == 200
    with get_db_context() as db:
        caller = kernel_caller(_token(kernel_id), db)
        user = get_user_or_internal_service(_token(kernel_id), None, kernel_id, db)
    assert caller.kernel_id == kernel_id
    assert (caller.user.id, caller.user.username) == (_owner_id(kernel_id), f"kernel_owner_{kernel_id[7:]}")
    assert (user.id, user.username) == (caller.user.id, caller.user.username)
    assert caller.user.is_admin is False and user.is_admin is False, "a kernel never carries its owner's admin flag"


def test_a_kernel_token_as_the_bearer_is_taken_too(kernel_of, mode):
    mode("docker")
    kernel_id = kernel_of()

    assert _status({"Authorization": f"Bearer {_token(kernel_id)}", "X-Kernel-Id": kernel_id}) == 200
    with get_db_context() as db:
        user = get_user_or_internal_service(_token(kernel_id), None, kernel_id, db)
    assert user.id == _owner_id(kernel_id)


def test_a_server_that_replaces_the_auth_dependencies_passes_a_kernel_on_as_it_passes_the_internal_token(
    kernel_of, mode
):
    """Such a server asks ``internal_token_valid`` about ``X-Internal-Token`` and hands what passes to core's
    ``get_user_or_internal_service``; a kernel's token passes it, the stopped kernel's too, which core then refuses."""
    mode("server")
    running, stopped = kernel_of(), kernel_of(token_id=None)

    assert internal_token_valid(_token(running)) and internal_token_valid(_token(stopped))
    assert internal_token_valid(get_internal_token()) and not internal_token_valid("not.a.token")
    assert not core_jwt.shared_token_valid(_token(running))
    with get_db_context() as db:
        user = get_user_or_internal_service(None, _token(running), running, db)
        with pytest.raises(HTTPException):
            get_user_or_internal_service(None, _token(stopped), stopped, db)
    assert user.id == _owner_id(running) and user.is_admin is False


def test_a_call_that_names_another_kernel_than_its_token_is_refused(kernel_of, mode):
    mode("docker")
    kernel_id, other = kernel_of(), kernel_of()

    assert _status({**_bearer(kernel_id), "X-Kernel-Id": other}) == 401
    assert _status({"Authorization": f"Bearer {_token(kernel_id)}"}) == 200, "the token names the kernel by itself"


@pytest.mark.parametrize("owner", ["disabled", "deleted"])
def test_a_kernel_whose_owner_is_disabled_or_gone_is_refused(kernel_of, mode, owner):
    mode("docker")
    kernel_id = kernel_of(disabled=owner == "disabled", delete_owner=owner == "deleted")

    assert _status(_bearer(kernel_id)) == 401


def test_a_token_ends_with_its_kernels_run(kernel_of, mode):
    mode("docker")
    stopped, restarted = kernel_of(token_id=None), kernel_of(token_id="t2")

    assert _status(_bearer(stopped)) == 401, "a stopped kernel has no token"
    assert _status(_bearer(restarted, "t1")) == 401, "a start ends the token of the run before it"
    assert _status(_bearer(restarted, "t2")) == 200
    assert _status({"Authorization": f"Bearer {create_kernel_token(1, 'no-such-kernel', 't1')}"}) == 401


def test_a_token_minted_for_another_owner_than_the_kernels_is_refused(kernel_of, mode):
    mode("docker")
    kernel_id, other = kernel_of(), kernel_of()

    for_another_owner = create_kernel_token(_owner_id(other), kernel_id, "t1")

    assert _status({"Authorization": f"Bearer {for_another_owner}"}) == 401


def test_a_refused_kernel_token_is_never_answered_as_someone_else(kernel_of, mode):
    mode("docker")
    kernel_id = kernel_of(token_id=None)

    with get_db_context() as db:
        with pytest.raises(HTTPException) as refused:
            get_user_or_internal_service(_token(kernel_id), get_internal_token(), None, db)
        with pytest.raises(HTTPException):
            kernel_caller(_token(kernel_id), db)
    assert refused.value.status_code == 401


def test_a_kernel_token_is_no_sign_in_and_no_internal_token(kernel_of, mode):
    mode("docker")
    kernel_id = kernel_of(is_admin=True)
    token = _token(kernel_id)
    client = TestClient(main.app)

    assert client.get("/auth/users/me", headers={"Authorization": f"Bearer {token}"}).status_code == 401
    assert client.post("/auth/refresh", data={"refresh_token": token}).status_code == 401
    assert client.post("/shutdown", headers={"X-Internal-Token": token}).status_code == 403
    assert not core_jwt.shared_token_valid(token)
    with get_db_context() as db:
        assert kernel_caller(None, db) is None and kernel_caller(get_internal_token(), db) is None
        with pytest.raises(HTTPException):
            core_jwt.get_current_user(token, db)


def test_a_log_line_is_taken_from_a_kernel_with_its_token(kernel_of, mode):
    mode("docker")
    kernel_id = kernel_of()
    line = {"flowfile_flow_id": -987654, "log_message": "x", "log_type": "INFO"}
    client = TestClient(main.app)

    assert client.post("/raw_logs", json=line, headers=_bearer(kernel_id)).status_code == 404, "signed, no such flow"
    as_bearer = {"Authorization": f"Bearer {_token(kernel_id)}"}
    assert client.post("/raw_logs", json=line, headers=as_bearer).status_code == 404
    assert client.post("/raw_logs", json=line, headers={"X-Internal-Token": get_internal_token()}).status_code == 404
    assert client.post("/raw_logs", json=line, headers=_bearer(kernel_of(token_id=None))).status_code == 401
    assert client.post("/raw_logs", json=line).status_code == 401


@pytest.mark.parametrize("name", DESKTOP_MODES)
def test_an_older_kernel_image_is_answered_as_today_on_a_desktop(kernel_of, mode, name):
    mode(name)
    kernel_id = kernel_of()

    assert _status(_older_image(kernel_id)) == 200
    assert _status(_older_image(None)) == 200
    with get_db_context() as db:
        owner = get_user_or_internal_service(None, get_internal_token(), kernel_id, db)
        nobody = get_user_or_internal_service(None, get_internal_token(), None, db)
    assert owner.id == _owner_id(kernel_id)
    assert nobody.username == "_internal_service"


@pytest.mark.parametrize("name", SERVER_MODES)
def test_an_older_kernel_image_is_refused_on_a_server(kernel_of, mode, name):
    mode(name)
    kernel_id = kernel_of()

    assert _status(_older_image(kernel_id)) == 401
    assert _status(_older_image(None)) == 401


@pytest.mark.parametrize("owner", ["disabled", "deleted"])
def test_an_older_kernel_image_whose_owner_is_disabled_or_gone_is_refused_on_a_desktop(kernel_of, mode, owner):
    mode("package")
    kernel_id = kernel_of(disabled=owner == "disabled", delete_owner=owner == "deleted")

    assert _status(_older_image(kernel_id)) == 401
