"""Docker: a kernel signs its calls to core with a token of its own, and core answers it as the kernel's owner;
core signs its calls to a kernel with that token, and the kernel takes no other call.

Real kernel containers against the session's core. The server cases start their kernel under ``FLOWFILE_MODE=docker``
for the whole module, since a kernel's token is signed with the key of the mode it was started in.
"""

import asyncio
import hashlib
import json
import uuid
from types import SimpleNamespace

import httpx
import pytest
from cryptography.fernet import Fernet

from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, KernelConfig
from tests.kernel_fixtures import CORE_TEST_PORT

pytestmark = pytest.mark.kernel

CORE = f"http://localhost:{CORE_TEST_PORT}"

_HELD = """
import hashlib, json, os
def _digest(value):
    return hashlib.pbkdf2_hmac("sha256", str(value).encode(), b"flowfile-kernel-test", 1000).hex()
_context = flowfile_ctx._context.get({})
_values = list(os.environ.values()) + [v for v in _context.values() if isinstance(v, str)]
print(json.dumps({"names": sorted(os.environ), "digests": sorted({_digest(v) for v in _values})}))
"""

_HEADERS = """
import json
print(json.dumps(dict(flowfile_ctx._get_internal_auth_headers())))
"""

_CALL = """
import json, httpx
headers = dict(flowfile_ctx._get_internal_auth_headers())
{edit}
_answer = httpx.get(flowfile_ctx._CORE_URL + "/catalog/namespaces", headers=headers, timeout=30)
_names = [row["name"] for row in _answer.json()] if _answer.status_code == 200 else []
print(json.dumps({{"status": _answer.status_code, "names": _names}}))
"""


def _digest(value: str) -> str:
    """The digest ``_HELD`` prints for a value, so a value is compared without being printed."""
    return hashlib.pbkdf2_hmac("sha256", value.encode(), b"flowfile-kernel-test", 1000).hex()


def _cell(manager: KernelManager, kernel_id: str, code: str) -> dict:
    """Run ``code`` as one cell and return the JSON object its last line printed."""
    result = manager.execute_sync(kernel_id, ExecuteRequest(node_id=1, code=code, flow_id=9001))
    assert result.success, result.error
    return json.loads(result.stdout.strip().splitlines()[-1])


def _call(manager: KernelManager, kernel_id: str, edit: str = "") -> dict:
    """The kernel lists the catalogs it may see, with its own headers after ``edit`` changed them."""
    return _cell(manager, kernel_id, _CALL.format(edit=edit))


_KNOCK = """
import json, httpx
_statuses = {{}}
for _route, _body in (("/execute", {{"node_id": 1, "code": "print(6 * 7)", "flow_id": 1}}), ("/artifacts", None)):
    _url = "http://{address}:9999" + _route
    _answer = httpx.post(_url, json=_body, timeout=30) if _body else httpx.get(_url, timeout=30)
    _statuses[_route] = _answer.status_code
_statuses["/health"] = httpx.get("http://{address}:9999/health", timeout=30).status_code
print(json.dumps(_statuses))
"""


def _address(manager: KernelManager, kernel_id: str) -> str:
    """The kernel container's address on Docker's network, where another container reaches it."""
    networks = manager._docker.containers.get(f"flowfile-kernel-{kernel_id}").attrs["NetworkSettings"]["Networks"]
    return next(network["IPAddress"] for network in networks.values() if network.get("IPAddress"))


def _as_an_older_image(kernel_id: str | None) -> str:
    """Headers as a kernel image from before a token per kernel sent them."""
    headers = {"X-Internal-Token": get_internal_token()}
    if kernel_id is not None:
        headers["X-Kernel-Id"] = kernel_id
    return f"headers = {headers!r}"


@pytest.fixture(scope="module")
def server(kernel_manager_with_core):
    """A multi-user server with two people: ``owner`` runs a kernel, ``other`` owns a kernel row and a private
    catalog. Yields the manager, the running kernel's id and the names to look for."""
    manager, _ = kernel_manager_with_core
    suffix = uuid.uuid4().hex[:8]
    kernel_id, other_kernel_id = f"token-{suffix}", f"token-other-{suffix}"
    names = SimpleNamespace(mine=f"kt_mine_{suffix}", theirs=f"kt_theirs_{suffix}", other_kernel=other_kernel_id)
    patch = pytest.MonkeyPatch()
    patch.setenv("FLOWFILE_MODE", "docker")
    patch.setenv("JWT_SECRET_KEY", "kernel-token-test-secret-key")
    patch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    with get_db_context() as db:
        owner = db_models.User(username=f"kt_owner_{suffix}", email=f"owner-{suffix}@flowfile.app")
        other = db_models.User(username=f"kt_other_{suffix}", email=f"other-{suffix}@flowfile.app")
        db.add_all([owner, other])
        db.commit()
        names.owner_id, names.other_id = owner.id, other.id
        db.add(db_models.CatalogNamespace(name=names.mine, level=0, owner_id=owner.id))
        db.add(db_models.CatalogNamespace(name=names.theirs, level=0, owner_id=other.id))
        db.add(db_models.Kernel(id=other_kernel_id, name="other", user_id=other.id))
        db.commit()
    try:
        asyncio.run(manager.create_kernel(KernelConfig(id=kernel_id, name="token test"), user_id=names.owner_id))
        manager.start_kernel_sync(kernel_id)
        yield manager, kernel_id, names
    finally:
        for step in (manager.stop_kernel, manager.delete_kernel):
            try:
                asyncio.run(step(kernel_id))
            except Exception:
                pass
        with get_db_context() as db:
            db.query(db_models.Kernel).filter(db_models.Kernel.id.in_([kernel_id, other_kernel_id])).delete(
                synchronize_session=False
            )
            db.query(db_models.CatalogNamespace).filter(
                db_models.CatalogNamespace.name.in_([names.mine, names.theirs])
            ).delete(synchronize_session=False)
            db.query(db_models.User).filter(db_models.User.id.in_([names.owner_id, names.other_id])).delete(
                synchronize_session=False
            )
            db.commit()
        patch.undo()


@pytest.fixture
def owner_row(server):
    """Change the kernel owner's ``users`` row for one test; the row is put back afterwards."""
    _, _, names = server

    def _set(**values) -> None:
        with get_db_context() as db:
            db.query(db_models.User).filter(db_models.User.id == names.owner_id).update(values)
            db.commit()

    yield _set
    _set(is_admin=False, disabled=False)


def test_a_kernel_holds_a_token_of_its_own_and_not_the_internal_one(kernel_manager_with_core):
    manager, kernel_id = kernel_manager_with_core

    held = _cell(manager, kernel_id, _HELD)

    assert "FLOWFILE_INTERNAL_TOKEN" not in held["names"]
    assert _digest(get_internal_token()) not in held["digests"]
    assert "FLOWFILE_KERNEL_TOKEN" in held["names"]


def test_a_kernel_takes_calls_from_core_only(kernel_manager_with_core):
    manager, kernel_id = kernel_manager_with_core
    url = f"http://127.0.0.1:{manager.get_kernel_sync(kernel_id).port}"
    cell = {"node_id": 1, "code": "print(6 * 7)", "flow_id": 9002}

    assert httpx.post(f"{url}/execute", json=cell, timeout=30).status_code == 401
    assert httpx.post(f"{url}/execute", json=cell, headers={"Authorization": "Bearer x"}, timeout=30).status_code == 401
    assert httpx.get(f"{url}/artifacts", timeout=30).status_code == 401
    assert httpx.get(f"{url}/health", timeout=30).status_code == 200, "the container's own health check stays open"

    signed = manager._call_headers(manager.get_kernel_sync(kernel_id))
    assert signed, "core holds the token the container was started with"
    answer = httpx.post(f"{url}/execute", json=cell, headers=signed, timeout=30)
    assert answer.status_code == 200 and answer.json()["stdout"].strip() == "42"
    assert _cell(manager, kernel_id, "import json; print(json.dumps({'ran': True}))") == {"ran": True}
    assert asyncio.run(manager.get_memory_stats(kernel_id)) is not None


def test_a_kernel_on_an_older_image_still_works_on_a_desktop(kernel_manager_with_core):
    manager, kernel_id = kernel_manager_with_core

    assert _call(manager, kernel_id, _as_an_older_image(kernel_id))["status"] == 200
    assert _call(manager, kernel_id, _as_an_older_image(None))["status"] == 200


def test_a_kernel_is_answered_as_its_owner(server):
    manager, kernel_id, names = server

    answer = _call(manager, kernel_id)

    assert answer["status"] == 200
    assert names.mine in answer["names"] and names.theirs not in answer["names"]


def test_a_call_that_names_another_kernel_is_refused(server):
    manager, kernel_id, names = server

    answer = _call(manager, kernel_id, f'headers["X-Kernel-Id"] = "{names.other_kernel}"')

    assert answer["status"] == 401


def test_a_call_that_names_no_kernel_is_its_owners_too(server):
    manager, kernel_id, names = server

    answer = _call(manager, kernel_id, 'headers.pop("X-Kernel-Id", None)')

    assert answer["status"] == 200
    assert names.mine in answer["names"] and names.theirs not in answer["names"]


def test_the_kernel_of_an_administrator_is_no_administrator(server, owner_row):
    manager, kernel_id, names = server
    owner_row(is_admin=True)

    answer = _call(manager, kernel_id)

    assert answer["status"] == 200
    assert names.mine in answer["names"] and names.theirs not in answer["names"]


def test_a_kernel_whose_owner_is_disabled_is_refused(server, owner_row):
    manager, kernel_id, _ = server
    owner_row(disabled=True)

    assert _call(manager, kernel_id)["status"] == 401


@pytest.mark.parametrize("mode", ["docker", "server"])
def test_a_kernel_on_an_older_image_is_refused_on_a_server(server, monkeypatch, mode):
    manager, kernel_id, _ = server
    monkeypatch.setenv("FLOWFILE_MODE", mode)

    assert _call(manager, kernel_id, _as_an_older_image(kernel_id))["status"] == 401
    assert _call(manager, kernel_id, _as_an_older_image(None))["status"] == 401


def test_a_kernel_takes_no_call_from_another_kernel(server, kernel_manager_with_core):
    manager, kernel_id, _ = server
    _, other_kernel_id = kernel_manager_with_core

    knocked = _cell(manager, kernel_id, _KNOCK.format(address=_address(manager, other_kernel_id)))

    assert knocked == {"/execute": 401, "/artifacts": 401, "/health": 200}


def test_a_token_ends_when_its_kernel_stops(server):
    manager, kernel_id, _ = server
    headers = _cell(manager, kernel_id, _HEADERS)
    assert httpx.get(f"{CORE}/catalog/namespaces", headers=headers, timeout=30).status_code == 200

    asyncio.run(manager.stop_kernel(kernel_id))
    assert httpx.get(f"{CORE}/catalog/namespaces", headers=headers, timeout=30).status_code == 401

    manager.start_kernel_sync(kernel_id)
    assert httpx.get(f"{CORE}/catalog/namespaces", headers=headers, timeout=30).status_code == 401
    assert _call(manager, kernel_id)["status"] == 200
