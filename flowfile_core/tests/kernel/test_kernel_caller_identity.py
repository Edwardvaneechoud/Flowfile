"""A kernel's calls to core, signed with the internal token and its X-Kernel-Id, act for the kernel's owner.

No Docker: the kernel rows are written to the test database and the calls are plain HTTP requests to the app.
"""

import uuid

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_internal_token, get_user_or_internal_service
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


@pytest.fixture
def kernel_of():
    """Make a kernel row owned by a fresh person; teardown removes both."""
    made: list[tuple[str, int]] = []

    def _make(*, disabled: bool = False, delete_owner: bool = False) -> str:
        suffix = uuid.uuid4().hex[:8]
        with get_db_context() as db:
            owner = db_models.User(username=f"kernel_owner_{suffix}", email=f"{suffix}@flowfile.app", disabled=disabled)
            db.add(owner)
            db.commit()
            kernel_id = f"kernel-{suffix}"
            db.add(db_models.Kernel(id=kernel_id, name="k", user_id=owner.id))
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


def _kernel_headers(kernel_id: str | None) -> dict[str, str]:
    headers = {"X-Internal-Token": get_internal_token()}
    if kernel_id is not None:
        headers["X-Kernel-Id"] = kernel_id
    return headers


@pytest.mark.parametrize("owner", ["disabled", "deleted"])
def test_a_kernel_whose_owner_is_disabled_or_gone_is_refused(kernel_of, owner):
    kernel_id = kernel_of(disabled=owner == "disabled", delete_owner=owner == "deleted")

    response = TestClient(main.app).get("/catalog/namespaces", headers=_kernel_headers(kernel_id))

    assert response.status_code == 401


def test_a_kernel_whose_owner_is_enabled_is_answered_as_its_owner(kernel_of):
    kernel_id = kernel_of()

    assert TestClient(main.app).get("/catalog/namespaces", headers=_kernel_headers(kernel_id)).status_code == 200
    with get_db_context() as db:
        owner_id = db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).one().user_id
        user = get_user_or_internal_service(
            token=None, x_internal_token=get_internal_token(), x_kernel_id=kernel_id, db=db
        )
    assert (user.id, user.username) == (owner_id, f"kernel_owner_{kernel_id.removeprefix('kernel-')}")


def test_a_call_without_a_kernel_id_is_answered_as_today():
    assert TestClient(main.app).get("/catalog/namespaces", headers=_kernel_headers(None)).status_code == 200
    with get_db_context() as db:
        user = get_user_or_internal_service(token=None, x_internal_token=get_internal_token(), x_kernel_id=None, db=db)
    assert user.username == "_internal_service"
