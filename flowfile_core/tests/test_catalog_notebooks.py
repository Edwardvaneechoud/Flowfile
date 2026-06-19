"""Tests for the catalog notebook API.

Covers CRUD on saved notebooks: mixed-cell (Python/SQL/Markdown) round-trip,
full-document update, name/namespace uniqueness, the summary listing, and the
grant-cleanup-on-delete invariant (SQLite rowid reuse hazard).
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogNotebook,
    ResourceGrant,
    UserGroup,
)


@pytest.fixture(scope="session")
def client() -> TestClient:
    with TestClient(main.app) as auth_c:
        token = auth_c.post("/auth/token").json()["access_token"]
    c = TestClient(main.app)
    c.headers = {"Authorization": f"Bearer {token}"}
    return c


def _cleanup():
    with get_db_context() as db:
        db.query(CatalogNotebook).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


@pytest.fixture(autouse=True)
def clean_notebooks():
    _cleanup()
    yield
    _cleanup()


def _make_namespace(name: str = "NbNs") -> int:
    with get_db_context() as db:
        ns = CatalogNamespace(name=name, parent_id=None, level=0, owner_id=1)
        db.add(ns)
        db.commit()
        db.refresh(ns)
        return ns.id


SAMPLE_CELLS = [
    {"id": "c1", "type": "markdown", "source": "# Explore orders", "metadata": {}},
    {"id": "c2", "type": "sql", "source": "SELECT * FROM orders", "metadata": {"max_rows": 100}},
    {"id": "c3", "type": "python", "source": "df = flowfile_ctx.read_table('orders')\ndf", "metadata": {}},
]


class TestNotebookCRUD:
    def test_create_with_mixed_cells_and_get(self, client):
        ns_id = _make_namespace()
        resp = client.post(
            "/catalog/notebooks",
            json={
                "name": "explore",
                "namespace_id": ns_id,
                "description": "scratch",
                "cells": SAMPLE_CELLS,
                "default_kernel_id": "kern-abc",
            },
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["name"] == "explore"
        assert body["namespace_id"] == ns_id
        assert body["default_kernel_id"] == "kern-abc"
        assert body["owner_id"] == 1
        assert body["namespace_name"] == "NbNs"
        # All three cell types round-trip with their metadata.
        assert body["cells"] == SAMPLE_CELLS

        got = client.get(f"/catalog/notebooks/{body['id']}")
        assert got.status_code == 200
        assert got.json()["cells"] == SAMPLE_CELLS

    def test_list_summary_omits_cells(self, client):
        client.post("/catalog/notebooks", json={"name": "a", "cells": SAMPLE_CELLS})
        client.post("/catalog/notebooks", json={"name": "b", "cells": []})
        lib = client.get("/catalog/notebooks")
        assert lib.status_code == 200
        items = lib.json()
        assert {i["name"] for i in items} == {"a", "b"}
        # Summary DTO has no "cells" key.
        assert all("cells" not in i for i in items)

    def test_full_document_update(self, client):
        created = client.post("/catalog/notebooks", json={"name": "nb", "cells": SAMPLE_CELLS}).json()
        new_cells = [{"id": "x", "type": "sql", "source": "SELECT 1", "metadata": {}}]
        resp = client.put(
            f"/catalog/notebooks/{created['id']}",
            json={"name": "renamed", "cells": new_cells, "default_kernel_id": "k2"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["name"] == "renamed"
        assert body["cells"] == new_cells
        assert body["default_kernel_id"] == "k2"

    def test_partial_update_preserves_cells(self, client):
        created = client.post("/catalog/notebooks", json={"name": "nb", "cells": SAMPLE_CELLS}).json()
        # Update only the description; cells must be untouched (not cleared).
        resp = client.put(f"/catalog/notebooks/{created['id']}", json={"description": "new desc"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["description"] == "new desc"
        assert body["cells"] == SAMPLE_CELLS

    def test_delete(self, client):
        created = client.post("/catalog/notebooks", json={"name": "nb", "cells": []}).json()
        assert client.delete(f"/catalog/notebooks/{created['id']}").status_code == 204
        assert client.get(f"/catalog/notebooks/{created['id']}").status_code == 404

    def test_name_namespace_uniqueness(self, client):
        ns_id = _make_namespace()
        first = client.post("/catalog/notebooks", json={"name": "dup", "namespace_id": ns_id, "cells": []})
        assert first.status_code == 201
        dup = client.post("/catalog/notebooks", json={"name": "dup", "namespace_id": ns_id, "cells": []})
        assert dup.status_code == 409, dup.text
        # Same name in a different namespace is fine.
        other_ns = _make_namespace("Other")
        ok = client.post("/catalog/notebooks", json={"name": "dup", "namespace_id": other_ns, "cells": []})
        assert ok.status_code == 201, ok.text

    def test_get_missing_returns_404(self, client):
        assert client.get("/catalog/notebooks/999999").status_code == 404


class TestNotebookGrantCleanup:
    def test_delete_clears_grants(self):
        """Deleting a notebook must remove its resource grants — SQLite reuses
        rowids, so a stale grant would silently attach to a future notebook."""
        with get_db_context() as db:
            ns = CatalogNamespace(name="GrantNs", parent_id=None, level=0, owner_id=1)
            db.add(ns)
            db.commit()
            db.refresh(ns)
            nb = CatalogNotebook(name="shared", namespace_id=ns.id, cells_json="[]", owner_id=1)
            db.add(nb)
            db.commit()
            db.refresh(nb)
            grp = UserGroup(name="g1", created_by=1)
            db.add(grp)
            db.commit()
            db.refresh(grp)
            db.add(
                ResourceGrant(
                    resource_type="catalog_notebook",
                    resource_id=nb.id,
                    group_id=grp.id,
                    permission="use",
                    granted_by=1,
                )
            )
            db.commit()
            notebook_id = nb.id

            SQLAlchemyCatalogRepository(db).delete_notebook(notebook_id)

            remaining = (
                db.query(ResourceGrant)
                .filter_by(resource_type="catalog_notebook", resource_id=notebook_id)
                .count()
            )
            assert remaining == 0
            # Cleanup
            db.query(ResourceGrant).delete()
            db.query(UserGroup).delete()
            db.query(CatalogNamespace).delete()
            db.commit()
