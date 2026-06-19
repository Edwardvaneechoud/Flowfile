"""Tests for the catalog notebook API.

Covers CRUD on saved notebooks: mixed-cell (Python/SQL/Markdown) round-trip,
full-document update, name/namespace uniqueness, the summary listing, and the
grant-cleanup-on-delete invariant (SQLite rowid reuse hazard).
"""

import datetime
import json

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
    {"id": "c3", "type": "python", "source": "df = flowfile_ctx.read_catalog_table('orders')\ndf", "metadata": {}},
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


class TestNotebookInNamespaceTree:
    def test_notebook_surfaced_in_namespace_tree(self, client):
        ns_id = _make_namespace("TreeNs")
        created = client.post(
            "/catalog/notebooks",
            json={"name": "tree_nb", "namespace_id": ns_id, "cells": SAMPLE_CELLS},
        ).json()

        tree = client.get("/catalog/namespaces/tree")
        assert tree.status_code == 200, tree.text
        node = next(n for n in tree.json() if n["id"] == ns_id)
        nb = next(nb for nb in node["notebooks"] if nb["id"] == created["id"])
        assert nb["name"] == "tree_nb"
        assert nb["namespace_id"] == ns_id
        # The tree carries the lightweight summary — no cells.
        assert "cells" not in nb

    def test_notebook_without_namespace_not_in_tree(self, client):
        _make_namespace("HasNs")
        client.post("/catalog/notebooks", json={"name": "orphan", "cells": []})

        tree = client.get("/catalog/namespaces/tree")
        all_names = [nb["name"] for node in tree.json() for nb in node.get("notebooks", [])]
        assert "orphan" not in all_names

    def test_create_defaults_to_general_default_namespace(self, client):
        # Recreate the General/default schema the autouse cleanup removes.
        with get_db_context() as db:
            general = CatalogNamespace(name="General", parent_id=None, level=0, owner_id=1)
            db.add(general)
            db.commit()
            db.refresh(general)
            default = CatalogNamespace(name="default", parent_id=general.id, level=1, owner_id=1)
            db.add(default)
            db.commit()
            db.refresh(default)
            default_id = default.id

        created = client.post("/catalog/notebooks", json={"name": "nb_default", "cells": []}).json()
        assert created["namespace_id"] == default_id


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
                db.query(ResourceGrant).filter_by(resource_type="catalog_notebook", resource_id=notebook_id).count()
            )
            assert remaining == 0
            # Cleanup
            db.query(ResourceGrant).delete()
            db.query(UserGroup).delete()
            db.query(CatalogNamespace).delete()
            db.commit()


class TestNotebookValidation:
    def test_create_in_missing_namespace_returns_404(self, client):
        resp = client.post("/catalog/notebooks", json={"name": "nb", "namespace_id": 999999, "cells": []})
        assert resp.status_code == 404, resp.text

    def test_update_name_null_returns_422(self, client):
        created = client.post("/catalog/notebooks", json={"name": "nb", "cells": []}).json()
        resp = client.put(f"/catalog/notebooks/{created['id']}", json={"name": None})
        assert resp.status_code == 422, resp.text

    def test_update_cells_null_returns_422(self, client):
        created = client.post("/catalog/notebooks", json={"name": "nb", "cells": []}).json()
        resp = client.put(f"/catalog/notebooks/{created['id']}", json={"cells": None})
        assert resp.status_code == 422, resp.text

    def test_parse_cells_degrades_gracefully_on_malformed_json(self):
        """Seeding a corrupt cells_json must not crash the read path — GET still
        returns the notebook with the bad cells dropped (pairs with NB-06)."""
        from flowfile_core.catalog.services.notebooks import NotebookService

        # Not valid JSON at all -> empty list, no raise.
        assert NotebookService._parse_cells("{not json") == []
        # Valid JSON but not a list -> empty list.
        assert NotebookService._parse_cells('{"a": 1}') == []
        # A list mixing one valid cell with junk -> only the valid cell survives.
        good = {"id": "c1", "type": "markdown", "source": "# ok", "metadata": {}}
        parsed = NotebookService._parse_cells(json.dumps([good, 42, {"missing": "type"}]))
        assert len(parsed) == 1
        assert parsed[0].id == "c1"

    def test_get_notebook_with_malformed_cells_json(self, client):
        """End-to-end: a notebook whose stored cells_json is corrupt still reads
        back over HTTP, with the malformed cells silently dropped."""
        with get_db_context() as db:
            nb = CatalogNotebook(name="corrupt", cells_json="{not a list", owner_id=1)
            db.add(nb)
            db.commit()
            db.refresh(nb)
            nb_id = nb.id
        resp = client.get(f"/catalog/notebooks/{nb_id}")
        assert resp.status_code == 200, resp.text
        assert resp.json()["cells"] == []


class TestNotebookTimestampsAndOrdering:
    def test_create_response_has_timestamps(self, client):
        body = client.post("/catalog/notebooks", json={"name": "ts", "cells": []}).json()
        assert body["created_at"] is not None
        assert body["updated_at"] is not None

    def test_updated_at_advances_after_put(self, client):
        created = client.post("/catalog/notebooks", json={"name": "adv", "cells": []}).json()
        # Force updated_at to an older value so the onupdate bump is observable.
        with get_db_context() as db:
            nb = db.query(CatalogNotebook).filter_by(id=created["id"]).first()
            nb.updated_at = datetime.datetime(2000, 1, 1)
            db.commit()
            old_updated = nb.updated_at.isoformat()

        updated = client.put(f"/catalog/notebooks/{created['id']}", json={"description": "touched"}).json()
        assert updated["updated_at"] > old_updated

    def test_list_returns_most_recently_updated_first(self, client):
        a = client.post("/catalog/notebooks", json={"name": "older", "cells": []}).json()
        b = client.post("/catalog/notebooks", json={"name": "newer", "cells": []}).json()
        # Pin distinct updated_at values so ordering doesn't depend on clock resolution.
        with get_db_context() as db:
            db.query(CatalogNotebook).filter_by(id=a["id"]).first().updated_at = datetime.datetime(2030, 1, 1)
            db.query(CatalogNotebook).filter_by(id=b["id"]).first().updated_at = datetime.datetime(2020, 1, 1)
            db.commit()

        names = [n["name"] for n in client.get("/catalog/notebooks").json()]
        assert names == ["older", "newer"]
