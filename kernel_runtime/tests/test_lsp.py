"""Hermetic tests for the kernel /lsp/* endpoints (FastAPI TestClient, no Docker)."""

from kernel_runtime import main


def test_capabilities(client):
    resp = client.get("/lsp/capabilities")
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert body["version"]  # kernel runtime __version__
    assert "complete" in body["features"]


def test_complete_polars_module(client):
    resp = client.post("/lsp/complete", json={"code": "pl.", "line": 1, "column": 3, "flow_id": 1})
    assert resp.status_code == 200
    labels = [i["label"] for i in resp.json()["items"]]
    # real installed polars resolves through the seeded namespace
    assert "DataFrame" in labels
    assert "col" in labels


def test_complete_flowfile_ctx(client):
    resp = client.post(
        "/lsp/complete",
        json={"code": "flowfile_ctx.", "line": 1, "column": 13, "flow_id": 1},
    )
    labels = [i["label"] for i in resp.json()["items"]]
    assert "read_input" in labels
    assert "publish_output" in labels


def test_complete_uses_live_namespace(client):
    """A variable bound by a prior executed cell is completable in a later cell."""
    flow_id = 4242
    ex = client.post(
        "/execute",
        json={
            "node_id": 1,
            "flow_id": flow_id,
            "code": "import polars as pl\ndf = pl.LazyFrame({'a': [1], 'b': [2]})",
        },
    )
    assert ex.status_code == 200 and ex.json()["success"], ex.json()

    resp = client.post("/lsp/complete", json={"code": "df.", "line": 1, "column": 3, "flow_id": flow_id})
    labels = [i["label"] for i in resp.json()["items"]]
    assert "select" in labels
    assert "filter" in labels


def test_complete_does_not_create_namespace(client):
    """Read-only peek: completing against an unseen flow_id must not allocate a slot."""
    unseen = -987654321
    main._namespace_store.pop(unseen, None)
    resp = client.post("/lsp/complete", json={"code": "pl.", "line": 1, "column": 3, "flow_id": unseen})
    assert resp.status_code == 200
    assert unseen not in main._namespace_store
    assert unseen not in main._namespace_access


def test_hover(client):
    resp = client.post("/lsp/hover", json={"code": "pl.col", "line": 1, "column": 6, "flow_id": 1})
    assert resp.status_code == 200
    assert resp.json()["contents"]


def test_signature(client):
    resp = client.post("/lsp/signature", json={"code": "pl.col(", "line": 1, "column": 7, "flow_id": 1})
    assert resp.status_code == 200
    assert len(resp.json()["signatures"]) >= 1


def test_diagnostics_syntax_error(client):
    resp = client.post("/lsp/diagnostics", json={"code": "def f(:\n  pass", "line": 1, "column": 0, "flow_id": 1})
    assert resp.status_code == 200
    diags = resp.json()["diagnostics"]
    assert len(diags) >= 1
    assert diags[0]["severity"] == "error"


def test_diagnostics_clean_code(client):
    resp = client.post(
        "/lsp/diagnostics",
        json={"code": "x = 1\ny = x + 1", "line": 1, "column": 0, "flow_id": 1},
    )
    assert resp.json()["diagnostics"] == []
