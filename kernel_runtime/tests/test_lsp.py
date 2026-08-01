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


def test_complete_dedupes_repeated_name_type_pairs(client, monkeypatch):
    """jedi.Interpreter can emit a name twice (static parse + live namespace); complete() dedupes."""
    from kernel_runtime.lsp import analysis

    class _FakeCompletion:
        def __init__(self, name, type_):
            self.name = name
            self.type = type_
            self.description = f"{type_} {name}"

        def docstring(self, raw=True):
            return ""

    class _FakeInterpreter:
        def complete(self, line, column):
            return [
                _FakeCompletion("catalog", "instance"),
                _FakeCompletion("catalog", "instance"),
                _FakeCompletion("catalog", "statement"),
                _FakeCompletion("other", "instance"),
            ]

    monkeypatch.setattr(analysis, "_interpreter", lambda code, live: _FakeInterpreter())
    resp = client.post("/lsp/complete", json={"code": "cat", "line": 1, "column": 3, "flow_id": 1})
    assert resp.status_code == 200
    pairs = [(i["label"], i["type"]) for i in resp.json()["items"]]
    assert pairs.count(("catalog", "instance")) == 1
    assert ("catalog", "statement") in pairs  # same name, different type stays a distinct entry
    assert ("other", "instance") in pairs


def test_hover(client):
    resp = client.post("/lsp/hover", json={"code": "pl.col", "line": 1, "column": 6, "flow_id": 1})
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "col"
    assert body["signature"]


def test_hover_titles_a_method_with_kind_name_and_signature(client):
    code = "import polars as pl\npl.LazyFrame.collect"
    body = client.post("/lsp/hover", json={"code": code, "line": 2, "column": 20, "flow_id": 1}).json()
    assert body["kind"] == "method"
    assert body["name"] == "collect"
    assert body["signature"].startswith("collect(")
    # contents is the docstring alone — the signature no longer rides inside it
    assert body["contents"].startswith("Materialize this")


def test_hover_kind_for_class_and_variable(client):
    cls = client.post(
        "/lsp/hover",
        json={"code": "import polars as pl\npl.LazyFrame", "line": 2, "column": 12, "flow_id": 1},
    ).json()
    assert cls["kind"] == "class" and cls["name"] == "LazyFrame"

    var = client.post(
        "/lsp/hover",
        json={"code": "import polars as pl\ndf = pl.DataFrame()\ndf", "line": 3, "column": 2, "flow_id": 1},
    ).json()
    assert var["kind"] == "variable" and var["name"] == "df"


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


class TestCleanDoc:
    """Docstring text handed to the tooltips is prose, not raw reStructuredText."""

    def test_drops_directive_blocks_and_keeps_following_content(self):
        from kernel_runtime.lsp.analysis import _clean_doc

        out = _clean_doc(
            "Summary line.\n"
            "\n"
            "Parameters\n"
            "----------\n"
            "type_coercion\n"
            "    Do type coercion.\n"
            "\n"
            "    .. deprecated:: 1.30.0\n"
            "        Use the ``optimizations`` parameter.\n"
            "predicate_pushdown\n"
            "    Do predicate pushdown.\n"
        )
        assert "deprecated" not in out
        assert "optimizations` parameter" not in out
        assert "Parameters" in out
        assert "----------" not in out
        assert "predicate_pushdown" in out
        assert "    Do predicate pushdown." in out

    def test_normalises_rst_markup(self):
        from kernel_runtime.lsp.analysis import _clean_doc

        out = _clean_doc("    Read a ``LazyFrame``.\n\n    See :meth:`DataFrame.head` and :class:`Foo`.")
        assert out.startswith("Read a `LazyFrame`.")
        assert "``" not in out
        assert ":meth:" not in out and ":class:" not in out
        assert "`DataFrame.head`" in out

    def test_collapses_blank_runs_and_trims(self):
        from kernel_runtime.lsp.analysis import _clean_doc

        assert _clean_doc("\n\nA.\n\n\n\nB.\n\n") == "A.\n\nB."

    def test_hover_returns_cleaned_text(self, client):
        resp = client.post(
            "/lsp/hover",
            json={"code": "import polars as pl\npl.LazyFrame.collect", "line": 2, "column": 20, "flow_id": 1},
        )
        contents = resp.json()["contents"]
        assert contents
        assert ".. deprecated::" not in contents
        assert "``" not in contents

    def test_uncapped_by_default_capped_only_for_completions(self):
        from kernel_runtime.lsp import analysis

        long_doc = "x" * 20000
        assert analysis._truncate(long_doc) == long_doc
        assert len(analysis._truncate(long_doc, analysis._MAX_COMPLETION_DOC_CHARS)) == (
            analysis._MAX_COMPLETION_DOC_CHARS + 1
        )

    def test_hover_and_signature_carry_the_whole_docstring(self, client):
        code = "import polars as pl\npl.LazyFrame.collect"
        hover = client.post("/lsp/hover", json={"code": code, "line": 2, "column": 20, "flow_id": 1}).json()
        assert len(hover["contents"]) > 3000 and not hover["contents"].endswith("…")

        sig_code = "import polars as pl\npl.LazyFrame().collect("
        sig = client.post("/lsp/signature", json={"code": sig_code, "line": 2, "column": 42, "flow_id": 1}).json()
        docs = [s["documentation"] for s in sig["signatures"]]
        assert docs and not any(d.endswith("…") for d in docs)
