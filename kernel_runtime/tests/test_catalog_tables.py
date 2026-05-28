"""Tests for the typed catalog API in flowfile_client.

The kernel performs Delta writes locally and talks to Core over HTTP. Tests
substitute Core with an ``httpx.MockTransport`` so the suite runs without a
running Core API. The on-disk Delta side is exercised against a real temp dir
so the Polars + ``deltalake`` integration is covered end-to-end.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pytest

from kernel_runtime import flowfile_client
from kernel_runtime.flowfile_client import CatalogRef, SchemaRef, TableRef


# ---------------------------------------------------------------------------
# Mock Core HTTP backend
# ---------------------------------------------------------------------------


class _MockCore:
    """Realistic in-memory stand-in for Core's catalog HTTP surface.

    Mirrors the real two-level hierarchy: top-level *catalog* namespaces
    have ``parent_id=None``, *schema* namespaces sit under a catalog. The
    default namespace is the seeded "General/default" schema (level 1).
    """

    # Stable ids the tests can reference
    CATALOG_GENERAL_ID = 1
    SCHEMA_DEFAULT_ID = 2

    def __init__(self):
        self.namespaces: list[dict[str, Any]] = [
            {"id": self.CATALOG_GENERAL_ID, "name": "General", "parent_id": None},
            {"id": self.SCHEMA_DEFAULT_ID, "name": "default", "parent_id": self.CATALOG_GENERAL_ID},
        ]
        self.default_namespace_id = self.SCHEMA_DEFAULT_ID
        self.tables: dict[int, dict[str, Any]] = {}
        self._next_id = 100
        self.refresh_calls: list[tuple[int, dict[str, Any]]] = []
        self.from_data_calls: list[dict[str, Any]] = []

    def add_schema(self, name: str, *, parent_id: int = CATALOG_GENERAL_ID) -> int:
        """Helper for tests that want extra schemas under a catalog."""
        sid = max((n["id"] for n in self.namespaces), default=0) + 1
        self.namespaces.append({"id": sid, "name": name, "parent_id": parent_id})
        return sid

    def add_catalog(self, name: str) -> int:
        """Helper for tests that need extra top-level catalogs."""
        cid = max((n["id"] for n in self.namespaces), default=0) + 1
        self.namespaces.append({"id": cid, "name": name, "parent_id": None})
        return cid

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = dict(request.url.params)
        if request.method == "GET" and path == "/catalog/default-namespace-id":
            return httpx.Response(200, json=self.default_namespace_id)
        if request.method == "GET" and path == "/catalog/namespaces":
            # Match FastAPI's strictness: an int|None query param accepts
            # "absent" or an int, but NOT an empty string. Returning 422 here
            # surfaces ``params={"parent_id": None}`` bugs at test time
            # instead of letting them through to a real Core.
            raw = request.url.params.get("parent_id")
            if raw == "":
                return httpx.Response(422, json={"detail": "empty parent_id"})
            if raw is None:
                rows = [n for n in self.namespaces if n["parent_id"] is None]
            else:
                try:
                    parent_int = int(raw)
                except ValueError:
                    return httpx.Response(422, json={"detail": "bad parent_id"})
                rows = [n for n in self.namespaces if n["parent_id"] == parent_int]
            return httpx.Response(200, json=rows)
        if request.method == "GET" and path == "/catalog/tables":
            ns = params.get("namespace_id")
            if ns:
                ns_int = int(ns)
                rows = [t for t in self.tables.values() if t.get("namespace_id") == ns_int]
            else:
                rows = list(self.tables.values())
            return httpx.Response(200, json=rows)
        if request.method == "GET" and path == "/catalog/tables/resolve":
            q = params["q"]
            ns = int(params["namespace_id"]) if params.get("namespace_id") else None
            for table in self.tables.values():
                if table["name"] == q and (ns is None or table["namespace_id"] == ns):
                    return httpx.Response(200, json={"table": table, "warnings": []})
            return httpx.Response(404, json={"detail": "not found"})
        if request.method == "POST" and path == "/catalog/tables/from-data":
            import json as _json

            body = _json.loads(request.content)
            self.from_data_calls.append(body)
            new_id = self._next_id
            self._next_id += 1
            record = {
                "id": new_id,
                "name": body["name"],
                "namespace_id": body.get("namespace_id"),
                "file_path": body["table_path"],
                "row_count": body.get("row_count"),
                "column_count": body.get("column_count"),
                "size_bytes": body.get("size_bytes"),
                "schema_columns": body.get("schema_columns") or [],
                "description": body.get("description"),
            }
            self.tables[new_id] = record
            return httpx.Response(201, json=record)
        if request.method == "POST" and path.startswith("/catalog/tables/") and path.endswith("/refresh"):
            import json as _json

            table_id = int(path.split("/")[3])
            body = _json.loads(request.content)
            self.refresh_calls.append((table_id, body))
            record = self.tables[table_id]
            if body.get("table_path"):
                record["file_path"] = body["table_path"]
            for key in (
                "row_count",
                "column_count",
                "size_bytes",
                "description",
                "schema_columns",
            ):
                if body.get(key) is not None:
                    record[key] = body[key]
            return httpx.Response(200, json=record)
        return httpx.Response(404, json={"detail": f"unhandled {request.method} {path}"})


@pytest.fixture()
def mock_core() -> _MockCore:
    return _MockCore()


@contextlib.contextmanager
def _patch_core_client(mock: _MockCore) -> Iterator[None]:
    """Replace ``httpx.Client(...)`` with one whose transport hits the mock."""
    original = httpx.Client

    def _factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(mock.handler)
        return original(*args, **kwargs)

    httpx.Client = _factory  # type: ignore[assignment]
    try:
        yield
    finally:
        httpx.Client = original  # type: ignore[assignment]


@pytest.fixture()
def kernel_catalog_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Wire the kernel client to write Delta dirs into a tmp directory."""
    catalog_dir = tmp_path / "catalog_tables"
    catalog_dir.mkdir()
    monkeypatch.setenv("FLOWFILE_KERNEL_CATALOG_TABLES_DIR", str(catalog_dir))
    monkeypatch.delenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", raising=False)
    monkeypatch.delenv("FLOWFILE_HOST_SHARED_DIR", raising=False)
    return catalog_dir


def _df(values: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(values)


# ---------------------------------------------------------------------------
# write_catalog_table — happy paths per write_mode (string-name form)
# ---------------------------------------------------------------------------


class TestWriteCatalogTableNewTable:
    def test_overwrite_creates_new_table(self, mock_core: _MockCore, kernel_catalog_dir: Path):
        df = _df([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        with _patch_core_client(mock_core):
            result = flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
        assert isinstance(result, TableRef)
        assert result.name == "orders"
        assert result.schema.id == mock_core.SCHEMA_DEFAULT_ID
        assert result.row_count == 2
        assert len(mock_core.from_data_calls) == 1
        assert mock_core.from_data_calls[0]["storage_format"] == "delta"
        assert mock_core.from_data_calls[0]["row_count"] == 2
        table_path = mock_core.from_data_calls[0]["table_path"]
        assert (Path(table_path) / "_delta_log").is_dir()

    def test_append_creates_new_table_when_missing(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1, "v": "a"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="append")
        assert len(mock_core.from_data_calls) == 1
        assert len(mock_core.refresh_calls) == 0

    def test_error_mode_creates_new_table(self, mock_core: _MockCore, kernel_catalog_dir: Path):
        df = _df([{"id": 1, "v": "a"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="error")
        assert len(mock_core.from_data_calls) == 1

    def test_lazyframe_accepted(self, mock_core: _MockCore, kernel_catalog_dir: Path):
        """Passing a LazyFrame collects internally before the Delta write."""
        lf = _df([{"id": 1, "v": "a"}]).lazy()
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(lf, "orders", write_mode="overwrite")
        assert len(mock_core.from_data_calls) == 1


class TestWriteCatalogTableExisting:
    def _seed_existing(self, mock_core: _MockCore, kernel_catalog_dir: Path) -> str:
        df = _df([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
        mock_core.from_data_calls.clear()
        mock_core.refresh_calls.clear()
        return mock_core.tables[100]["file_path"]

    def test_error_mode_raises_when_table_exists(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        self._seed_existing(mock_core, kernel_catalog_dir)
        df = _df([{"id": 3, "v": "c"}])
        with _patch_core_client(mock_core):
            with pytest.raises(FileExistsError):
                flowfile_client.write_catalog_table(df, "orders", write_mode="error")

    def test_overwrite_refreshes_existing(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        path = self._seed_existing(mock_core, kernel_catalog_dir)
        df = _df([{"id": 10, "v": "z"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
        assert len(mock_core.from_data_calls) == 0
        assert len(mock_core.refresh_calls) == 1
        table_id, body = mock_core.refresh_calls[0]
        assert table_id == 100
        assert body["row_count"] == 1
        out = pl.scan_delta(path).collect()
        assert out["id"].to_list() == [10]

    def test_append_adds_rows(self, mock_core: _MockCore, kernel_catalog_dir: Path):
        path = self._seed_existing(mock_core, kernel_catalog_dir)
        df = _df([{"id": 3, "v": "c"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="append")
        assert len(mock_core.refresh_calls) == 1
        out = pl.scan_delta(path).collect().sort("id")
        assert out["id"].to_list() == [1, 2, 3]

    def test_upsert_inserts_and_updates(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        path = self._seed_existing(mock_core, kernel_catalog_dir)
        df = _df([{"id": 1, "v": "A"}, {"id": 3, "v": "C"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(
                df, "orders", write_mode="upsert", merge_keys=["id"]
            )
        out = pl.scan_delta(path).collect().sort("id")
        assert out["id"].to_list() == [1, 2, 3]
        assert out["v"].to_list() == ["A", "b", "C"]

    def test_update_modifies_only_matches(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        path = self._seed_existing(mock_core, kernel_catalog_dir)
        df = _df([{"id": 1, "v": "A"}, {"id": 99, "v": "Z"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(
                df, "orders", write_mode="update", merge_keys=["id"]
            )
        out = pl.scan_delta(path).collect().sort("id")
        assert out["id"].to_list() == [1, 2]
        assert out["v"].to_list() == ["A", "b"]

    def test_delete_removes_matching(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        path = self._seed_existing(mock_core, kernel_catalog_dir)
        df = _df([{"id": 1, "v": "ignored"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(
                df, "orders", write_mode="delete", merge_keys=["id"]
            )
        out = pl.scan_delta(path).collect().sort("id")
        assert out["id"].to_list() == [2]


# ---------------------------------------------------------------------------
# write_catalog_table — validation
# ---------------------------------------------------------------------------


class TestWriteCatalogTableValidation:
    def test_unknown_write_mode(self, mock_core: _MockCore, kernel_catalog_dir: Path):
        df = _df([{"id": 1}])
        with _patch_core_client(mock_core):
            with pytest.raises(ValueError, match="Unknown write_mode"):
                flowfile_client.write_catalog_table(df, "x", write_mode="bogus")

    def test_merge_mode_requires_merge_keys(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1}])
        with _patch_core_client(mock_core):
            with pytest.raises(ValueError, match="merge_keys"):
                flowfile_client.write_catalog_table(df, "x", write_mode="upsert")

    def test_schema_and_namespace_id_mutually_exclusive(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1}])
        with _patch_core_client(mock_core):
            with pytest.raises(ValueError, match="either 'schema' or 'namespace_id'"):
                flowfile_client.write_catalog_table(
                    df, "x", schema="default", namespace_id=2, write_mode="overwrite"
                )

    def test_ref_with_explicit_schema_rejected(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1}])
        ref = TableRef(
            name="x",
            schema=SchemaRef(
                id=mock_core.SCHEMA_DEFAULT_ID,
                name="default",
                catalog=CatalogRef(id=mock_core.CATALOG_GENERAL_ID, name="General"),
            ),
        )
        with _patch_core_client(mock_core):
            with pytest.raises(ValueError, match="must be omitted"):
                flowfile_client.write_catalog_table(df, ref, schema="default")


# ---------------------------------------------------------------------------
# Discovery / typed navigation
# ---------------------------------------------------------------------------


class TestListCatalogs:
    def test_list_catalogs_returns_top_level_refs(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        mock_core.add_catalog("Analytics")
        with _patch_core_client(mock_core):
            result = flowfile_client.list_catalogs()
        assert all(isinstance(c, CatalogRef) for c in result)
        assert sorted(c.name for c in result) == ["Analytics", "General"]

    def test_list_schemas_unfiltered_flattens_across_catalogs(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        analytics_id = mock_core.add_catalog("Analytics")
        mock_core.add_schema("sales", parent_id=analytics_id)
        mock_core.add_schema("ml", parent_id=analytics_id)
        with _patch_core_client(mock_core):
            result = flowfile_client.list_schemas()
        names = sorted(s.name for s in result)
        assert names == ["default", "ml", "sales"]
        # Every schema knows its parent catalog
        assert all(isinstance(s.catalog, CatalogRef) for s in result)

    def test_list_schemas_by_catalog_name(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        analytics_id = mock_core.add_catalog("Analytics")
        mock_core.add_schema("sales", parent_id=analytics_id)
        mock_core.add_schema("ml", parent_id=analytics_id)
        with _patch_core_client(mock_core):
            result = flowfile_client.list_schemas(catalog="Analytics")
        names = sorted(s.name for s in result)
        assert names == ["ml", "sales"]
        assert all(s.catalog.name == "Analytics" for s in result)

    def test_list_schemas_by_catalog_id(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        ops_id = mock_core.add_schema("ops")
        with _patch_core_client(mock_core):
            result = flowfile_client.list_schemas(catalog_id=mock_core.CATALOG_GENERAL_ID)
        names = sorted(s.name for s in result)
        assert names == ["default", "ops"]
        assert any(s.id == ops_id for s in result)

    def test_list_schemas_unknown_catalog_raises(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            with pytest.raises(LookupError, match="Catalog 'nope' not found"):
                flowfile_client.list_schemas(catalog="nope")

    def test_list_schemas_mutually_exclusive_args(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            with pytest.raises(ValueError, match="either 'catalog' or 'catalog_id'"):
                flowfile_client.list_schemas(catalog="default", catalog_id=2)


class TestGetCatalogAndDefaultSchema:
    def test_get_catalog_returns_ref(self, mock_core: _MockCore, kernel_catalog_dir: Path):
        with _patch_core_client(mock_core):
            cat = flowfile_client.get_catalog("General")
        assert isinstance(cat, CatalogRef)
        assert cat.id == mock_core.CATALOG_GENERAL_ID
        assert cat.name == "General"

    def test_get_catalog_unknown_raises(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            with pytest.raises(LookupError, match="Catalog 'missing' not found"):
                flowfile_client.get_catalog("missing")

    def test_default_schema_returns_seeded_ref(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            sch = flowfile_client.default_schema()
        assert isinstance(sch, SchemaRef)
        assert sch.id == mock_core.SCHEMA_DEFAULT_ID
        assert sch.name == "default"
        assert sch.catalog.name == "General"


class TestListCatalogTables:
    def test_list_empty(self, mock_core: _MockCore, kernel_catalog_dir: Path):
        with _patch_core_client(mock_core):
            result = flowfile_client.list_catalog_tables()
        assert result == []

    def test_list_after_writes(self, mock_core: _MockCore, kernel_catalog_dir: Path):
        df = _df([{"id": 1}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
            flowfile_client.write_catalog_table(df, "customers", write_mode="overwrite")
            result = flowfile_client.list_catalog_tables()
        names = sorted(t.name for t in result)
        assert names == ["customers", "orders"]
        # Each ref carries a fully-populated SchemaRef
        for t in result:
            assert isinstance(t, TableRef)
            assert t.schema.name == "default"

    def test_list_namespace_filter(self, mock_core: _MockCore, kernel_catalog_dir: Path):
        sales_id = mock_core.add_schema("sales")
        df = _df([{"id": 1}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
            flowfile_client.write_catalog_table(
                df, "leads", namespace_id=sales_id, write_mode="overwrite"
            )
            scoped = flowfile_client.list_catalog_tables(namespace_id=sales_id)
        assert [t.name for t in scoped] == ["leads"]
        assert scoped[0].schema.name == "sales"

    def test_schema_and_namespace_id_mutually_exclusive(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            with pytest.raises(ValueError, match="either 'schema' or 'namespace_id'"):
                flowfile_client.list_catalog_tables(schema="default", namespace_id=2)


# ---------------------------------------------------------------------------
# read_catalog_table
# ---------------------------------------------------------------------------


class TestReadCatalogTable:
    def test_read_returns_lazyframe(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
            result = flowfile_client.read_catalog_table("orders")
        assert isinstance(result, pl.LazyFrame)
        materialized = result.collect().sort("id")
        assert materialized["id"].to_list() == [1, 2]

    def test_read_missing_raises_keyerror(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            with pytest.raises(KeyError):
                flowfile_client.read_catalog_table("does_not_exist")

    def test_delta_version_time_travel(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            df_v0 = _df([{"id": 1, "v": "a"}])
            flowfile_client.write_catalog_table(df_v0, "orders", write_mode="overwrite")
            df_v1 = _df([{"id": 1, "v": "REPLACED"}])
            flowfile_client.write_catalog_table(df_v1, "orders", write_mode="overwrite")
            current = flowfile_client.read_catalog_table("orders").collect()
            historic = flowfile_client.read_catalog_table(
                "orders", delta_version=0
            ).collect()
        assert current["v"].to_list() == ["REPLACED"]
        assert historic["v"].to_list() == ["a"]


# ---------------------------------------------------------------------------
# Typed navigation: CatalogRef / SchemaRef / TableRef methods
# ---------------------------------------------------------------------------


class TestRefNavigation:
    def test_catalog_ref_get_schema_happy_path(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            cat = flowfile_client.get_catalog("General")
            sch = cat.get_schema("default")
        assert isinstance(sch, SchemaRef)
        assert sch.id == mock_core.SCHEMA_DEFAULT_ID
        assert sch.catalog == cat

    def test_catalog_ref_get_schema_missing_raises(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            cat = flowfile_client.get_catalog("General")
            with pytest.raises(LookupError, match="Schema 'nope' not found"):
                cat.get_schema("nope")

    def test_catalog_ref_list_tables_flattens(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        sales_id = mock_core.add_schema("sales")
        df = _df([{"id": 1}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
            flowfile_client.write_catalog_table(
                df, "leads", namespace_id=sales_id, write_mode="overwrite"
            )
            cat = flowfile_client.get_catalog("General")
            tables = cat.list_tables()
        names = sorted(t.name for t in tables)
        assert names == ["leads", "orders"]

    def test_catalog_ref_get_table_ref_shortcut(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
            cat = flowfile_client.get_catalog("General")
            ref = cat.get_table_ref(schema_name="default", table_name="orders")
        assert isinstance(ref, TableRef)
        assert ref.exists()
        assert ref.name == "orders"
        assert ref.schema.name == "default"

    def test_schema_ref_get_table_ref_missing_returns_lazy_ref(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        with _patch_core_client(mock_core):
            sch = flowfile_client.default_schema()
            ref = sch.get_table_ref("not_yet_created")
        assert isinstance(ref, TableRef)
        assert not ref.exists()
        assert ref.id is None
        assert ref.name == "not_yet_created"
        assert ref.schema == sch

    def test_schema_ref_get_table_ref_existing_populates_metadata(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1}, {"id": 2}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
            sch = flowfile_client.default_schema()
            ref = sch.get_table_ref("orders")
        assert ref.exists()
        assert ref.id == 100
        assert ref.row_count == 2
        assert ref.file_path is not None


class TestTableRefReadWrite:
    def test_table_ref_write_creates_then_read_roundtrips(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        with _patch_core_client(mock_core):
            sch = flowfile_client.default_schema()
            ref = sch.get_table_ref("orders")
            assert not ref.exists()
            written = ref.write(df, write_mode="overwrite")
            assert written.exists()
            assert written.row_count == 2
            out = written.read().collect().sort("id")
        assert out["v"].to_list() == ["a", "b"]

    def test_table_ref_refresh_repopulates(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1}, {"id": 2}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
            sch = flowfile_client.default_schema()
            # Build a lazy ref (no metadata) then refresh
            lazy = TableRef(name="orders", schema=sch)
            refreshed = lazy.refresh()
        assert refreshed.exists()
        assert refreshed.row_count == 2

    def test_schema_ref_read_write_convenience(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1, "v": "a"}])
        with _patch_core_client(mock_core):
            sch = flowfile_client.default_schema()
            sch.write_table(df, "via_schema", write_mode="overwrite")
            out = sch.read_table("via_schema").collect()
        assert out["v"].to_list() == ["a"]

    def test_schema_ref_catalog_table_aliases(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        """`schema.write_catalog_table` / `read_catalog_table` mirror the top-level names."""
        df = _df([{"id": 1, "v": "alias"}])
        with _patch_core_client(mock_core):
            sch = flowfile_client.default_schema()
            sch.write_catalog_table(df, "via_alias", write_mode="overwrite")
            out = sch.read_catalog_table("via_alias").collect()
        assert out["v"].to_list() == ["alias"]


class TestSchemaRefArtifactMethods:
    """SchemaRef artifact methods delegate to the top-level global-artifact
    functions with ``namespace_id`` baked in.

    These tests stub the underlying functions rather than spinning up the full
    /artifacts/* HTTP mock surface — that's already covered by the dedicated
    global-artifact tests; here we only need to confirm the delegation contract.
    """

    def test_publish_artifact_forwards_namespace_id(
        self, mock_core: _MockCore, kernel_catalog_dir: Path, monkeypatch: pytest.MonkeyPatch
    ):
        captured: dict[str, Any] = {}

        def fake_publish_global(name, obj, **kwargs):
            captured["name"] = name
            captured["obj"] = obj
            captured.update(kwargs)
            return 42

        monkeypatch.setattr(flowfile_client, "publish_global", fake_publish_global)
        with _patch_core_client(mock_core):
            sch = flowfile_client.default_schema()
            result = sch.publish_artifact(
                "my_model", {"weights": [1, 2, 3]}, description="d", tags=["ml"]
            )
        assert result == 42
        assert captured["name"] == "my_model"
        assert captured["obj"] == {"weights": [1, 2, 3]}
        assert captured["namespace_id"] == mock_core.SCHEMA_DEFAULT_ID
        assert captured["description"] == "d"
        assert captured["tags"] == ["ml"]

    def test_read_artifact_forwards_namespace_id(
        self, mock_core: _MockCore, kernel_catalog_dir: Path, monkeypatch: pytest.MonkeyPatch
    ):
        captured: dict[str, Any] = {}

        def fake_get_global(name, **kwargs):
            captured["name"] = name
            captured.update(kwargs)
            return "the-object"

        monkeypatch.setattr(flowfile_client, "get_global", fake_get_global)
        with _patch_core_client(mock_core):
            sch = flowfile_client.default_schema()
            result = sch.read_artifact("my_model", version=2)
        assert result == "the-object"
        assert captured["name"] == "my_model"
        assert captured["version"] == 2
        assert captured["namespace_id"] == mock_core.SCHEMA_DEFAULT_ID

    def test_list_and_delete_artifact_forward_namespace_id(
        self, mock_core: _MockCore, kernel_catalog_dir: Path, monkeypatch: pytest.MonkeyPatch
    ):
        list_calls: list[dict[str, Any]] = []
        delete_calls: list[dict[str, Any]] = []

        def fake_list(**kwargs):
            list_calls.append(kwargs)
            return []

        def fake_delete(name, **kwargs):
            delete_calls.append({"name": name, **kwargs})

        monkeypatch.setattr(flowfile_client, "list_global_artifacts", fake_list)
        monkeypatch.setattr(flowfile_client, "delete_global_artifact", fake_delete)
        with _patch_core_client(mock_core):
            sch = flowfile_client.default_schema()
            sch.list_artifacts(tags=["ml"])
            sch.delete_artifact("old", version=1)
        assert list_calls == [{"namespace_id": mock_core.SCHEMA_DEFAULT_ID, "tags": ["ml"]}]
        assert delete_calls == [
            {"name": "old", "namespace_id": mock_core.SCHEMA_DEFAULT_ID, "version": 1}
        ]


class TestTopLevelAcceptsRef:
    def test_read_catalog_table_with_ref(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1, "v": "a"}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
            sch = flowfile_client.default_schema()
            ref = sch.get_table_ref("orders")
            out = flowfile_client.read_catalog_table(ref).collect()
        assert out["v"].to_list() == ["a"]

    def test_write_catalog_table_with_ref(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1}])
        with _patch_core_client(mock_core):
            sch = flowfile_client.default_schema()
            ref = sch.get_table_ref("orders")  # lazy, doesn't exist yet
            result = flowfile_client.write_catalog_table(df, ref, write_mode="overwrite")
        assert isinstance(result, TableRef)
        assert result.exists()
        assert result.name == "orders"

    def test_read_with_ref_rejects_extra_schema_arg(
        self, mock_core: _MockCore, kernel_catalog_dir: Path
    ):
        df = _df([{"id": 1}])
        with _patch_core_client(mock_core):
            flowfile_client.write_catalog_table(df, "orders", write_mode="overwrite")
            sch = flowfile_client.default_schema()
            ref = sch.get_table_ref("orders")
            with pytest.raises(ValueError, match="must be omitted"):
                flowfile_client.read_catalog_table(ref, schema="default")
