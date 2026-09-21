"""Unit + endpoint tests for /lsp/dataframe_schemas and the namespace generation/revision bookkeeping."""

import polars as pl
import pytest

from kernel_runtime import main
from kernel_runtime.lsp.dataframe_schemas import collect_dataframe_schemas


def _by_name(frames: list[dict]) -> dict[str, dict]:
    return {f["name"]: f for f in frames}


class TestCollectDataframeSchemas:
    def test_same_column_name_keeps_its_own_dtype(self):
        namespace = {
            "orders": pl.DataFrame({"id": [1], "amount": [1.5]}),
            "lookup": pl.DataFrame({"id": ["a"], "label": ["x"]}),
        }
        frames = _by_name(collect_dataframe_schemas(namespace))
        assert frames["orders"]["columns"] == [
            {"name": "id", "dtype": "Int64"},
            {"name": "amount", "dtype": "Float64"},
        ]
        assert frames["lookup"]["columns"][0] == {"name": "id", "dtype": "String"}

    def test_lazyframe_is_unresolved_without_touching_the_plan(self):
        frames = collect_dataframe_schemas({"lazy": pl.scan_parquet("/nonexistent.parquet")})
        assert frames == [{"name": "lazy", "kind": "LazyFrame", "state": "unresolved", "columns": []}]

    def test_columns_are_capped_and_flagged(self):
        wide = pl.DataFrame({f"c{i}": [i] for i in range(5)})
        frames = collect_dataframe_schemas({"wide": wide}, max_columns=3)
        assert len(frames[0]["columns"]) == 3
        assert frames[0]["truncated"] is True

    def test_not_truncated_when_under_the_cap(self):
        frames = collect_dataframe_schemas({"df": pl.DataFrame({"a": [1]})}, max_columns=3)
        assert frames[0]["truncated"] is False

    def test_frame_cap_keeps_the_alphabetically_first(self):
        namespace = {name: pl.DataFrame({"a": [1]}) for name in ("delta", "alpha", "charlie", "bravo")}
        frames = collect_dataframe_schemas(namespace, max_frames=2)
        assert [f["name"] for f in frames] == ["alpha", "bravo"]

    def test_private_context_module_and_series_names_are_absent(self):
        namespace = {
            "_private": pl.DataFrame({"a": [1]}),
            "flowfile_ctx": pl.DataFrame({"a": [1]}),
            "pl": pl,
            "series": pl.Series("s", [1, 2]),
            "kept": pl.DataFrame({"a": [1]}),
        }
        assert [f["name"] for f in collect_dataframe_schemas(namespace)] == ["kept"]

    def test_hostile_object_is_ignored_without_raising(self):
        class _Hostile:
            @property
            def __class__(self):  # a lying __class__ must not fool the dispatch
                return pl.DataFrame

            @property
            def schema(self):
                raise RuntimeError("schema must not be read")

            def __getattr__(self, name):
                raise RuntimeError(f"attribute {name} must not be read")

        assert collect_dataframe_schemas({"hostile": _Hostile()}) == []

    def test_subclass_overriding_schema_still_reports_the_real_schema(self):
        class _Overridden(pl.DataFrame):
            @property
            def schema(self):
                raise RuntimeError("overridden schema must not be invoked")

        frames = collect_dataframe_schemas({"df": _Overridden({"a": [1], "b": ["x"]})})
        assert frames[0]["kind"] == "DataFrame"
        assert frames[0]["columns"] == [{"name": "a", "dtype": "Int64"}, {"name": "b", "dtype": "String"}]

    def test_non_string_key_is_ignored(self):
        frames = collect_dataframe_schemas({7: pl.DataFrame({"a": [1]}), "df": pl.DataFrame({"a": [1]})})
        assert [f["name"] for f in frames] == ["df"]


def _execute(client, flow_id: int, code: str) -> dict:
    resp = client.post("/execute", json={"node_id": 1, "flow_id": flow_id, "code": code})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _schemas(client, flow_id: int):
    resp = client.post("/lsp/dataframe_schemas", json={"flow_id": flow_id})
    assert resp.status_code == 200, resp.text
    return resp


class TestDataframeSchemasEndpoint:
    def test_ready_after_execution(self, client):
        flow_id = 7001
        body = _execute(client, flow_id, "import polars as pl\norders = pl.DataFrame({'id': [1], 'amount': [1.5]})")
        assert body["success"], body
        assert body["namespace_generation"]
        assert body["revision"] == 1

        payload = _schemas(client, flow_id).json()
        assert payload["state"] == "ready"
        assert payload["namespace_generation"] == body["namespace_generation"]
        assert payload["revision"] == 1
        assert _by_name(payload["dataframes"])["orders"]["columns"][0] == {"name": "id", "dtype": "Int64"}

    def test_revision_increments_and_generation_is_stable(self, client):
        flow_id = 7002
        first = _execute(client, flow_id, "x = 1")
        second = _execute(client, flow_id, "y = 2")
        assert second["namespace_generation"] == first["namespace_generation"]
        assert second["revision"] == 2
        assert _schemas(client, flow_id).json()["revision"] == 2

    def test_failed_execution_still_increments_the_revision(self, client):
        flow_id = 7003
        _execute(client, flow_id, "a = 1")
        _execute(client, flow_id, "b = 2")
        failed = _execute(client, flow_id, "1 / 0")
        assert failed["success"] is False
        assert failed["revision"] == 3
        assert failed["namespace_generation"]
        assert _schemas(client, flow_id).json()["revision"] == 3

    def test_clear_namespace_mints_a_new_generation(self, client):
        flow_id = 7004
        first = _execute(client, flow_id, "df = 1")
        assert client.post("/clear_namespace", params={"flow_id": flow_id}).status_code == 200
        second = _execute(client, flow_id, "df = 2")
        assert second["namespace_generation"] != first["namespace_generation"]
        assert second["revision"] == 1

    def test_lru_recreation_mints_a_new_generation(self, client, monkeypatch):
        monkeypatch.setattr(main, "_MAX_NAMESPACES", 1)
        first = _execute(client, 7005, "a = 1")
        _execute(client, 7006, "b = 2")
        again = _execute(client, 7005, "a = 3")
        assert again["namespace_generation"] != first["namespace_generation"]
        assert again["revision"] == 1

    def test_unseen_flow_is_unavailable_and_allocates_nothing(self, client):
        unseen = -777001
        payload = _schemas(client, unseen).json()
        assert payload == {"namespace_generation": "", "revision": 0, "state": "unavailable", "dataframes": []}
        assert unseen not in main._namespace_store
        assert unseen not in main._namespace_generation

    def test_busy_namespace_reports_generation_without_frames(self, client):
        flow_id = 7007
        body = _execute(client, flow_id, "import polars as pl\ndf = pl.DataFrame({'a': [1]})")
        main._executing_flow_ids[flow_id] = 1
        try:
            payload = _schemas(client, flow_id).json()
        finally:
            main._executing_flow_ids.pop(flow_id, None)
        assert payload["state"] == "busy"
        assert payload["dataframes"] == []
        assert payload["namespace_generation"] == body["namespace_generation"]
        assert payload["revision"] == 1

    def test_response_never_carries_row_values(self, client):
        flow_id = 7008
        sentinel = "SENTINEL_ROW_VALUE_9f3a"
        _execute(client, flow_id, f"import polars as pl\nsecrets = pl.DataFrame({{'token': ['{sentinel}']}})")
        resp = _schemas(client, flow_id)
        assert "token" in resp.text
        assert sentinel not in resp.text


@pytest.mark.parametrize("feature", ["complete", "dataframe_schemas"])
def test_capabilities_advertises_the_feature(client, feature):
    assert feature in client.get("/lsp/capabilities").json()["features"]
