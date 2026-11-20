"""Tests for the chart profile: the payloads sent to the worker, masking, the cut-off, the
cache, the time budget and the fallback. The worker calls are the same seam the catalog
visualization tests patch (``flowfile_core.catalog.service.trigger_visualize_*``).
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import patch

import polars as pl
import pytest

from flowfile_core.ai import chart_profile
from flowfile_core.ai.chart_profile import profile_table, render_profile, stats_payload, type_class
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable
from shared.storage_config import storage

USER = PydanticUser(id=1, username="local_user")

SCHEMA = [
    {"name": "region", "dtype": "String"},
    {"name": "contact", "dtype": "String"},
    {"name": "revenue", "dtype": "Float64"},
    {"name": "quantity", "dtype": "Int64"},
    {"name": "order_date", "dtype": "Date"},
    {"name": "is_return", "dtype": "Boolean"},
    {"name": "tags", "dtype": "List(String)"},
    {"name": "blob", "dtype": "Binary"},
]


@pytest.fixture(autouse=True)
def _fresh_cache() -> Iterator[None]:
    chart_profile.clear_cache()
    yield
    chart_profile.clear_cache()


@pytest.fixture
def table() -> Iterator[Any]:
    with get_db_context() as db:
        ns = CatalogNamespace(name="ChartProfileNs", parent_id=None, level=0, owner_id=1)
        db.add(ns)
        db.commit()
        db.refresh(ns)
        row = CatalogTable(
            name="profiled",
            namespace_id=ns.id,
            owner_id=1,
            file_path="/tmp/flowfile-test/profiled",
            storage_format="delta",
            schema_json=json.dumps(SCHEMA),
            table_type="physical",
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        ids = (row.id, ns.id)
    yield _table_out(ids[0])
    with get_db_context() as db:
        db.query(CatalogTable).filter(CatalogTable.id == ids[0]).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.id == ids[1]).delete()
        db.commit()


def _table_out(table_id: int) -> Any:
    with get_db_context() as db:
        return CatalogService(SQLAlchemyCatalogRepository(db)).get_table(table_id, 1)


class FakeWorker:
    """Answers the fields call and each query shape; records every payload it was sent."""

    def __init__(self, *, stats_delay: float = 0.0, fields_error: Exception | None = None) -> None:
        self.payloads: list[dict] = []
        self.stats_delay = stats_delay
        self.fields_error = fields_error
        self.fields_calls = 0
        self.done = threading.Event()

    def fields(self, worker_source, *, worker_url):
        self.fields_calls += 1
        if self.fields_error is not None:
            raise self.fields_error
        return {
            "fields": [
                {"fid": "region", "analyticType": "dimension"},
                {"fid": "contact", "analyticType": "dimension"},
                {"fid": "revenue", "analyticType": "measure"},
                {"fid": "quantity", "analyticType": "dimension"},
                {"fid": "order_date", "analyticType": "dimension"},
                {"fid": "is_return", "analyticType": "dimension"},
            ]
        }

    def query(self, worker_source, payload, max_rows, *, worker_url):
        self.payloads.append(payload)
        query = payload["workflow"][0]["query"][0]
        if query["op"] == "raw":
            rows = [
                {
                    "region": "West",
                    "contact": "alice@example.com",
                    "revenue": 10.5,
                    "quantity": 4111111111111111,
                    "order_date": "2025-01-02",
                    "is_return": False,
                },
                {
                    "region": "East",
                    "contact": "x" * 60,
                    "revenue": 3.25,
                    "quantity": 2,
                    "order_date": "2025-01-03",
                    "is_return": True,
                },
            ]
        elif query["groupBy"]:
            column = query["groupBy"][0]
            values = {"region": ["West", "East", None], "contact": ["bob@example.com", "carol"]}[column]
            rows = [{column: v} for v in values]
        else:
            if self.stats_delay:
                time.sleep(self.stats_delay)
            rows = [
                {
                    "__rows": 100,
                    "c0_nonnull": 98,
                    "c0_distinct": 3,
                    "c1_nonnull": 100,
                    "c1_distinct": 2,
                    "c2_nonnull": 100,
                    "c2_distinct": 90,
                    "c2_min": 1.5,
                    "c2_max": 999.75,
                    "c3_nonnull": 100,
                    "c3_distinct": 40,
                    "c3_min": 1,
                    "c3_max": 50,
                    "c4_nonnull": 100,
                    "c4_distinct": 300,
                    "c4_min": "2024-01-01",
                    "c4_max": "2025-12-31",
                    "c5_nonnull": 100,
                    "c5_distinct": 2,
                }
            ]
            self.done.set()
        return {"rows": rows, "total_rows": len(rows), "truncated": False, "elapsed_ms": 1.0, "cache_hit": False}

    def patches(self):
        return (
            patch("flowfile_core.catalog.service.trigger_visualize_fields", side_effect=self.fields),
            patch("flowfile_core.catalog.service.trigger_visualize_query", side_effect=self.query),
        )


async def _profile(worker: FakeWorker, table: Any, **kwargs: Any):
    p1, p2 = worker.patches()
    with p1, p2:
        return await profile_table(table, USER, **kwargs)


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        ("Int64", "integer"),
        ("UInt8", "integer"),
        ("Float64", "float"),
        ("Decimal(precision=10, scale=2)", "float"),
        ("String", "string"),
        ("Categorical(ordering='physical')", "string"),
        ("Enum(categories=['a'])", "string"),
        ("Boolean", "boolean"),
        ("Date", "date"),
        ("Datetime(time_unit='us', time_zone=None)", "datetime"),
        ("Time", "other"),
        ("Duration(time_unit='us')", "other"),
        ("List(String)", "other"),
        ("Struct({'a': Int64})", "other"),
        ("Binary", "other"),
        ("int64", "integer"),
        ("double", "float"),
        ("large_string", "string"),
        ("bool", "boolean"),
        ("date32[day]", "date"),
        ("timestamp[us, tz=UTC]", "datetime"),
        ("decimal128(10, 2)", "float"),
        ("dictionary<values=string, indices=int32, ordered=0>", "string"),
    ],
)
def test_type_class_maps_polars_and_pyarrow_dtypes(dtype: str, expected: str) -> None:
    assert type_class(dtype) == expected


@pytest.mark.asyncio
async def test_aggregate_payload_skips_columns_that_cannot_take_it(table: Any) -> None:
    worker = FakeWorker()
    profile = await _profile(worker, table)
    assert profile.complete is True
    stats = worker.payloads[0]
    measures = stats["workflow"][0]["query"][0]["measures"]
    assert stats["workflow"][0]["query"][0]["groupBy"] == []
    assert measures[0] == {"field": "*", "agg": "count", "asFieldKey": "__rows"}
    by_field: dict[str, list[str]] = {}
    for m in measures[1:]:
        by_field.setdefault(m["field"], []).append(m["agg"])
    assert by_field == {
        "region": ["count", "distinctCount"],
        "contact": ["count", "distinctCount"],
        "revenue": ["count", "distinctCount", "min", "max"],
        "quantity": ["count", "distinctCount", "min", "max"],
        "order_date": ["count", "distinctCount", "min", "max"],
        "is_return": ["count", "distinctCount"],
    }
    assert stats == stats_payload(profile.columns)


@pytest.mark.asyncio
async def test_rows_and_value_list_payloads(table: Any) -> None:
    worker = FakeWorker()
    await _profile(worker, table)
    rows = worker.payloads[1]
    assert rows == {
        "workflow": [
            {
                "type": "view",
                "query": [
                    {
                        "op": "raw",
                        "fields": ["region", "contact", "revenue", "quantity", "order_date", "is_return"],
                    }
                ],
            }
        ],
        "limit": 10,
    }
    value_lists = [p["workflow"][0]["query"][0] for p in worker.payloads[2:]]
    assert value_lists == [
        {"op": "aggregate", "groupBy": ["region"], "measures": []},
        {"op": "aggregate", "groupBy": ["contact"], "measures": []},
    ]


@pytest.mark.asyncio
async def test_profile_holds_roles_stats_examples_and_values(table: Any) -> None:
    profile = await _profile(FakeWorker(), table)
    by_name = {c.name: c for c in profile.columns}
    assert profile.row_count == 100
    region = by_name["region"]
    assert (region.type, region.role, region.non_null, region.distinct) == ("string", "dimension", 98, 3)
    assert region.values == ("East", "West")
    assert region.values_exact is True
    revenue = by_name["revenue"]
    assert (revenue.role, revenue.min, revenue.max, revenue.examples) == ("measure", 1.5, 999.75, (10.5, 3.25))
    assert by_name["tags"].type == "other"
    infos = {c.name: c for c in profile.column_infos()}
    assert infos["region"].values == ("East", "West")


@pytest.mark.asyncio
async def test_values_are_masked_and_cut_to_forty_characters(table: Any) -> None:
    profile = await _profile(FakeWorker(), table)
    contact = {c.name: c for c in profile.columns}["contact"]
    assert contact.examples == ("<<email>>", "x" * 39 + "…")
    assert contact.values == ("<<email>>", "carol")
    assert contact.values_exact is False
    assert {c.name: c for c in profile.column_infos()}["contact"].values is None
    quantity = {c.name: c for c in profile.columns}["quantity"]
    assert quantity.examples == ("<<card>>", 2)
    text = render_profile(profile)
    assert "alice@example.com" not in text
    assert "bob@example.com" not in text
    assert "4111111111111111" not in text


@pytest.mark.asyncio
async def test_rendered_profile(table: Any) -> None:
    text = render_profile(await _profile(FakeWorker(), table))
    assert text.splitlines() == [
        'Table "profiled": 100 rows, 8 columns.',
        "Columns (name: type, role; profile):",
        '- "region": string, dimension; 98 non-null, 3 distinct; values: "East", "West"',
        '- "contact": string, dimension; 100 non-null, 2 distinct; values: "<<email>>", "carol"',
        '- "revenue": float, measure; 100 non-null, 90 distinct; min 1.5, max 999.75; e.g. 10.5, 3.25',
        '- "quantity": integer, dimension; 100 non-null, 40 distinct; min 1, max 50; e.g. "<<card>>", 2',
        '- "order_date": date, dimension; 100 non-null, 300 distinct; min "2024-01-01", max "2025-12-31"; '
        'e.g. "2025-01-02", "2025-01-03"',
        '- "is_return": boolean, dimension; 100 non-null, 2 distinct; e.g. false, true',
        '- "tags": other (List(String)), cannot be charted',
        '- "blob": other (Binary), cannot be charted',
    ]


@pytest.mark.asyncio
async def test_second_call_is_served_from_the_cache(table: Any) -> None:
    worker = FakeWorker()
    first = await _profile(worker, table)
    sent = len(worker.payloads)
    second = await _profile(worker, table)
    assert second is first
    assert len(worker.payloads) == sent
    assert worker.fields_calls == 1


@pytest.mark.asyncio
async def test_cache_is_per_user(table: Any) -> None:
    worker = FakeWorker()
    await _profile(worker, table)
    p1, p2 = worker.patches()
    with p1, p2:
        await profile_table(table, PydanticUser(id=2, username="other"))
    assert worker.fields_calls == 2


@pytest.mark.asyncio
async def test_a_changed_table_is_profiled_again(table: Any) -> None:
    worker = FakeWorker()
    await _profile(worker, table)
    with get_db_context() as db:
        row = db.get(CatalogTable, table.id)
        row.updated_at = row.updated_at.replace(year=row.updated_at.year + 1)
        db.commit()
    await _profile(worker, _table_out(table.id))
    assert worker.fields_calls == 2


@pytest.mark.asyncio
async def test_a_new_delta_version_is_profiled_again(table: Any) -> None:
    target = storage.catalog_tables_directory / "chart_profile_delta"
    pl.DataFrame({"region": ["a"]}).write_delta(str(target))
    try:
        with get_db_context() as db:
            db.get(CatalogTable, table.id).file_path = str(target)
            db.commit()
        current = _table_out(table.id)
        worker = FakeWorker()
        await _profile(worker, current)
        await _profile(worker, current)
        assert worker.fields_calls == 1
        pl.DataFrame({"region": ["b"]}).write_delta(str(target), mode="overwrite")
        await _profile(worker, current)
        assert worker.fields_calls == 2
    finally:
        import shutil

        shutil.rmtree(target, ignore_errors=True)


@pytest.mark.asyncio
async def test_time_budget_returns_what_was_finished_and_caches_the_late_result(table: Any) -> None:
    worker = FakeWorker(stats_delay=0.6)
    p1, p2 = worker.patches()
    with p1, p2:
        profile = await profile_table(table, USER, budget=0.2)
        assert profile.complete is False
        assert profile.problem == "timed out after 0.2 s"
        roles = {c.name: c.role for c in profile.columns}
        assert roles["revenue"] == "measure"
        assert all(c.non_null is None for c in profile.columns)
        assert "Statistics and example values are not available" in render_profile(profile)
        assert worker.done.wait(5)
        for _ in range(50):
            late = await profile_table(table, USER, budget=0.2)
            if late.complete:
                break
            time.sleep(0.05)
    assert late.complete is True
    assert late.row_count == 100


@pytest.mark.asyncio
async def test_requests_for_one_table_share_one_build_on_the_profile_threads(table: Any) -> None:
    worker = FakeWorker(stats_delay=0.3)
    threads: list[str] = []
    fill = chart_profile._fill

    def _fill(*args: Any, **kwargs: Any) -> None:
        threads.append(threading.current_thread().name)
        fill(*args, **kwargs)

    p1, p2 = worker.patches()
    with p1, p2, patch.object(chart_profile, "_fill", _fill):
        first, second, third = await asyncio.gather(*(profile_table(table, USER) for _ in range(3)))
    assert first.complete and second is first and third is first
    assert worker.fields_calls == 1
    assert len(threads) == 1 and threads[0].startswith("chart-profile")


@pytest.mark.asyncio
async def test_a_request_that_times_out_does_not_start_a_second_build(table: Any) -> None:
    worker = FakeWorker(stats_delay=0.5)
    p1, p2 = worker.patches()
    with p1, p2:
        early = await profile_table(table, USER, budget=0.1)
        again = await profile_table(table, USER, budget=5)
    assert early.complete is False
    assert again.complete is True
    assert worker.fields_calls == 1


@pytest.mark.asyncio
async def test_failure_falls_back_to_names_and_types(table: Any) -> None:
    profile = await _profile(FakeWorker(fields_error=RuntimeError("worker down")), table)
    assert profile.complete is False
    assert profile.problem == "failed"
    assert [(c.name, c.type, c.role) for c in profile.columns][:3] == [
        ("region", "string", None),
        ("contact", "string", None),
        ("revenue", "float", None),
    ]
    assert '- "revenue": float' in render_profile(profile)


@pytest.mark.asyncio
async def test_a_failed_profile_is_not_cached(table: Any) -> None:
    worker = FakeWorker(fields_error=RuntimeError("worker down"))
    await _profile(worker, table)
    worker.fields_error = None
    profile = await _profile(worker, table)
    assert profile.complete is True
    assert worker.fields_calls == 2
