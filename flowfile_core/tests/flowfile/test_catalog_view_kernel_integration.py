"""Docker E2E: a kernel cell reads a catalog view through ``flowfile_ctx``.

The only test exercising the real chain: kernel container → Core materialize
endpoint → worker IPC write into the kernel-shared volume → host→container
path translation → ``pl.scan_ipc`` inside the container.

Requires Docker, a running worker (session conftest), and a kernel image that
includes the view-read client (kernel_runtime >= 0.5.1).
"""

import asyncio
import shutil
import tempfile

import polars as pl
import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import ExecuteRequest, ExecuteResult

pytestmark = pytest.mark.kernel

_SAMPLE_DATA = [
    {"name": "Alice", "age": 30, "city": "Amsterdam"},
    {"name": "Bob", "age": 25, "city": "Berlin"},
    {"name": "Charlie", "age": 35, "city": "Copenhagen"},
]


def _run(coro):
    """Run an async coroutine from sync test code."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@pytest.fixture()
def seeded_view():
    """Physical Delta table + query view over it; targeted teardown."""
    delta_dir = tempfile.mkdtemp(prefix="view_e2e_people_")
    pl.DataFrame(_SAMPLE_DATA).write_delta(delta_dir)
    created_table_ids: list[int] = []
    created_ns_ids: list[int] = []
    with get_db_context() as db:
        cat = CatalogNamespace(name="ViewE2ECat", level=0, owner_id=1)
        db.add(cat)
        db.commit()
        db.refresh(cat)
        schema = CatalogNamespace(name="ViewE2ESch", level=1, parent_id=cat.id, owner_id=1)
        db.add(schema)
        db.commit()
        db.refresh(schema)
        created_ns_ids += [schema.id, cat.id]
        physical = CatalogTable(
            name="people_view_e2e",
            namespace_id=schema.id,
            owner_id=1,
            file_path=delta_dir,
            storage_format="delta",
        )
        db.add(physical)
        db.commit()
        db.refresh(physical)
        created_table_ids.append(physical.id)
        view = CatalogTable(
            name="adults_view_e2e",
            namespace_id=schema.id,
            owner_id=1,
            file_path=None,
            storage_format="delta",
            table_type="virtual",
            sql_query="SELECT name, age FROM people_view_e2e WHERE age > 28",
        )
        db.add(view)
        db.commit()
        db.refresh(view)
        created_table_ids.append(view.id)

    yield "adults_view_e2e"

    with get_db_context() as db:
        for table_id in created_table_ids:
            row = db.get(CatalogTable, table_id)
            if row is not None:
                db.delete(row)
        for ns_id in created_ns_ids:
            ns = db.get(CatalogNamespace, ns_id)
            if ns is not None:
                db.delete(ns)
        db.commit()
    shutil.rmtree(delta_dir, ignore_errors=True)


class TestKernelReadsCatalogView:
    def test_execute_cell_reading_view_returns_rows(
        self, kernel_manager_with_core: tuple[KernelManager, str], seeded_view: str
    ):
        """`flowfile_ctx.read_catalog_table` on a view materialises and scans it."""
        manager, kernel_id = kernel_manager_with_core

        code = f"""
df = flowfile_ctx.read_catalog_table("{seeded_view}", schema="ViewE2ESch").collect()
print("VIEW_ROWS:", df.height)
print("VIEW_NAMES:", sorted(df["name"].to_list()))
"""
        result: ExecuteResult = _run(
            manager.execute(
                kernel_id,
                ExecuteRequest(
                    node_id=1,
                    code=code,
                    input_paths={},
                    output_dir="/shared/test_view_read",
                ),
            )
        )
        assert result.success, f"Execution failed: {result.error}"
        assert "VIEW_ROWS: 2" in result.stdout
        assert "VIEW_NAMES: ['Alice', 'Charlie']" in result.stdout
