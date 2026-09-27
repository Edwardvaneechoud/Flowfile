"""The PR 1 done-when test: the showcase demo's ``build_sales_analytics`` builds in notebook mode with no write.

``fixtures/demo_catalog_pipeline.py`` is a tracked copy of the repo-root demo, so CI has it. The
fixture seeds its catalog (tables ``sales`` and ``regions``), publishes its child flow outside the
mode and installs the ``mood_emoji`` custom node from the community-node test registry. The final
``schema.register_flow`` is replaced by a recorder, since notebook mode refuses it.
"""

import importlib.util
import shutil
from pathlib import Path

import polars as pl
import pytest

import flowfile as fl
import flowfile_core.kernel as kernel_package
from flowfile_core.configs import node_store
from flowfile_core.flowfile.user_defined.registry import registry
from flowfile_frame import notebook
from flowfile_frame.catalog_reference import SchemaReference
from flowfile_frame.native import is_side_effect_node_type
from flowfile_frame.notebook_cells import clean_run
from shared.storage_config import storage

FIXTURE = Path(__file__).parent / "fixtures" / "demo_catalog_pipeline.py"
MOOD_EMOJI = (
    Path(__file__).parents[2]
    / "flowfile_core/tests/flowfile/community_nodes/fixture_registry/nodes/mood_emoji/node.py"
)
WRITTEN_TABLES = {
    "sales_monthly",
    "sales_forecast",
    "sales_top_products",
    "sales_vs_target",
    "sales_detail_full",
    "sales_summary",
}
SALES = {
    "order_id": [1, 2, 3, 4, 5, 6],
    "region": ["N", "S", "N", "S", "N", "S"],
    "product": ["a", "b", "a", "c", "b", "a"],
    "category": ["x", "y", "x", "y", "x", "x"],
    "status": ["Completed"] * 5 + ["Cancelled"],
    "amount": [30.0, 50.0, 70.0, 20.0, 90.0, 40.0],
    "quantity": [1, 2, 3, 1, 2, 1],
    "order_date": pl.date_range(pl.date(2024, 1, 1), pl.date(2024, 6, 1), "1mo", eager=True).to_list(),
}
REGIONS = {"region": ["N", "S"], "manager": ["Ann", "Bob"], "target_sales": [100.0, 100.0]}


def _load_demo():
    spec = importlib.util.spec_from_file_location("notebook_demo_catalog_pipeline", FIXTURE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _files() -> set[Path]:
    """Every file under the storage dir (catalog tables, flows, outputs), minus the catalog DB and logs.

    Outside Docker the user-data dir is the home directory, so it is not scanned.
    """
    return {
        path
        for path in Path(storage.base_directory).rglob("*")
        if path.is_file()
        and not any(part.endswith("logs") or part == "__pycache__" for part in path.parts)
        and ".db" not in path.name
    }


@pytest.fixture(scope="module")
def demo_catalog():
    directory = registry.directory
    directory.mkdir(parents=True, exist_ok=True)
    node_file = directory / "mood_emoji.py"
    installed = not node_file.exists()
    saved = dict(node_store.node_dict), list(node_store.nodes_list), dict(node_store.CUSTOM_NODE_STORE._overrides)
    if installed:
        shutil.copyfile(MOOD_EMOJI, node_file)
        registry.load_file(node_file)

    demo = _load_demo()
    schema = fl.CatalogReference(demo.CATALOG, auto_create=True).schema(demo.SCHEMA, auto_create=True)
    schema.write_table(fl.from_dict(SALES), "sales")
    schema.write_table(fl.from_dict(REGIONS), "regions")
    clean_ref = demo.publish_clean_orders(schema)
    yield demo, schema, clean_ref

    if installed:
        registry.remove_file(node_file)
        node_file.unlink()
        node_store.node_dict.clear()
        node_store.node_dict.update(saved[0])
        node_store.nodes_list[:] = saved[1]
        node_store.CUSTOM_NODE_STORE.clear()
        node_store.CUSTOM_NODE_STORE.update(saved[2])


@pytest.fixture
def recorded_registrations(monkeypatch):
    names: list[str] = []

    def record(self, flow_or_frame, *, name, overwrite=False):
        names.append(name)

    monkeypatch.setattr(SchemaReference, "register_flow", record)
    return names


def _state(schema):
    return sorted(t.name for t in schema.list_tables()), _files()


def test_build_sales_analytics_builds_in_notebook_mode_without_writing(demo_catalog, recorded_registrations):
    demo, schema, clean_ref = demo_catalog
    before = _state(schema)
    assert not WRITTEN_TABLES & set(before[0])

    with notebook.notebook_mode() as mode:
        graph = demo.build_sales_analytics(schema, clean_ref)
        assert graph is mode.graph
        types = sorted(n.node_type for n in graph.nodes)
        writers = [n for n in graph.nodes if is_side_effect_node_type(n.node_type)]
        assert sum(n.node_type == "catalog_writer" for n in writers) == 6
        assert all(n.deferred_until_run for n in writers)
        assert "mood_emoji" in types and "run_flow" in types and "python_script" in types

    assert recorded_registrations == ["Sales analytics"]
    assert _state(schema) == before
    assert kernel_package.get_kernel_manager_if_initialized() is None

    cell = (
        FIXTURE.read_text()
        + "\nschema = fl.get_catalog(CATALOG).get_schema(SCHEMA)"
        + "\nclean_ref = schema.get_flow('Clean orders')"
        + "\nbuild_sales_analytics(schema, clean_ref)\n"
    )
    result = clean_run([("demo", cell)], ceiling=0)
    assert result["ok"], result.get("error")
    assert sorted(n["type"] for n in result["flowfile_data"]["nodes"]) == types
    assert sorted(result["cells"]["demo"]) == sorted(n["id"] for n in result["flowfile_data"]["nodes"])
    assert result["refusals"] == []
    assert recorded_registrations == ["Sales analytics", "Sales analytics"]
    assert _state(schema) == before
    assert kernel_package.get_kernel_manager_if_initialized() is None
