"""The showcase demo's ``build_sales_analytics`` builds in notebook mode with no write.

``fixtures/demo_catalog_pipeline.py`` is the showcase demo script. The
fixture seeds its catalog (tables ``sales`` and ``regions``), publishes its child flow outside the
mode and installs the ``mood_emoji`` custom node from the community-node test registry. The final
``schema.register_flow`` is replaced by a recorder, since notebook mode refuses it.
"""

import pytest

import flowfile as fl
import flowfile_core.kernel as kernel_package
from flowfile_frame import notebook
from flowfile_frame.catalog_reference import SchemaReference
from flowfile_frame.native import is_side_effect_node_type
from flowfile_frame.notebook_cells import clean_run, exec_cell
from test_utils.notebook_demo import DEMO_FILE, REGIONS, SALES, installed_mood_emoji, load_demo, storage_files

WRITTEN_TABLES = {
    "sales_monthly",
    "sales_forecast",
    "sales_top_products",
    "sales_vs_target",
    "sales_detail_full",
    "sales_summary",
}


@pytest.fixture(scope="module")
def demo_catalog():
    with installed_mood_emoji():
        demo = load_demo()
        schema = fl.CatalogReference(demo.CATALOG, auto_create=True).schema(demo.SCHEMA, auto_create=True)
        schema.write_table(fl.from_dict(SALES), "sales")
        schema.write_table(fl.from_dict(REGIONS), "regions")
        clean_ref = demo.publish_clean_orders(schema)
        yield demo, schema, clean_ref


@pytest.fixture
def recorded_registrations(monkeypatch):
    names: list[str] = []

    def record(self, flow_or_frame, *, name, overwrite=False):
        names.append(name)

    monkeypatch.setattr(SchemaReference, "register_flow", record)
    return names


def _state(schema):
    return sorted(t.name for t in schema.list_tables()), storage_files()


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
        DEMO_FILE.read_text()
        + "\nschema = fl.get_catalog(CATALOG).get_schema(SCHEMA)"
        + "\nclean_ref = schema.get_flow('Clean orders')"
        + "\nbuild_sales_analytics(schema, clean_ref)\n"
    )
    result = clean_run([("demo", cell)], ceiling=0, user_id=1, executor=exec_cell)
    assert result["ok"], result.get("error")
    assert sorted(n["type"] for n in result["flowfile_data"]["nodes"]) == types
    assert sorted(result["cells"]["demo"]) == sorted(n["id"] for n in result["flowfile_data"]["nodes"])
    assert result["refusals"] == []
    assert recorded_registrations == ["Sales analytics", "Sales analytics"]
    assert _state(schema) == before
    assert kernel_package.get_kernel_manager_if_initialized() is None
