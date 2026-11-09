"""A flow saved before a hub table was named by its name carries the hub's row ids; they are converted at open
through the hub, and the next save writes the names (``hub_client.legacy_ids``)."""

import re

import pytest
import yaml

from flowfile_core.flowfile.flow_graph.builders.catalog import unnamed_hub_table
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.hub_client import client, session
from flowfile_core.schemas import input_schema
from tests.flowfile.execution.graph_helpers import add_promise, connect, make_graph
from tests.hub_client.conftest import HUB_URL, FakeHubWithCatalog

COLUMNS = [{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "String"}]


@pytest.fixture
def hub(monkeypatch) -> FakeHubWithCatalog:
    fake = FakeHubWithCatalog()
    monkeypatch.setattr(client, "_module_http", fake.http())
    sales = fake.seed_catalog("sales", storage_uri="s3://acme/sales")
    raw = fake.seed_schema(sales, "raw")
    fake.seed_table(raw, "orders", file_path="s3://acme/sales/orders_1", is_remote_storage=True, schema_columns=COLUMNS)
    fake.ids = {"raw": raw["id"], "orders": fake.tables[0]["id"]}
    return fake


@pytest.fixture
def connected(store, hub) -> FakeHubWithCatalog:
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")
    session.save(1, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"]})
    hub.calls.clear()
    return hub


def saved_by_an_older_flowfile(tmp_path, hub: FakeHubWithCatalog, flow_id: int):
    """A flow file as the release before this one wrote it: a reader of the hub's ``orders`` and a writer into its
    ``raw`` schema, each naming the hub by its row ids, with no ``source`` and no qualified names."""
    graph = make_graph(flow_id, execution_location="local")
    add_promise(graph, 1, "catalog_reader")
    graph.add_catalog_reader(
        input_schema.NodeCatalogReader(flow_id=flow_id, node_id=1, user_id=1, catalog_table_name="orders")
    )
    add_promise(graph, 2, "catalog_writer")
    graph.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(table_name="orders_eu"),
        )
    )
    connect(graph, 1, 2)
    path = tmp_path / f"older_{flow_id}.yaml"
    graph.save_flow(str(path))
    document = yaml.safe_load(path.read_text())
    reader, writer = (node["setting_input"] for node in document["nodes"])
    reader.pop("source", None)
    reader.update({"hub_table_id": hub.ids["orders"], "hub_namespace_id": hub.ids["raw"]})
    writer["catalog_write_settings"].pop("source", None)
    writer["catalog_write_settings"]["hub_namespace_id"] = hub.ids["raw"]
    path.write_text(yaml.safe_dump(document, sort_keys=False))
    return path


def settings_of(path) -> tuple[dict, dict]:
    document = yaml.safe_load(path.read_text())
    reader, writer = (node["setting_input"] for node in document["nodes"])
    return reader, writer["catalog_write_settings"]


def test_a_flow_opened_while_connected_names_the_hubs_tables_by_name_and_saves_them_so(tmp_path, connected):
    path = saved_by_an_older_flowfile(tmp_path, connected, 8801)

    graph = open_flow(path, user_id=1)

    reader = graph.get_node(1).setting_input
    assert (reader.source, reader.catalog_full_table_name, reader.catalog_table_name) == (
        "hub",
        "sales.raw.orders",
        "orders",
    )
    assert (reader.catalog_table_id, reader.catalog_namespace_id) == (None, None)
    assert graph.get_node(1).results.errors is None
    writer = graph.get_node(2).setting_input.catalog_write_settings
    assert (writer.source, writer.namespace_full_name, writer.namespace_id) == ("hub", "sales.raw", None)
    assert ("GET", f"/catalog/tables/{connected.ids['orders']}") in connected.calls

    graph.save_flow(str(path))
    saved_reader, saved_writer = settings_of(path)
    assert not {"hub_table_id", "hub_namespace_id"} & saved_reader.keys()
    assert "hub_namespace_id" not in saved_writer
    assert (saved_reader["source"], saved_reader["catalog_full_table_name"]) == ("hub", "sales.raw.orders")
    assert (saved_writer["source"], saved_writer["namespace_full_name"]) == ("hub", "sales.raw")


def test_a_flow_saved_with_names_asks_the_hub_nothing_at_open(tmp_path, connected):
    path = saved_by_an_older_flowfile(tmp_path, connected, 8802)
    open_flow(path, user_id=1).save_flow(str(path))
    connected.calls.clear()

    graph = open_flow(path, user_id=1)

    assert graph.get_node(1).setting_input.catalog_full_table_name == "sales.raw.orders"
    # The reader's build resolves the name on the hub; nothing is read by id and no catalog is walked.
    by_id = re.compile(r"^/catalog/tables/\d+$")
    assert not any(by_id.match(route) or route == "/catalog/namespaces" for _, route in connected.calls)
    assert ("GET", "/catalog/tables/resolve") in connected.calls


def test_a_flow_opened_without_the_hub_keeps_the_step_on_the_hub_and_says_so_at_the_node(tmp_path, store, hub):
    path = saved_by_an_older_flowfile(tmp_path, hub, 8803)

    graph = open_flow(path, user_id=1)

    reader = graph.get_node(1).setting_input
    assert (reader.source, reader.catalog_full_table_name, reader.catalog_table_name) == ("hub", None, "orders")
    assert graph.get_node(1).results.errors == unnamed_hub_table(reader)
    assert "an id from an older Flowfile" in graph.get_node(1).results.errors
    writer = graph.get_node(2).setting_input.catalog_write_settings
    assert (writer.source, writer.namespace_full_name) == ("hub", None)
    assert hub.calls == []


def test_a_flow_opened_for_no_user_is_converted_by_nobody(tmp_path, connected):
    path = saved_by_an_older_flowfile(tmp_path, connected, 8804)

    graph = open_flow(path)

    reader = graph.get_node(1).setting_input
    assert (reader.source, reader.catalog_full_table_name) == ("hub", None)
    assert connected.calls == []
