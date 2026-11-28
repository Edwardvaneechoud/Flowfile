"""The Delta Sharing protocol answered for catalog tables on local disk, read back as a client would."""

from __future__ import annotations

import io
import json
import socket
import threading
from urllib.parse import urlsplit

import polars as pl
import pytest
import uvicorn

from flowfile_core import main

BASE = "/delta-sharing"


def _lines(response) -> list[dict]:
    assert response.headers["content-type"].startswith("application/x-ndjson"), response.headers
    return [json.loads(line) for line in response.text.splitlines() if line.strip()]


def _download(client, url: str) -> bytes:
    parts = urlsplit(url)
    response = client.get(parts.path)
    assert response.status_code == 200, response.text
    return response.content


def _frame(client, lines: list[dict]) -> pl.DataFrame:
    """What a client reads: every file's rows, with the file's partition values added as columns."""
    frames = []
    for line in lines:
        if "file" not in line:
            continue
        frame = pl.read_parquet(io.BytesIO(_download(client, line["file"]["url"])))
        for column, value in line["file"]["partitionValues"].items():
            frame = frame.with_columns(pl.lit(value).alias(column))
        frames.append(frame)
    return pl.concat(frames, how="diagonal") if frames else pl.DataFrame()


def _query(anonymous, shared, table: str, body: dict | None = None, schema: str = "sales"):
    return anonymous.post(
        f"{BASE}/shares/{shared.share['name']}/schemas/{schema}/tables/{table}/query",
        json=body or {},
        headers=shared.headers,
    )


def test_listing_names_only_what_the_share_holds(anonymous, catalog, share_tables):
    orders_id, _ = catalog.table("orders", pl.DataFrame({"id": [1, 2, 3], "amount": [10.0, 20.5, 7.25]}))
    catalog.table("not_shared", pl.DataFrame({"x": [1]}))
    shared = share_tables(orders_id)

    shares = anonymous.get(f"{BASE}/shares", headers=shared.headers)
    assert shares.status_code == 200
    assert [s["name"] for s in shares.json()["items"]] == ["retail"]
    assert shares.json()["items"][0]["id"] == shared.share["uuid"]

    one = anonymous.get(f"{BASE}/shares/retail", headers=shared.headers)
    assert one.json() == {"share": {"name": "retail", "id": shared.share["uuid"]}}

    schemas = anonymous.get(f"{BASE}/shares/retail/schemas", headers=shared.headers)
    assert schemas.json()["items"] == [{"name": "sales", "share": "retail"}]

    tables = anonymous.get(f"{BASE}/shares/retail/schemas/sales/tables", headers=shared.headers).json()["items"]
    assert [(t["schema"], t["name"], t["share"]) for t in tables] == [("sales", "orders", "retail")]
    all_tables = anonymous.get(f"{BASE}/shares/retail/all-tables", headers=shared.headers).json()["items"]
    assert all_tables == tables


def test_pagination_hands_out_a_token_until_the_last_page(anonymous, catalog, share_tables):
    ids = [catalog.table(f"t{i}", pl.DataFrame({"x": [i]}))[0] for i in range(3)]
    shared = share_tables(*ids)
    first = anonymous.get(f"{BASE}/shares/retail/all-tables?maxResults=2", headers=shared.headers).json()
    assert len(first["items"]) == 2 and "nextPageToken" in first
    second = anonymous.get(
        f"{BASE}/shares/retail/all-tables?maxResults=2&pageToken={first['nextPageToken']}", headers=shared.headers
    ).json()
    assert len(second["items"]) == 1 and "nextPageToken" not in second


def test_version_metadata_and_query_read_back_the_table(anonymous, catalog, share_tables):
    source = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", None], "amount": [10.0, 20.5, 7.25]})
    table_id, path = catalog.table("orders", source)
    pl.DataFrame({"id": [4], "name": ["d"], "amount": [1.0]}).write_delta(path, mode="append")
    shared = share_tables(table_id)
    table_url = f"{BASE}/shares/retail/schemas/sales/tables/orders"

    version = anonymous.get(f"{table_url}/version", headers=shared.headers)
    assert version.status_code == 200 and version.headers["Delta-Table-Version"] == "1"
    head = anonymous.head(f"{table_url}/version", headers=shared.headers)
    assert head.status_code == 200 and head.headers["Delta-Table-Version"] == "1"

    metadata = _lines(anonymous.get(f"{table_url}/metadata", headers=shared.headers))
    assert metadata[0] == {"protocol": {"minReaderVersion": 1}}
    meta = metadata[1]["metaData"]
    assert meta["format"] == {"provider": "parquet"}
    assert [f["name"] for f in json.loads(meta["schemaString"])["fields"]] == ["id", "name", "amount"]

    response = _query(anonymous, shared, "orders")
    assert response.status_code == 200, response.text
    assert response.headers["Delta-Table-Version"] == "1"
    lines = _lines(response)
    files = [line["file"] for line in lines if "file" in line]
    assert len(files) == 2 and lines[1]["metaData"]["numFiles"] == 2
    assert all(json.loads(f["stats"])["numRecords"] >= 1 for f in files)
    assert len({f["id"] for f in files}) == 2
    got = _frame(anonymous, lines).sort("id")
    expected = pl.concat([source, pl.DataFrame({"id": [4], "name": ["d"], "amount": [1.0]})])
    assert got.equals(expected)


def test_a_version_and_a_timestamp_read_the_table_as_it_was(anonymous, catalog, share_tables):
    table_id, path = catalog.table("orders", pl.DataFrame({"id": [1, 2]}))
    pl.DataFrame({"id": [3]}).write_delta(path, mode="append")
    shared = share_tables(table_id)

    at_zero = _query(anonymous, shared, "orders", {"version": 0})
    assert at_zero.headers["Delta-Table-Version"] == "0"
    lines = _lines(at_zero)
    assert all(line["file"]["version"] == 0 for line in lines if "file" in line)
    assert _frame(anonymous, lines).sort("id")["id"].to_list() == [1, 2]

    future = _query(anonymous, shared, "orders", {"timestamp": "2999-01-01T00:00:00Z"})
    assert future.headers["Delta-Table-Version"] == "1"

    missing = _query(anonymous, shared, "orders", {"version": 7})
    assert missing.status_code == 400 and missing.json()["errorCode"] == "INVALID_PARAMETER_VALUE"

    cdf = _query(anonymous, shared, "orders", {"startingVersion": 0})
    assert cdf.status_code == 400


def test_a_table_shared_without_history_reads_only_its_head(anonymous, catalog, share_tables):
    table_id, path = catalog.table("orders", pl.DataFrame({"id": [1]}))
    pl.DataFrame({"id": [2]}).write_delta(path, mode="append")
    shared = share_tables(table_id, history=False)
    assert _query(anonymous, shared, "orders").status_code == 200
    refused = _query(anonymous, shared, "orders", {"version": 0})
    assert refused.status_code == 403 and refused.json()["errorCode"] == "PERMISSION_DENIED"


def test_partition_values_travel_with_each_file(anonymous, catalog, share_tables):
    frame = pl.DataFrame({"id": [1, 2, 3], "region": ["eu", "us", "eu"]})
    table_id, _ = catalog.table("by_region", frame, delta_write_options={"partition_by": ["region"]})
    shared = share_tables(table_id)
    lines = _lines(_query(anonymous, shared, "by_region"))
    assert lines[1]["metaData"]["partitionColumns"] == ["region"]
    assert {line["file"]["partitionValues"]["region"] for line in lines if "file" in line} == {"eu", "us"}
    got = _frame(anonymous, lines).select("id", "region").sort("id")
    assert got.equals(frame)


@pytest.fixture()
def live_core():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    server = uvicorn.Server(uvicorn.Config(main.app, lifespan="off", log_level="warning"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    server.should_exit = True
    thread.join(timeout=10)


def test_a_client_reads_the_files_straight_from_their_urls(live_core, designer, catalog, share_tables):
    """Over a real socket, the URLs a query hands out are what Polars (or any client) downloads."""
    import httpx

    frame = pl.DataFrame({"id": list(range(100)), "v": [i * 0.5 for i in range(100)]})
    table_id, _ = catalog.table("wide", frame)
    shared = share_tables(table_id)
    share_id = shared.share["id"]
    profile = designer.post(
        f"/delta-sharing/manage/shares/{share_id}/profile",
        json={"consumer_id": shared.consumer["id"], "endpoint": live_core},
    ).json()
    endpoint = profile["profile"]["endpoint"]
    assert endpoint == f"{live_core}/delta-sharing"
    headers = {"Authorization": f"Bearer {profile['profile']['bearerToken']}"}

    response = httpx.post(f"{endpoint}/shares/retail/schemas/sales/tables/wide/query", json={}, headers=headers)
    assert response.status_code == 200, response.text
    urls = [json.loads(line)["file"]["url"] for line in response.text.splitlines() if '"file"' in line]
    assert urls and all(url.startswith(f"{live_core}/delta-sharing/files/") for url in urls)
    got = pl.concat([pl.read_parquet(url) for url in urls]).sort("id")
    assert got.equals(frame)


def test_file_urls_follow_the_address_a_reverse_proxy_reports(anonymous, catalog, share_tables, monkeypatch):
    """Behind nginx (``/api/`` → core) the URLs go back through the proxy; malformed headers are ignored."""
    shared = share_tables(catalog.table("orders", pl.DataFrame({"id": [1]}))[0])

    def first_url(headers: dict[str, str]) -> str:
        response = anonymous.post(
            f"{BASE}/shares/retail/schemas/sales/tables/orders/query",
            json={},
            headers={**shared.headers, **headers},
        )
        return next(json.loads(line)["file"]["url"] for line in response.text.splitlines() if '"file"' in line)

    proxied = first_url({"X-Forwarded-Host": "data.example.com", "X-Forwarded-Proto": "https",
                         "X-Forwarded-Prefix": "/api"})
    assert proxied.startswith("https://data.example.com/api/delta-sharing/files/")
    bogus = first_url({"X-Forwarded-Host": "evil.com/x?y", "X-Forwarded-Prefix": "/api?x"})
    assert bogus.startswith("http://testserver/delta-sharing/files/")
    monkeypatch.setenv("FLOWFILE_DELTA_SHARING_ENDPOINT", "https://share.example.org/ds/")
    assert first_url({"X-Forwarded-Host": "data.example.com"}).startswith("https://share.example.org/ds/files/")
