"""The signed URLs core hands out for a table on local disk: one file, for a while, and nothing beside it."""

from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl

from flowfile_core.delta_sharing import file_tokens, presign


def _file_urls(anonymous, shared, table="orders") -> list[str]:
    response = anonymous.post(
        f"/delta-sharing/shares/{shared.share['name']}/schemas/sales/tables/{table}/query",
        json={},
        headers=shared.headers,
    )
    return [json.loads(line)["file"]["url"] for line in response.text.splitlines() if '"file"' in line]


def _entry_id(shared) -> int:
    return shared.share["tables"][0]["id"]


def test_a_valid_url_streams_the_stored_bytes(anonymous, catalog, share_tables):
    table_id, path = catalog.table("orders", pl.DataFrame({"id": [1, 2, 3]}))
    shared = share_tables(table_id)
    [url] = _file_urls(anonymous, shared)
    body = anonymous.get(urlsplit(url).path)
    assert body.status_code == 200
    stored = next(p for p in Path(path).rglob("*.parquet"))
    assert body.content == stored.read_bytes()


def test_a_tampered_expired_or_foreign_token_is_refused(anonymous, catalog, share_tables):
    table_id, path = catalog.table("orders", pl.DataFrame({"id": [1]}))
    shared = share_tables(table_id)
    [url] = _file_urls(anonymous, shared)
    token = urlsplit(url).path.rsplit("/", 1)[-1]
    payload, signature = token.split(".")

    flipped = payload[:-2] + ("A" if payload[-2] != "A" else "B") + payload[-1]
    assert anonymous.get(f"/delta-sharing/files/{flipped}.{signature}").status_code == 404
    assert anonymous.get("/delta-sharing/files/garbage").status_code == 404

    rel = next(p for p in Path(path).rglob("*.parquet")).relative_to(path).as_posix()
    expired = file_tokens.mint(_entry_id(shared), rel, str(path), int(time.time()) - 5)
    assert anonymous.get(f"/delta-sharing/files/{expired}").status_code == 404

    other_table = file_tokens.mint(_entry_id(shared), rel, str(path) + "_moved", int(time.time()) + 60)
    assert anonymous.get(f"/delta-sharing/files/{other_table}").status_code == 404


def test_a_token_never_reaches_outside_the_table_directory(anonymous, catalog, share_tables, tmp_path):
    table_id, path = catalog.table("orders", pl.DataFrame({"id": [1]}))
    shared = share_tables(table_id)
    outside = tmp_path / "secret.txt"
    outside.write_text("not yours")
    (Path(path) / "link.parquet").symlink_to(outside)
    for rel in ("../../../etc/passwd", "/etc/passwd", "_delta_log/../../x", "link.parquet", ""):
        token = file_tokens.mint(_entry_id(shared), rel, str(path), int(time.time()) + 60)
        assert anonymous.get(f"/delta-sharing/files/{token}").status_code == 404, rel
    assert presign.local_file(str(path), "link.parquet") is None


def test_removing_the_table_from_the_share_ends_its_urls(anonymous, designer, catalog, share_tables):
    table_id, _ = catalog.table("orders", pl.DataFrame({"id": [1]}))
    shared = share_tables(table_id)
    [url] = _file_urls(anonymous, shared)
    assert anonymous.get(urlsplit(url).path).status_code == 200
    designer.delete(f"/delta-sharing/manage/shares/{shared.share['id']}/tables/{_entry_id(shared)}")
    assert anonymous.get(urlsplit(url).path).status_code == 404


def test_tokens_round_trip_and_are_bound_to_the_master_key():
    token = file_tokens.mint(5, "part-0.parquet", "/data/t", int(time.time()) + 60)
    grant = file_tokens.verify(token)
    assert grant is not None and grant.share_table_id == 5 and grant.rel_path == "part-0.parquet"
    assert grant.table_digest == file_tokens.table_digest("/data/t")
    assert file_tokens.verify(token, now=time.time() + 120) is None
