"""What Flowfile gains from an Iceberg REST catalog that hands out storage keys.

The helpers in ``shared/iceberg_utils.py`` (Polars underneath) read and write one of its tables without any storage
key of Flowfile's own. Asked for a table, the catalog answers with temporary keys for that table's files; a process
that was given only the catalog's address reads with them. A second process that knows no catalog reads the same
table with what the first one handed it. Rows another engine deleted stay deleted, and what is appended or
overwritten is what the catalog and another engine see. The catalog in these tests asks for no sign-in.

Every test skips unless the catalog runs: ``python -m test_utils.iceberg_rest start``.
"""

import dataclasses
import json
import os

import polars as pl
import pytest
import requests
from botocore.exceptions import ClientError

from shared.iceberg_utils import scan_table, storage_properties, table_access
from shared.tests.iceberg.table_child import expected, rows_frame
from shared.tests.iceberg.write_probe import probe_batch
from test_utils.iceberg_rest import fixtures

pytestmark = pytest.mark.iceberg_rest

CHILD = "shared.tests.iceberg.table_child"
CHILD_AWS_ENV = ["AWS_CONFIG_FILE", "AWS_EC2_METADATA_DISABLED", "AWS_ENDPOINT_URL", "AWS_SHARED_CREDENTIALS_FILE"]
KEY_NAMES = {"s3.access-key-id", "s3.secret-access-key", "s3.session-token", "s3.endpoint"}
CATALOG_NAMES = {"uri", "warehouse", "token", "credential", "auth.manager", "type"}


def _raw_table_answer(namespace: str, name: str) -> dict:
    config = requests.get(f"{fixtures.CATALOG_URI}/v1/config", params={"warehouse": fixtures.WAREHOUSE}, timeout=10)
    config.raise_for_status()
    prefix = {**config.json()["defaults"], **config.json()["overrides"]}["prefix"]
    answer = requests.get(
        f"{fixtures.CATALOG_URI}/v1/{prefix}/namespaces/{namespace}/tables/{name}",
        headers={"X-Iceberg-Access-Delegation": "vended-credentials"},
        timeout=10,
    )
    answer.raise_for_status()
    return answer.json()


def test_catalog_hands_out_storage_keys(rest_catalog, namespace, seed_table):
    """A table loaded from the catalog carries temporary keys to its files, separate from the catalog's own settings."""
    seed_table("handed_keys", 10)
    props = storage_properties(rest_catalog.load_table(f"{namespace}.handed_keys"))

    assert KEY_NAMES <= set(props), f"missing: {sorted(KEY_NAMES - set(props))}"
    assert props["s3.endpoint"].rstrip("/") == fixtures.S3_ENDPOINT
    assert {"s3.region", "client.region"} & set(props), "no region among the storage properties"
    is_root_key = props["s3.access-key-id"] == fixtures.MINIO_ROOT_USER
    assert not is_root_key, "the access key is the store's root key"
    root_secret_handed_out = fixtures.MINIO_ROOT_PASSWORD in props.values()
    assert not root_secret_handed_out, "the store's root secret was handed out"
    assert props.get("s3.remote-signing-enabled", "false").lower() != "true", "remote signing is on"
    assert not CATALOG_NAMES & set(props), f"catalog settings leaked in: {sorted(CATALOG_NAMES & set(props))}"
    not_text = sorted(name for name, value in props.items() if not isinstance(value, str))
    assert not not_text, f"not text: {not_text}"
    json.dumps(props)

    raw = _raw_table_answer(namespace, "handed_keys")
    in_config = sorted(name for name in raw.get("config", {}) if name.startswith("s3."))
    in_credentials = sorted(
        {name for credential in raw.get("storage-credentials") or [] for name in credential.get("config", {})}
    )
    print(json.dumps({"storage_property_names": sorted(props), "raw_config_s3_names": in_config,
                      "raw_storage_credentials_names": in_credentials,
                      "access_key_differs_from_root": props["s3.access-key-id"] != fixtures.MINIO_ROOT_USER,
                      "session_token_length": len(props["s3.session-token"]),
                      "has_expiry": "s3.session-token-expires-at-ms" in props}))
    assert {"s3.access-key-id", "s3.session-token"} <= {*in_config, *in_credentials}


def test_reads_with_only_the_catalogs_keys(rest_catalog, namespace, seed_table, child_env, run_child):
    """Flowfile reads a catalog's table when the only thing it was given is the catalog's address."""
    seed_table("read_by_address", 1000, files=2)

    answer = run_child(
        CHILD,
        {"op": "read", "catalog": fixtures.catalog_properties(), "identifier": f"{namespace}.read_by_address"},
        child_env,
    )

    print(json.dumps(answer))
    assert (answer["rows"], answer["id_sum"]) == expected(1000)
    assert answer["readers"]["default"] == "ok"
    assert answer["aws_env"] == CHILD_AWS_ENV


def test_another_process_reads_with_what_it_was_handed(rest_catalog, namespace, seed_table, child_env, run_child):
    """One process asks the catalog; another that cannot reach any catalog reads the table with what it was handed."""
    seed_table("handed_over", 1000, files=2)
    access = table_access(rest_catalog.load_table(f"{namespace}.handed_over"))
    handed = json.dumps(dataclasses.asdict(access))

    forbidden = {"the catalog's address": [fixtures.CATALOG_URL], "the catalog's token": ['"dummy"'],
                 "a warehouse setting": ['"warehouse"'], "a root secret": fixtures.secret_values()}
    named = [what for what, texts in forbidden.items() if any(text in handed for text in texts)]
    assert not named, f"the handed-over access holds: {named}"
    shown_in_repr = sorted(name for name, value in access.storage_properties.items() if value in repr(access))
    assert not shown_in_repr, f"repr shows: {shown_in_repr}"
    assert scan_table(access).select(pl.len(), pl.col("id").sum()).collect().row(0) == expected(1000)

    with fixtures.catalog_paused():
        assert fixtures.catalog_is_paused()
        with pytest.raises(requests.RequestException):
            requests.get(f"{fixtures.CATALOG_URL}/health", timeout=2)
        answer = run_child(CHILD, {"op": "read", "access": json.loads(handed)}, child_env)

    print(json.dumps({"handed_keys": sorted(json.loads(handed)), "handed_property_names": sorted(
        access.storage_properties), "handed_bytes": len(handed.encode()), **answer}))
    assert (answer["rows"], answer["id_sum"]) == expected(1000)
    assert answer["readers"]["default"] == "ok"


def test_the_keys_open_only_their_own_table(rest_catalog, namespace, seed_table):
    """The keys the catalog hands out for one table are refused on another table's files."""
    mine, other = seed_table("keys_mine", 10), seed_table("keys_other", 10)
    props = table_access(mine).storage_properties

    own_bucket, own_key = fixtures.first_data_file(mine)
    fixtures.s3_client(props).head_object(Bucket=own_bucket, Key=own_key)
    bucket, key = fixtures.first_data_file(other)
    with pytest.raises(ClientError) as refused:
        fixtures.s3_client(props).get_object(Bucket=bucket, Key=key)
    code = refused.value.response["Error"]["Code"]
    print(json.dumps({"own_data_file": "opened", "other_tables_data_file": code}))
    assert code == "AccessDenied"


def test_without_the_handed_out_keys_the_table_does_not_open(rest_catalog, namespace, seed_table, child_env, run_child):
    """Without the three key values the same read fails, so it was the keys, not an open bucket, that opened it."""
    access = dataclasses.asdict(table_access(seed_table("no_keys", 10)))
    secrets = fixtures.key_values(access["storage_properties"])
    for name in ("s3.access-key-id", "s3.secret-access-key", "s3.session-token"):
        access["storage_properties"].pop(name)

    with pytest.raises(AssertionError, match="exited") as failed:
        run_child(CHILD, {"op": "read", "access": access}, child_env, secrets=secrets)
    print(json.dumps({"read_without_keys": str(failed.value).splitlines()[0]}))


def _deleted_rows_summary(table) -> dict:
    summary = table.current_snapshot().summary
    return {
        "delete_files": table.inspect.delete_files().num_rows,
        "total_position_deletes": int(summary.get("total-position-deletes") or 0),
        "total_delete_files": int(summary.get("total-delete-files") or 0),
        "operation": str(summary.operation.value),
    }


def test_reads_after_a_copy_on_write_delete(rest_catalog, namespace, seed_table, child_env, run_child):
    """Rows deleted by rewriting a file stay deleted when Flowfile reads the table."""
    table = seed_table("copy_on_write", 1000, files=2)
    table.delete("id < 100")
    table = rest_catalog.load_table(f"{namespace}.copy_on_write")
    summary = _deleted_rows_summary(table)
    assert summary["delete_files"] == 0, "a copy-on-write delete left delete files"

    answer = run_child(CHILD, {"op": "read", "access": dataclasses.asdict(table_access(table)), "bound": 500},
                       child_env)

    print(json.dumps({**summary, **answer}))
    assert (answer["rows"], answer["id_sum"]) == (900, sum(range(100, 1000)))
    assert answer["len_pushdown"] == 900
    assert answer["filtered_rows"] == 499


def _seed_through_duckdb(rest_catalog, duck, identifier: str, format_version: str) -> None:
    """Create the table and append its rows in two inserts from DuckDB (PyIceberg 0.12 cannot append to version 3)."""
    arrow = rows_frame(1000).collect().to_arrow(compat_level=pl.CompatLevel.oldest())
    rest_catalog.create_table(identifier, schema=arrow.schema, properties={"format-version": format_version})
    fixtures.attach_lake(duck)
    for offset in (0, 500):
        duck.register("seed_rows", arrow.slice(offset, 500))
        duck.execute(f"INSERT INTO lake.{identifier} SELECT * FROM seed_rows")
        duck.unregister("seed_rows")


@pytest.mark.parametrize(
    "format_version, delete_file_kind", [("2", "position delete files"), ("3", "deletion vectors")]
)
def test_reads_a_table_with_delete_files_written_by_another_engine(
    rest_catalog, namespace, seed_table, child_env, run_child, duck, format_version, delete_file_kind
):
    """Rows another engine (DuckDB) marked as deleted in delete files stay deleted when Flowfile reads the table."""
    name = f"delete_files_v{format_version}"
    if format_version == "2":
        seed_table(name, 1000, files=2, properties={"format-version": format_version})
    else:
        _seed_through_duckdb(rest_catalog, duck, f"{namespace}.{name}", format_version)
    fixtures.attach_lake(duck)
    duck.execute(f"DELETE FROM lake.{namespace}.{name} WHERE id % 7 = 0")
    table = rest_catalog.load_table(f"{namespace}.{name}")
    summary = _deleted_rows_summary(table)
    assert summary["delete_files"] >= 1 and summary["total_position_deletes"] > 0, f"no delete files: {summary}"

    answer = run_child(CHILD, {"op": "read", "access": dataclasses.asdict(table_access(table)), "bound": 500},
                       child_env)

    kept = [i for i in range(1000) if i % 7]
    formats = sorted(set(table.inspect.delete_files().column("file_format").to_pylist()))
    print(json.dumps({"format_version": format_version, "delete_file_formats": formats, **summary, **answer}))
    assert (answer["rows"], answer["id_sum"]) == (len(kept), sum(kept))
    assert answer["len_pushdown"] == len(kept)
    assert answer["filtered_rows"] == len([i for i in kept if i > 500])
    assert answer["readers"]["default"] == "ok"
    assert answer["readers"]["pyiceberg"] == "ok"
    assert formats == (["PARQUET"] if format_version == "2" else ["PUFFIN"]), f"{delete_file_kind}: {formats}"


def _catalog_view(namespace: str, name: str) -> dict:
    metadata = _raw_table_answer(namespace, name)["metadata"]
    snapshots = {snapshot["snapshot-id"]: snapshot for snapshot in metadata.get("snapshots", [])}
    current = snapshots[metadata["current-snapshot-id"]]
    return {
        "rows": int(current["summary"]["total-records"]),
        "snapshots": len(snapshots),
        "operations": [snapshot["summary"]["operation"] for snapshot in metadata.get("snapshots", [])],
    }


def test_append_then_overwrite_is_what_the_catalog_reports(
    rest_catalog, namespace, child_env, run_child, duck
):
    """What Flowfile appends or overwrites is what the catalog, another engine (DuckDB) and a fresh read all see."""
    identifier = f"{namespace}.written"
    rest_catalog.create_table(identifier, schema=rows_frame(1).collect().to_arrow(
        compat_level=pl.CompatLevel.oldest()).schema)
    warehouse = fixtures.warehouse_id()
    writes = [("append", 1000, 0, expected(1000)), ("append", 500, 1000, expected(1500)),
              ("overwrite", 300, 5000, expected(300, 5000))]
    snapshots_before = 0
    for mode, rows, start, (total, id_sum) in writes:
        written = run_child(CHILD, {"op": "write", "catalog": fixtures.catalog_properties(), "identifier": identifier,
                                    "mode": mode, "rows": rows, "start": start}, child_env)
        assert written["aws_env"] == CHILD_AWS_ENV

        catalog = _catalog_view(namespace, "written")
        statistics = requests.get(f"{fixtures.CATALOG_URL}/management/v1/warehouse/{warehouse}/statistics", timeout=10)
        fixtures.attach_lake(duck)
        duck_rows, duck_sum = duck.execute(f"SELECT count(*), sum(id) FROM lake.{identifier}").fetchone()
        table = rest_catalog.load_table(identifier)
        polars = run_child(CHILD, {"op": "read", "access": dataclasses.asdict(table_access(table))}, child_env)

        print(json.dumps({"write": f"{mode} {rows}", "catalog": catalog, "duckdb": [duck_rows, int(duck_sum)],
                          "polars": [polars["rows"], polars["id_sum"]], "statistics_status": statistics.status_code,
                          "statistics": statistics.json().get("stats", [])[:1]}))
        assert statistics.status_code == 200
        assert catalog["rows"] == total
        assert (duck_rows, int(duck_sum)) == (total, id_sum)
        assert (polars["rows"], polars["id_sum"]) == (total, id_sum)
        assert catalog["snapshots"] > snapshots_before
        snapshots_before = catalog["snapshots"]


WRITE_ROWS = [int(rows) for rows in os.environ.get("ICEBERG_PROOF_WRITE_ROWS", "100000,1000000,10000000").split(",")]


@pytest.mark.slow
@pytest.mark.parametrize("rows", WRITE_ROWS)
@pytest.mark.parametrize("path", ["sink", "in_memory"])
def test_write_sizes(rest_catalog, namespace, child_env, run_child, path, rows):
    """How long a write of each size takes and the memory it needs, streamed by Polars or collected first."""
    identifier = f"{namespace}.size_{path}_{rows}"
    rest_catalog.create_table(identifier, schema=probe_batch(0, 1).to_arrow(
        compat_level=pl.CompatLevel.oldest()).schema)

    probe = run_child("shared.tests.iceberg.write_probe",
                      {"catalog": fixtures.catalog_properties(), "identifier": identifier, "path": path, "rows": rows},
                      child_env)

    summary = rest_catalog.load_table(identifier).current_snapshot().summary
    recorded = {"catalog_records": int(summary["total-records"]), "catalog_bytes": int(summary["total-files-size"]),
                "data_files": int(summary["total-data-files"])}
    print(json.dumps({**probe, **recorded}))
    assert int(summary["total-records"]) == rows
