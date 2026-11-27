"""Flowfile's Iceberg helpers on a catalog that asks for a sign-in: Apache Polaris on the same MinIO.

A client id and secret, or a token fetched with them, is all Flowfile is given. With it the helpers list schemas and
tables and describe a table without asking for a storage key, a fresh process reads a table with the stored sign-in,
and a write that outlasts a token commits when Flowfile can ask for a new one and says it did not when it cannot.
Every token this catalog issues lives ``fixtures.TOKEN_LIFETIME`` (12 seconds by default), so a test that waits for
one to run out takes that long.

Every test skips unless the catalog runs: ``python -m test_utils.iceberg_rest start``.
"""

import base64
import dataclasses
import time
import uuid

import polars as pl
import pytest
import requests

from shared import iceberg_utils
from shared.iceberg_utils import (
    IcebergCatalogRef,
    client_credentials_sign_in,
    describe_table,
    list_schemas,
    list_table_names,
    scan_iceberg_table,
    storage_properties,
    table_access,
    write_iceberg_table,
    write_table,
)
from shared.tests.iceberg.table_child import expected, rows_frame
from test_utils.iceberg_rest import fixtures

pytestmark = pytest.mark.iceberg_rest

CHILD = "shared.tests.iceberg.table_child"
CHILD_AWS_ENV = ["AWS_CONFIG_FILE", "AWS_EC2_METADATA_DISABLED", "AWS_ENDPOINT_URL", "AWS_SHARED_CREDENTIALS_FILE"]
URI = fixtures.SIGNED_CATALOG_URI


class _Values(list):
    """Secret values a test searches for; its ``repr``, which pytest prints for a failing test, holds none."""

    def __repr__(self) -> str:
        return f"<{len(self)} secret values>"


def _forms(principal, *tokens: str) -> _Values:
    pair = principal.credential
    return _Values([principal.client_secret, pair, base64.b64encode(pair.encode()).decode(), *tokens])


def _client_ref(sealing, principal) -> IcebergCatalogRef:
    sign_in = client_credentials_sign_in(principal.client_id, principal.client_secret, scope=fixtures.SIGNED_SCOPE)
    return IcebergCatalogRef(URI, fixtures.SIGNED_WAREHOUSE, sealing(sign_in))


def _token_ref(sealing, token: str) -> IcebergCatalogRef:
    return IcebergCatalogRef(URI, fixtures.SIGNED_WAREHOUSE, sealing(token))


def _slow(rows: int, start: int, seconds: float) -> pl.LazyFrame:
    """Rows that take ``seconds`` to produce, so the write they feed outlasts a token."""
    frame = rows_frame(rows, start)

    def wait(batch: pl.DataFrame) -> pl.DataFrame:
        time.sleep(seconds)
        return batch

    return frame.map_batches(wait, schema=frame.collect_schema())


def _rows(signed, identifier: str) -> tuple[int, int]:
    access = table_access(signed.load_table(identifier))
    counted = iceberg_utils.scan_table(access).select(pl.len(), pl.col("id").sum()).collect()
    return int(counted.item(0, 0)), int(counted.item(0, 1) or 0)


def _skip_when_tokens_live_long():
    if fixtures.token_seconds() > 30:
        pytest.skip(f"the catalog's tokens live {fixtures.token_seconds()} seconds; start it with the default lifetime")


def test_the_signed_catalog_refuses_a_request_without_a_sign_in_and_one_with_a_wrong_secret(principal):
    config = f"{URI}/v1/config"

    unsigned = requests.get(config, params={"warehouse": fixtures.SIGNED_WAREHOUSE}, timeout=10)
    wrong = requests.post(
        fixtures.SIGNED_TOKEN_ENDPOINT,
        auth=(principal.client_id, "not-the-secret"),
        data={"grant_type": "client_credentials", "scope": fixtures.SIGNED_SCOPE},
        timeout=10,
    )

    assert (unsigned.status_code, wrong.status_code) == (401, 401)
    leaked = [name for name, answer in (("unsigned", unsigned), ("wrong", wrong)) if principal.client_secret in answer.text]
    assert leaked == []


def test_a_client_id_and_secret_lists_schemas_and_tables(signed, signed_namespace, seed_signed, principal, sealing):
    second = f"{signed_namespace}_b"
    signed.create_namespace(second)
    signed.create_namespace((signed_namespace, "nested"))
    try:
        seed_signed("orders", 3)
        seed_signed("customers", 0)
        ref = _client_ref(sealing, principal)

        shown, left_out = list_schemas(ref)
        tables = list_table_names(ref, signed_namespace)
    finally:
        signed.drop_namespace((signed_namespace, "nested"))
        signed.drop_namespace(second)

    ours = [name for name in shown if name.startswith(signed_namespace)]
    assert ours == [signed_namespace, second]
    assert left_out >= 1
    assert tables == ["customers", "orders"]


def test_nested_and_dotted_namespaces_are_counted_and_not_shown(signed, signed_namespace, principal, sealing):
    ref = _client_ref(sealing, principal)
    _, before = list_schemas(ref)
    dotted = f"{signed_namespace}.dot"
    signed.create_namespace((dotted,))
    signed.create_namespace((signed_namespace, "inner"))
    try:
        shown, after = list_schemas(ref)
    finally:
        signed.drop_namespace((signed_namespace, "inner"))
        signed.drop_namespace((dotted,))

    assert after - before == 2
    assert dotted not in shown and signed_namespace in shown


def test_a_pasted_token_lists_schemas_and_tables(signed_namespace, seed_signed, principal, sealing):
    seed_signed("orders", 3)
    token, _ = fixtures.fetch_token(principal)
    ref = _token_ref(sealing, token)

    shown, _ = list_schemas(ref)
    tables = list_table_names(ref, signed_namespace)

    assert signed_namespace in shown
    assert tables == ["orders"]


def test_no_sign_in_and_a_wrong_secret_are_each_one_sentence(principal, sealing):
    wrong = client_credentials_sign_in(principal.client_id, "not-the-secret", scope=fixtures.SIGNED_SCOPE)
    no_scope = client_credentials_sign_in(principal.client_id, principal.client_secret)
    said = {}
    for name, ref in (
        ("none", IcebergCatalogRef(URI, fixtures.SIGNED_WAREHOUSE)),
        ("wrong", IcebergCatalogRef(URI, fixtures.SIGNED_WAREHOUSE, sealing(wrong))),
        ("scope", IcebergCatalogRef(URI, fixtures.SIGNED_WAREHOUSE, sealing(no_scope))),
        ("warehouse", IcebergCatalogRef(URI, "nope", sealing(client_credentials_sign_in(
            principal.client_id, principal.client_secret, scope=fixtures.SIGNED_SCOPE)))),
        ("closed", IcebergCatalogRef("http://127.0.0.1:9/api/catalog", fixtures.SIGNED_WAREHOUSE)),
    ):
        with pytest.raises(ValueError) as refused:
            list_schemas(ref)
        said[name] = str(refused.value)

    expected_sentences = {
        "none": f"The catalog at {URI} asks for a sign-in.",
        "wrong": f"The catalog at {URI} refused the client id and secret.",
        "scope": f"The catalog at {URI} refused the scope; Polaris and Snowflake Open Catalog take PRINCIPAL_ROLE:ALL.",
        "warehouse": f"The catalog at {URI} has no warehouse nope.",
        "closed": "No catalog answers at http://127.0.0.1:9/api/catalog.",
    }
    differing = sorted(name for name in expected_sentences if said[name] != expected_sentences[name])
    assert differing == []
    leaked = [name for name, text in said.items() if any(form in text for form in _forms(principal))]
    assert leaked == []


def test_describing_a_table_hands_this_process_no_storage_key(signed_namespace, seed_signed, principal, sealing):
    seed_signed("orders", 10)
    ref = _client_ref(sealing, principal).table(f"{signed_namespace}.orders")
    loaded = []
    build = iceberg_utils._catalog

    def recording(*args, **kwargs):
        catalog = build(*args, **kwargs)
        load = catalog.load_table

        def kept(identifier):
            table = load(identifier)
            loaded.append(table)
            return table

        catalog.load_table = kept
        return catalog

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(iceberg_utils, "_catalog", recording)
        described = describe_table(ref)

    assert list(described.columns) == ["id", "grp", "val", "name"]
    assert described.columns["id"] == pl.Int64 and described.columns["name"] == pl.String
    assert described.format_version == 2
    assert described.location.startswith(f"{fixtures.SIGNED_LOCATION}/")
    loads = len(loaded)
    assert loads == 1
    handed = sorted(name for name in storage_properties(loaded[0]) if name in ("s3.access-key-id", "s3.session-token"))
    assert handed == []


def test_a_tables_description_says_when_it_last_changed_how_and_how_many_snapshots_it_keeps(
    signed, signed_namespace, seed_signed, principal, sealing
):
    """An overwrite through Polars commits a delete and then an append, so the current snapshot's operation is append."""
    seed_signed("orders", 0)
    identifier = f"{signed_namespace}.orders"
    ref = _client_ref(sealing, principal).table(identifier)

    empty = describe_table(ref)
    write_table(signed.load_table(identifier), rows_frame(10), "append")
    appended = describe_table(ref)
    write_table(signed.load_table(identifier), rows_frame(5, 100), "overwrite")
    overwritten = describe_table(ref)

    assert (empty.last_commit_ms, empty.last_operation, empty.snapshot_count) == (None, None, 0)
    assert (appended.last_operation, appended.snapshot_count) == ("append", 1)
    assert abs(time.time() * 1000 - appended.last_commit_ms) < 60_000
    operations = [snapshot.summary.operation.value for snapshot in signed.load_table(identifier).metadata.snapshots]
    assert operations == ["append", "delete", "append"]
    assert (overwritten.last_operation, overwritten.snapshot_count) == ("append", 3)
    assert overwritten.last_commit_ms >= appended.last_commit_ms
    counts = [f.name for f in dataclasses.fields(overwritten) if f.name != "snapshot_count" and _names_a_count(f.name)]
    assert counts == []


def _names_a_count(name: str) -> bool:
    return any(word in name for word in ("count", "rows", "records", "summary"))


def test_what_another_process_is_handed_holds_no_part_of_the_sign_in(signed_namespace, seed_signed, principal, sealing):
    seed_signed("orders", 10)
    ref = _client_ref(sealing, principal).table(f"{signed_namespace}.orders")

    access = table_access(iceberg_utils.load_table(ref))

    names = sorted(access.storage_properties)
    catalog_names = [name for name in names if name in ("credential", "token", "scope", "uri", "oauth2-server-uri")]
    assert catalog_names == []
    assert "s3.access-key-id" in names
    values = " ".join(access.storage_properties.values())
    leaked = [index for index, form in enumerate(_forms(principal)) if form in values]
    assert leaked == []


def test_a_fresh_process_reads_a_table_with_a_stored_client_id_and_secret(
    signed_namespace, seed_signed, principal, sealing, child_env, run_child
):
    seed_signed("orders", 1000)
    catalog = _client_ref(sealing, principal)
    frame = scan_iceberg_table(catalog.table(f"{signed_namespace}.orders"))
    blob = frame.serialize()

    leaked = [index for index, form in enumerate(_forms(principal)) if form.encode() in blob]
    assert leaked == []
    carries_ciphertext = catalog.token.encode() in blob
    assert carries_ciphertext
    payload = {
        "op": "collect_plan",
        "plan": base64.b64encode(blob).decode(),
        "decrypts": fixtures.sealed_map(catalog.token),
    }
    answer = run_child(CHILD, payload, child_env, secrets=_forms(principal))

    assert (answer["rows"], answer["id_sum"]) == expected(1000)
    assert answer["aws_env"] == CHILD_AWS_ENV


def test_a_token_that_has_run_out_is_refused_with_the_sentence(signed_namespace, principal, sealing):
    _skip_when_tokens_live_long()
    token, lifetime = fixtures.fetch_token(principal)
    ref = _token_ref(sealing, token)
    time.sleep(lifetime + 1)

    with pytest.raises(ValueError) as refused:
        list_schemas(ref)

    said = str(refused.value)
    assert said == (
        f"The catalog at {URI} refused the sign-in: it is wrong or has run out. A token is not renewed;"
        " a client id and secret is."
    )


def test_a_write_that_outlasts_a_pasted_token_does_not_commit_and_says_so(
    signed, signed_namespace, seed_signed, principal, sealing
):
    _skip_when_tokens_live_long()
    seed_signed("orders", 100)
    identifier = f"{signed_namespace}.orders"
    token, lifetime = fixtures.fetch_token(principal)
    ref = _token_ref(sealing, token).table(identifier)

    with pytest.raises(RuntimeError) as failed:
        write_iceberg_table(ref, _slow(50, 100, lifetime + 3), "append")

    said = str(failed.value)
    assert said.startswith(f"The write to {identifier} did not commit: the catalog refused the sign-in")
    leaked = [index for index, form in enumerate(_forms(principal, token)) if form in said]
    assert leaked == []
    assert _rows(signed, identifier) == expected(100)


def test_a_write_that_outlasts_the_token_commits_with_a_client_id_and_secret(
    signed, signed_namespace, seed_signed, principal, sealing
):
    _skip_when_tokens_live_long()
    seed_signed("orders", 100)
    identifier = f"{signed_namespace}.orders"
    ref = _client_ref(sealing, principal).table(identifier)

    write_iceberg_table(ref, _slow(50, 100, fixtures.token_seconds() + 3), "append")

    assert _rows(signed, identifier) == expected(150)


def test_a_new_principal_is_its_own(signed_ready):
    name = f"flowfile_{uuid.uuid4().hex[:8]}"
    made = fixtures.new_principal(name)
    try:
        shown = repr(made)
        hidden = made.client_secret not in shown and made.client_id not in shown
        assert hidden
    finally:
        fixtures.drop_principal(name)
    after = fixtures._token_answer(made.client_id, made.client_secret).status_code
    assert after == 401
