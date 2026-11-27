"""A catalog's sign-in becomes PyIceberg's properties and nothing else, and every refusal is one sentence without it.

These run without any service: the sign-in is turned into properties by hand, the refusals are PyIceberg's own
exception classes raised here, and a table's description is read from a PyIceberg SQL catalog on a SQLite file.
"""

import base64
import datetime
import json

import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    AuthorizationExpiredError,
    CommitStateUnknownException,
    ForbiddenError,
    NoSuchNamespaceError,
    OAuthError,
    RESTError,
    UnauthorizedError,
)
from requests.exceptions import ConnectionError as RequestsConnectionError

from shared import iceberg_utils
from shared.cloud_credential_provider import register_secret_decryptor
from shared.iceberg_utils import (
    IcebergCatalogRef,
    catalog_properties,
    client_credentials_sign_in,
    explain_catalog_error,
    sign_in_properties,
)
from shared.rest_api.fetch import mask_secrets
from test_utils.iceberg_rest import fixtures

URI = "http://127.0.0.1:9/api/catalog"
CLIENT_ID = "flowfile-client"
CLIENT_SECRET = "s3cr3t-client-value-for-tests"
TOKEN = "bearer-token-value-for-tests"


class _Forms(tuple):
    """The forms of a sign-in a test searches for; its ``repr``, which pytest prints on a failure, holds none."""

    def __repr__(self) -> str:
        return f"<{len(self)} sign-in forms>"


SECRET_FORMS = _Forms(
    (
        CLIENT_SECRET,
        TOKEN,
        f"{CLIENT_ID}:{CLIENT_SECRET}",
        base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode(),
    )
)


@pytest.fixture
def decryptor(monkeypatch):
    from shared import cloud_credential_provider

    monkeypatch.setattr(cloud_credential_provider, "_decrypt", cloud_credential_provider._decrypt)
    register_secret_decryptor(fixtures.open_sealed)


def test_a_token_and_a_client_id_and_secret_become_the_properties_pyiceberg_takes():
    as_token = sign_in_properties(URI, TOKEN)
    pair = sign_in_properties(URI, client_credentials_sign_in(CLIENT_ID, CLIENT_SECRET, scope="PRINCIPAL_ROLE:ALL"))

    token_names = sorted(as_token)
    assert token_names == ["token"]
    assert as_token["token"] == TOKEN
    pair_names = sorted(pair)
    assert pair_names == ["credential", "oauth2-server-uri", "scope"]
    credential_is_the_pair = pair["credential"] == f"{CLIENT_ID}:{CLIENT_SECRET}"
    assert credential_is_the_pair
    assert pair["scope"] == "PRINCIPAL_ROLE:ALL"


def test_the_token_endpoint_is_always_named_and_defaults_to_the_catalogs_own():
    own = sign_in_properties(URI + "/", client_credentials_sign_in(CLIENT_ID, CLIENT_SECRET))
    given = sign_in_properties(
        URI, client_credentials_sign_in(CLIENT_ID, CLIENT_SECRET, token_endpoint="https://idp.example/token")
    )

    assert own["oauth2-server-uri"] == f"{URI}/v1/oauth/tokens"
    assert "scope" not in own
    assert given["oauth2-server-uri"] == "https://idp.example/token"


@pytest.mark.parametrize(
    "stored",
    [
        {"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "s3.access-key-id": "AKIA0000"},
        {"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "uri": "http://elsewhere"},
        {"client_id": CLIENT_ID},
        {"client_secret": CLIENT_SECRET},
        {"client_id": CLIENT_ID, "client_secret": 5},
    ],
    ids=["a storage key", "another address", "no secret", "no id", "a secret that is no text"],
)
def test_a_stored_sign_in_with_any_other_key_is_refused(stored):
    with pytest.raises(ValueError) as refused:
        sign_in_properties(URI, json.dumps(stored))

    said = str(refused.value)
    assert said == "The stored sign-in is not one Flowfile wrote; enter it again."


def test_a_client_id_with_a_colon_is_refused():
    with pytest.raises(ValueError, match="cannot hold ':'"):
        client_credentials_sign_in("realm:client", CLIENT_SECRET)
    with pytest.raises(ValueError, match="not one Flowfile wrote"):
        sign_in_properties(URI, json.dumps({"client_id": "realm:client", "client_secret": CLIENT_SECRET}))


def test_a_catalog_ref_refuses_a_sign_in_that_is_not_ciphertext_and_keeps_it_out_of_its_repr():
    with pytest.raises(ValueError, match="travels encrypted"):
        IcebergCatalogRef(URI, "signed", client_credentials_sign_in(CLIENT_ID, CLIENT_SECRET))
    ciphertext = "$ffsec$1$1$not-a-real-sign-in"
    ref = IcebergCatalogRef(URI, "signed", ciphertext)

    shown = ciphertext in repr(ref)
    assert not shown
    table = ref.table("sales.orders")
    assert (table.catalog_uri, table.table, table.warehouse) == (URI, "sales.orders", "signed")
    carries_the_sign_in = table.token == ciphertext
    assert carries_the_sign_in


def test_asking_without_keys_empties_the_delegation_header_and_adds_no_storage_setting(decryptor):
    ref = IcebergCatalogRef(URI, "signed", fixtures.seal(client_credentials_sign_in(CLIENT_ID, CLIENT_SECRET)))

    with_keys = catalog_properties(ref)
    without = catalog_properties(ref, keys=False)

    assert sorted(with_keys) == ["credential", "oauth2-server-uri", "uri", "warehouse"]
    assert sorted(set(without) - set(with_keys)) == ["header.X-Iceberg-Access-Delegation"]
    assert without["header.X-Iceberg-Access-Delegation"] == ""
    storage = [name for name in without if name.startswith(("s3.", "client."))]
    assert storage == []


def _signed_ref():
    return IcebergCatalogRef(URI, "signed", "$ffsec$1$1$stand-in")


def _raised(error: BaseException, cause: BaseException | None = None) -> BaseException:
    """``error`` raised from ``cause``, as PyIceberg raises it, so the chain is a real one."""
    try:
        try:
            if cause is not None:
                raise cause
        finally:
            raise error
    except BaseException as caught:  # noqa: BLE001
        return caught


@pytest.mark.parametrize(
    ("error", "ref", "sentence"),
    [
        (OAuthError(f"invalid_scope: {CLIENT_SECRET}"), _signed_ref(),
         f"The catalog at {URI} refused the scope; Polaris and Snowflake Open Catalog take PRINCIPAL_ROLE:ALL."),
        (OAuthError(f"unauthorized_client: The client is not authorized {CLIENT_ID}:{CLIENT_SECRET}"), _signed_ref(),
         f"The catalog at {URI} refused the client id and secret."),
        (OAuthError("RESTError 400: Received unexpected JSON Payload: <?xml version"), _signed_ref(),
         f"{URI} did not answer as an Iceberg REST catalog."),
        (UnauthorizedError("NotAuthorizedException: Failed to verify the token"), IcebergCatalogRef(URI, "signed"),
         f"The catalog at {URI} asks for a sign-in."),
        (UnauthorizedError(f"NotAuthorizedException: {TOKEN}"), _signed_ref(),
         f"The catalog at {URI} refused the sign-in: it is wrong or has run out. A token is not renewed; a client id"
         " and secret is."),
        (AuthorizationExpiredError(TOKEN), _signed_ref(),
         f"The catalog at {URI} refused the sign-in: it is wrong or has run out. A token is not renewed; a client id"
         " and secret is."),
        (ForbiddenError(f"ForbiddenException: {CLIENT_ID}"), _signed_ref(),
         f"The catalog at {URI} knows this sign-in and does not allow it this."),
        (RequestsConnectionError(f"HTTPConnectionPool {TOKEN}"), _signed_ref(), f"No catalog answers at {URI}."),
        (RESTError("NotFoundException: Unable to find warehouse nope"), _signed_ref(),
         f"The catalog at {URI} has no warehouse signed."),
        (NoSuchNamespaceError("Warehouse nope does not exist"), _signed_ref(),
         f"The catalog at {URI} has no warehouse signed."),
        (RESTError(f"Received unexpected answer {CLIENT_SECRET}"), _signed_ref(),
         f"{URI} did not answer as an Iceberg REST catalog."),
    ],
    ids=["scope", "client", "not json", "no sign-in", "token", "expired", "forbidden", "closed port",
         "warehouse", "warehouse namespace", "not a catalog"],
)
def test_each_refusal_of_a_catalog_is_one_sentence_that_holds_no_part_of_the_sign_in(error, ref, sentence):
    wrapped = _raised(RuntimeError("while loading"), error)

    said = explain_catalog_error(wrapped, ref)

    matches = said == sentence
    assert matches, f"the sentence differs for {type(error).__name__}"
    one_sentence = said.count(". ") <= 1 and "\n" not in said
    assert one_sentence
    found = [index for index, form in enumerate(SECRET_FORMS) if form in said]
    assert found == []
    masked_is_unchanged = mask_secrets(said, tuple(SECRET_FORMS)) == said
    assert masked_is_unchanged


def test_an_error_that_is_not_the_catalogs_has_no_sentence():
    assert explain_catalog_error(ZeroDivisionError(), _signed_ref()) is None


def _sql_catalog(tmp_path) -> SqlCatalog:
    """A SQLite catalog in ``tmp_path``. PyIceberg reads ``file:///C:/x`` as the path ``/C:/x``, so the drive goes
    where a URI has its host."""
    warehouse = f"file://{(tmp_path / 'wh').as_posix()}"
    return SqlCatalog("local", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=warehouse)


def test_the_columns_from_a_tables_description_equal_the_scans(tmp_path, monkeypatch):
    catalog = _sql_catalog(tmp_path)
    catalog.create_namespace("ns")
    schema = pa.schema(
        [
            ("i32", pa.int32()), ("i64", pa.int64()), ("f32", pa.float32()), ("f64", pa.float64()),
            ("s", pa.string()), ("b", pa.bool_()), ("d", pa.date32()), ("ts", pa.timestamp("us")),
            ("tstz", pa.timestamp("us", tz="UTC")), ("dec", pa.decimal128(12, 3)), ("bin", pa.binary()),
            ("t", pa.time64("us")), ("lst", pa.list_(pa.int64())),
            ("st", pa.struct([("a", pa.int64()), ("b", pa.string())])), ("mp", pa.map_(pa.string(), pa.int64())),
        ]
    )
    table = catalog.create_table("ns.t", schema=schema)
    monkeypatch.setattr(iceberg_utils, "_catalog", lambda ref, **kw: catalog)

    described = iceberg_utils.describe_table(IcebergCatalogRef(URI).table("ns.t"))
    scanned = pl.scan_iceberg(table, storage_options={"s3.endpoint": "http://127.0.0.1:9"}).collect_schema()

    column_count = len(described.columns)
    assert column_count == 15
    assert described.columns == dict(scanned)
    assert described.format_version == table.metadata.format_version
    assert described.location == table.metadata.location
    assert described.partitioned_by == ()
    assert isinstance(described.updated_at_ms, int)
    assert datetime.datetime.fromtimestamp(described.updated_at_ms / 1000).year >= 2024


def test_a_commit_the_catalog_refused_for_its_sign_in_is_said_not_to_have_committed(tmp_path, monkeypatch):
    catalog = _sql_catalog(tmp_path)
    catalog.create_namespace("ns")
    table = catalog.create_table("ns.t", schema=pa.schema([("id", pa.int64())]))
    refusal = _raised(
        CommitStateUnknownException("could not verify the outcome of a failed Iceberg commit"),
        UnauthorizedError(f"NotAuthorizedException: Failed to verify the token {TOKEN}"),
    )

    def refused(*args, **kwargs):
        raise refusal

    monkeypatch.setattr(iceberg_utils, "write_table", refused)

    with pytest.raises(RuntimeError) as failed:
        iceberg_utils._sink(table, pl.LazyFrame({"id": [1]}), "append")

    said = str(failed.value)
    assert said.startswith("The write to ns.t did not commit: the catalog refused the sign-in when the write finished")
    assert "a token is not." in said
    assert "may not have committed" not in said
    assert said.endswith(" It left no files under data/.") or "could not be listed" in said
    found = [index for index, form in enumerate(SECRET_FORMS) if form in said]
    assert found == []
    chained = failed.value.__cause__ is None and failed.value.__suppress_context__
    assert chained
