"""Seven lines on what a catalog's sign-in does: none, a wrong one, a client id and secret, and a token that runs out.

Run ``python -m test_utils.iceberg_rest signin`` with the catalogs running. Each line is a check; one that does not
hold ends in ``-- NOT AS EXPECTED`` and the walk exits 1. No secret or token is printed.
"""

import base64
import contextlib
import os
import tempfile
import time
from pathlib import Path

from test_utils.iceberg_rest import fixtures

CHILD = "shared.tests.iceberg.table_child"
SCHEMAS = ("finance", "sales")
ORDERS = "sales.orders"
_AWS_IDENTITY = ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


class _Walk:
    def __init__(self) -> None:
        self.step = 0
        self.failed = False

    def say(self, held: bool, line: str, *more: str) -> None:
        self.step += 1
        self.failed |= not held
        print(f"{self.step}. {line}{'' if held else ' -- NOT AS EXPECTED'}")
        for extra in more:
            print(f"   {extra}")


def _isolate(home: Path) -> None:
    """Cut this process off any AWS identity of the machine: empty credential files, no metadata service."""
    for name in _AWS_IDENTITY:
        os.environ.pop(name, None)
    os.environ.update({name: value for name, value in fixtures.clean_env(home).items() if name.startswith("AWS_")})


def _seed(signed) -> None:
    import polars as pl

    from shared.iceberg_utils import write_table
    from shared.tests.iceberg.table_child import rows_frame

    for schema in SCHEMAS:
        for identifier in signed.list_tables(schema) if signed.namespace_exists(schema) else []:
            signed.drop_table(identifier)
        signed.create_namespace_if_not_exists(schema)
    arrow = rows_frame(1).collect().to_arrow(compat_level=pl.CompatLevel.oldest())
    orders = signed.create_table(ORDERS, schema=arrow.schema)
    write_table(orders, rows_frame(1000), "append")
    signed.create_table("sales.customers", schema=arrow.schema)
    signed.create_table("finance.invoices", schema=arrow.schema)


@contextlib.contextmanager
def _token_requests():
    """Counts the requests this process sends to the signed catalog's token endpoint while the block runs."""
    import requests

    sent: list[str] = []
    send = requests.Session.send

    def counting(self, request, **kwargs):
        if request.url.split("?")[0] == fixtures.SIGNED_TOKEN_ENDPOINT:
            sent.append(request.method)
        return send(self, request, **kwargs)

    requests.Session.send = counting
    try:
        yield sent
    finally:
        requests.Session.send = send


def run_signin_demo() -> int:
    if not fixtures.signed_catalog_ready():
        print(f"No Iceberg REST catalog that asks for a sign-in answers at {fixtures.SIGNED_CATALOG_URI}.")
        print("Start it with: python -m test_utils.iceberg_rest start")
        return 1
    import polars as pl

    from shared import iceberg_utils
    from shared.cloud_credential_provider import register_secret_decryptor
    from shared.iceberg_utils import (
        IcebergCatalogRef,
        client_credentials_sign_in,
        describe_table,
        list_schemas,
        list_table_names,
        scan_iceberg_table,
        storage_properties,
        write_iceberg_table,
    )
    from shared.tests.iceberg.table_child import rows_frame

    register_secret_decryptor(fixtures.open_sealed)
    iceberg_utils.register_run_log(lambda flow_id, node_id, message: None)
    uri, warehouse, walk = fixtures.SIGNED_CATALOG_URI, fixtures.SIGNED_WAREHOUSE, _Walk()
    lifetime = fixtures.token_seconds()
    principal = fixtures.new_principal()
    secrets = fixtures.sign_in_values([client_credentials_sign_in(principal.client_id, principal.client_secret)])
    try:
        with tempfile.TemporaryDirectory() as scratch:
            _isolate(Path(scratch) / "this_process")
            signed = fixtures.signed_catalog(principal)
            _seed(signed)
            sign_in = client_credentials_sign_in(principal.client_id, principal.client_secret, fixtures.SIGNED_SCOPE)
            catalog = IcebergCatalogRef(uri, warehouse, fixtures.seal(sign_in))

            print("Flowfile's Iceberg helpers and an Apache Polaris catalog on a local MinIO. This catalog asks for a")
            print(f"sign-in, and every token it gives lives {lifetime} seconds.\n")

            def refusal(ref) -> str:
                try:
                    list_schemas(ref)
                except ValueError as exc:
                    return str(exc)
                return ""

            said = refusal(IcebergCatalogRef(uri, warehouse))
            walk.say(said == f"The catalog at {uri} asks for a sign-in.", f'Asked with no sign-in: refused. "{said}"')
            wrong = client_credentials_sign_in(principal.client_id, "not-the-secret", fixtures.SIGNED_SCOPE)
            said = refusal(IcebergCatalogRef(uri, warehouse, fixtures.seal(wrong)))
            walk.say(
                said == f"The catalog at {uri} refused the client id and secret.",
                f'Signed in with a client id and a wrong secret: refused. "{said}"',
            )

            shown, _ = list_schemas(catalog)
            tables = list_table_names(catalog, "sales")
            walk.say(
                set(SCHEMAS) <= set(shown) and tables == ["customers", "orders"],
                f"Signed in with the client id and secret: {len(shown)} schemas ({', '.join(shown)}); sales holds"
                f" {', '.join(tables)}.",
            )

            described = describe_table(catalog.table(ORDERS))
            quiet = iceberg_utils._catalog(catalog, keys=False).load_table(ORDERS)
            no_key = "s3.access-key-id" not in storage_properties(quiet)
            walk.say(
                list(described.columns) == ["id", "grp", "val", "name"] and no_key,
                f"Asked for the columns of {ORDERS}: {', '.join(described.columns)}."
                f" The catalog handed this process {'no' if no_key else 'a'} storage key.",
            )

            plan = scan_iceberg_table(catalog.table(ORDERS)).serialize()
            in_plan = [index for index, value in enumerate(secrets) if value.encode() in plan]
            child_env = fixtures.clean_env(Path(scratch) / "child")
            payload = {"op": "collect_plan", "plan": base64.b64encode(plan).decode()}
            answer = fixtures.run_child(
                CHILD, {**payload, "decrypts": fixtures.sealed_map(catalog.token)}, child_env, secrets
            )
            keys = [name for name in answer["aws_env"] if "KEY" in name or "TOKEN" in name]
            walk.say(
                answer["rows"] == 1000 and not keys and not in_plan,
                f"A fresh process read {ORDERS} with the stored sign-in: {answer['rows']:,} rows."
                f" Storage keys in its environment: {len(keys) or 'none'}.",
            )

            def rows() -> int:
                access = iceberg_utils.table_access(signed.load_table(ORDERS))
                return int(iceberg_utils.scan_table(access).select(pl.len()).collect().item())

            def slow(start: int) -> pl.LazyFrame:
                frame = rows_frame(50, start)

                def wait(batch):
                    time.sleep(lifetime + 3)
                    return batch

                return frame.map_batches(wait, schema=frame.collect_schema())

            started = time.monotonic()
            with _token_requests() as sent:
                write_iceberg_table(catalog.table(ORDERS), slow(1000), "append")
            took, after = time.monotonic() - started, rows()
            walk.say(
                after == 1050 and took > lifetime and len(sent) >= 2,
                f"A write that took {took:.0f} seconds, longer than a token lives, committed: {after:,} rows."
                f" Flowfile asked for {'a new token' if len(sent) >= 2 else 'no new token'}.",
            )

            token, _ = fixtures.fetch_token(principal)
            secrets.append(token)
            pasted = IcebergCatalogRef(uri, warehouse, fixtures.seal(token))
            try:
                write_iceberg_table(pasted.table(ORDERS), slow(2000), "append")
                said = ""
            except RuntimeError as exc:
                said = str(exc)
            still = rows()
            leaked = [index for index, value in enumerate(secrets) if value in said]
            walk.say(
                said.startswith(f"The write to {ORDERS} did not commit: the catalog refused the sign-in")
                and still == 1050
                and not leaked,
                f"The same write signed in with a pasted token did not commit, and said so: {still:,} rows still.",
            )
    finally:
        fixtures.drop_principal(principal.name)
    return 1 if walk.failed else 0
