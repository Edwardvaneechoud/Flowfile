"""The Iceberg helpers refuse a table that came with no key Polars can use, rather than fall back to the machine's.

These run without any service: a PyIceberg SQL catalog on a SQLite file gives real tables, one whose properties hold
no storage key and one whose catalog turned on remote signing.
"""

import polars as pl
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from shared.iceberg_utils import storage_properties, table_access, write_table


def _table(tmp_path, **catalog_properties):
    # PyIceberg reads file:///C:/x as the path /C:/x, so the drive goes where a URI has its host
    warehouse = f"file://{(tmp_path / 'warehouse').as_posix()}"
    catalog = SqlCatalog("local", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=warehouse, **catalog_properties)
    catalog.create_namespace_if_not_exists("ns")
    if not catalog.table_exists("ns.t"):
        catalog.create_table("ns.t", schema=pa.schema([("id", pa.int64())]))
    return catalog.load_table("ns.t")


def test_a_table_without_handed_out_keys_is_refused(tmp_path):
    """A table that came with no storage key is refused, so a read never runs on the machine's own credentials."""
    table = _table(tmp_path)

    with pytest.raises(ValueError, match="handed out no storage keys"):
        table_access(table)
    with pytest.raises(ValueError, match="handed out no storage keys"):
        write_table(table, pl.LazyFrame({"id": [1]}), "append")


def test_a_table_whose_catalog_signs_requests_is_refused(tmp_path):
    """A catalog that signs each request itself is refused with a plain sentence, and its signer address is not kept."""
    table = _table(tmp_path, **{"s3.remote-signing-enabled": "true", "s3.signer.uri": "http://127.0.0.1:9/catalog"})

    assert "s3.signer.uri" not in storage_properties(table)
    with pytest.raises(ValueError, match="remote signing"):
        table_access(table)
    with pytest.raises(ValueError, match="remote signing"):
        write_table(table, pl.LazyFrame({"id": [1]}), "append")


REF_FIELDS = ["catalog_uri", "table", "warehouse", "token"]


def _ref(**overrides):
    from shared.iceberg_utils import IcebergTableRef

    values = {"catalog_uri": "http://127.0.0.1:9/catalog", "table": "demo.orders", "warehouse": "proof", **overrides}
    return IcebergTableRef(**values)


def test_a_table_ref_refuses_a_token_that_is_not_ciphertext_and_keeps_it_out_of_its_repr():
    """A clear token cannot be put in a ref, so it cannot be serialized with a plan; a ciphertext is not shown."""
    with pytest.raises(ValueError, match="travels encrypted"):
        _ref(token="clear-token-value")
    ciphertext = "$ffsec$1$1$not-a-real-token"
    shown = ciphertext in repr(_ref(token=ciphertext))
    assert not shown
    for bad_name in ("orders", "s3://bucket/orders", "demo/orders", "demo. orders", ".orders", "demo."):
        with pytest.raises(ValueError, match="namespace.table"):
            _ref(table=bad_name)


def test_a_table_ref_has_no_field_that_could_hold_a_storage_key():
    import dataclasses

    from shared.iceberg_utils import IcebergTableRef

    assert [f.name for f in dataclasses.fields(IcebergTableRef)] == REF_FIELDS


def test_the_plan_names_the_table_and_its_catalog_and_serializes_without_asking_the_catalog():
    """With the columns given, building and serializing the plan asks nothing of the catalog (its address is closed).

    A fresh Python that never built the plan deserializes it: the source is a module-level object, pickled by name.
    """
    import subprocess
    import sys
    from pathlib import Path

    from shared.iceberg_utils import scan_iceberg_table

    frame = scan_iceberg_table(_ref(), schema={"id": pl.Int64, "grp": pl.Int64})
    blob = frame.serialize()
    names_table_and_catalog = b"demo.orders" in blob and b"http://127.0.0.1:9/catalog" in blob
    assert names_table_and_catalog
    plan = frame.filter(pl.col("id") > 3).select("id").explain()
    assert "ICEBERG" in plan and "demo.orders" in plan and 'col("id") > 3' in plan

    child = "import io, sys, polars as pl; print(pl.LazyFrame.deserialize(io.BytesIO(sys.stdin.buffer.read())).explain())"
    result = subprocess.run(
        [sys.executable, "-c", child], input=blob, capture_output=True, cwd=Path(__file__).resolve().parents[3]
    )
    assert result.returncode == 0, result.stderr.decode()[-1000:]
    assert "ICEBERG" in result.stdout.decode() and "demo.orders" in result.stdout.decode()


def test_catalog_properties_hold_the_address_and_the_decrypted_token_and_no_storage_setting():
    from shared import cloud_credential_provider
    from shared.iceberg_utils import catalog_properties

    previous = cloud_credential_provider._decrypt
    cloud_credential_provider.register_secret_decryptor(lambda value: value.removeprefix("$ffsec$1$1$") + "-plain")
    try:
        properties = catalog_properties(_ref(token="$ffsec$1$1$bearer"))
    finally:
        cloud_credential_provider._decrypt = previous

    assert sorted(properties) == ["token", "uri", "warehouse"]
    assert properties["token"] == "bearer-plain"
    assert sorted(catalog_properties(_ref(warehouse=None))) == ["uri"]


def test_the_time_left_is_read_from_the_credentials_expiry_and_is_unknown_without_one():
    from shared.iceberg_utils import credential_seconds_left

    assert credential_seconds_left({"s3.session-token-expires-at-ms": "1000000"}, now=400.0) == 600.0
    assert credential_seconds_left({}, now=400.0) is None


def test_a_credential_with_too_little_time_left_is_refused_by_name():
    import time

    from shared.iceberg_utils import refuse_short_credential

    soon = {"s3.session-token-expires-at-ms": str(int((time.time() + 30) * 1000))}
    with pytest.raises(ValueError, match=r"credential for demo\.orders runs out in \d+ seconds, too soon to read it"):
        refuse_short_credential(soon, "demo.orders", 60, "read")
    later = {"s3.session-token-expires-at-ms": str(int((time.time() + 3600) * 1000))}
    assert refuse_short_credential(later, "demo.orders", 60, "write") > 3000
    assert refuse_short_credential({}, "demo.orders", 60, "read") is None


def test_the_planners_equality_delete_error_becomes_one_sentence():
    """The sentence is matched on PyIceberg's own wording, which this test pins against the installed PyIceberg."""
    import inspect

    import pyiceberg.table

    from shared.iceberg_utils import _equality_deletes

    assert "does not yet support equality deletes" in inspect.getsource(pyiceberg.table)
    refusal = _equality_deletes("demo.orders", ValueError("PyIceberg does not yet support equality deletes: x.parquet"))
    assert str(refusal) == (
        "demo.orders has equality deletes (rows deleted by value, as Flink writes them), which neither Polars nor"
        " PyIceberg reads yet."
    )
    assert _equality_deletes("demo.orders", ValueError("something else")) is None


def test_the_native_readers_refusal_is_read_without_the_file_it_names():
    from shared.iceberg_utils import _native_failure

    error = pl.exceptions.ComputeError(
        "iceberg reader_override='native' failed: position delete file not limited to one data file: s3://b/x.parquet"
    )
    assert _native_failure(error) == "position delete file not limited to one data file"
    assert _native_failure(pl.exceptions.ComputeError("another failure")) is None
    assert _native_failure(ValueError("iceberg reader_override='native' failed: x")) is None


def test_the_planner_warning_is_silenced_only_when_the_plugin_is_absent(monkeypatch):
    """Importing the module filters out Polars' "planning with PyIceberg" warning, and no environment is written."""
    import importlib.util
    import os
    import warnings

    from shared import iceberg_utils

    message = "No module named 'polars_iceberg'. Planning the Iceberg scan with PyIceberg instead, which is slower."
    environment = dict(os.environ)
    with warnings.catch_warnings(record=True) as seen:
        warnings.resetwarnings()
        monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
        iceberg_utils._quiet_planner()
        warnings.warn(message, pl.exceptions.PerformanceWarning, stacklevel=1)
    assert seen == []
    written = sorted(name for name in {*os.environ, *environment} if os.environ.get(name) != environment.get(name))
    assert written == []

    with warnings.catch_warnings(record=True) as seen:
        warnings.resetwarnings()
        monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
        iceberg_utils._quiet_planner()
        warnings.warn(message, pl.exceptions.PerformanceWarning, stacklevel=1)
    assert [type(w.message) for w in seen] == [pl.exceptions.PerformanceWarning]


def test_a_version_3_table_is_refused_by_its_format_version(tmp_path):
    """PyIceberg cannot create a version 3 table here, so the refusal is checked on the table's metadata alone;
    ``test_table_ref.py`` checks it on a real one with nothing written."""
    from types import SimpleNamespace

    from shared.iceberg_utils import refuse_unwritable

    v3 = SimpleNamespace(metadata=SimpleNamespace(format_version=3), name=lambda: ("ns", "v3"))
    with pytest.raises(ValueError, match=r"ns\.v3 is an Iceberg version 3 table, which this Flowfile reads and cannot"):
        refuse_unwritable(v3)
    refuse_unwritable(_table(tmp_path))


def test_unsigned_columns_get_the_smallest_signed_type_that_holds_them_nested_ones_too():
    from shared.iceberg_utils import creatable_schema

    index = pl.get_index_type()
    given = {"u8": pl.UInt8(), "u16": pl.UInt16(), "u32": pl.UInt32(), "u64": pl.UInt64(), "idx": index,
             "names": pl.List(pl.UInt16()), "pair": pl.Struct({"n": pl.UInt64(), "s": pl.String()}),
             "grid": pl.Array(pl.UInt16(), (2, 3))}
    assert creatable_schema(given) == {
        "u8": pl.Int32(), "u16": pl.Int32(), "u32": pl.Int64(), "u64": pl.Int64(), "idx": pl.Int64(),
        "names": pl.List(pl.Int32()), "pair": pl.Struct({"n": pl.Int64(), "s": pl.String()}),
        "grid": pl.Array(pl.Int32(), (2, 3)),
    }


def test_a_multi_dimensional_array_keeps_its_shape():
    from shared.iceberg_utils import creatable_schema

    assert creatable_schema({"grid": pl.Array(pl.Int64, (2, 3))}) == {"grid": pl.Array(pl.Int64, (2, 3))}
    assert creatable_schema({"cube": pl.Array(pl.UInt8, (2, 3, 4))}) == {"cube": pl.Array(pl.Int32, (2, 3, 4))}
    assert creatable_schema({"ids": pl.List(pl.UInt32)}) == {"ids": pl.List(pl.Int64)}


def test_a_duration_named_by_its_class_inside_a_list_is_refused_too():
    from shared.iceberg_utils import creatable_schema

    with pytest.raises(ValueError, match="^Column spans is a List holding a Duration, which an Iceberg table"):
        creatable_schema({"spans": pl.List(pl.Duration)})


UNCHANGED = {
    "i8": pl.Int8(), "i16": pl.Int16(), "i32": pl.Int32(), "i64": pl.Int64(), "f32": pl.Float32(),
    "f64": pl.Float64(), "s": pl.String(), "b": pl.Boolean(), "bin": pl.Binary(), "d": pl.Date(),
    "dec": pl.Decimal(10, 2), "cat": pl.Categorical(), "enum": pl.Enum(["a", "b"]), "t": pl.Time(),
    "us": pl.Datetime("us"), "ms_utc": pl.Datetime("ms", "UTC"),
    "list": pl.List(pl.List(pl.Int16())), "arr": pl.Array(pl.Float64(), (2, 3)),
    "nested": pl.Struct({"a": pl.List(pl.Struct({"x": pl.Int64(), "y": pl.String()})), "b": pl.Array(pl.Int8(), 4)}),
}


def test_a_type_that_needs_no_conversion_comes_out_exactly_as_it_went_in():
    from shared.iceberg_utils import creatable_schema

    made = creatable_schema(UNCHANGED)
    assert {name: (dtype, made[name]) for name, dtype in UNCHANGED.items() if made[name] != dtype} == {}
    assert [str(dtype) for dtype in made.values()] == [str(dtype) for dtype in UNCHANGED.values()]


def test_a_zone_other_than_utc_becomes_utc_nanoseconds_become_microseconds_and_a_time_stays_a_time():
    from shared.iceberg_utils import _creatable_arrow_schema, creatable_schema

    given = {"at": pl.Time(), "when": pl.Datetime("us", "Europe/Amsterdam"), "utc": pl.Datetime("ms", "UTC"),
             "ns": pl.Datetime("ns"), "ns_zone": pl.Datetime("ns", "Europe/Amsterdam"),
             "ns_list": pl.List(pl.Datetime("ns"))}
    assert creatable_schema(given) == {
        "at": pl.Time(), "when": pl.Datetime("us", "UTC"), "utc": pl.Datetime("ms", "UTC"),
        "ns": pl.Datetime("us"), "ns_zone": pl.Datetime("us", "UTC"), "ns_list": pl.List(pl.Datetime("us")),
    }
    arrow = _creatable_arrow_schema({"at": pl.Time(), "times": pl.List(pl.Time())})
    assert [str(field.type) for field in arrow] == ["time64[us]", "large_list<item: time64[us]>"]


def test_a_plan_of_a_table_with_no_columns_reads_no_column_for_its_height():
    from shared.iceberg_utils import _narrowest_column

    assert _narrowest_column(pl.Schema()) is None
    assert _narrowest_column(pl.Schema({"s": pl.String(), "n": pl.Int8()})) == "n"
    assert _narrowest_column(pl.Schema({"s": pl.String()})) == "s"


@pytest.mark.parametrize(("dtype", "sentence"), [
    (pl.Duration("ms"), 'Column c is a Duration, which an Iceberg table cannot hold; cast it to a number in the unit'
                        ' you mean, such as pl.col("c").dt.total_microseconds().'),
    (pl.Array(pl.Duration(), (2, 2)), "Column c is an Array holding a Duration, which an Iceberg table cannot hold;"
                                      " cast the durations in it to a number in the unit you mean."),
    (pl.Struct({"d": pl.Duration()}), "Column c is a Struct holding a Duration, which an Iceberg table cannot hold;"
                                      " cast the durations in it to a number in the unit you mean."),
    (pl.Null(), "Column c has no type (every value is null), so a new Iceberg table cannot be made with it; cast it to"
                " the type it should have, such as pl.String."),
    (pl.List(pl.Null()), "Column c is a List holding values with no type (every value is null), so a new Iceberg table"
                         " cannot be made with it; cast it to the type it should have."),
])
def test_a_type_no_iceberg_table_holds_is_refused_before_a_table_is_created(dtype, sentence):
    from shared.iceberg_utils import creatable_schema

    with pytest.raises(ValueError) as refused:
        creatable_schema({"id": pl.Int64(), "c": dtype})
    assert str(refused.value) == sentence


def test_a_catalog_that_never_answers_fails_within_the_timeout(monkeypatch):
    """A catalog that accepts the connection and never answers fails the call within ``CATALOG_TIMEOUT``, in a sentence."""
    import socket
    import time

    from shared import iceberg_utils

    silent = socket.socket()
    silent.bind(("127.0.0.1", 0))
    silent.listen(8)
    monkeypatch.setattr(iceberg_utils, "CATALOG_TIMEOUT", (1, 1))
    ref = _ref(catalog_uri=f"http://127.0.0.1:{silent.getsockname()[1]}/catalog")
    started = time.monotonic()
    try:
        with pytest.raises(ValueError, match="No catalog answers at"):
            iceberg_utils.table_schema(ref)
    finally:
        silent.close()
    assert time.monotonic() - started < 10


class _Table:
    """The parts of a table ``_sink`` reads, for its failure sentences."""

    metadata = type("Metadata", (), {"location": "s3://bucket/warehouse/t"})()
    io = object()

    @staticmethod
    def name():
        return ("ns", "t")


def _failed_sink(monkeypatch, error: Exception, folders) -> str:
    from shared import iceberg_utils

    def fail(table, frame, mode):
        raise error

    monkeypatch.setattr(iceberg_utils, "write_table", fail)
    listings = iter(folders)
    monkeypatch.setattr(iceberg_utils, "_data_folders", lambda table: next(listings))
    with pytest.raises(RuntimeError) as failed:
        iceberg_utils._sink(_Table(), pl.LazyFrame({"id": [1]}), "append")
    return str(failed.value)


def test_a_commit_the_catalog_did_not_confirm_is_not_called_failed(monkeypatch):
    from pyiceberg.exceptions import CommitStateUnknownException

    message = _failed_sink(monkeypatch, CommitStateUnknownException("502 Bad Gateway"), [set(), {"s3://b/x"}])
    assert message.startswith("The write to ns.t may not have committed (502 Bad Gateway): the catalog did not say.")
    assert "Load the table before removing anything." in message and "s3://b/x" not in message


def test_a_failed_commit_names_new_folders_only_as_possibly_its_own(monkeypatch):
    new = _failed_sink(monkeypatch, ValueError("refused"), [{"s3://b/a"}, {"s3://b/a", "s3://b/new"}])
    assert new == (
        "The write to ns.t did not commit (refused). New under its data/ folder since this write began: s3://b/new. If"
        " no other write ran at the same time they are this write's files, which the table does not use; nothing"
        " removes them."
    )
    nothing = _failed_sink(monkeypatch, ValueError("refused"), [set(), set()])
    assert nothing == "The write to ns.t did not commit (refused). It left no files under data/."
