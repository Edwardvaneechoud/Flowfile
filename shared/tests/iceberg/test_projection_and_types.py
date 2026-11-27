"""What a plan reads of a table when it uses none, some or all of its columns, and which column types a write takes.

A count (``pl.len()``) or a row index over a scan uses none of the table's columns; the scan still yields frames of the
table's height. Iceberg has no unsigned integers, durations or zones other than UTC, and version 2 keeps times and
datetimes in microseconds: a write converts where the values survive, stores nanoseconds as microseconds as the
catalog's own Delta tables do, and refuses the rest in one sentence naming the column. Every test skips unless the
catalog runs: ``python -m test_utils.iceberg_rest start``.
"""

import datetime as dt

import polars as pl
import pytest

from shared import iceberg_utils
from shared.iceberg_utils import IcebergTableRef, scan_iceberg_table, write_iceberg_table
from shared.tests.iceberg.table_child import expected
from test_utils.iceberg_rest import fixtures

pytestmark = pytest.mark.iceberg_rest

TYPE_NAMES = ("pyarrow", "pa.", "time64", "timestamp[", "duration[", "Type(", "IntegerType", "LongType", "TimeType",
              "TimestampType", "TimestamptzType")


def _ref(namespace: str, name: str) -> IcebergTableRef:
    return IcebergTableRef(fixtures.CATALOG_URI, f"{namespace}.{name}", fixtures.WAREHOUSE)


@pytest.fixture
def batches_read(monkeypatch):
    """The columns and heights of every batch the scan's source yielded in this test, the source still run for real."""
    seen: list[tuple[list[str], int]] = []
    batches = iceberg_utils._TableScan._batches

    def recording(*args, **kwargs):
        for batch in batches(*args, **kwargs):
            seen.append((batch.columns, batch.height))
            yield batch

    monkeypatch.setattr(iceberg_utils._TableScan, "_batches", staticmethod(recording))
    return seen


def test_a_plan_that_uses_no_column_of_the_table_counts_its_rows(namespace, seed_table, batches_read):
    seed_table("types_counted_rows", 1000, files=3)
    scan = scan_iceberg_table(_ref(namespace, "types_counted_rows"))

    assert scan.select(pl.len()).collect().item() == 1000
    assert scan.with_row_index().select(pl.len()).collect().item() == 1000
    assert scan.with_row_index("Record ID").select("Record ID").collect()["Record ID"].to_list() == list(range(1000))
    assert scan.with_row_index().filter(pl.col("id") >= 900).select(pl.len()).collect().item() == 100
    assert scan.with_row_index().head(7).select(pl.len()).collect().item() == 7
    widths = {len(columns) for columns, _ in batches_read}
    assert 0 in widths
    assert sum(height for columns, height in batches_read if not columns) >= 1000 * 2


def test_a_plan_that_uses_some_columns_reads_only_those_and_one_that_uses_all_reads_all(
    namespace, seed_table, batches_read
):
    seed_table("types_projected", 100)
    scan = scan_iceberg_table(_ref(namespace, "types_projected"))

    assert scan.select(pl.col("id").sum()).collect().item() == expected(100)[1]
    some = {tuple(columns) for columns, _ in batches_read}
    batches_read.clear()
    assert scan.filter(pl.col("grp") == 3).select("name").collect().height == 10
    filtered = {tuple(sorted(columns)) for columns, _ in batches_read}
    batches_read.clear()
    assert scan.collect().shape == (100, 4)
    every = {tuple(columns) for columns, _ in batches_read}

    assert some == {("id",)}
    assert filtered == {("grp", "name")}
    assert every == {("id", "grp", "val", "name")}


def test_the_count_survives_serialization_and_another_engines_deletes(
    rest_catalog, namespace, seed_table, duck, child_env, run_child
):
    import base64

    seed_table("types_counted_after_deletes", 1000, files=2, properties={"format-version": "2"})
    fixtures.attach_lake(duck)
    duck.execute(f"DELETE FROM lake.{namespace}.types_counted_after_deletes WHERE id % 7 = 0")
    plan = scan_iceberg_table(_ref(namespace, "types_counted_after_deletes")).with_row_index().select(pl.len())

    answer = run_child("shared.tests.iceberg.table_child",
                       {"op": "collect_count", "plan": base64.b64encode(plan.serialize()).decode()}, child_env)

    assert answer["count"] == sum(1 for i in range(1000) if i % 7)


def _written(namespace: str, name: str, frame: pl.LazyFrame) -> pl.DataFrame:
    write_iceberg_table(_ref(namespace, name), frame, "append")
    return scan_iceberg_table(_ref(namespace, name)).collect()


def _refusal(namespace: str, name: str, frame: pl.LazyFrame) -> str:
    with pytest.raises(ValueError) as refused:
        write_iceberg_table(_ref(namespace, name), frame, "append")
    message = str(refused.value)
    print(message)
    assert [word for word in TYPE_NAMES if word in message] == []
    assert message.count(". ") == 0 and message.endswith(".")
    return message


def test_unsigned_and_index_columns_are_written_and_read_back(rest_catalog, namespace):
    index = pl.get_index_type()
    frame = pl.LazyFrame(
        {"u8": [0, 255], "u16": [0, 65535], "u32": [0, 2**32 - 1], "u64": [0, 2**63 - 1], "idx": [0, 1]},
        schema={"u8": pl.UInt8, "u16": pl.UInt16, "u32": pl.UInt32, "u64": pl.UInt64, "idx": index},
    ).with_columns(n=pl.len())

    back = _written(namespace, "types_unsigned", frame)

    assert dict(back.schema) == {"u8": pl.Int32, "u16": pl.Int32, "u32": pl.Int64, "u64": pl.Int64, "idx": pl.Int64,
                                 "n": pl.Int64}
    assert back.cast(pl.Int64).to_dicts() == frame.collect().cast(pl.Int64).to_dicts()
    again = _written(namespace, "types_unsigned", frame)
    assert again.height == 4


def test_a_count_and_a_row_index_column_are_written_on_this_runtime(rest_catalog, namespace):
    frame = pl.LazyFrame({"grp": [1, 1, 2]}).group_by("grp").agg(pl.len().alias("n")).with_row_index("Record ID")

    back = _written(namespace, "types_count_and_index", frame).sort("grp")

    assert back.select("grp", "n").rows() == [(1, 2), (2, 1)]
    assert sorted(back["Record ID"].to_list()) == [0, 1]


def test_a_multi_dimensional_array_is_written_and_read_back(rest_catalog, namespace):
    frame = pl.LazyFrame({"grid": [[[1, 2, 3], [4, 5, 6]]], "small": [[[1, 2, 3], [4, 5, 6]]]},
                         schema={"grid": pl.Array(pl.Int64, (2, 3)), "small": pl.Array(pl.UInt16, (2, 3))})

    back = _written(namespace, "types_grid", frame)

    assert back["grid"].to_list() == [[[1, 2, 3], [4, 5, 6]]]
    assert back["small"].to_list() == [[[1, 2, 3], [4, 5, 6]]]
    assert _written(namespace, "types_grid", frame).height == 2


def test_a_uint64_value_beyond_a_long_is_refused_naming_the_column(rest_catalog, namespace):
    frame = pl.LazyFrame({"big": [1, 2**64 - 1]}, schema={"big": pl.UInt64})

    message = _refusal(namespace, "types_too_big", frame)

    assert message == (f"Column big holds a value above 9223372036854775807, the largest an Iceberg long holds, so the"
                       f" write to {namespace}.types_too_big did not commit.")
    with pytest.raises(ValueError) as again:
        write_iceberg_table(_ref(namespace, "types_too_big"), frame, "overwrite")
    assert str(again.value) == message


def test_text_into_a_long_column_is_refused_without_claiming_a_range(rest_catalog, namespace):
    _written(namespace, "types_text_into_long", pl.LazyFrame({"x": [1]}))

    message = _refusal(namespace, "types_text_into_long", pl.LazyFrame({"x": ["abc"]}))

    assert message == (f"Column x holds values that are not numbers an Iceberg long holds, so the write to"
                       f" {namespace}.types_text_into_long did not commit.")


AMSTERDAM = dt.datetime(2024, 6, 1, 12, 30, tzinfo=dt.timezone(dt.timedelta(hours=2)))
NANOSECONDS = 1_700_000_000_123_456_789


def test_a_time_and_a_zone_other_than_utc_are_written_and_the_instant_is_kept(rest_catalog, namespace):
    frame = pl.LazyFrame({
        "at": [dt.time(1, 2, 3, 456789)],
        "when": [AMSTERDAM],
        "ms": [dt.datetime(2024, 1, 1, 1, 2, 3, 456000)],
    }).with_columns(pl.col("when").dt.convert_time_zone("Europe/Amsterdam"), pl.col("ms").cast(pl.Datetime("ms")))

    back = _written(namespace, "types_times", frame)

    assert dict(back.schema) == {"at": pl.Time, "when": pl.Datetime("us", "UTC"), "ms": pl.Datetime("us")}
    assert back["at"].to_list() == [dt.time(1, 2, 3, 456789)]
    assert back["when"].to_list()[0] == AMSTERDAM
    assert back["ms"].to_list() == [dt.datetime(2024, 1, 1, 1, 2, 3, 456000)]
    assert _written(namespace, "types_times", frame).height == 2


def _nanoseconds(zone: str | None = None) -> pl.LazyFrame:
    stamp = pl.lit(NANOSECONDS, dtype=pl.Int64).cast(pl.Datetime("ns"))
    return pl.LazyFrame({"id": [1]}).with_columns(stamp=stamp.dt.replace_time_zone(zone) if zone else stamp)


def test_nanosecond_datetimes_create_a_microsecond_table_as_the_catalogs_delta_tables_do(rest_catalog, namespace):
    back = _written(namespace, "types_ns", _nanoseconds())
    zoned_frame = _nanoseconds("Europe/Amsterdam")
    zoned = _written(namespace, "types_ns_zone", zoned_frame)

    assert back.schema["stamp"] == pl.Datetime("us")
    assert back["stamp"].cast(pl.Int64).to_list() == [NANOSECONDS // 1000]
    assert zoned.schema["stamp"] == pl.Datetime("us", "UTC")
    instant = zoned_frame.collect()["stamp"].cast(pl.Int64).item()
    assert zoned["stamp"].cast(pl.Int64).to_list() == [instant // 1000]


def test_nanosecond_datetimes_are_cast_into_an_existing_tables_timestamp_column_as_before(rest_catalog, namespace):
    arrow = pl.DataFrame({"id": [0], "stamp": [dt.datetime(2024, 1, 1)]}).to_arrow(compat_level=pl.CompatLevel.oldest())
    rest_catalog.create_table(f"{namespace}.types_ns_existing", schema=arrow.schema)

    write_iceberg_table(_ref(namespace, "types_ns_existing"), _nanoseconds(), "append")
    write_iceberg_table(_ref(namespace, "types_ns_existing"), _nanoseconds(), "overwrite")
    back = scan_iceberg_table(_ref(namespace, "types_ns_existing")).collect()

    assert back["stamp"].cast(pl.Int64).to_list() == [NANOSECONDS // 1000]


REFUSED = {
    "a duration": (
        pl.LazyFrame({"waited": [dt.timedelta(seconds=5)]}),
        "Column waited is a Duration, which an Iceberg table cannot hold; cast it to a number in the unit you mean,"
        " such as pl.col(\"waited\").dt.total_microseconds().",
    ),
    "a nested duration": (
        pl.LazyFrame({"spans": [[dt.timedelta(seconds=5)]]}),
        "Column spans is a List holding a Duration, which an Iceberg table cannot hold; cast the durations in it to a"
        " number in the unit you mean.",
    ),
}


@pytest.mark.parametrize("case", sorted(REFUSED))
def test_a_type_iceberg_cannot_hold_is_refused_in_one_sentence_on_create_and_on_append(
    rest_catalog, namespace, seed_table, case
):
    frame, sentence = REFUSED[case]
    name = "types_refused_" + case.replace(" ", "_")

    assert _refusal(namespace, name, frame) == sentence
    with pytest.raises(Exception):
        rest_catalog.load_table(f"{namespace}.{name}")

    column = frame.collect_schema().names()[0]
    arrow = pl.DataFrame({column: ["x"]}).to_arrow(compat_level=pl.CompatLevel.oldest())
    rest_catalog.create_table(f"{namespace}.{name}_existing", schema=arrow.schema)
    assert _refusal(namespace, f"{name}_existing", frame) == sentence


def test_a_column_with_no_type_is_refused_on_create_in_one_sentence(rest_catalog, namespace):
    assert _refusal(namespace, "types_untyped", pl.LazyFrame({"empty": [None]})) == (
        "Column empty has no type (every value is null), so a new Iceberg table cannot be made with it; cast it to"
        " the type it should have, such as pl.String."
    )


def test_a_struct_field_that_does_not_fit_names_no_column(rest_catalog, namespace):
    _written(namespace, "types_struct_field", pl.LazyFrame({"st": [{"n": 1}]}))

    message = _refusal(namespace, "types_struct_field", pl.LazyFrame({"st": [{"n": "abc"}]}))

    assert message == (f"A value in the data does not fit its column in the table, so the write to"
                       f" {namespace}.types_struct_field did not commit.")


def test_a_struct_field_named_like_a_top_level_column_does_not_blame_that_column(rest_catalog, namespace):
    _written(namespace, "types_struct_field_shared", pl.LazyFrame({"n": ["x"], "st": [{"n": 1}]}))

    message = _refusal(namespace, "types_struct_field_shared", pl.LazyFrame({"n": ["y"], "st": [{"n": "abc"}]}))

    assert message == (f"A value in the data does not fit its column in the table, so the write to"
                       f" {namespace}.types_struct_field_shared did not commit.")
