import datetime
import decimal

import polars as pl
import pytest

from shared.db_writer import write_dataframe_to_database


def _uri(tmp_path, name="out.db"):
    return f"sqlite:///{tmp_path / name}"


def test_sqlite_roundtrip(tmp_path):
    uri = _uri(tmp_path)
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    write_dataframe_to_database(df, database_type="sqlite", uri=uri, table_name="people", if_exists="replace")
    back = pl.read_database_uri("SELECT * FROM people", uri)
    assert back.sort("id").equals(df)


def test_sqlite_mixed_dtypes(tmp_path):
    uri = _uri(tmp_path, "mixed.db")
    df = pl.DataFrame({
        "i": pl.Series([1, 2], dtype=pl.Int64),
        "f": [1.5, None],
        "b": [True, False],
        "d": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
        "ts": [datetime.datetime(2024, 1, 1, 3, 4, 5), datetime.datetime(2024, 1, 2, 0, 0, 0)],
        "dec": pl.Series([decimal.Decimal("1.23"), decimal.Decimal("4.56")], dtype=pl.Decimal(10, 2)),
        "tags": [["x", "y"], []],
        "meta": [{"k": 1}, {"k": 2}],
    })
    write_dataframe_to_database(df, database_type="sqlite", uri=uri, table_name="main.events", if_exists="replace")
    back = pl.read_database_uri("SELECT * FROM main.events", uri)
    assert len(back) == 2
    assert back["b"].to_list() == [1, 0]
    assert back["tags"].to_list() == ['["x", "y"]', "[]"]
    assert back["meta"][0] == '{"k": 1}'
    assert back["ts"][0] == "2024-01-01 03:04:05.000000"


def test_sqlite_if_exists_modes(tmp_path):
    uri = _uri(tmp_path, "modes.db")
    df = pl.DataFrame({"id": [1, 2]})

    def count():
        return pl.read_database_uri("SELECT count(*) c FROM t", uri)["c"][0]

    write_dataframe_to_database(df, database_type="sqlite", uri=uri, table_name="t", if_exists="replace")
    write_dataframe_to_database(df, database_type="sqlite", uri=uri, table_name="t", if_exists="append")
    assert count() == 4
    write_dataframe_to_database(df, database_type="sqlite", uri=uri, table_name="t", if_exists="replace")
    assert count() == 2
    with pytest.raises(ValueError, match="already exists"):
        write_dataframe_to_database(df, database_type="sqlite", uri=uri, table_name="t", if_exists="fail")


def test_sqlite_empty_frame(tmp_path):
    uri = _uri(tmp_path, "empty.db")
    write_dataframe_to_database(
        pl.DataFrame({"id": [1]}).head(0), database_type="sqlite", uri=uri, table_name="t", if_exists="replace"
    )
    assert pl.read_database_uri("SELECT count(*) c FROM t", uri)["c"][0] == 0
