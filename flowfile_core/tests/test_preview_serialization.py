"""Preview rows must survive JSON serialization whatever the column dtype.

A ``pl.Binary`` column used to take the whole ``/node/data`` response down with a
``PydanticSerializationError``, so a single geometry blob hid every other column.
The failure was data-dependent: bytes that happened to be valid UTF-8 serialized
silently as mangled text instead. Both are covered here.
"""

import datetime as dt
import json
from decimal import Decimal
from enum import Enum

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from flowfile_core.schemas.output_model import FileColumn, TableExample

# A real EPSG:4326 WKB point — the bytes a spatial reader hands the preview.
WKB_POINT = bytes.fromhex("0101000000EE5A423EE8991340F1F44A5986304A40")


def _table_example(df: pl.DataFrame) -> TableExample:
    schema = [FileColumn(name=c, data_type=str(t)) for c, t in zip(df.columns, df.dtypes)]
    return TableExample(
        node_id=1,
        number_of_records=df.height,
        number_of_columns=df.width,
        name="preview",
        table_schema=schema,
        columns=df.columns,
        data=df.to_dicts(),
    )


def _first_row(df: pl.DataFrame) -> dict:
    return json.loads(_table_example(df).model_dump_json())["data"][0]


def test_binary_column_does_not_break_the_preview():
    row = _first_row(pl.DataFrame({"id": [1], "geometry": [WKB_POINT]}))
    assert row["id"] == 1
    assert row["geometry"].startswith("0x0101000000EE5A423E")
    assert "(21 bytes)" in row["geometry"]


def test_utf8_decodable_bytes_are_not_silently_shown_as_text():
    # Previously serialized as the plain string "POINT", indistinguishable from data.
    row = _first_row(pl.DataFrame({"geometry": [b"POINT"]}))
    assert row["geometry"] == "0x504F494E54"


def test_binary_nested_in_list_and_struct_is_coerced():
    row = _first_row(pl.DataFrame({"parts": [[WKB_POINT]], "meta": [{"g": WKB_POINT, "n": 1}]}))
    assert row["parts"][0].startswith("0x")
    assert row["meta"]["g"].startswith("0x")
    assert row["meta"]["n"] == 1


def test_object_column_is_coerced_even_though_its_group_is_not_binary():
    class Opaque:
        def __repr__(self) -> str:
            return "<opaque>"

    df = pl.DataFrame({"o": [Opaque()]}, strict=False)
    assert df.dtypes == [pl.Object]
    assert _first_row(df)["o"] == "<opaque>"


def test_null_binary_cell_stays_null():
    rows = json.loads(_table_example(pl.DataFrame({"g": [WKB_POINT, None]})).model_dump_json())["data"]
    assert rows[1]["g"] is None


@pytest.mark.parametrize(
    "df, column, expected",
    [
        (pl.DataFrame({"g": ["POINT (4.9041 52.3676)"]}), "g", "POINT (4.9041 52.3676)"),
        (pl.DataFrame({"s": [{"a": 1}]}), "s", {"a": 1}),
        (pl.DataFrame({"l": [[1, 2]]}), "l", [1, 2]),
        (pl.DataFrame({"d": [Decimal("1.5")]}), "d", "1.5"),
        (pl.DataFrame({"t": [dt.datetime(2020, 1, 1)]}), "t", "2020-01-01T00:00:00"),
    ],
)
def test_ordinary_dtypes_are_untouched(df, column, expected):
    """WKT text stays text and nested types stay JSON — the frontend renders those."""
    assert _first_row(df)[column] == expected


def test_binary_column_serializes_over_http():
    """The regression was an HTTP 500, so prove the response_model path too."""
    app = FastAPI()
    example = _table_example(pl.DataFrame({"id": [1], "geometry": [WKB_POINT]}))

    @app.get("/node/data", response_model=TableExample)
    def node_data() -> TableExample:
        return example

    response = TestClient(app).get("/node/data")
    assert response.status_code == 200
    assert response.json()["data"][0]["geometry"].startswith("0x")


def _object_frame(**columns) -> pl.DataFrame:
    return pl.DataFrame({k: [v] for k, v in columns.items()}, schema={k: pl.Object for k in columns})


def test_object_whose_str_raises_does_not_take_down_the_preview():
    class Broken:
        def __repr__(self) -> str:
            raise RuntimeError("boom")

    row = _first_row(_object_frame(o=Broken()).with_columns(pl.lit(1).alias("id")))
    assert row["id"] == 1
    assert row["o"] == "<unrepresentable Broken>"


def test_pathological_nesting_is_bounded():
    deep: list = []
    cursor = deep
    for _ in range(5000):
        nxt: list = []
        cursor.append(nxt)
        cursor = nxt
    loop: list = []
    loop.append(loop)
    row = _first_row(_object_frame(deep=deep, loop=loop))
    assert "nested too deep" in json.dumps(row["deep"])
    assert "nested too deep" in json.dumps(row["loop"])


def test_dict_keys_and_enum_values_are_coerced():
    class Tag(Enum):
        WKB = b"\x01\x02"

    row = _first_row(_object_frame(o={b"\xff": 1, 2: "two"}, e=Tag.WKB))
    assert row["o"] == {"0xFF": 1, "2": "two"}
    assert row["e"] == "0x0102"


def test_object_text_fallback_is_bounded_like_bytes():
    class Huge:
        def __repr__(self) -> str:
            return "x" * 50_000

    row = _first_row(_object_frame(o=Huge()))
    assert row["o"].endswith("\u2026 (50000 chars)")
    assert len(row["o"]) < 1100
