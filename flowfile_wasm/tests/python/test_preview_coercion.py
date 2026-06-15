"""Preview/explore cell coercion contract.

Polars `.rows()`/`.to_dicts()` yield native Python `date`/`datetime`/`time`/
`timedelta`/`Decimal`/`bytes` objects for temporal/decimal/binary columns. Those
cross Pyodide's `toJs()` bridge as PyProxies that render as `{}` in the grid, so
the engine must coerce every preview cell to a JSON-safe primitive before it
leaves Python. `to_json_safe_value` is the single source of truth shared by the
preview builders and the explore (Graphic Walker) path.
"""
import datetime
from decimal import Decimal

import polars as pl

import engine
from engine import state
from engine.dtypes import to_json_safe_value

_PRIMITIVE = (str, int, float, bool, type(None))


def _temporal_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "d": pl.Series([datetime.date(2024, 1, 1)], dtype=pl.Date),
            "dt": pl.Series([datetime.datetime(2024, 1, 1, 12, 30, 0)], dtype=pl.Datetime),
            "t": pl.Series([datetime.time(12, 30, 0)], dtype=pl.Time),
            "dur": pl.Series([datetime.timedelta(seconds=90)], dtype=pl.Duration),
            "dec": pl.Series([Decimal("1.50")]),
            "bin": pl.Series([b"hi"], dtype=pl.Binary),
            "i": pl.Series([7], dtype=pl.Int64),
            "s": pl.Series(["ok"], dtype=pl.String),
        }
    )


def _row_as_dict(preview: dict) -> dict:
    cols = preview["columns"]
    row = preview["data"][0]
    return dict(zip(cols, row, strict=True))


def test_to_json_safe_value_branches():
    assert to_json_safe_value(None) is None
    assert to_json_safe_value(datetime.date(2024, 1, 1)) == "2024-01-01"
    assert to_json_safe_value(datetime.datetime(2024, 1, 1, 12, 30)) == "2024-01-01T12:30:00"
    assert to_json_safe_value(datetime.time(12, 30)) == "12:30:00"
    assert to_json_safe_value(datetime.timedelta(seconds=90)) == 90.0
    dec = to_json_safe_value(Decimal("1.50"))
    assert isinstance(dec, float) and dec == 1.5
    assert to_json_safe_value(b"hi") == "hi"
    # Primitives pass straight through.
    assert to_json_safe_value(7) == 7
    assert to_json_safe_value("ok") == "ok"
    assert to_json_safe_value(True) is True


def test_fetch_preview_coerces_every_cell():
    state.store_lazyframe(1, _temporal_frame().lazy())
    result = engine.fetch_preview(1)
    assert result["success"] is True

    cells = result["data"]["data"][0]
    assert all(isinstance(c, _PRIMITIVE) for c in cells), cells

    row = _row_as_dict(result["data"])
    assert row["d"] == "2024-01-01"
    assert row["dt"] == "2024-01-01T12:30:00"
    assert row["t"] == "12:30:00"
    assert row["dur"] == 90.0
    assert row["dec"] == 1.5
    assert row["bin"] == "hi"
    assert row["i"] == 7
    assert row["s"] == "ok"


def test_df_to_preview_coerces_every_cell():
    preview = engine.df_to_preview(_temporal_frame())
    cells = preview["data"][0]
    assert all(isinstance(c, _PRIMITIVE) for c in cells), cells

    row = _row_as_dict(preview)
    assert row["d"] == "2024-01-01"
    assert row["dur"] == 90.0
    assert row["dec"] == 1.5


def test_explore_data_shares_the_same_coercion():
    state.store_lazyframe(5, _temporal_frame().lazy())
    result = engine.execute_explore_data(6, 5, {})
    assert result["success"] is True

    data_row = result["graphic_walker_input"]["dataModel"]["data"][0]
    assert all(isinstance(v, _PRIMITIVE) for v in data_row.values()), data_row
    assert data_row["d"] == "2024-01-01"
    assert data_row["dec"] == 1.5
