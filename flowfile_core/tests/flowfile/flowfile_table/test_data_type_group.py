from datetime import date

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine


def test_readable_data_type_group_classification():
    """Every polars dtype maps to its correct readable group; Boolean/Binary/Complex
    are their own groups (not folded into Numeric/Other)."""
    engine = FlowDataEngine(
        pl.DataFrame(
            {
                "i": [1, 2],
                "f": [1.5, 2.5],
                "s": ["a", "b"],
                "b": [True, False],
                "bin": [b"x", b"y"],
                "d": [date(2024, 1, 1), date(2024, 1, 2)],
                "lst": [[1, 2], [3]],
            }
        )
    )
    groups = {c.column_name: c.data_type_group for c in engine.schema}
    assert groups["i"] == "Numeric"
    assert groups["f"] == "Numeric"
    assert groups["s"] == "String"
    assert groups["b"] == "Boolean"
    assert groups["bin"] == "Binary"
    assert groups["d"] == "Date"
    assert groups["lst"] == "Complex"
