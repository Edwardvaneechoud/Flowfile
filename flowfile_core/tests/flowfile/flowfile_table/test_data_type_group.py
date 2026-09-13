from datetime import date

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas.output_model import FileColumn


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


def test_geoarrow_extension_dtype_is_geometry_grouped_by_its_storage_type():
    """A declared GeoArrow extension dtype is the only thing that marks a column as
    geometry; it groups by its storage type so dtype-filtered pickers keep working."""
    wkb = pl.Extension("geoarrow.wkb", pl.Binary, "{}")
    engine = FlowDataEngine(
        pl.DataFrame(
            {
                "geom": pl.Series([b"\x01"], dtype=wkb),
                "s": ["POINT (1 2)"],
            }
        )
    )
    columns = {c.column_name: c for c in engine.schema}
    assert columns["geom"].data_type == "Extension('geoarrow.wkb', Binary, '{}')"
    assert columns["geom"].data_type_group == "Binary"
    assert columns["geom"].semantic_type == "geometry"
    # WKT-looking text is still just a String: values are never sniffed.
    assert columns["s"].semantic_type is None
    assert columns["s"].data_type_group == "String"


def test_semantic_type_ships_on_the_wire_model():
    native_point = pl.Extension("geoarrow.point", pl.Struct({"x": pl.Float64, "y": pl.Float64}), "{}")
    column = FlowfileColumn.create_from_polars_dtype("geom", native_point)
    assert column.data_type_group == "Complex"
    assert column.semantic_type == "geometry"
    wire = FileColumn.model_validate(column.get_column_repr())
    assert wire.semantic_type == "geometry"

    plain = FileColumn(name="s", data_type="String")
    assert plain.semantic_type is None


def test_non_geo_extension_dtype_has_no_semantic_type():
    column = FlowfileColumn.create_from_polars_dtype("u", pl.Extension("arrow.uuid", pl.Binary))
    assert column.data_type_group == "Binary"
    assert column.semantic_type is None
