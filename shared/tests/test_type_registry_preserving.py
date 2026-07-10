import polars as pl
import pytest

from shared.node_designer._type_registry import normalize_type_spec_preserving_groups
from shared.node_designer.types import DataType, TypeGroup, Types
from shared.node_designer.ui_components import normalize_input_to_data_types_preserving_groups


@pytest.mark.parametrize(
    "spec, expected",
    [
        # TypeGroups preserved as their canonical name (NOT expanded to members)
        (Types.Numeric, {"Numeric"}),
        ("numeric", {"Numeric"}),
        ("Numeric", {"Numeric"}),
        (TypeGroup.String, {"String"}),
        # Ambiguous bare string resolves to the group; the enum stays specific
        ("String", {"String"}),
        (DataType.String, {"String"}),
        # Specific types / aliases / polars types -> canonical DataType name
        (DataType.Int64, {"Int64"}),
        ("Int64", {"Int64"}),
        ("int", {"Int64"}),
        (pl.Int32, {"Int32"}),
        (pl.Datetime(time_unit="ms"), {"Datetime"}),
        # Combinations
        ([Types.Numeric, "Int32"], {"Numeric", "Int32"}),
        (["int", TypeGroup.Boolean, TypeGroup.Date], {"Boolean", "Date", "Int64"}),
        # Sentinels
        ("ALL", "ALL"),
        (Types.All, "ALL"),
        ([], set()),
    ],
)
def test_normalize_type_spec_preserving_groups(spec, expected):
    assert normalize_type_spec_preserving_groups(spec) == expected


@pytest.mark.parametrize(
    "spec, expected",
    [
        ("ALL", "ALL"),
        ([], "ALL"),
        (Types.Numeric, ["Numeric"]),
        ([Types.Numeric, "Int32"], ["Int32", "Numeric"]),
        # Idempotent on already-canonical token lists
        (["Numeric"], ["Numeric"]),
        (["Int64", "String"], ["Int64", "String"]),
        (["String", "Int64"], ["Int64", "String"]),
    ],
)
def test_normalize_input_to_data_types_preserving_groups(spec, expected):
    assert normalize_input_to_data_types_preserving_groups(spec) == expected
