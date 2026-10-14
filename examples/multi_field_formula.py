"""Apply one formula to many columns at once with multi_field_formula."""

# --8<-- [start:example]
import flowfile as ff

budget = ff.from_dict(
    {
        "region": ["north", "south"],
        "jan": [1200, 900],
        "feb": [1500, 1100],
        "total": [2700, 2000],
    }
)

# [_CurrentField_] is the value of the column being processed. A prefix or a suffix
# writes the results to new columns and leaves the source columns untouched.
with_vat = budget.multi_field_formula(
    "round([_CurrentField_] * 1.21, 2)",
    data_type="Numeric",
    suffix="_incl_vat",
    output_data_type="Float64",
).collect()

# Without an affix the results overwrite their source columns. Every expression reads
# the original input, so [total] is still the input total while `total` is overwritten.
percent = budget.multi_field_formula(
    "round([_CurrentField_] / [total] * 100, 1)",
    columns=["total", "jan", "feb"],
).collect()

# [_CurrentFieldName_] and [_CurrentFieldType_] bind the column's name and its dtype.
labelled = budget.multi_field_formula(
    'concat([_CurrentFieldName_], " is ", [_CurrentFieldType_])',
    data_type="String",
    prefix="about_",
).collect()
# --8<-- [end:example]

assert with_vat.columns == ["region", "jan", "feb", "total", "jan_incl_vat", "feb_incl_vat", "total_incl_vat"]
assert with_vat["jan_incl_vat"].to_list() == [1452.0, 1089.0]
assert with_vat["total_incl_vat"].to_list() == [3267.0, 2420.0]
assert with_vat["jan"].to_list() == [1200, 900]

assert percent.columns == ["region", "jan", "feb", "total"]
assert percent["jan"].to_list() == [44.4, 45.0]
assert percent["feb"].to_list() == [55.6, 55.0]
assert percent["total"].to_list() == [100.0, 100.0]

assert labelled.columns == ["region", "jan", "feb", "total", "about_region"]
assert labelled["about_region"].to_list() == ["region is String", "region is String"]
