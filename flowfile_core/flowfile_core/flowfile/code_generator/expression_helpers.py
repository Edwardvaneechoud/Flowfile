import re

from flowfile_core.flowfile.code_generator.base import ConverterMixinBase
from flowfile_core.schemas import transform_schema


class ExpressionHelpersMixin(ConverterMixinBase):
    """Filter-expression parsing and Polars dtype / aggregation helpers."""

    def _parse_filter_expression(self, expr: str) -> str:
        """Parse Flowfile filter expression to Polars expression."""

        # Pattern: [column_name]operator"value" or [column_name]operatorvalue
        pattern = r'\[([^\]]+)\]([><=!]+)"?([^"]*)"?'

        def replace_expr(match):
            col, op, val = match.groups()

            op_map = {"=": "==", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<="}

            polars_op = op_map.get(op, op)

            try:
                float(val)
                return f'{self.framework}.col("{col}") {polars_op} {val}'
            except ValueError:
                return f'{self.framework}.col("{col}") {polars_op} "{val}"'

        return re.sub(pattern, replace_expr, expr)

    def _create_basic_filter_expr(self, basic: transform_schema.BasicFilter, field_dtype: str | None = None) -> str:
        """Create Polars expression from basic filter.

        Generates proper Polars code for all supported filter operators. When the column
        dtype is known to be Boolean, comparison values render as Python bool literals
        (``True``/``False``) instead of strings, matching the runtime filter semantics.

        Args:
            basic: The BasicFilter configuration.
            field_dtype: Predicted column dtype string (e.g. ``"Boolean"``), or None.

        Returns:
            A string containing valid Polars filter expression code.
        """
        from flowfile_core.schemas.transform_schema import FilterOperator

        col = f"{self.framework}.col({self._py_str(basic.field)})"
        value = basic.value
        value2 = basic.value2
        is_boolean = field_dtype == "Boolean"

        def render(v: str) -> str:
            """Render a comparison value: bool literal for boolean columns, a bare number
            for numeric strings, otherwise an escaped string literal."""
            if is_boolean:
                return "True" if v.strip().lower() in ("true", "1") else "False"
            if v and v.replace(".", "", 1).replace("-", "", 1).isnumeric():
                return v
            return self._py_str(v)

        # Determine if value is numeric (for proper quoting in the BETWEEN branch)
        is_numeric = value.replace(".", "", 1).replace("-", "", 1).isnumeric() if value else False

        try:
            operator = basic.get_operator()
        except (ValueError, AttributeError):
            operator = FilterOperator.from_symbol(str(basic.operator))

        if operator == FilterOperator.EQUALS:
            return f"{col} == {render(value)}"

        elif operator == FilterOperator.NOT_EQUALS:
            return f"{col} != {render(value)}"

        elif operator == FilterOperator.GREATER_THAN:
            return f"{col} > {render(value)}"

        elif operator == FilterOperator.GREATER_THAN_OR_EQUALS:
            return f"{col} >= {render(value)}"

        elif operator == FilterOperator.LESS_THAN:
            return f"{col} < {render(value)}"

        elif operator == FilterOperator.LESS_THAN_OR_EQUALS:
            return f"{col} <= {render(value)}"

        elif operator == FilterOperator.CONTAINS:
            return f"{col}.str.contains({self._py_str(value)})"

        elif operator == FilterOperator.NOT_CONTAINS:
            return f"{col}.str.contains({self._py_str(value)}).not_()"

        elif operator == FilterOperator.STARTS_WITH:
            return f"{col}.str.starts_with({self._py_str(value)})"

        elif operator == FilterOperator.ENDS_WITH:
            return f"{col}.str.ends_with({self._py_str(value)})"

        elif operator == FilterOperator.IS_NULL:
            return f"{col}.is_null()"

        elif operator == FilterOperator.IS_NOT_NULL:
            return f"{col}.is_not_null()"

        elif operator == FilterOperator.IN:
            values = [v.strip() for v in value.split(",")]
            if all(v.replace(".", "", 1).replace("-", "", 1).isnumeric() for v in values):
                values_str = ", ".join(values)
            else:
                values_str = ", ".join(self._py_str(v) for v in values)
            return f"{col}.is_in([{values_str}])"

        elif operator == FilterOperator.NOT_IN:
            values = [v.strip() for v in value.split(",")]
            if all(v.replace(".", "", 1).replace("-", "", 1).isnumeric() for v in values):
                values_str = ", ".join(values)
            else:
                values_str = ", ".join(self._py_str(v) for v in values)
            return f"{col}.is_in([{values_str}]).not_()"

        elif operator == FilterOperator.BETWEEN:
            if value2 is None:
                return f"{col}  # BETWEEN requires two values"
            if is_numeric and value2.replace(".", "", 1).replace("-", "", 1).isnumeric():
                return f"({col} >= {value}) & ({col} <= {value2})"
            return f"({col} >= {self._py_str(value)}) & ({col} <= {self._py_str(value2)})"

        return col

    def _get_polars_dtype(self, dtype_str: str) -> str:
        fw = self.framework
        dtype_map = {
            "String": f"{fw}.Utf8",
            "Integer": f"{fw}.Int64",
            "Double": f"{fw}.Float64",
            "Boolean": f"{fw}.Boolean",
            "Date": f"{fw}.Date",
            "Datetime": f"{fw}.Datetime",
            "Float32": f"{fw}.Float32",
            "Float64": f"{fw}.Float64",
            "Int32": f"{fw}.Int32",
            "Int64": f"{fw}.Int64",
            "Utf8": f"{fw}.Utf8",
        }
        return dtype_map.get(dtype_str, f"{fw}.Utf8")

    def _get_agg_function(self, agg: str) -> str:
        """Get Polars aggregation function name."""
        agg_map = {
            "avg": "mean",
            "average": "mean",
            "concat": "str.concat",
        }
        return agg_map.get(agg, agg)
