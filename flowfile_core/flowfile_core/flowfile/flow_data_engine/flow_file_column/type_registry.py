
from typing import Type, Literal, List, Dict, Union, Tuple
import polars as pl
DataTypeGroup = Literal['numeric', 'string', 'datetime', 'boolean', 'binary', 'complex', 'unknown']


def convert_pl_type_to_string(pl_type: pl.DataType, inner: bool = False) -> str:
    if isinstance(pl_type, pl.List):
        inner_str = convert_pl_type_to_string(pl_type.inner, inner=True)
        return f"pl.List({inner_str})"
    elif isinstance(pl_type, pl.Array):
        inner_str = convert_pl_type_to_string(pl_type.inner, inner=True)
        return f"pl.Array({inner_str})"
    elif isinstance(pl_type, pl.Decimal):
        precision = pl_type.precision if hasattr(pl_type, 'precision') else None
        scale = pl_type.scale if hasattr(pl_type, 'scale') else None
        if precision is not None and scale is not None:
            return f"pl.Decimal({precision}, {scale})"
        elif precision is not None:
            return f"pl.Decimal({precision})"
        else:
            return "pl.Decimal()"
    elif isinstance(pl_type, pl.Struct):
        # Handle Struct with field definitions
        fields = []
        if hasattr(pl_type, 'fields'):
            for field in pl_type.fields:
                field_name = field.name
                field_type = convert_pl_type_to_string(field.dtype, inner=True)
                fields.append(f'pl.Field("{field_name}", {field_type})')
        field_str = ", ".join(fields)
        return f"pl.Struct([{field_str}])"
    else:
        # For base types, we want the full pl.TypeName format
        return str(pl_type.base_type()) if not inner else f"pl.{pl_type}"

