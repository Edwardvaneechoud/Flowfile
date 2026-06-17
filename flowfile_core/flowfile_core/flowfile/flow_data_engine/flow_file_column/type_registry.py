from typing import Literal

import polars as pl

DataTypeGroup = Literal["numeric", "string", "datetime", "boolean", "binary", "complex", "unknown"]


def convert_pl_type_to_string(pl_type: pl.DataType) -> str:
    """Convert a Polars DataType to its string representation."""
    return str(pl_type)
