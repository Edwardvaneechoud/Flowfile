from __future__ import annotations
import polars as pl
from typing import Any, Optional, Union, List


class Series:
    """
    A wrapper around polars.Series that represents itself as the code to create it.
    """

    def __init__(
            self,
            name: str | list | pl.Series | None = None,
            values: list | None = None,
            dtype: Any = None,
            **kwargs  # Ignored parameters
    ):
        """
        Initialize a FlowSeries with the same API as pl.Series.
        """
        # Store the original arguments for proper representation
        self._name = name
        self._values = values
        self._dtype = dtype

        # Handle the different initialization forms
        if isinstance(name, pl.Series):
            self._s = name
            # Update our attributes to match the series
            self._name = name.name
            self._values = name.to_list()
            self._dtype = name.dtype
        elif isinstance(name, (list, tuple)) and values is None:
            self._s = pl.Series(values=name, dtype=dtype)
            self._name = ""  # Default name is empty string
            self._values = name
        else:
            self._s = pl.Series(name=name, values=values, dtype=dtype)

    def __repr__(self) -> str:
        """
        Return a string that looks like the code to create this Series.
        Example: pl.Series("c", [1, 2, 3])
        """
        # Format name
        if self._name:
            name_str = f'"{self._name}"'
        else:
            name_str = '""'

        # Format values
        if self._values is None:
            values_str = "[]"
        elif len(self._values) <= 10:
            values_str = str(self._values)
        else:
            # Show first few elements for long lists
            sample = self._values[:3]
            values_str = f"[{', '.join(map(str, sample))}, ...]"

        # Format dtype if provided
        dtype_str = ""
        if self._dtype is not None:
            dtype_str = f", dtype={self._dtype}"

        return f"pl.Series({name_str}, {values_str}{dtype_str})"

    def __str__(self) -> str:
        """Same as __repr__."""
        return self.__repr__()