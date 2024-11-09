import polars as pl
from typing import Optional, Union, List, Tuple, Any


class PolarsSim:
    """Mock class for polars_sim functionality when the package is not available."""

    @staticmethod
    def join_sim(left: Union[pl.DataFrame, pl.LazyFrame],
                 right: Union[pl.DataFrame, pl.LazyFrame],
                 left_on: Union[str, List[str]],
                 right_on: Optional[Union[str, List[str]]] = None,
                 ntop: int = 1,
                 add_similarity: bool = True,
                 method: str = "jaro_winkler",
                 threshold: float = 0.0,
                 **kwargs: Any) -> pl.DataFrame:
        """
        Mock implementation of polars_sim.join_sim that falls back to cross join.

        Args:
            left: Left DataFrame
            right: Right DataFrame
            left_on: Column(s) from left DataFrame to join on
            right_on: Column(s) from right DataFrame to join on
            ntop: Number of top matches to return per left row
            add_similarity: Whether to add similarity scores
            method: Similarity method to use
            threshold: Minimum similarity threshold
            **kwargs: Additional arguments

        Returns:
            pl.DataFrame: Joined DataFrame
        """
        if isinstance(left, pl.LazyFrame):
            left = left.collect()
        if isinstance(right, pl.LazyFrame):
            right = right.collect()

        # Default to left_on if right_on not specified
        if right_on is None:
            right_on = left_on

        # Convert single column to list
        left_cols = [left_on] if isinstance(left_on, str) else left_on
        right_cols = [right_on] if isinstance(right_on, str) else right_on

        # Perform cross join
        joined = left.join(right, how="cross")

        return joined
