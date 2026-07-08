import polars as pl


def helper() -> pl.DataFrame:
    return pl.DataFrame({"a": [1, 2, 3]})
