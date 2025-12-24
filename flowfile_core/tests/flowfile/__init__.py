import polars as pl


def run_etl_pipeline():
    """
    ETL Pipeline: test_flow
    Generated from Flowfile
    """

    df_1 = pl.DataFrame(
        [['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02', '2024-01-03'], ['A', 'B', 'A', 'B', 'C'],
         [10, 20, 15, 25, 30], [100.0, 200.0, 100.0, 200.0, 150.0], ['North', 'North', 'South', 'South', 'East']],
        schema=pl.Schema([("date", pl.String), ("product", pl.String), ("quantity", pl.Int64), ("price", pl.Float64),
                          ("region", pl.String)]), strict=False)
    df_2 = df_1.pivot(
        values='quantity',
        columns='product',
        aggregate_function='sum'
    )
    return df_2


if __name__ == "__main__":
    pipeline_output = run_etl_pipeline()
