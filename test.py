import flowfile as ff


def run_etl_pipeline():
    """
    ETL Pipeline: test_flow
    Generated from Flowfile
    """

    # Read from catalog table: code_gen_table
    df_1 = ff.read_catalog_table(
        "code_gen_table",
        namespace_id=4,
    )
    return df_1


if __name__ == "__main__":
    pipeline_output = run_etl_pipeline()
