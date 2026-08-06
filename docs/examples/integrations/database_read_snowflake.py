"""Read from Snowflake through a stored database connection.

Runs only when FLOWFILE_TEST_SNOWFLAKE_* credentials are configured (see
flowfile_core/tests/docs_examples/test_docs_examples.py); the account,
warehouse, and role travel in the connection's extra_params.
"""

import os

import flowfile as ff

ff.create_database_connection_if_not_exists(
    "analytics-snowflake",
    database_type="snowflake",
    database=os.environ.get("FLOWFILE_TEST_SNOWFLAKE_DATABASE", "SNOWFLAKE_SAMPLE_DATA"),
    username=os.environ["FLOWFILE_TEST_SNOWFLAKE_USER"],
    password=os.environ["FLOWFILE_TEST_SNOWFLAKE_PASSWORD"],
    extra_params={
        "account": os.environ["FLOWFILE_TEST_SNOWFLAKE_ACCOUNT"],
        "warehouse": os.environ["FLOWFILE_TEST_SNOWFLAKE_WAREHOUSE"],
    },
)

# --8<-- [start:example]
suppliers = ff.read_database(
    "analytics-snowflake",
    query="""
        SELECT s_name, s_acctbal
        FROM tpch_sf1.supplier
        WHERE s_acctbal > 9900
    """,
).collect()

nations = ff.read_database(
    "analytics-snowflake", schema_name="TPCH_SF1", table_name="NATION"
).collect()
# --8<-- [end:example]

assert nations.height == 25
assert suppliers.height > 0
