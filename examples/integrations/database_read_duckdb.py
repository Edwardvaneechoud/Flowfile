"""Read from a local DuckDB file through a stored database connection."""

import os
import tempfile

import duckdb

import flowfile as ff

# Create a small local DuckDB database (any existing .duckdb file works).
db_path = os.path.join(tempfile.gettempdir(), "flowfile_docs_example.duckdb")
con = duckdb.connect(db_path)
con.execute(
    "CREATE OR REPLACE TABLE movies AS SELECT * FROM (VALUES "
    "('The Matrix', 8.7), ('Inception', 8.8), ('Alien', 8.5)) AS t(title, rating)"
)
con.close()

ff.create_database_connection_if_not_exists(
    "local-duckdb",
    database_type="duckdb",
    database=db_path,
)

# --8<-- [start:example]
high_rated = ff.read_database(
    "local-duckdb",
    query="SELECT title, rating FROM movies WHERE rating > 8.6 ORDER BY rating DESC",
).collect()

all_movies = ff.read_database("local-duckdb", table_name="movies").collect()
# --8<-- [end:example]

assert all_movies.height == 3
assert high_rated.height == 2
assert high_rated["title"].to_list() == ["Inception", "The Matrix"]
