"""Read from SQL Server through a stored database connection."""

import os

import flowfile as ff

# Point at the seeded SQL Server test fixture (movies sample database).
ff.create_database_connection_if_not_exists(
    "analytics-sqlserver",
    database_type="mssql",
    host=os.environ.get("DOCS_MSSQL_HOST", "localhost"),
    port=int(os.environ.get("DOCS_MSSQL_PORT", 1434)),
    database="testdb",
    username="testuser",
    password="testpass",
)

# --8<-- [start:example]
top_rated = ff.read_database(
    "analytics-sqlserver",
    query="""
        SELECT title, rating, director
        FROM movies
        WHERE rating >= 8.8
    """,
).collect()

all_movies = ff.read_database(
    "analytics-sqlserver", schema_name="dbo", table_name="movies"
).collect()
# --8<-- [end:example]

assert all_movies.height == 5
assert "title" in all_movies.columns

assert top_rated.height == 4
titles = set(top_rated["title"].to_list())
assert "The Shawshank Redemption" in titles
assert "Inception" in titles
