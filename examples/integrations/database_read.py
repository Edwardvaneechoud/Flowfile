"""Read from PostgreSQL through a stored database connection."""

import os

import flowfile as ff

# Point at the seeded Postgres test fixture (movies sample database).
ff.create_database_connection_if_not_exists(
    "analytics-postgres",
    database_type="postgresql",
    host=os.environ.get("DOCS_PG_HOST", "localhost"),
    port=int(os.environ.get("DOCS_PG_PORT", 5433)),
    database="testdb",
    username="testuser",
    password="testpass",
)

# --8<-- [start:example]
big_budget = ff.read_database(
    "analytics-postgres",
    query="""
        SELECT title, budget, vote_average
        FROM public.movies
        WHERE budget > 200000000
        ORDER BY budget DESC
    """,
).collect()

all_movies = ff.read_database(
    "analytics-postgres", schema_name="public", table_name="movies"
).collect()
# --8<-- [end:example]

assert all_movies.height == 4803
assert "title" in all_movies.columns

assert big_budget.height > 0
assert big_budget["budget"].max() == 380000000
titles = set(big_budget["title"].to_list())
assert "Avatar" in titles
assert "Pirates of the Caribbean: On Stranger Tides" in titles
