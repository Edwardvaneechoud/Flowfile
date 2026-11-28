"""Read a table, enrich and aggregate it, then write the ranked result back to a new table."""

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
# 1. Read the source table — one row per film.
movies = ff.read_database(
    "analytics-postgres", schema_name="public", table_name="movies"
)

# 2. Derive a new column, then keep only films that reported real financials.
with_profit = movies.with_columns(
    (ff.col("revenue") - ff.col("budget")).alias("profit")
).filter((ff.col("budget") > 0) & (ff.col("revenue") > 0))

# 3. Aggregate several metrics per original language.
by_language = with_profit.group_by("original_language").agg(
    ff.col("title").count().alias("films"),
    ff.col("vote_average").mean().alias("avg_score"),
    ff.col("profit").sum().alias("total_profit"),
    ff.col("profit").median().alias("median_profit"),
)

# 4. Keep well-sampled languages and rank them by total profit.
ranked = by_language.filter(ff.col("films") >= 10).sort(
    "total_profit", descending=True
)

# 5. Write the ranked result back to a new table.
ranked.write_database(
    "analytics-postgres",
    table_name="language_profitability",
    schema_name="public",
    if_exists="replace",
)

# 6. Read the destination table back to confirm the round trip.
result = ff.read_database(
    "analytics-postgres", schema_name="public", table_name="language_profitability"
).collect()
# --8<-- [end:example]

languages = result["original_language"].to_list()
by_lang = {row["original_language"]: row for row in result.to_dicts()}

assert result.height == 5
assert languages[0] == "en"  # ranked by total_profit, descending
assert languages == ["en", "ja", "zh", "fr", "es"]
assert by_lang["en"]["films"] == 3102
assert by_lang["en"]["total_profit"] == 257105728489
assert round(by_lang["en"]["avg_score"], 3) == 6.287
assert round(by_lang["fr"]["median_profit"], 1) == 303838.0
