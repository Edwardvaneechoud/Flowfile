"""Read only what changed since the last run with a catalog change feed."""

# --8<-- [start:example]
import flowfile as ff

schema = ff.default_schema()

# track_changes turns the table's change feed on; every write after it records its row changes.
day1 = ff.from_dict({"order_id": [1, 2], "status": ["new", "new"]})
ff.write_catalog_table(
    day1, "docs_orders_changes", schema=schema,
    write_mode="upsert", merge_keys=["order_id"], track_changes=True,
)

day2 = ff.from_dict({"order_id": [2, 3], "status": ["shipped", "new"]})
ff.write_catalog_table(
    day2, "docs_orders_changes", schema=schema,
    write_mode="upsert", merge_keys=["order_id"], track_changes=True,
)

# A named cursor is stored per table, so a scheduled flow resumes where it left off.
# "beginning" replays everything tracked; "now" (the default) starts at the current version.
changes = ff.read_catalog_table(
    "docs_orders_changes", schema=schema,
    changes_since="last_run", changes_consumer="docs-order-feed", changes_start="beginning",
)
# --8<-- [end:example]

feed = changes.collect()
# Every row carries the feed's three columns next to the table's own.
assert {"_change_type", "_commit_version", "_commit_timestamp"} <= set(feed.columns)
assert "update_preimage" not in feed["_change_type"].to_list()

latest = feed.filter(feed["_commit_version"] == feed["_commit_version"].max())
assert sorted(latest["order_id"].to_list()) == [2, 3]
assert sorted(latest["status"].to_list()) == ["new", "shipped"]
