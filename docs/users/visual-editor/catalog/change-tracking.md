# Change Tracking

Change tracking lets a flow read only what changed in a catalog table since the last time it ran, instead of re-reading the whole table every run. It is Flowfile's incremental-processing primitive: turn it on for a table, point a [Catalog Reader](../nodes/input.md#catalog-reader) at it in one of the **Changes** modes, and each run returns the inserts, updates and deletes committed since the run before it. This page covers how to turn it on, what the reader returns, how the per-consumer cursor behaves, and the cases where tracking is refused or unsafe.

Underneath it is Delta Lake's change data feed: a table property that makes every write record its row-level changes alongside the data. Flowfile turns the property on, remembers the version it was turned on at, and keeps a cursor per consumer.

## Turning it on

Tracking is **off by default**, **per table**, and **enable-only** — nothing in the UI turns it back off, because the changes a table stops recording cannot be recovered later. There are three ways to turn it on, all of which do the same thing:

- The [Catalog Writer](../nodes/output.md#catalog-writer)'s **Track changes** checkbox. On a new table the property is set at creation; on an existing untracked table the writer enables it just before writing, so that write is the first tracked commit.
- The **Change tracking** card on the table's detail page in the catalog browser.
- The **Enable change tracking** button in the Catalog Reader's settings, shown when the selected table is not tracked yet.

In Python, pass `track_changes=True` to [`write_catalog_table`](../../python-api/reference/writing-data.md#catalog-writing) or `SchemaReference.write_table`.

!!! warning "Only commits made after enabling are tracked"
    Enabling records the Delta version it happened at, and that version is the floor for every cursor. History written before it cannot be replayed — the change feed for those versions is either missing or inconsistent, so Flowfile refuses to read below the floor. Turn tracking on when you create the table, not when you first need a change feed.

Tracking is refused on virtual tables, legacy parquet tables and [SCD2](slowly-changing-dimensions.md)-tracked tables. An SCD2 table already keeps row history in its `valid_from` / `valid_to` / `is_current` columns — read those instead.

## Reading changes

A Catalog Reader on a tracked table shows a **Read** selector above the History selector:

![Catalog Reader set to "Changes since last run", showing the cursor status line and Reset cursor button](../../../assets/images/guides/catalog/change-tracking-reader.png)

| Option | What the run returns |
|--------|----------------------|
| **Full table** (default) | The table as it is now. No change feed. |
| **Changes since last run** | Everything committed after the version this consumer last processed. Advances a cursor. |
| **Changes since version** | Everything committed after the version you pick. No cursor — the same window every run. |
| **Changes since time** | Everything committed at or after a timestamp (never below the tracking floor). No cursor. |

**Changes since version** and **Changes since time** also accept a flow parameter (`${name}`, offered in the drawer when the flow defines one of the matching type), so a scheduled, CLI or API run can pass the starting point at run time.

Every change mode adds three columns to the table's own:

| Column | Meaning |
|--------|---------|
| `_change_type` | `insert`, `update_postimage`, `delete` — plus `update_preimage` when preimages are included |
| `_commit_version` | The Delta commit version the row change belongs to |
| `_commit_timestamp` | When that commit landed |

By default the reader drops `update_preimage` rows, so an updated row appears once, with its new values. Tick **Include row values from before each update** (`include_change_preimage` in Python) to get the before-image rows as well; pair them with `_commit_version` to see what a row looked like before and after the same commit.

A run whose window contains nothing returns an empty frame with the full schema, not an error — a scheduled flow that fires during a quiet hour simply processes zero rows.

## Cursors

Only **Changes since last run** keeps a cursor. A cursor stores one number: the last Delta commit version that was fully processed. The next run reads from the version after it, up to the table's head at the moment the read starts.

**Which runs advance it.** A cursor moves only after a full flow run in which the reader and everything downstream of it completed — running the flow from the designer, from a schedule, or headlessly. Previewing a node, running a single node, or cancelling a run never advances it. A failure anywhere downstream leaves the cursor where it was, so the next run replays the same window.

**Delivery is at-least-once.** Because the cursor advances after the run rather than during it, a run that succeeds but whose commit fails, or one that is killed between writing and committing, will re-deliver its window. Make the downstream write idempotent — upsert on a key rather than append.

**Cursor identity.** By default a cursor belongs to the node in the flow it lives in: two flows reading the same table each get their own position, and each sees every change. Give the reader a **Cursor name** to share one position across flows or notebooks instead — named cursors are global to the table, so whichever run reads first consumes the window for all of them. Names may contain letters, digits and `_ . : -`.

!!! note "A named cursor is required outside a saved flow"
    The default cursor key is built from the flow's identity, so a reader in an unsaved flow has nothing to key on. Save the flow first, or give the cursor a name. In `flowfile_frame`, `changes_since="last_run"` needs `changes_consumer=` unless the graph is registered in the catalog.

**Where a cursor starts.** The first run with no cursor yet is governed by **Start from**: *Now* (the default) initializes the cursor at the current version and reads nothing, so processing starts with the next change; *Beginning* replays everything recorded since tracking was enabled.

**Reset and delete.** The reader's settings and the table detail page both list the table's cursors with how far behind they are, and offer **Reset** (move to the current version, skipping unread changes) and **Delete** (forget the position entirely — the next run re-initializes per **Start from**). Resetting to the beginning replays all tracked history.

## What each write mode produces

Any write to a tracked table records changes, but what shows up in the feed depends on how the data was written:

- **Upsert, update, delete** record exactly the rows they touched, with `update_preimage`/`update_postimage` pairs for modified rows. This is the combination change tracking is built for.
- **Append** records its new rows as inserts.
- **Overwrite** is not offered with **Track changes** on the writer (`track_changes` with `write_mode="overwrite"`, `"virtual"` or `"scd2"` is a validation error). A tracked table that some other flow overwrites still produces a feed — but as a full set of deletes followed by a full set of inserts, which is rarely what a downstream consumer wants.

## Vacuum and history retention

A change feed can only be read while the underlying Delta history is still on disk. [Vacuum](index.md#delta-table-history) reclaims files older than the retention window, which can put the versions a cursor still needs out of reach.

Flowfile guards against that: a vacuum that would drop history a cursor still points into is refused with a warning listing the cursors at risk, and the dialog offers **Vacuum anyway** if you accept the loss. If a read later lands on a version that no longer exists, the reader fails with an error telling you to reset the cursor.

Overwrite commits are the fragile case: their change feed is reconstructed from the files the overwrite replaced, so a vacuum breaks it. Merge-based writes (upsert, update, delete) materialize their changes explicitly and survive.

## From Python

`read_catalog_table` and `SchemaReference.read_table` take `changes_since` — an `int` for a version, `"last_run"` for a cursor, or an ISO-8601 string / `datetime` for a timestamp — plus `changes_consumer`, `changes_start` and `include_change_preimage`. See [Catalog References](../../python-api/reference/catalog-references.md) for the full signatures.

The tested example below writes a table twice with tracking on, then reads its change feed through a named cursor:

```python
--8<-- "docs/examples/catalog_change_feed.py:example"
```

## Related documentation

- [Catalog Reader](../nodes/input.md#catalog-reader) — the Read selector
- [Catalog Writer](../nodes/output.md#catalog-writer) — the Track changes flag
- [Slowly Changing Dimensions](slowly-changing-dimensions.md) — row-level history in ordinary columns, the alternative to a change feed
- [Catalog](index.md#delta-table-history) — Delta version history and vacuum
- [Schedules](schedules.md) — running an incremental flow on a timer
