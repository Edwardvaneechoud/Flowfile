# Database Backups

Flowfile keeps its catalog metadata in one SQLite database and snapshots that file before anything is likely to change it. This page covers what a snapshot contains, when one is taken, where snapshots live, and how to restore one.

## What is in a snapshot

A snapshot is a copy of the catalog database, `flowfile_catalog.db`: catalogs, schemas and table definitions, connections, encrypted secrets, schedules, users and groups, and run history. It is taken with SQLite's own backup API, so it is consistent even while Flowfile is running.

It does not contain your data or your work:

- Table data (Delta files on disk or in object storage) is not copied.
- Flow files, uploads and outputs are not copied — they are ordinary files under your user data directory, so back them up the way you back up any other folder.

## When one is taken

- **Before a schema migration.** When a version of Flowfile starts against a database that is behind its schema (or stamped at a revision it does not recognize), it snapshots first, then migrates.
- **Before a desktop update.** The [update dialog](desktop.md#updating) takes one after downloading and before installing.
- **On demand.** The **Back up now** button in the Backups tab, described below.

Snapshotting is best effort: a failure is logged and never blocks startup. The desktop update dialog tells you, and lets you continue without a snapshot.

## Where they live

Snapshots go in a `db_backups/` directory beside the database file:

| Deployment | Location |
|---|---|
| Desktop and Python package | `~/.flowfile/database/db_backups/` (or `$FLOWFILE_STORAGE_DIR/database/db_backups/`) |
| Docker | `/app/internal_storage/database/db_backups/`, inside the internal-storage volume |

Each file name carries the reason and a UTC timestamp — for example `flowfile_catalog.pre-update.20260903T101500Z.db` for the snapshot taken before an update, `.manual.` for one you asked for, and `<from>-to-<to>` for a migration snapshot naming the schema revisions it sits between.

Flowfile keeps the newest ten and prunes the rest. `FLOWFILE_DB_BACKUP_KEEP` changes that count; setting it to `0` disables snapshots entirely, including the ones taken before a migration.

## The Backups tab

**Compute → Backups** lists the snapshots for the running deployment — when each was taken, the app version that wrote it, why, and how large it is. **Back up now** adds one. On the desktop app, each row can be revealed in your file manager.

The tab is admin-only and lists snapshots only: there is no restore, delete or download button. Restoring is a deliberate act with the app stopped, described next.

## Restoring one

1. Quit Flowfile (or stop the Docker stack).
2. Copy the snapshot over `flowfile_catalog.db`, keeping the original name.
3. Start Flowfile again.

Three things to know before you do:

- You are restoring catalog metadata only. Table data and flow files are whatever is on disk right now, so a restore can leave the catalog describing tables that have since changed.
- If the snapshot came from an older version of Flowfile, the version you start will migrate the restored database up to its own schema — and snapshot it first, so the restored copy is preserved.
- Your secrets still decrypt. They are encrypted with the master key, which lives outside internal storage — your `.env` or Docker secret on a server, the app's own secure store on the desktop — so restoring the database never touches it.

## Related

- [Desktop App](desktop.md) — the update flow that takes a snapshot before installing.
- [Docker](docker.md) — internal-storage volume and operator configuration.
- [Secrets & Encryption](../visual-editor/catalog/secrets.md) — the master key that keeps restored secrets readable.
