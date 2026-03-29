"""One-time migration script: convert existing Parquet catalog tables to Delta Lake.

Usage:
    python -m flowfile_core.catalog.migrate_parquet_to_delta [--dry-run]

This script:
1. Queries all CatalogTable rows with storage_format="parquet"
2. For each: reads the .parquet file, writes a Delta table to a new directory,
   verifies the result, deletes the old .parquet file, and updates the DB record
3. Supports --dry-run to preview changes without modifying anything

Run this manually after upgrading to Delta-based catalog storage.  It is NOT
auto-run on startup.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import polars as pl

from flowfile_core.catalog.delta_utils import is_delta_table
from flowfile_core.database.connection import SessionLocal
from flowfile_core.database.models import CatalogTable

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def migrate_table(table: CatalogTable, *, dry_run: bool = False) -> bool:
    """Migrate a single table from Parquet to Delta.

    Returns True on success, False on skip/error.
    """
    old_path = Path(table.file_path)

    if not old_path.exists():
        logger.warning("  [SKIP] File does not exist: %s", old_path)
        return False

    if not old_path.is_file() or old_path.suffix.lower() != ".parquet":
        logger.warning("  [SKIP] Not a .parquet file: %s", old_path)
        return False

    # New delta directory: same stem, no extension
    new_dir = old_path.parent / old_path.stem
    if new_dir.exists():
        if is_delta_table(new_dir):
            logger.info("  [SKIP] Delta table already exists at %s", new_dir)
            return False
        logger.warning("  [SKIP] Directory exists but is not a delta table: %s", new_dir)
        return False

    if dry_run:
        logger.info("  [DRY-RUN] Would convert %s -> %s", old_path, new_dir)
        return True

    try:
        df = pl.read_parquet(old_path)
        df.write_delta(str(new_dir), mode="error")

        # Verify
        verify_df = pl.scan_delta(str(new_dir))
        row_count = verify_df.select(pl.len()).collect().item()
        if row_count != df.height:
            logger.error(
                "  [ERROR] Row count mismatch: parquet=%d delta=%d. Leaving original intact.",
                df.height,
                row_count,
            )
            return False

        # Calculate new size
        size_bytes = sum(f.stat().st_size for f in new_dir.rglob("*.parquet"))

        # Update DB record
        table.file_path = str(new_dir)
        table.storage_format = "delta"
        table.size_bytes = size_bytes

        # Delete old parquet
        old_path.unlink()
        logger.info("  [OK] Migrated %s -> %s (%d rows)", old_path, new_dir, row_count)
        return True

    except Exception:
        logger.exception("  [ERROR] Failed to migrate %s", old_path)
        return False


def main(dry_run: bool = False) -> int:
    db = SessionLocal()
    try:
        tables = db.query(CatalogTable).filter(CatalogTable.storage_format == "parquet").all()
        logger.info("Found %d tables with storage_format='parquet'", len(tables))

        migrated = 0
        for table in tables:
            logger.info("Processing table #%d: %s (path: %s)", table.id, table.name, table.file_path)
            if migrate_table(table, dry_run=dry_run):
                migrated += 1

        if not dry_run:
            db.commit()
            logger.info("Committed DB changes for %d migrated tables", migrated)
        else:
            logger.info("[DRY-RUN] Would migrate %d tables (no changes made)", migrated)

        return migrated
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate catalog tables from Parquet to Delta Lake")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without modifying anything")
    args = parser.parse_args()

    count = main(dry_run=args.dry_run)
    sys.exit(0 if count >= 0 else 1)
