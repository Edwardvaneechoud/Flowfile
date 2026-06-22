"""Fix workspace_projects uniqueness, add partial active-project index, enforce viz/dashboard uuid NOT NULL.

1. Drop the global ``folder_path`` unique constraint on ``workspace_projects`` and replace it
   with a composite ``(owner_id, folder_path)`` unique constraint (Contract 2 / H2 / M-A4).
2. Add a partial unique index ``ON workspace_projects(owner_id) WHERE is_active = 1``
   (I4 — one active project per owner guaranteed at the DB level).
3. ``ALTER COLUMN`` ``viz_uuid`` / ``dashboard_uuid`` to ``NOT NULL``
   (L5 — migration 024 backfilled them but left them nullable).

All three steps are idempotent and safe on both a fresh DB (running from 025) and a populated DB
that already has rows (deduplication guards run before constraint creation).

Revision ID: 026
Revises: 025
Create Date: 2026-06-19
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text

revision: str = "026"
down_revision: str | None = "025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_index(table: str, index_name: str) -> bool:
    return index_name in {i["name"] for i in inspect(op.get_bind()).get_indexes(table)}


def _has_unique_constraint(table: str, constraint_name: str) -> bool:
    return constraint_name in {uc["name"] for uc in inspect(op.get_bind()).get_unique_constraints(table)}


def _dedup_active_projects(bind) -> None:
    """Keep only the lowest-id active row per owner; deactivate duplicates."""
    rows = bind.execute(
        text(
            "SELECT owner_id, MIN(id) AS keep_id FROM workspace_projects "
            "WHERE is_active = 1 GROUP BY owner_id HAVING COUNT(*) > 1"
        )
    ).fetchall()
    for owner_id, keep_id in rows:
        bind.execute(
            text("UPDATE workspace_projects SET is_active = 0 " "WHERE owner_id = :o AND is_active = 1 AND id != :k"),
            {"o": owner_id, "k": keep_id},
        )


def _dedup_owner_path(bind) -> None:
    """Keep the lowest-id row per (owner_id, folder_path); delete duplicates."""
    rows = bind.execute(
        text(
            "SELECT owner_id, folder_path, MIN(id) AS keep_id FROM workspace_projects "
            "GROUP BY owner_id, folder_path HAVING COUNT(*) > 1"
        )
    ).fetchall()
    for owner_id, folder_path, keep_id in rows:
        bind.execute(
            text("DELETE FROM workspace_projects " "WHERE owner_id = :o AND folder_path = :p AND id != :k"),
            {"o": owner_id, "p": folder_path, "k": keep_id},
        )


def _rebuild_workspace_projects_composite(bind) -> None:
    """Rebuild workspace_projects with composite (owner_id, folder_path) unique.

    SQLite does not support DROP CONSTRAINT, so the only portable way to replace
    an unnamed inline UNIQUE constraint is a table rebuild. We do this with raw
    SQL (CREATE TABLE new / INSERT / DROP old / RENAME) rather than
    batch_alter_table because batch_alter_table copies ALL existing constraints
    (including the unnamed one) into the new table before applying batch ops,
    which would leave both constraints in place.
    """
    bind.execute(
        text(
            "CREATE TABLE _wp_new ("
            "  id INTEGER NOT NULL,"
            "  name VARCHAR NOT NULL,"
            "  folder_path VARCHAR NOT NULL,"
            "  owner_id INTEGER NOT NULL,"
            "  is_active BOOLEAN DEFAULT 0 NOT NULL,"
            "  last_synced_head_sha VARCHAR(40),"
            "  created_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL,"
            "  updated_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL,"
            "  track_data_artifacts BOOLEAN DEFAULT 1 NOT NULL,"
            "  PRIMARY KEY (id),"
            "  CONSTRAINT uq_project_owner_path UNIQUE (owner_id, folder_path),"
            "  FOREIGN KEY(owner_id) REFERENCES users (id)"
            ")"
        )
    )
    bind.execute(
        text(
            "INSERT INTO _wp_new (id, name, folder_path, owner_id, is_active,"
            "  last_synced_head_sha, created_at, updated_at, track_data_artifacts)"
            " SELECT id, name, folder_path, owner_id, is_active,"
            "  last_synced_head_sha, created_at, updated_at, track_data_artifacts"
            " FROM workspace_projects"
        )
    )
    bind.execute(text("DROP TABLE workspace_projects"))
    bind.execute(text("ALTER TABLE _wp_new RENAME TO workspace_projects"))
    # Restore the id index (was on the old table).
    bind.execute(text("CREATE INDEX ix_workspace_projects_id ON workspace_projects (id)"))


def upgrade() -> None:
    bind = op.get_bind()

    # ── 1. workspace_projects composite uniqueness ────────────────────────────
    # Dedup first so constraint creation can't fail on dirty data.
    _dedup_owner_path(bind)
    _dedup_active_projects(bind)

    if not _has_unique_constraint("workspace_projects", "uq_project_owner_path"):
        _rebuild_workspace_projects_composite(bind)

    # ── 2. Partial unique index: one active project per owner ─────────────────
    if not _has_index("workspace_projects", "ix_workspace_projects_active_owner"):
        op.execute(
            text(
                "CREATE UNIQUE INDEX ix_workspace_projects_active_owner "
                "ON workspace_projects(owner_id) WHERE is_active = 1"
            )
        )

    # ── 3. viz_uuid / dashboard_uuid NOT NULL ─────────────────────────────────
    # Migration 023 backfilled all NULL rows with fresh uuids before creating the
    # unique index, so every row already has a value; alter_column is safe here.
    for table, column in (
        ("catalog_visualizations", "viz_uuid"),
        ("catalog_dashboards", "dashboard_uuid"),
    ):
        col_info = next(
            (c for c in inspect(op.get_bind()).get_columns(table) if c["name"] == column),
            None,
        )
        if col_info is not None and col_info.get("nullable", True):
            bind.execute(
                text(f"UPDATE {table} SET {column} = :v WHERE {column} IS NULL"),
                {"v": str(uuid.uuid4())},
            )
            with op.batch_alter_table(table) as batch:
                batch.alter_column(
                    column,
                    existing_type=sa.String(length=36),
                    nullable=False,
                )


def downgrade() -> None:
    # ── 3. Revert viz/dashboard uuid back to nullable ─────────────────────────
    for table, column in (
        ("catalog_visualizations", "viz_uuid"),
        ("catalog_dashboards", "dashboard_uuid"),
    ):
        col_info = next(
            (c for c in inspect(op.get_bind()).get_columns(table) if c["name"] == column),
            None,
        )
        if col_info is not None and not col_info.get("nullable", True):
            with op.batch_alter_table(table) as batch:
                batch.alter_column(
                    column,
                    existing_type=sa.String(length=36),
                    nullable=True,
                )

    # ── 2. Drop partial active-project index ──────────────────────────────────
    if _has_index("workspace_projects", "ix_workspace_projects_active_owner"):
        op.execute(text("DROP INDEX ix_workspace_projects_active_owner"))

    # ── 1. Revert composite uniqueness back to per-path unique ────────────────
    if _has_unique_constraint("workspace_projects", "uq_project_owner_path"):
        bind = op.get_bind()
        bind.execute(
            text(
                "CREATE TABLE _wp_old ("
                "  id INTEGER NOT NULL,"
                "  name VARCHAR NOT NULL,"
                "  folder_path VARCHAR NOT NULL UNIQUE,"
                "  owner_id INTEGER NOT NULL,"
                "  is_active BOOLEAN DEFAULT 0 NOT NULL,"
                "  last_synced_head_sha VARCHAR(40),"
                "  created_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL,"
                "  updated_at DATETIME DEFAULT (CURRENT_TIMESTAMP) NOT NULL,"
                "  track_data_artifacts BOOLEAN DEFAULT 1 NOT NULL,"
                "  PRIMARY KEY (id),"
                "  FOREIGN KEY(owner_id) REFERENCES users (id)"
                ")"
            )
        )
        bind.execute(
            text(
                "INSERT INTO _wp_old (id, name, folder_path, owner_id, is_active,"
                "  last_synced_head_sha, created_at, updated_at, track_data_artifacts)"
                " SELECT id, name, folder_path, owner_id, is_active,"
                "  last_synced_head_sha, created_at, updated_at, track_data_artifacts"
                " FROM workspace_projects"
            )
        )
        bind.execute(text("DROP TABLE workspace_projects"))
        bind.execute(text("ALTER TABLE _wp_old RENAME TO workspace_projects"))
        bind.execute(text("CREATE INDEX ix_workspace_projects_id ON workspace_projects (id)"))
