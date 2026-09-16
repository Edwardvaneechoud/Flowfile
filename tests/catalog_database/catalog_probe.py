"""Exercise startup in a fresh process so all catalog consumers use the test URL."""

# ruff: noqa: E402

import os
import sys

from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

# Import without automatic migration/seeding, then exercise the public startup path.
os.environ["FLOWFILE_SKIP_STARTUP_MIGRATION"] = "1"
os.environ["FLOWFILE_SKIP_INIT_DB"] = "1"
from alembic import command
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory

from flowfile_core.database import migration, models
from flowfile_core.database.connection import engine
from flowfile_core.database.init_db import init_db

cfg = migration._get_alembic_config()
assert not migration._catalog_db_exists()
if engine.dialect.name != "sqlite":

    def refuse_legacy_lookup():
        raise AssertionError("Server catalogs must never inspect the legacy SQLite file")

    migration._storage_config.get_legacy_database_path = refuse_legacy_lookup
    migration.migrate_data_from_legacy_db()
if sys.argv[1] == "populated":
    command.upgrade(cfg, "019")
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO users (username) VALUES ('local_user')"))
        owner = conn.scalar(text("SELECT id FROM users WHERE username = 'local_user'"))
        conn.execute(
            text("INSERT INTO catalog_namespaces (name, level, owner_id) VALUES ('General', 0, :o)"), {"o": owner}
        )
        general = conn.scalar(text("SELECT id FROM catalog_namespaces WHERE name = 'General'"))
        conn.execute(
            text("INSERT INTO catalog_namespaces (name, level, owner_id, parent_id) VALUES ('default', 1, :o, :g)"),
            {"o": owner, "g": general},
        )
    command.upgrade(cfg, "025")
    with Session(engine) as db:
        db.add_all(
            [
                models.WorkspaceProject(name=f"old-{i}", folder_path=f"/old-{i}", owner_id=owner, is_active=True)
                for i in range(2)
            ]
        )
        db.commit()
    command.upgrade(cfg, "026")
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM workspace_projects WHERE is_active = true")) == 1
        assert conn.scalar(text("SELECT COUNT(*) FROM catalog_namespaces WHERE is_public = true")) == 2
    if engine.dialect.name == "postgresql":
        with engine.begin() as conn:
            conn.execute(
                text(
                    "ALTER TABLE workspace_projects RENAME CONSTRAINT uq_project_owner_path "
                    "TO custom_owner_path_unique"
                )
            )
            conn.execute(text("CREATE INDEX workspace_projects_folder_path_key ON users (id)"))
    command.downgrade(cfg, "025")
    constraints = inspect(engine).get_unique_constraints("workspace_projects")
    assert any(c["column_names"] == ["folder_path"] for c in constraints)
    assert not any(set(c["column_names"]) == {"owner_id", "folder_path"} for c in constraints)
    command.upgrade(cfg, "026")
    constraints = inspect(engine).get_unique_constraints("workspace_projects")
    assert any(set(c["column_names"]) == {"owner_id", "folder_path"} for c in constraints)
    assert not any(c["column_names"] == ["folder_path"] for c in constraints)

migration.run_startup_migration()
init_db()
assert migration._catalog_db_exists()
with engine.connect() as conn:
    assert (
        MigrationContext.configure(conn).get_current_revision() == ScriptDirectory.from_config(cfg).get_current_head()
    )
with Session(engine) as db:
    admin = db.query(models.User).filter_by(username="catalog_admin").one()
    assert admin.is_admin
    owner = admin.id
    db.add_all(
        [
            models.WorkspaceProject(
                name=f"project-{i}", folder_path=f"/project-{i}", owner_id=owner, is_active=(i == 2)
            )
            for i in range(3)
        ]
    )
    db.commit()
    db.add(models.WorkspaceProject(name="extra-active", folder_path="/extra", owner_id=owner, is_active=True))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
    else:
        raise AssertionError("A second active project must be rejected")

# A restart must preserve the existing catalog and its seeded admin.
migration.run_startup_migration()
init_db()
with Session(engine) as db:
    assert db.query(models.User).filter_by(username="catalog_admin").count() == 1

from fastapi import FastAPI
from fastapi.testclient import TestClient

from flowfile_core.auth.jwt import get_current_admin_user
from flowfile_core.routes.system_backups import router

app = FastAPI()
app.include_router(router)
app.dependency_overrides[get_current_admin_user] = lambda: None
with TestClient(app) as client:
    listing = client.get("/db_backups")
    assert listing.status_code == 200
    result = client.post("/db_backups", json={"reason": "manual"})
    if engine.dialect.name == "sqlite":
        assert listing.json()["enabled"] is True
        assert result.status_code == 200, result.text
    else:
        assert listing.json()["enabled"] is False
        assert listing.json()["backups"] == []
        assert result.status_code == 409, result.text
        assert result.json()["detail"]["error_code"] == "BACKUPS_DISABLED"
# Verify the ORM-created partial index too (the migration index is tested above).
models.WorkspaceProject.__table__.drop(engine)
models.WorkspaceProject.__table__.create(engine)
with Session(engine) as db:
    db.add_all(
        [
            models.WorkspaceProject(name=f"model-{i}", folder_path=f"/model-{i}", owner_id=owner, is_active=(i == 2))
            for i in range(3)
        ]
    )
    db.commit()
    db.add(models.WorkspaceProject(name="model-extra", folder_path="/model-extra", owner_id=owner, is_active=True))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
    else:
        raise AssertionError("ORM index must reject a second active project")
engine.dispose()
