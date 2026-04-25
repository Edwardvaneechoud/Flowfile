import logging
import os
import secrets
import string
from importlib.metadata import PackageNotFoundError, version

from passlib.context import CryptContext
from sqlalchemy.orm import Session

from flowfile_core.auth.password import get_password_hash
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import SessionLocal
from flowfile_core.database.migration import run_startup_migration

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Ensure a basic logging config exists so warnings emitted at import time
# (before main.py's lifespan configures logging) are visible in container logs.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

logger = logging.getLogger(__name__)

# Run Alembic-based migrations (replaces the old manual run_migrations + create_all).
# Skipped when FLOWFILE_SKIP_STARTUP_MIGRATION is set so the alembic CLI can import
# our metadata without recursively re-entering migration machinery.
if not os.environ.get("FLOWFILE_SKIP_STARTUP_MIGRATION"):
    run_startup_migration()


def create_default_local_user(db: Session):
    local_user = db.query(db_models.User).filter(db_models.User.username == "local_user").first()
    if not local_user:
        random_password = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
        hashed_password = pwd_context.hash(random_password)

        local_user = db_models.User(
            username="local_user",
            email="local@flowfile.app",
            full_name="Local User",
            hashed_password=hashed_password,
            must_change_password=False,  # Local user doesn't need to change password
        )
        db.add(local_user)
        db.commit()
        return True
    return False


def create_docker_admin_user(db: Session):
    """
    Create admin user for Docker mode from environment variables.
    Only runs when FLOWFILE_MODE=docker.
    Reads FLOWFILE_ADMIN_USER and FLOWFILE_ADMIN_PASSWORD from environment.
    """
    # Only run in Docker mode
    if os.environ.get("FLOWFILE_MODE") != "docker":
        return False

    # Read environment variables
    admin_username = os.environ.get("FLOWFILE_ADMIN_USER")
    admin_password = os.environ.get("FLOWFILE_ADMIN_PASSWORD")

    # Skip if either is not set
    if not admin_username or not admin_password:
        logger.warning(
            "Docker mode detected but FLOWFILE_ADMIN_USER or FLOWFILE_ADMIN_PASSWORD "
            "not set. Admin user will not be created."
        )
        return False

    # Check if user already exists
    existing_user = db.query(db_models.User).filter(db_models.User.username == admin_username).first()

    if existing_user:
        # Ensure existing admin user has is_admin=True
        if not existing_user.is_admin:
            existing_user.is_admin = True
            db.commit()
            logger.info(f"Admin user '{admin_username}' updated with admin privileges.")
        else:
            logger.info(f"Admin user '{admin_username}' already exists with admin privileges.")
        return False

    # Create user with hashed password and admin privileges
    hashed_password = get_password_hash(admin_password)
    admin_user = db_models.User(
        username=admin_username,
        email=f"{admin_username}@flowfile.app",
        full_name="Admin User",
        hashed_password=hashed_password,
        is_admin=True,
        must_change_password=True,  # Force password change on first login
    )
    db.add(admin_user)
    db.commit()
    logger.info(f"Admin user '{admin_username}' created successfully.")
    return True


def create_default_catalog_namespace(db: Session):
    """Create the default 'General' catalog with a 'default' schema if they don't exist."""
    local_user = db.query(db_models.User).filter(db_models.User.username == "local_user").first()
    if not local_user:
        return

    general = db.query(db_models.CatalogNamespace).filter_by(name="General", parent_id=None).first()
    if not general:
        general = db_models.CatalogNamespace(
            name="General",
            parent_id=None,
            level=0,
            description="Default catalog",
            owner_id=local_user.id,
        )
        db.add(general)
        db.commit()
        db.refresh(general)

    default_schema = db.query(db_models.CatalogNamespace).filter_by(name="default", parent_id=general.id).first()
    if not default_schema:
        default_schema = db_models.CatalogNamespace(
            name="default",
            parent_id=general.id,
            level=1,
            description="Default schema for user flows",
            owner_id=local_user.id,
        )
        db.add(default_schema)
        db.commit()

    # Dedicated schema for quick-created / unnamed flows so they don't clutter 'default'
    unnamed_schema = (
        db.query(db_models.CatalogNamespace).filter_by(name="Unnamed Flows", parent_id=general.id).first()
    )
    if not unnamed_schema:
        unnamed_schema = db_models.CatalogNamespace(
            name="Unnamed Flows",
            parent_id=general.id,
            level=1,
            description="Quick-created flows that have not yet been named",
            owner_id=local_user.id,
        )
        db.add(unnamed_schema)
        db.commit()

    # Dedicated schema for flows saved to arbitrary disk locations, distinct from
    # catalog-managed flows so users can tell disk-backed and catalog flows apart.
    local_schema = (
        db.query(db_models.CatalogNamespace).filter_by(name="Local Flows", parent_id=general.id).first()
    )
    if not local_schema:
        local_schema = db_models.CatalogNamespace(
            name="Local Flows",
            parent_id=general.id,
            level=1,
            description="Flows saved to disk at user-chosen paths",
            owner_id=local_user.id,
        )
        db.add(local_schema)
        db.commit()


def update_db_info(db: Session):
    """Upsert the application version into the db_info table."""
    try:
        app_version = version("Flowfile")
    except PackageNotFoundError:
        app_version = "unknown"

    row = db.query(db_models.DbInfo).filter(db_models.DbInfo.id == 1).first()
    if row:
        row.app_version = app_version
    else:
        db.add(db_models.DbInfo(id=1, app_version=app_version))
    db.commit()
    logger.info("Database info updated: app_version=%s", app_version)


def init_db():
    db = SessionLocal()
    try:
        create_default_local_user(db)
        create_docker_admin_user(db)
        create_default_catalog_namespace(db)
        update_db_info(db)
    finally:
        db.close()


if __name__ == "__main__":
    init_db()
    print("Local user created successfully")
