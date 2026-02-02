# Generate a random secure password and hash it
import logging
import os
import secrets
import string

from passlib.context import CryptContext
from sqlalchemy import text
from sqlalchemy.orm import Session

from flowfile_core.auth.password import get_password_hash
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import SessionLocal, engine

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

logger = logging.getLogger(__name__)


def run_migrations():
    """Run database migrations to update schema for existing databases."""
    with engine.connect() as conn:
        # Check if users table exists
        result = conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        ))
        if not result.fetchone():
            logger.info("Users table does not exist, will be created with new schema")
            return

        # Check existing columns
        result = conn.execute(text("PRAGMA table_info(users)"))
        columns = [row[1] for row in result.fetchall()]

        # Add is_admin column if missing
        if 'is_admin' not in columns:
            logger.info("Adding is_admin column to users table")
            conn.execute(text("ALTER TABLE users ADD COLUMN is_admin BOOLEAN DEFAULT 0"))
            conn.commit()
            logger.info("Migration complete: is_admin column added")

        # Add must_change_password column if missing
        if 'must_change_password' not in columns:
            logger.info("Adding must_change_password column to users table")
            conn.execute(text("ALTER TABLE users ADD COLUMN must_change_password BOOLEAN DEFAULT 0"))
            conn.commit()
            logger.info("Migration complete: must_change_password column added")


# Run migrations BEFORE create_all to update existing tables
run_migrations()
# Then create any new tables (this will include is_admin for new databases)
db_models.Base.metadata.create_all(bind=engine)


def create_default_local_user(db: Session):
    local_user = db.query(db_models.User).filter(db_models.User.username == "local_user").first()
    if not local_user:
        random_password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
        hashed_password = pwd_context.hash(random_password)

        local_user = db_models.User(
            username="local_user",
            email="local@flowfile.app",
            full_name="Local User",
            hashed_password=hashed_password,
            must_change_password=False  # Local user doesn't need to change password
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
    existing_user = db.query(db_models.User).filter(
        db_models.User.username == admin_username
    ).first()

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
        must_change_password=True  # Force password change on first login
    )
    db.add(admin_user)
    db.commit()
    logger.info(f"Admin user '{admin_username}' created successfully.")
    return True


def create_default_catalog_namespace(db: Session):
    """Create the default 'General' catalog with a 'user_flows' schema if they don't exist."""
    local_user = db.query(db_models.User).filter(db_models.User.username == "local_user").first()
    if not local_user:
        return

    general = (
        db.query(db_models.CatalogNamespace)
        .filter_by(name="General", parent_id=None)
        .first()
    )
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

    user_flows = (
        db.query(db_models.CatalogNamespace)
        .filter_by(name="user_flows", parent_id=general.id)
        .first()
    )
    if not user_flows:
        user_flows = db_models.CatalogNamespace(
            name="user_flows",
            parent_id=general.id,
            level=1,
            description="Default schema for user flows",
            owner_id=local_user.id,
        )
        db.add(user_flows)
        db.commit()


def init_db():
    db = SessionLocal()
    try:
        create_default_local_user(db)
        create_docker_admin_user(db)
        create_default_catalog_namespace(db)
    finally:
        db.close()


if __name__ == "__main__":
    init_db()
    print("Local user created successfully")

