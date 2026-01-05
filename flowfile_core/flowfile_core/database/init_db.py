# Generate a random secure password and hash it
import os
import secrets
import string
import logging
from sqlalchemy.orm import Session
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import engine, SessionLocal
from flowfile_core.auth.password import get_password_hash

from passlib.context import CryptContext
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

logger = logging.getLogger(__name__)

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
            hashed_password=hashed_password
        )
        db.add(local_user)
        db.commit()
        return True
    else:
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
        logger.info(f"Admin user '{admin_username}' already exists, skipping creation.")
        return False

    # Create user with hashed password and admin privileges
    hashed_password = get_password_hash(admin_password)
    admin_user = db_models.User(
        username=admin_username,
        email=f"{admin_username}@flowfile.app",
        full_name="Admin User",
        hashed_password=hashed_password,
        is_admin=True
    )
    db.add(admin_user)
    db.commit()
    logger.info(f"Admin user '{admin_username}' created successfully.")
    return True


def init_db():
    db = SessionLocal()
    try:
        create_default_local_user(db)
        create_docker_admin_user(db)
    finally:
        db.close()


if __name__ == "__main__":
    init_db()
    print("Local user created successfully")

