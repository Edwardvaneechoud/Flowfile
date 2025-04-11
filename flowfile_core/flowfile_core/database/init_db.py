# Generate a random secure password and hash it
import secrets
import string
from sqlalchemy.orm import Session
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import engine, SessionLocal

from passlib.context import CryptContext
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

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


def init_db():
    db = SessionLocal()
    try:
        create_default_local_user(db)
    finally:
        db.close()


if __name__ == "__main__":
    init_db()
    print("Local user created successfully")

