"""Auth utility helpers used by CLI entry-points."""

from __future__ import annotations


def get_local_user_id() -> int:
    """Resolve the local_user's ID from the database for CLI execution."""
    try:
        from flowfile_core.database import models as db_models
        from flowfile_core.database.connection import SessionLocal

        db = SessionLocal()
        try:
            local_user = db.query(db_models.User).filter(db_models.User.username == "local_user").first()
            if local_user:
                return local_user.id
        finally:
            db.close()
    except Exception:
        pass
    return 1  # Fallback — matches jwt.py convention
