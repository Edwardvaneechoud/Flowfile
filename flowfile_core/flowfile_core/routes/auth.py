# app_routes/auth.py

import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status, Request, Form
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user, create_access_token
from flowfile_core.auth.models import Token, User
from flowfile_core.auth.password import verify_password
from flowfile_core.database.connection import get_db
from flowfile_core.database import models as db_models

router = APIRouter()


@router.post("/token", response_model=Token)
async def login_for_access_token(
    request: Request,
    db: Session = Depends(get_db),
    username: Optional[str] = Form(None),
    password: Optional[str] = Form(None)
):
    # In Electron mode, auto-authenticate without requiring form data
    if os.environ.get("FLOWFILE_MODE") == "electron":
        access_token = create_access_token(data={"sub": "local_user"})
        return {"access_token": access_token, "token_type": "bearer"}
    else:
        # In Docker mode, authenticate against database
        if not username or not password:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        user = db.query(db_models.User).filter(
            db_models.User.username == username
        ).first()

        if not user or not verify_password(password, user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        access_token = create_access_token(data={"sub": user.username})
        return {"access_token": access_token, "token_type": "bearer"}


# Get current user endpoint
@router.get("/users/me", response_model=User)
async def read_users_me(current_user=Depends(get_current_active_user)):
    return current_user
