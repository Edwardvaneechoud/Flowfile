# app_routes/auth.py

import os

from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user, create_access_token
from flowfile_core.auth.models import Token, User
from flowfile_core.database.connection import get_db

router = APIRouter()


@router.post("/token", response_model=Token)
async def login_for_access_token(request: Request, db: Session = Depends(get_db)):
    # In Electron mode, auto-authenticate without requiring form data
    if os.environ.get("FLOWFILE_MODE") == "electron" or 1 == 1:
        access_token = create_access_token(data={"sub": "local_user"})
        return {"access_token": access_token, "token_type": "bearer"}
    else:
        # In Docker mode, authenticate against database
        # Would typically process form data here
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Docker mode authentication not implemented yet",
            headers={"WWW-Authenticate": "Bearer"},
        )


# Get current user endpoint
@router.get("/users/me", response_model=User)
async def read_users_me(current_user=Depends(get_current_active_user)):
    return current_user


# Simple token endpoint for testing
@router.get("/electron-token")
async def get_electron_token():
    """Get a token without needing form data (for testing)"""
    access_token = create_access_token(data={"sub": "local_user"})
    return {"access_token": access_token, "token_type": "bearer"}


# Protected endpoint for testing
@router.get("/authenticated-endpoint")
async def authenticated_endpoint(current_user=Depends(get_current_active_user)):
    """This endpoint is protected and requires authentication"""
    print("Current user: ", current_user, type(current_user))
    return {
        "message": "You have successfully accessed a protected endpoint",
        "user": {
            "username": current_user.username if hasattr(current_user, 'username') else current_user.get("username"),
            "id": current_user.id
        }
    }
