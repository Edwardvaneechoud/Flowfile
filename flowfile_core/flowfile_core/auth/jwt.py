# jwt.py

import os
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy.orm import Session
from flowfile_core.auth.secrets import get_password, set_password

from flowfile_core.auth.models import User, TokenData
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db

router = APIRouter()

# Constants for JWT
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token", auto_error=False)


def get_jwt_secret():
    if os.environ.get("FLOWFILE_MODE") == "electron":
        key = get_password("flowfile", "jwt_secret")
        if not key:
            key = secrets.token_hex(32)
            set_password("flowfile", "jwt_secret", key)
        return key
    else:
        key = os.environ.get("JWT_SECRET_KEY")
        if not key:
            raise Exception("JWT_SECRET_KEY environment variable must be set in Docker mode")
        return key


async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    # Require token in all modes
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not token:
        raise credentials_exception

    try:
        # Decode token in all modes (Electron and Docker)
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception

    # In Electron mode, if token is valid, return default user
    if os.environ.get("FLOWFILE_MODE") == "electron":
        if token_data.username == "local_user":
            electron_user = User(username="local_user", id=1, disabled=False)
            return electron_user
        else:
            # Invalid username in token
            raise credentials_exception
    else:
        # In Docker mode, get user from database
        user = db.query(db_models.User).filter(db_models.User.username == token_data.username).first()
        if user is None:
            raise credentials_exception
        if user.disabled:
            raise HTTPException(status_code=400, detail="Inactive user")
        return user


def get_current_active_user(current_user=Depends(get_current_user)):
    if hasattr(current_user, 'disabled') and current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, get_jwt_secret(), algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user_from_query(
    access_token: str = Query(..., description="JWT access token"),
    db: Session = Depends(get_db)
):
    """
    Authenticate user using only the query parameter token.
    Specialized for log streaming where header-based auth isn't possible.
    """
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not access_token:
        raise credentials_exception

    try:
        # Decode token
        payload = jwt.decode(access_token, get_jwt_secret(), algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception

    # Handle authentication based on deployment mode (same as your existing logic)
    if os.environ.get("FLOWFILE_MODE") == "electron":
        if token_data.username == "local_user":
            electron_user = User(username="local_user", id=1, disabled=False)
            return electron_user
        else:
            raise credentials_exception
    else:
        # In Docker mode, get user from database
        user = db.query(db_models.User).filter(db_models.User.username == token_data.username).first()
        if user is None:
            raise credentials_exception
        if user.disabled:
            raise HTTPException(status_code=400, detail="Inactive user")
        return user
