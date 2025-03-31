# jwt.py

import os
import secrets
from datetime import datetime, timedelta
from typing import Optional

import keyring
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy.orm import Session

from flowfile_core.auth.models import User, TokenData
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db

router = APIRouter()

# Constants for JWT
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
CREDENTIALS_EXCEPTION = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token", auto_error=False)


def get_jwt_secret():
    if os.environ.get("FLOWFILE_MODE") == "electron" or 1 == 1:
        key = keyring.get_password("flowfile", "jwt_secret")
        if not key:
            key = secrets.token_hex(32)
            keyring.set_password("flowfile", "jwt_secret", key)
        return key
    else:
        key = os.environ.get("JWT_SECRET_KEY")
        if not key:
            raise Exception("JWT_SECRET_KEY environment variable must be set in Docker mode")
        return key


async def get_server_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):

    if not token:
        raise CREDENTIALS_EXCEPTION

    try:
        # Decode token
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise CREDENTIALS_EXCEPTION
        token_data = TokenData(username=username)
    except JWTError:
        raise CREDENTIALS_EXCEPTION

    # Get user from database
    user = db.query(db_models.User).filter(db_models.User.username == token_data.username).first()
    if user is None:
        raise CREDENTIALS_EXCEPTION
    if user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")

    return user


async def get_electron_user(token: str = Depends(oauth2_scheme)):
    if not token:
        raise CREDENTIALS_EXCEPTION

    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None or username != "local_user":
            raise CREDENTIALS_EXCEPTION
        return User(username="local_user", id=1, disabled=False)
    except JWTError:
        raise CREDENTIALS_EXCEPTION


def get_current_user():
    if os.environ.get("FLOWFILE_MODE") == "electron":
        return Depends(get_electron_user)
    else:
        return Depends(get_server_user)


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

