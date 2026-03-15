# app_routes/auth.py

import os

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import create_access_token, get_current_active_user, get_current_admin_user
from flowfile_core.auth.models import ChangePassword, Token, User, UserCreate, UserUpdate
from flowfile_core.auth.password import PASSWORD_REQUIREMENTS, get_password_hash, validate_password, verify_password
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db

router = APIRouter()


@router.post("/token", response_model=Token)
async def login_for_access_token(
    request: Request,
    db: Session = Depends(get_db),
    username: str | None = Form(None),
    password: str | None = Form(None),
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

        user = db.query(db_models.User).filter(db_models.User.username == username).first()

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


# ============= Admin User Management Endpoints =============


@router.get("/users", response_model=list[User])
async def list_users(current_user: User = Depends(get_current_admin_user), db: Session = Depends(get_db)):
    """List all users (admin only)"""
    users = db.query(db_models.User).all()
    return [
        User(
            username=u.username,
            id=u.id,
            email=u.email,
            full_name=u.full_name,
            disabled=u.disabled,
            is_admin=u.is_admin,
            must_change_password=u.must_change_password,
        )
        for u in users
    ]


@router.post("/users", response_model=User)
async def create_user(
    user_data: UserCreate, current_user: User = Depends(get_current_admin_user), db: Session = Depends(get_db)
):
    """Create a new user (admin only)"""
    # Check if username already exists
    existing_user = db.query(db_models.User).filter(db_models.User.username == user_data.username).first()
    if existing_user:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username already exists")

    # Check if email already exists (if provided)
    if user_data.email:
        existing_email = db.query(db_models.User).filter(db_models.User.email == user_data.email).first()
        if existing_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already exists")

    # Validate password requirements
    is_valid, error_message = validate_password(user_data.password)
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)

    # Create new user with must_change_password=True
    hashed_password = get_password_hash(user_data.password)
    new_user = db_models.User(
        username=user_data.username,
        email=user_data.email or f"{user_data.username}@flowfile.app",
        full_name=user_data.full_name,
        hashed_password=hashed_password,
        is_admin=user_data.is_admin,
        must_change_password=True,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return User(
        username=new_user.username,
        id=new_user.id,
        email=new_user.email,
        full_name=new_user.full_name,
        disabled=new_user.disabled,
        is_admin=new_user.is_admin,
        must_change_password=new_user.must_change_password,
    )


@router.put("/users/{user_id}", response_model=User)
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db),
):
    """Update a user (admin only)"""
    user = db.query(db_models.User).filter(db_models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Prevent admin from disabling themselves
    if user.id == current_user.id and user_data.disabled:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot disable your own account")

    # Prevent admin from removing their own admin status
    if user.id == current_user.id and user_data.is_admin is False:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot remove your own admin privileges")

    # Update fields
    if user_data.email is not None:
        # Check if email already exists for another user
        existing_email = (
            db.query(db_models.User)
            .filter(db_models.User.email == user_data.email, db_models.User.id != user_id)
            .first()
        )
        if existing_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already exists")
        user.email = user_data.email

    if user_data.full_name is not None:
        user.full_name = user_data.full_name

    if user_data.disabled is not None:
        user.disabled = user_data.disabled

    if user_data.is_admin is not None:
        user.is_admin = user_data.is_admin

    if user_data.password is not None:
        # Validate password requirements
        is_valid, error_message = validate_password(user_data.password)
        if not is_valid:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
        user.hashed_password = get_password_hash(user_data.password)
        # Reset must_change_password when admin sets a new password
        user.must_change_password = True

    if user_data.must_change_password is not None:
        user.must_change_password = user_data.must_change_password

    db.commit()
    db.refresh(user)

    return User(
        username=user.username,
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        disabled=user.disabled,
        is_admin=user.is_admin,
        must_change_password=user.must_change_password,
    )


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int, current_user: User = Depends(get_current_admin_user), db: Session = Depends(get_db)
):
    """Delete a user (admin only)"""
    user = db.query(db_models.User).filter(db_models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Prevent admin from deleting themselves
    if user.id == current_user.id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete your own account")

    # Delete user's secrets and connections first (cascade)
    db.query(db_models.Secret).filter(db_models.Secret.user_id == user_id).delete()
    db.query(db_models.DatabaseConnection).filter(db_models.DatabaseConnection.user_id == user_id).delete()
    db.query(db_models.CloudStorageConnection).filter(db_models.CloudStorageConnection.user_id == user_id).delete()

    db.delete(user)
    db.commit()

    return {"message": f"User '{user.username}' deleted successfully"}


# ============= User Self-Service Endpoints =============


@router.post("/users/me/change-password", response_model=User)
async def change_own_password(
    password_data: ChangePassword, current_user: User = Depends(get_current_active_user), db: Session = Depends(get_db)
):
    """Change the current user's password"""
    user = db.query(db_models.User).filter(db_models.User.id == current_user.id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Verify current password
    if not verify_password(password_data.current_password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Current password is incorrect")

    # Validate new password requirements
    is_valid, error_message = validate_password(password_data.new_password)
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)

    # Update password and clear must_change_password flag
    user.hashed_password = get_password_hash(password_data.new_password)
    user.must_change_password = False
    db.commit()
    db.refresh(user)

    return User(
        username=user.username,
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        disabled=user.disabled,
        is_admin=user.is_admin,
        must_change_password=user.must_change_password,
    )


@router.get("/password-requirements")
async def get_password_requirements():
    """Get password requirements for client-side validation"""
    return PASSWORD_REQUIREMENTS
