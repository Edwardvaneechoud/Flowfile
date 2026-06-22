# app_routes/auth.py

import os

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from sqlalchemy.orm import Session

from flowfile_core.auth import sharing
from flowfile_core.auth.jwt import (
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    get_current_active_user,
    get_current_admin_user,
)
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
        refresh_token = create_refresh_token(data={"sub": user.username})
        return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}


@router.post("/refresh", response_model=Token)
async def refresh_access_token(
    refresh_token: str = Form(...),
    db: Session = Depends(get_db),
):
    """Exchange a valid refresh token for a new access token and refresh token."""
    username = decode_refresh_token(refresh_token)

    user = db.query(db_models.User).filter(db_models.User.username == username).first()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User no longer exists",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if user.disabled:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is disabled",
            headers={"WWW-Authenticate": "Bearer"},
        )

    new_access_token = create_access_token(data={"sub": user.username})
    new_refresh_token = create_refresh_token(data={"sub": user.username})
    return {"access_token": new_access_token, "refresh_token": new_refresh_token, "token_type": "bearer"}


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
    if user_data.username == sharing.INTERNAL_SERVICE_USERNAME:
        # Reserved sentinel: sharing classifies principals as the synthetic kernel
        # identity by this username, so a real user named this way would be locked
        # out of their own resources.
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username is reserved")
    existing_user = db.query(db_models.User).filter(db_models.User.username == user_data.username).first()
    if existing_user:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username already exists")

    if user_data.email:
        existing_email = db.query(db_models.User).filter(db_models.User.email == user_data.email).first()
        if existing_email:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already exists")

    is_valid, error_message = validate_password(user_data.password)
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)

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

    if user_data.email is not None:
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
        is_valid, error_message = validate_password(user_data.password)
        if not is_valid:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
        user.hashed_password = get_password_hash(user_data.password)
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

    # Personal credential rows — everything keyed by user_id in the sharing
    # registry: secrets + all connection types — die with the user, including any
    # sharing grants on them: SQLite reuses rowids, so a stale grant would attach
    # to a future resource created with the same id. Catalog content
    # (owner_id/created_by keyed) is deliberately retained, so its grants stay
    # meaningful. Groups the user ran persist; global admins can administer them.
    for resource_type, spec in sharing.RESOURCE_REGISTRY.items():
        if spec.owner_attr != "user_id":
            continue
        resource_ids = [row[0] for row in db.query(spec.model.id).filter(spec.model.user_id == user_id)]
        if not resource_ids:
            continue
        db.query(db_models.ResourceGrant).filter(
            db_models.ResourceGrant.resource_type == resource_type,
            db_models.ResourceGrant.resource_id.in_(resource_ids),
        ).delete(synchronize_session=False)
        db.query(spec.model).filter(spec.model.id.in_(resource_ids)).delete(synchronize_session=False)
    sharing.delete_memberships_for_user(db, user_id)

    # workspace_projects is not share-registered, so plain row deletion suffices.
    # Removing these prevents a future user receiving the same rowid from inheriting
    # an orphan is_active=True project (rowid reuse; SQLite FK enforcement is off).
    db.query(db_models.WorkspaceProject).filter(db_models.WorkspaceProject.owner_id == user_id).delete(
        synchronize_session=False
    )

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

    if not verify_password(password_data.current_password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Current password is incorrect")

    is_valid, error_message = validate_password(password_data.new_password)
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)

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
