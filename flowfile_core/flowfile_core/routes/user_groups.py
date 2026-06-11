"""User-group management endpoints (multi-user mode only).

Groups are the principals resources get shared with (see routes/shares.py).
Only global admins create groups; each group then runs itself via member roles
(owner/manager). The whole router 404s in electron mode — the feature does not
exist on desktop. Not to be confused with node-canvas groups (/editor/create_group/).
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func
from sqlalchemy.orm import Session

from flowfile_core.auth import sharing
from flowfile_core.auth.jwt import get_current_active_user, get_current_admin_user
from flowfile_core.auth.models import User
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.schemas.sharing_schema import (
    GroupMemberAdd,
    GroupMemberOut,
    GroupMemberUpdate,
    UserGroupCreate,
    UserGroupDetail,
    UserGroupOut,
    UserGroupUpdate,
)


def require_sharing_enabled() -> None:
    """404 (not 403/503): in single-user mode the feature does not exist at all."""
    if not sharing.sharing_enabled():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")


router = APIRouter(dependencies=[Depends(get_current_active_user), Depends(require_sharing_enabled)])


def _get_group_or_404(db: Session, group_id: int) -> db_models.UserGroup:
    group = db.get(db_models.UserGroup, group_id)
    if group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Group not found")
    return group


def _members_of(db: Session, group_id: int) -> list[tuple[db_models.UserGroupMembership, db_models.User]]:
    return (
        db.query(db_models.UserGroupMembership, db_models.User)
        .join(db_models.User, db_models.User.id == db_models.UserGroupMembership.user_id)
        .filter(db_models.UserGroupMembership.group_id == group_id)
        .all()
    )


def _groups_out(db: Session, groups: list[db_models.UserGroup], user_id: int) -> list[UserGroupOut]:
    """Build the DTOs with one member-count query and one my-role query for the whole list."""
    gids = [g.id for g in groups]
    counts: dict[int, int] = {}
    roles: dict[int, str] = {}
    if gids:
        counts = dict(
            db.query(db_models.UserGroupMembership.group_id, func.count())
            .filter(db_models.UserGroupMembership.group_id.in_(gids))
            .group_by(db_models.UserGroupMembership.group_id)
        )
        roles = dict(
            db.query(db_models.UserGroupMembership.group_id, db_models.UserGroupMembership.role).filter(
                db_models.UserGroupMembership.group_id.in_(gids),
                db_models.UserGroupMembership.user_id == user_id,
            )
        )
    return [
        UserGroupOut(
            id=g.id,
            name=g.name,
            description=g.description,
            created_by=g.created_by,
            member_count=counts.get(g.id, 0),
            my_role=roles.get(g.id),
            created_at=g.created_at,
        )
        for g in groups
    ]


def _group_out(db: Session, group: db_models.UserGroup, user_id: int) -> UserGroupOut:
    return _groups_out(db, [group], user_id)[0]


def _require_group_admin(db: Session, group: db_models.UserGroup, user: User, owner_only: bool = False) -> None:
    """Group administration check: global admin, or group owner (and manager unless owner_only)."""
    if user.is_admin:
        return
    role = sharing.group_role(db, group.id, user.id)
    allowed = (sharing.ROLE_OWNER,) if owner_only else (sharing.ROLE_OWNER, sharing.ROLE_MANAGER)
    if role not in allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not allowed to manage this group")


def _owner_count(db: Session, group_id: int) -> int:
    return (
        db.query(db_models.UserGroupMembership)
        .filter(
            db_models.UserGroupMembership.group_id == group_id,
            db_models.UserGroupMembership.role == sharing.ROLE_OWNER,
        )
        .count()
    )


@router.post("", response_model=UserGroupOut)
def create_group(
    group_data: UserGroupCreate,
    current_user: User = Depends(get_current_admin_user),
    db: Session = Depends(get_db),
):
    """Create a group (admin only). The creator is auto-added as group owner."""
    existing = db.query(db_models.UserGroup).filter(db_models.UserGroup.name == group_data.name).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Group name already exists")
    group = db_models.UserGroup(
        name=group_data.name,
        description=group_data.description,
        created_by=current_user.id,
    )
    db.add(group)
    db.commit()
    db.refresh(group)
    db.add(db_models.UserGroupMembership(group_id=group.id, user_id=current_user.id, role=sharing.ROLE_OWNER))
    db.commit()
    return _group_out(db, group, current_user.id)


@router.get("", response_model=list[UserGroupOut])
def list_groups(
    all: bool = False,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Groups the user belongs to; ``?all=true`` lists every group (admin only)."""
    if all:
        if not current_user.is_admin:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required")
        groups = db.query(db_models.UserGroup).order_by(db_models.UserGroup.name).all()
    else:
        gids = sharing.user_group_ids(db, current_user.id)
        groups = (
            db.query(db_models.UserGroup)
            .filter(db_models.UserGroup.id.in_(gids))
            .order_by(db_models.UserGroup.name)
            .all()
            if gids
            else []
        )
    return _groups_out(db, groups, current_user.id)


@router.get("/{group_id}", response_model=UserGroupDetail)
def get_group(
    group_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    group = _get_group_or_404(db, group_id)
    role = sharing.group_role(db, group_id, current_user.id)
    if role is None and not current_user.is_admin:
        # Uniform with the missing-group response: membership is not probeable.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Group not found")
    members = [
        GroupMemberOut(user_id=user.id, username=user.username, full_name=user.full_name, role=membership.role)
        for membership, user in _members_of(db, group_id)
    ]
    base = _group_out(db, group, current_user.id)
    return UserGroupDetail(**base.model_dump(), members=members)


@router.patch("/{group_id}", response_model=UserGroupOut)
def update_group(
    group_id: int,
    group_data: UserGroupUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    group = _get_group_or_404(db, group_id)
    _require_group_admin(db, group, current_user, owner_only=True)
    if group_data.name is not None and group_data.name != group.name:
        existing = db.query(db_models.UserGroup).filter(db_models.UserGroup.name == group_data.name).first()
        if existing:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Group name already exists")
        group.name = group_data.name
    if group_data.description is not None:
        group.description = group_data.description
    db.commit()
    db.refresh(group)
    return _group_out(db, group, current_user.id)


@router.delete("/{group_id}", status_code=204)
def delete_group(
    group_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    group = _get_group_or_404(db, group_id)
    _require_group_admin(db, group, current_user, owner_only=True)
    # Explicit app-level cascade (SQLite FK enforcement is off).
    sharing.delete_grants_for_group(db, group_id)
    sharing.delete_memberships_for_group(db, group_id)
    db.delete(group)
    db.commit()


@router.post("/{group_id}/members", response_model=GroupMemberOut)
def add_member(
    group_id: int,
    member_data: GroupMemberAdd,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    group = _get_group_or_404(db, group_id)
    _require_group_admin(db, group, current_user, owner_only=(member_data.role == sharing.ROLE_OWNER))
    user = db.get(db_models.User, member_data.user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if sharing.group_role(db, group_id, member_data.user_id) is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User is already a member")
    db.add(db_models.UserGroupMembership(group_id=group_id, user_id=member_data.user_id, role=member_data.role))
    db.commit()
    return GroupMemberOut(user_id=user.id, username=user.username, full_name=user.full_name, role=member_data.role)


@router.patch("/{group_id}/members/{user_id}", response_model=GroupMemberOut)
def update_member(
    group_id: int,
    user_id: int,
    member_data: GroupMemberUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    group = _get_group_or_404(db, group_id)
    membership = (
        db.query(db_models.UserGroupMembership)
        .filter(
            db_models.UserGroupMembership.group_id == group_id,
            db_models.UserGroupMembership.user_id == user_id,
        )
        .first()
    )
    if membership is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Member not found")
    # Touching an owner membership, or granting owner, is owner/admin territory.
    owner_involved = sharing.ROLE_OWNER in (membership.role, member_data.role)
    _require_group_admin(db, group, current_user, owner_only=owner_involved)
    if (
        membership.role == sharing.ROLE_OWNER
        and member_data.role != sharing.ROLE_OWNER
        and _owner_count(db, group_id) <= 1
    ):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot demote the last group owner")
    membership.role = member_data.role
    db.commit()
    user = db.get(db_models.User, user_id)
    return GroupMemberOut(
        user_id=user_id,
        username=user.username if user else str(user_id),
        full_name=user.full_name if user else None,
        role=member_data.role,
    )


@router.delete("/{group_id}/members/{user_id}", status_code=204)
def remove_member(
    group_id: int,
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    group = _get_group_or_404(db, group_id)
    membership = (
        db.query(db_models.UserGroupMembership)
        .filter(
            db_models.UserGroupMembership.group_id == group_id,
            db_models.UserGroupMembership.user_id == user_id,
        )
        .first()
    )
    if membership is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Member not found")
    if user_id != current_user.id:  # leaving a group yourself needs no role
        _require_group_admin(db, group, current_user, owner_only=(membership.role == sharing.ROLE_OWNER))
    if membership.role == sharing.ROLE_OWNER and _owner_count(db, group_id) <= 1:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot remove the last group owner")
    db.delete(membership)
    db.commit()
