"""Request/response models for user groups and resource sharing (/user-groups, /shares)."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

ResourceType = Literal[
    "secret",
    "database_connection",
    "cloud_connection",
    "ga_connection",
    "kafka_connection",
    "catalog_namespace",
    "catalog_table",
    "flow",
    "visualization",
    "dashboard",
    "global_artifact",
]
PermissionLevel = Literal["use", "manage"]
GroupRole = Literal["owner", "manager", "member"]


class AccessInfo(BaseModel):
    """How the requesting user can access a resource; attached to list/detail responses."""

    is_owner: bool
    access_level: Literal["owner", "manage", "use"]
    shared_by: str | None = None


class UserGroupCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: str | None = None


class UserGroupUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    description: str | None = None


class GroupMemberOut(BaseModel):
    user_id: int
    username: str
    full_name: str | None = None
    role: GroupRole


class UserGroupOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    created_by: int
    member_count: int = 0
    my_role: GroupRole | None = None
    created_at: datetime | None = None


class UserGroupDetail(UserGroupOut):
    members: list[GroupMemberOut] = []


class GroupMemberAdd(BaseModel):
    user_id: int
    role: GroupRole = "member"


class GroupMemberUpdate(BaseModel):
    role: GroupRole


class ShareCreate(BaseModel):
    resource_type: ResourceType
    resource_id: int
    group_id: int
    permission: PermissionLevel = "use"


class ShareUpdate(BaseModel):
    permission: PermissionLevel


class ShareOut(BaseModel):
    id: int
    resource_type: ResourceType
    resource_id: int
    group_id: int
    group_name: str
    permission: PermissionLevel
    granted_by: int | None = None
    granted_by_username: str | None = None
    created_at: datetime | None = None
