"""Route-level authorization helpers for mutations on (possibly shared) connections.

Policy (see auth/sharing.py): owners and admins mutate freely; manage-grantees may
edit/delete, but changing a connection's TARGET (host/endpoint/protocol fields)
while keeping the owner's bundled credential would let them harvest it by
repointing the connection at a server they control — so target changes by
non-owners require re-entering the credentials.
"""

from fastapi import HTTPException, status

from flowfile_core.auth import sharing


def authorize_connection_mutation(db, current_user, resource_type: str, db_connection) -> bool:
    """Gate update/delete on an already-resolved connection row.

    Returns True when the caller is NOT the owner (manage-grantee or admin), so
    callers know to apply the target-change rule. Unauthorized callers get the
    same 404 as a missing connection — no enumeration oracle.
    """
    owner_id = db_connection.user_id
    if owner_id == current_user.id:
        return False
    if not sharing.can_manage(db, current_user, resource_type, db_connection.id, owner_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    return True


def changed_target_fields(db_connection, incoming, fields: tuple[str, ...]) -> list[str]:
    return [field for field in fields if getattr(incoming, field, None) != getattr(db_connection, field, None)]


def require_credentials_on_target_change(
    changed_fields: list[str], has_new_credentials: bool, has_bundled_secrets: bool
) -> None:
    if changed_fields and has_bundled_secrets and not has_new_credentials:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Changing the connection target ({', '.join(changed_fields)}) on a shared "
                "connection requires re-entering the credentials"
            ),
        )
