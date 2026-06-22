"""WorkspaceProject row access. One active project per owner."""

from __future__ import annotations

from sqlalchemy.orm import Session

from flowfile_core.database.models import WorkspaceProject


def owned_or_none(db: Session, model: type, owner_col: str, owner_id: int, **filters):
    """Return the single row matching **filters that is owned by owner_id, else None.

    The one owner-scoped lookup primitive for the importer. owner_col is the model's owner
    column NAME ("owner_id" | "user_id" | "created_by") — see the owner-column map in
    SECURITY_REVIEW_status.md. A row owned by another user is invisible (returns None), which
    is what makes a cross-owner global-id collision look like 'not found' to the caller, so the
    caller mints a fresh id / creates-new instead of touching another tenant's row.
    """
    return db.query(model).filter_by(**filters).filter(getattr(model, owner_col) == owner_id).first()


def get_active_projects(db: Session) -> list[WorkspaceProject]:
    return db.query(WorkspaceProject).filter(WorkspaceProject.is_active.is_(True)).all()


def get_by_path(db: Session, folder_path: str) -> WorkspaceProject | None:
    return db.query(WorkspaceProject).filter(WorkspaceProject.folder_path == folder_path).first()


def upsert_active(
    db: Session, name: str, folder_path: str, owner_id: int, track_data_artifacts: bool = True
) -> WorkspaceProject:
    """Register/activate a project, deactivating the owner's other projects."""
    for p in db.query(WorkspaceProject).filter(WorkspaceProject.owner_id == owner_id).all():
        p.is_active = False
    # Owner-scoped probe: the update branch only ever touches the caller's OWN row. A foreign-owned
    # path is stopped by the cross-owner 403 in the service layer before reaching here (defense-in-depth).
    proj = db.query(WorkspaceProject).filter_by(folder_path=folder_path, owner_id=owner_id).first()
    if proj is None:
        proj = WorkspaceProject(
            name=name,
            folder_path=folder_path,
            owner_id=owner_id,
            is_active=True,
            track_data_artifacts=track_data_artifacts,
        )
        db.add(proj)
    else:
        proj.name = name
        proj.is_active = True
        proj.track_data_artifacts = track_data_artifacts
    db.commit()
    db.refresh(proj)
    return proj


def set_head_sha(db: Session, project_id: int, sha: str | None) -> None:
    proj = db.query(WorkspaceProject).filter(WorkspaceProject.id == project_id).first()
    if proj is not None:
        proj.last_synced_head_sha = sha
        db.commit()


def set_track_data_artifacts(db: Session, project_id: int, value: bool) -> None:
    proj = db.query(WorkspaceProject).filter(WorkspaceProject.id == project_id).first()
    if proj is not None:
        proj.track_data_artifacts = value
        db.commit()


def deactivate_owner(db: Session, owner_id: int) -> None:
    for p in get_active_projects(db):
        if p.owner_id == owner_id:
            p.is_active = False
    db.commit()
