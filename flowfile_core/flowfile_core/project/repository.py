"""WorkspaceProject row access. One active project per owner."""

from __future__ import annotations

from sqlalchemy.orm import Session

from flowfile_core.database.models import WorkspaceProject


def get_active_projects(db: Session) -> list[WorkspaceProject]:
    return db.query(WorkspaceProject).filter(WorkspaceProject.is_active.is_(True)).all()


def get_by_path(db: Session, folder_path: str) -> WorkspaceProject | None:
    return db.query(WorkspaceProject).filter(WorkspaceProject.folder_path == folder_path).first()


def upsert_active(db: Session, name: str, folder_path: str, owner_id: int) -> WorkspaceProject:
    """Register/activate a project, deactivating the owner's other projects."""
    for p in db.query(WorkspaceProject).filter(WorkspaceProject.owner_id == owner_id).all():
        p.is_active = False
    proj = get_by_path(db, folder_path)
    if proj is None:
        proj = WorkspaceProject(name=name, folder_path=folder_path, owner_id=owner_id, is_active=True)
        db.add(proj)
    else:
        proj.name = name
        proj.owner_id = owner_id
        proj.is_active = True
    db.commit()
    db.refresh(proj)
    return proj


def set_head_sha(db: Session, project_id: int, sha: str | None) -> None:
    proj = db.query(WorkspaceProject).filter(WorkspaceProject.id == project_id).first()
    if proj is not None:
        proj.last_synced_head_sha = sha
        db.commit()


def deactivate_owner(db: Session, owner_id: int) -> None:
    for p in get_active_projects(db):
        if p.owner_id == owner_id:
            p.is_active = False
    db.commit()
