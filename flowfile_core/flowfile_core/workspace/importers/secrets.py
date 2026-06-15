"""Refill secret values on apply (re-encrypted under the local user's key).

Reads the secret manifest (names only), resolves each value from env / ``.env``,
and upserts a ``Secret`` row. A name already present in the DB with no env value
is treated as already-satisfied (we don't overwrite an existing local value);
a name with neither is reported as missing -- non-fatal, per the plan.
"""

from __future__ import annotations

from pydantic import SecretStr
from sqlalchemy.orm import Session

from flowfile_core.auth.models import SecretInput
from flowfile_core.database.models import Secret
from flowfile_core.secret_manager.secret_manager import encrypt_secret, store_secret
from flowfile_core.workspace.layout import ProjectLayout
from flowfile_core.workspace.models import SecretRequirement
from flowfile_core.workspace.normalize import canonical_yaml_load
from flowfile_core.workspace.secret_resolver import SecretResolver


def _existing_secret(db: Session, user_id: int, name: str) -> Secret | None:
    return (
        db.query(Secret)
        .filter(Secret.user_id == user_id, Secret.name == name)
        .order_by(Secret.id.asc())
        .first()
    )


def import_secrets(
    db: Session, user_id: int, layout: ProjectLayout, resolver: SecretResolver
) -> tuple[list[SecretRequirement], list[SecretRequirement]]:
    """Return ``(resolved, missing)`` lists of :class:`SecretRequirement`."""
    resolved: list[SecretRequirement] = []
    missing: list[SecretRequirement] = []

    manifest_path = layout.secrets_manifest_path
    if not manifest_path.exists():
        return resolved, missing

    data = canonical_yaml_load(manifest_path.read_text(encoding="utf-8")) or {}
    for entry in data.get("secrets", []) or []:
        name = entry.get("name")
        if not name:
            continue
        required_by = entry.get("required_by", []) or []
        value, source = resolver.resolve(name)
        existing = _existing_secret(db, user_id, name)

        if value is not None:
            if existing is not None:
                existing.encrypted_value = encrypt_secret(value, user_id)
            else:
                store_secret(db, SecretInput(name=name, value=SecretStr(value)), user_id)
            resolved.append(SecretRequirement(name=name, required_by=required_by, resolved=True, source=source))
        elif existing is not None:
            resolved.append(
                SecretRequirement(name=name, required_by=required_by, resolved=True, source="existing")
            )
        else:
            missing.append(SecretRequirement(name=name, required_by=required_by, resolved=False))

    db.commit()
    return resolved, missing
