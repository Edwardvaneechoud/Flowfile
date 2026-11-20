"""Persist successful Microsoft Graph windows without editing saved flows."""

import hashlib
import json

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration, SharePointDeltaCursor
from flowfile_core.flowfile.catalog_helpers import resolve_source_registration_id


def cursor_scope(graph, settings, wire) -> str:
    resolve_source_registration_id(graph)
    registration_id = graph.flow_settings.source_registration_id
    with get_db_context() as db:
        registration = db.get(FlowRegistration, registration_id) if registration_id else None
        if registration is None or not registration.flow_uuid:
            raise ValueError("Register this flow before using incremental SharePoint reads.")
        identity = (
            registration.flow_uuid, settings.node_id, settings.user_id, settings.connection_id,
            str(wire.tenant_id), str(wire.client_id), wire.auth_method,
            wire.drive_id, wire.file_path, settings.csv_delimiter, settings.delta_generation,
        )
        return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def load_cursor(scope: str) -> str | None:
    with get_db_context() as db:
        cursor = db.get(SharePointDeltaCursor, scope)
        return cursor.state if cursor is not None else None


def commit_cursor(scope: str, previous: str | None, state: str) -> None:
    """An overlapping run must not overwrite a newer checkpoint."""
    with get_db_context() as db:
        if previous is None:
            db.add(SharePointDeltaCursor(scope=scope, state=state))
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                if db.get(SharePointDeltaCursor, scope) is None:
                    raise
        else:
            db.execute(
                update(SharePointDeltaCursor)
                .where(SharePointDeltaCursor.scope == scope, SharePointDeltaCursor.state == previous)
                .values(state=state)
            )
            db.commit()
