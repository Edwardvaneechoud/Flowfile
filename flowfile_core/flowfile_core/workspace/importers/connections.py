"""Recreate connection rows from the tree, linking already-imported secrets.

Connections are upserted by ``(connection_name, user_id)``. Secret foreign keys
are resolved from ``${secret:NAME}`` placeholders to the ``Secret.id`` that the
secrets importer created (run earlier in apply). We never create or mutate secret
rows here -- a missing value leaves the FK unchanged (update) or null (create).
"""

from __future__ import annotations

import json
import logging

from sqlalchemy.orm import Session

from flowfile_core.database.models import (
    CloudStorageConnection,
    DatabaseConnection,
    GoogleAnalyticsConnection,
    KafkaConnection,
    Secret,
)
from flowfile_core.workspace.layout import ProjectLayout
from flowfile_core.workspace.normalize import canonical_yaml_load
from flowfile_core.workspace.secret_resolver import parse_placeholder

logger = logging.getLogger(__name__)

_CLOUD_SECRET_FIELDS = {
    "aws_secret_access_key": "aws_secret_access_key_id",
    "aws_session_token": "aws_session_token_id",
    "azure_account_key": "azure_account_key_id",
    "azure_client_secret": "azure_client_secret_id",
    "azure_sas_token": "azure_sas_token_id",
    "gcs_service_account_key": "gcs_service_account_key_id",
}


class _ImportStats:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.warnings: list[str] = []

    def bump(self, kind: str) -> None:
        self.counts[kind] = self.counts.get(kind, 0) + 1


def _resolve_secret_id(db: Session, user_id: int, placeholder: str | None) -> int | None:
    name = parse_placeholder(placeholder)
    if not name:
        return None
    row = (
        db.query(Secret)
        .filter(Secret.user_id == user_id, Secret.name == name)
        .order_by(Secret.id.asc())
        .first()
    )
    return row.id if row else None


def _dump_extra_config(value) -> str | None:
    if value in (None, {}, ""):
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


def import_connections(db: Session, user_id: int, layout: ProjectLayout) -> _ImportStats:
    stats = _ImportStats()
    for path in layout.iter_connection_files():
        doc = canonical_yaml_load(path.read_text(encoding="utf-8")) or {}
        kind = doc.get("kind")
        rel = layout.rel(path)
        try:
            if kind == "database_connection":
                _upsert_database(db, user_id, doc, stats)
            elif kind == "cloud_connection":
                _upsert_cloud(db, user_id, doc, stats)
            elif kind == "ga_connection":
                _upsert_ga(db, user_id, doc, rel, stats)
            elif kind == "kafka_connection":
                _upsert_kafka(db, user_id, doc, stats)
            else:
                stats.warnings.append(f"{rel}: unknown connection kind '{kind}'; skipped")
        except Exception as exc:  # noqa: BLE001 - one bad file must not abort apply
            stats.warnings.append(f"{rel}: failed to import ({exc}); skipped")
    db.commit()
    return stats


def _existing(db: Session, model, user_id: int, name: str):
    return (
        db.query(model)
        .filter(model.connection_name == name, model.user_id == user_id)
        .order_by(model.id.asc())
        .first()
    )


def _link(resolved_id: int | None, existing_id: int | None) -> int | None:
    """Prefer a freshly-resolved secret; otherwise keep what the row already had."""
    return resolved_id if resolved_id is not None else existing_id


def _upsert_database(db: Session, user_id: int, doc: dict, stats: _ImportStats) -> None:
    name = doc["connection_name"]
    row = _existing(db, DatabaseConnection, user_id, name)
    pw_id = _link(_resolve_secret_id(db, user_id, doc.get("password")), row.password_id if row else None)
    fields = dict(
        database_type=doc.get("database_type", "postgresql"),
        username=doc.get("username"),
        host=doc.get("host"),
        port=doc.get("port"),
        database=doc.get("database"),
        ssl_enabled=doc.get("ssl_enabled", False),
        password_id=pw_id,
    )
    if row is None:
        row = DatabaseConnection(connection_name=name, user_id=user_id, **fields)
        db.add(row)
    else:
        for key, value in fields.items():
            setattr(row, key, value)
    stats.bump("database_connection")


def _upsert_cloud(db: Session, user_id: int, doc: dict, stats: _ImportStats) -> None:
    name = doc["connection_name"]
    row = _existing(db, CloudStorageConnection, user_id, name)
    fields = dict(
        storage_type=doc.get("storage_type"),
        auth_method=doc.get("auth_method"),
        aws_region=doc.get("aws_region"),
        aws_access_key_id=doc.get("aws_access_key_id"),
        aws_role_arn=doc.get("aws_role_arn"),
        aws_allow_unsafe_html=doc.get("aws_allow_unsafe_html"),
        azure_account_name=doc.get("azure_account_name"),
        azure_tenant_id=doc.get("azure_tenant_id"),
        azure_client_id=doc.get("azure_client_id"),
        gcs_project_id=doc.get("gcs_project_id"),
        endpoint_url=doc.get("endpoint_url"),
        verify_ssl=doc.get("verify_ssl", True),
        extra_config=_dump_extra_config(doc.get("extra_config")),
    )
    for doc_field, column in _CLOUD_SECRET_FIELDS.items():
        fields[column] = _link(
            _resolve_secret_id(db, user_id, doc.get(doc_field)),
            getattr(row, column) if row else None,
        )
    if row is None:
        row = CloudStorageConnection(connection_name=name, user_id=user_id, **fields)
        db.add(row)
    else:
        for key, value in fields.items():
            setattr(row, key, value)
    stats.bump("cloud_connection")


def _upsert_ga(db: Session, user_id: int, doc: dict, rel: str, stats: _ImportStats) -> None:
    name = doc["connection_name"]
    row = _existing(db, GoogleAnalyticsConnection, user_id, name)
    cred_id = _link(
        _resolve_secret_id(db, user_id, doc.get("credential")),
        row.credential_secret_id if row else None,
    )
    if cred_id is None:
        # credential_secret_id is NOT NULL -- can't create without a value.
        stats.warnings.append(f"{rel}: GA credential secret unresolved; connection skipped")
        return
    fields = dict(
        description=doc.get("description"),
        default_property_id=doc.get("default_property_id"),
        auth_method=doc.get("auth_method", "oauth"),
        oauth_user_email=doc.get("oauth_user_email"),
        credential_secret_id=cred_id,
    )
    if row is None:
        row = GoogleAnalyticsConnection(connection_name=name, user_id=user_id, **fields)
        db.add(row)
    else:
        for key, value in fields.items():
            setattr(row, key, value)
    stats.bump("ga_connection")


def _upsert_kafka(db: Session, user_id: int, doc: dict, stats: _ImportStats) -> None:
    name = doc["connection_name"]
    row = _existing(db, KafkaConnection, user_id, name)
    fields = dict(
        bootstrap_servers=doc.get("bootstrap_servers"),
        security_protocol=doc.get("security_protocol", "PLAINTEXT"),
        sasl_mechanism=doc.get("sasl_mechanism"),
        sasl_username=doc.get("sasl_username"),
        ssl_ca_location=doc.get("ssl_ca_location"),
        ssl_cert_location=doc.get("ssl_cert_location"),
        schema_registry_url=doc.get("schema_registry_url"),
        extra_config=_dump_extra_config(doc.get("extra_config")),
        sasl_password_id=_link(
            _resolve_secret_id(db, user_id, doc.get("sasl_password")),
            row.sasl_password_id if row else None,
        ),
        ssl_key_id=_link(
            _resolve_secret_id(db, user_id, doc.get("ssl_key")),
            row.ssl_key_id if row else None,
        ),
    )
    if row is None:
        row = KafkaConnection(connection_name=name, user_id=user_id, **fields)
        db.add(row)
    else:
        for key, value in fields.items():
            setattr(row, key, value)
    stats.bump("kafka_connection")
