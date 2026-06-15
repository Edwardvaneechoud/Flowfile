"""Export database / cloud / Google Analytics / Kafka connections.

Connection definitions are secret-free: every secret foreign key is resolved to
the secret *name* and emitted as a ``${secret:NAME}`` placeholder. The secret
value never leaves the DB.
"""

from __future__ import annotations

import json

from sqlalchemy.orm import Session

from flowfile_core.database.models import (
    CloudStorageConnection,
    DatabaseConnection,
    GoogleAnalyticsConnection,
    KafkaConnection,
    Secret,
)
from flowfile_core.workspace.exporters import ExportBundle, drop_none
from flowfile_core.workspace.layout import ProjectLayout
from flowfile_core.workspace.normalize import canonical_yaml_dump
from flowfile_core.workspace.secret_resolver import make_placeholder

# (db column holding the secret FK, exported field name) per connection type.
_CLOUD_SECRET_FIELDS = [
    ("aws_secret_access_key_id", "aws_secret_access_key"),
    ("aws_session_token_id", "aws_session_token"),
    ("azure_account_key_id", "azure_account_key"),
    ("azure_client_secret_id", "azure_client_secret"),
    ("azure_sas_token_id", "azure_sas_token"),
    ("gcs_service_account_key_id", "gcs_service_account_key"),
]


def _secret_name(db: Session, secret_id: int | None) -> str | None:
    if secret_id is None:
        return None
    row = db.query(Secret).filter(Secret.id == secret_id).first()
    return row.name if row else None


def _parse_extra_config(raw: str | None) -> dict | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    return parsed or None


def _add_secret_ref(
    bundle: ExportBundle, db: Session, secret_id: int | None, rel_path: str, field_label: str
) -> str | None:
    """Resolve a secret FK to a placeholder, recording the requirement."""
    name = _secret_name(db, secret_id)
    if name is None:
        if secret_id is not None:
            bundle.warnings.append(
                f"{rel_path}: secret reference '{field_label}' (id={secret_id}) is dangling; omitted"
            )
        return None
    bundle.secret_refs.setdefault(name, []).append(rel_path)
    return make_placeholder(name)


def export_connections(db: Session, user_id: int, layout: ProjectLayout) -> ExportBundle:
    bundle = ExportBundle()
    _export_database(db, user_id, layout, bundle)
    _export_cloud(db, user_id, layout, bundle)
    _export_ga(db, user_id, layout, bundle)
    _export_kafka(db, user_id, layout, bundle)
    return bundle


def _export_database(db: Session, user_id: int, layout: ProjectLayout, bundle: ExportBundle) -> None:
    rows = db.query(DatabaseConnection).filter(DatabaseConnection.user_id == user_id).all()
    for row in rows:
        rel = layout.rel(layout.connection_path("database_connection", row.connection_name))
        doc = drop_none(
            {
                "kind": "database_connection",
                "connection_name": row.connection_name,
                "database_type": row.database_type,
                "username": row.username,
                "host": row.host,
                "port": row.port,
                "database": row.database,
                "ssl_enabled": row.ssl_enabled,
                "password": _add_secret_ref(bundle, db, row.password_id, rel, "password"),
            }
        )
        bundle.artifacts[rel] = canonical_yaml_dump(doc)


def _export_cloud(db: Session, user_id: int, layout: ProjectLayout, bundle: ExportBundle) -> None:
    rows = db.query(CloudStorageConnection).filter(CloudStorageConnection.user_id == user_id).all()
    for row in rows:
        rel = layout.rel(layout.connection_path("cloud_connection", row.connection_name))
        doc = {
            "kind": "cloud_connection",
            "connection_name": row.connection_name,
            "storage_type": row.storage_type,
            "auth_method": row.auth_method,
            "aws_region": row.aws_region,
            "aws_access_key_id": row.aws_access_key_id,
            "aws_role_arn": row.aws_role_arn,
            "aws_allow_unsafe_html": row.aws_allow_unsafe_html,
            "azure_account_name": row.azure_account_name,
            "azure_tenant_id": row.azure_tenant_id,
            "azure_client_id": row.azure_client_id,
            "gcs_project_id": row.gcs_project_id,
            "endpoint_url": row.endpoint_url,
            "verify_ssl": row.verify_ssl,
            "extra_config": _parse_extra_config(row.extra_config),
        }
        for column, field_name in _CLOUD_SECRET_FIELDS:
            doc[field_name] = _add_secret_ref(bundle, db, getattr(row, column), rel, field_name)
        bundle.artifacts[rel] = canonical_yaml_dump(drop_none(doc))


def _export_ga(db: Session, user_id: int, layout: ProjectLayout, bundle: ExportBundle) -> None:
    rows = db.query(GoogleAnalyticsConnection).filter(GoogleAnalyticsConnection.user_id == user_id).all()
    for row in rows:
        rel = layout.rel(layout.connection_path("ga_connection", row.connection_name))
        doc = drop_none(
            {
                "kind": "ga_connection",
                "connection_name": row.connection_name,
                "description": row.description,
                "default_property_id": row.default_property_id,
                "auth_method": row.auth_method,
                "oauth_user_email": row.oauth_user_email,
                "credential": _add_secret_ref(bundle, db, row.credential_secret_id, rel, "credential"),
            }
        )
        bundle.artifacts[rel] = canonical_yaml_dump(doc)


def _export_kafka(db: Session, user_id: int, layout: ProjectLayout, bundle: ExportBundle) -> None:
    rows = db.query(KafkaConnection).filter(KafkaConnection.user_id == user_id).all()
    for row in rows:
        rel = layout.rel(layout.connection_path("kafka_connection", row.connection_name))
        doc = {
            "kind": "kafka_connection",
            "connection_name": row.connection_name,
            "bootstrap_servers": row.bootstrap_servers,
            "security_protocol": row.security_protocol,
            "sasl_mechanism": row.sasl_mechanism,
            "sasl_username": row.sasl_username,
            "ssl_ca_location": row.ssl_ca_location,
            "ssl_cert_location": row.ssl_cert_location,
            "schema_registry_url": row.schema_registry_url,
            "extra_config": _parse_extra_config(row.extra_config),
            "sasl_password": _add_secret_ref(bundle, db, row.sasl_password_id, rel, "sasl_password"),
            "ssl_key": _add_secret_ref(bundle, db, row.ssl_key_id, rel, "ssl_key"),
        }
        bundle.artifacts[rel] = canonical_yaml_dump(drop_none(doc))
