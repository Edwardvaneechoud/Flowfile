"""CRUD operations for Kafka connections and sync offset tracking."""

from __future__ import annotations

import json
import logging

from sqlalchemy.orm import Session

from flowfile_core.database.models import KafkaConnection, KafkaSyncOffset, Secret
from flowfile_core.schemas.kafka_schemas import (
    KafkaConnectionCreate,
    KafkaConnectionOut,
    KafkaConnectionTestResult,
    KafkaConnectionUpdate,
    KafkaSyncOffsetOut,
    KafkaTopicInfo,
)
from flowfile_core.secret_manager.secret_manager import SecretInput, decrypt_secret, store_secret

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Kafka Connection CRUD
# ---------------------------------------------------------------------------


def store_kafka_connection(db: Session, connection: KafkaConnectionCreate, user_id: int) -> KafkaConnectionOut:
    existing = (
        db.query(KafkaConnection)
        .filter(KafkaConnection.connection_name == connection.connection_name, KafkaConnection.user_id == user_id)
        .first()
    )
    if existing:
        raise ValueError(
            f"Kafka connection with name '{connection.connection_name}' already exists for user {user_id}."
        )

    # Encrypt secrets
    sasl_password_id = None
    if connection.sasl_password:
        secret = store_secret(
            db, SecretInput(name=f"kafka_{connection.connection_name}_sasl", value=connection.sasl_password), user_id
        )
        sasl_password_id = secret.id

    ssl_key_id = None
    if connection.ssl_key_pem:
        secret = store_secret(
            db, SecretInput(name=f"kafka_{connection.connection_name}_ssl_key", value=connection.ssl_key_pem), user_id
        )
        ssl_key_id = secret.id

    db_conn = KafkaConnection(
        connection_name=connection.connection_name,
        bootstrap_servers=connection.bootstrap_servers,
        security_protocol=connection.security_protocol,
        sasl_mechanism=connection.sasl_mechanism,
        sasl_username=connection.sasl_username,
        sasl_password_id=sasl_password_id,
        ssl_ca_location=connection.ssl_ca_location,
        ssl_cert_location=connection.ssl_cert_location,
        ssl_key_id=ssl_key_id,
        schema_registry_url=connection.schema_registry_url,
        extra_config=json.dumps(connection.extra_config) if connection.extra_config else None,
        user_id=user_id,
    )
    db.add(db_conn)
    db.commit()
    db.refresh(db_conn)
    return _to_connection_out(db_conn)


def get_kafka_connection(db: Session, connection_id: int, user_id: int) -> KafkaConnection | None:
    return (
        db.query(KafkaConnection)
        .filter(KafkaConnection.id == connection_id, KafkaConnection.user_id == user_id)
        .first()
    )


def get_kafka_connection_by_name(db: Session, connection_name: str, user_id: int) -> KafkaConnection | None:
    return (
        db.query(KafkaConnection)
        .filter(KafkaConnection.connection_name == connection_name, KafkaConnection.user_id == user_id)
        .first()
    )


def list_kafka_connections(db: Session, user_id: int) -> list[KafkaConnectionOut]:
    connections = db.query(KafkaConnection).filter(KafkaConnection.user_id == user_id).all()
    return [_to_connection_out(c) for c in connections]


def update_kafka_connection(
    db: Session, connection_id: int, update: KafkaConnectionUpdate, user_id: int
) -> KafkaConnectionOut:
    db_conn = get_kafka_connection(db, connection_id, user_id)
    if db_conn is None:
        raise ValueError(f"Kafka connection {connection_id} not found for user {user_id}.")

    if update.connection_name is not None:
        db_conn.connection_name = update.connection_name
    if update.bootstrap_servers is not None:
        db_conn.bootstrap_servers = update.bootstrap_servers
    if update.security_protocol is not None:
        db_conn.security_protocol = update.security_protocol
    if update.sasl_mechanism is not None:
        db_conn.sasl_mechanism = update.sasl_mechanism
    if update.sasl_username is not None:
        db_conn.sasl_username = update.sasl_username
    if update.schema_registry_url is not None:
        db_conn.schema_registry_url = update.schema_registry_url
    if update.ssl_ca_location is not None:
        db_conn.ssl_ca_location = update.ssl_ca_location
    if update.ssl_cert_location is not None:
        db_conn.ssl_cert_location = update.ssl_cert_location
    if update.extra_config is not None:
        db_conn.extra_config = json.dumps(update.extra_config)

    # Update secrets if provided
    if update.sasl_password is not None:
        password_value = update.sasl_password.get_secret_value()
        if password_value:
            if db_conn.sasl_password_id:
                secret = db.query(Secret).filter(Secret.id == db_conn.sasl_password_id).first()
                if secret:
                    from flowfile_core.secret_manager.secret_manager import encrypt_secret

                    secret.encrypted_value = encrypt_secret(password_value, user_id)
            else:
                new_secret = store_secret(
                    db,
                    SecretInput(name=f"kafka_{db_conn.connection_name}_sasl", value=update.sasl_password),
                    user_id,
                )
                db_conn.sasl_password_id = new_secret.id

    if update.ssl_key_pem is not None:
        key_value = update.ssl_key_pem.get_secret_value()
        if key_value:
            if db_conn.ssl_key_id:
                secret = db.query(Secret).filter(Secret.id == db_conn.ssl_key_id).first()
                if secret:
                    from flowfile_core.secret_manager.secret_manager import encrypt_secret

                    secret.encrypted_value = encrypt_secret(key_value, user_id)
            else:
                new_secret = store_secret(
                    db,
                    SecretInput(name=f"kafka_{db_conn.connection_name}_ssl_key", value=update.ssl_key_pem),
                    user_id,
                )
                db_conn.ssl_key_id = new_secret.id

    db.commit()
    db.refresh(db_conn)
    return _to_connection_out(db_conn)


def delete_kafka_connection(db: Session, connection_id: int, user_id: int) -> None:
    db_conn = get_kafka_connection(db, connection_id, user_id)
    if db_conn is None:
        raise ValueError(f"Kafka connection {connection_id} not found for user {user_id}.")

    # Clean up associated secrets
    secret_ids = [db_conn.sasl_password_id, db_conn.ssl_key_id]
    db.delete(db_conn)
    for sid in secret_ids:
        if sid:
            secret = db.query(Secret).filter(Secret.id == sid).first()
            if secret:
                db.delete(secret)
    db.commit()


def build_consumer_config(db: Session, db_conn: KafkaConnection, user_id: int) -> dict:
    """Build a confluent-kafka consumer config dict from a stored connection."""
    config: dict[str, str] = {
        "bootstrap.servers": db_conn.bootstrap_servers,
        "enable.auto.commit": "false",
    }
    if db_conn.security_protocol != "PLAINTEXT":
        config["security.protocol"] = db_conn.security_protocol
    if db_conn.sasl_mechanism:
        config["sasl.mechanism"] = db_conn.sasl_mechanism
    if db_conn.sasl_username:
        config["sasl.username"] = db_conn.sasl_username
    if db_conn.sasl_password_id:
        secret = db.query(Secret).filter(Secret.id == db_conn.sasl_password_id).first()
        if secret:
            config["sasl.password"] = decrypt_secret(secret.encrypted_value, user_id).get_secret_value()
    if db_conn.ssl_ca_location:
        config["ssl.ca.location"] = db_conn.ssl_ca_location
    if db_conn.ssl_cert_location:
        config["ssl.certificate.location"] = db_conn.ssl_cert_location
    if db_conn.ssl_key_id:
        secret = db.query(Secret).filter(Secret.id == db_conn.ssl_key_id).first()
        if secret:
            config["ssl.key.pem"] = decrypt_secret(secret.encrypted_value, user_id).get_secret_value()
    if db_conn.extra_config:
        config.update(json.loads(db_conn.extra_config))
    return config


# ---------------------------------------------------------------------------
# Connection testing & topic discovery
# ---------------------------------------------------------------------------


def test_kafka_connection(db: Session, connection_id: int, user_id: int) -> KafkaConnectionTestResult:
    db_conn = get_kafka_connection(db, connection_id, user_id)
    if db_conn is None:
        return KafkaConnectionTestResult(success=False, message="Connection not found")

    try:
        from confluent_kafka import Consumer

        config = build_consumer_config(db, db_conn, user_id)
        config["group.id"] = "flowfile-connection-test"
        consumer = Consumer(config)
        try:
            metadata = consumer.list_topics(timeout=10.0)
            topic_count = len(metadata.topics)
            return KafkaConnectionTestResult(
                success=True,
                message=f"Connected successfully. Found {topic_count} topics.",
                topics_found=topic_count,
            )
        finally:
            consumer.close()
    except Exception as e:
        logger.warning("Kafka connection test failed for connection %d: %s", connection_id, e)
        return KafkaConnectionTestResult(success=False, message=str(e))


def list_topics(db: Session, connection_id: int, user_id: int) -> list[KafkaTopicInfo]:
    db_conn = get_kafka_connection(db, connection_id, user_id)
    if db_conn is None:
        raise ValueError(f"Kafka connection {connection_id} not found for user {user_id}.")

    from confluent_kafka import Consumer

    config = build_consumer_config(db, db_conn, user_id)
    config["group.id"] = "flowfile-topic-discovery"
    consumer = Consumer(config)
    try:
        metadata = consumer.list_topics(timeout=10.0)
        topics = []
        for name, topic_meta in metadata.topics.items():
            if not name.startswith("_"):  # Skip internal topics
                topics.append(KafkaTopicInfo(name=name, partition_count=len(topic_meta.partitions)))
        return sorted(topics, key=lambda t: t.name)
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# Offset tracking
# ---------------------------------------------------------------------------


def get_sync_offsets(db: Session, sync_name: str, topic: str) -> dict[int, int]:
    """Get stored offsets for a sync. Returns {partition: offset} dict."""
    rows = (
        db.query(KafkaSyncOffset).filter(KafkaSyncOffset.sync_name == sync_name, KafkaSyncOffset.topic == topic).all()
    )
    return {row.partition: row.committed_offset for row in rows}


def update_sync_offsets(db: Session, sync_name: str, topic: str, offsets: dict[int, int]) -> None:
    """Update stored offsets after a successful consume."""
    for partition, offset in offsets.items():
        existing = (
            db.query(KafkaSyncOffset)
            .filter(
                KafkaSyncOffset.sync_name == sync_name,
                KafkaSyncOffset.topic == topic,
                KafkaSyncOffset.partition == partition,
            )
            .first()
        )
        if existing:
            existing.committed_offset = offset
        else:
            db.add(
                KafkaSyncOffset(
                    sync_name=sync_name,
                    topic=topic,
                    partition=partition,
                    committed_offset=offset,
                )
            )
    db.commit()


def reset_sync_offsets(db: Session, sync_name: str) -> None:
    """Delete all stored offsets for a sync (forces re-read from start_offset)."""
    db.query(KafkaSyncOffset).filter(KafkaSyncOffset.sync_name == sync_name).delete()
    db.commit()


def get_sync_offset_info(db: Session, sync_name: str) -> KafkaSyncOffsetOut | None:
    """Get offset info for display in the API."""
    rows = db.query(KafkaSyncOffset).filter(KafkaSyncOffset.sync_name == sync_name).all()
    if not rows:
        return None
    return KafkaSyncOffsetOut(
        sync_name=sync_name,
        topic=rows[0].topic,
        offsets={row.partition: row.committed_offset for row in rows},
        updated_at=max(row.updated_at for row in rows),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_connection_out(db_conn: KafkaConnection) -> KafkaConnectionOut:
    return KafkaConnectionOut(
        id=db_conn.id,
        connection_name=db_conn.connection_name,
        bootstrap_servers=db_conn.bootstrap_servers,
        security_protocol=db_conn.security_protocol,
        sasl_mechanism=db_conn.sasl_mechanism,
        sasl_username=db_conn.sasl_username,
        schema_registry_url=db_conn.schema_registry_url,
        created_at=db_conn.created_at,
        updated_at=db_conn.updated_at,
    )
