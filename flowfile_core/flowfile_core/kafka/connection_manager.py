"""CRUD operations for Kafka connections and consumer group management."""

from __future__ import annotations

import json
import logging

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from sqlalchemy.orm import Session

from flowfile_core.database.models import KafkaConnection, Secret
from flowfile_core.schemas.kafka_schemas import (
    KafkaConnectionCreate,
    KafkaConnectionOut,
    KafkaConnectionTestResult,
    KafkaConnectionUpdate,
    KafkaTopicInfo,
)
from flowfile_core.secret_manager.secret_manager import SecretInput, decrypt_secret, encrypt_secret, store_secret

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

    # Only update fields explicitly provided in the request (allows clearing via null)
    provided = update.model_fields_set
    if "connection_name" in provided:
        db_conn.connection_name = update.connection_name
    if "bootstrap_servers" in provided:
        db_conn.bootstrap_servers = update.bootstrap_servers
    if "security_protocol" in provided:
        db_conn.security_protocol = update.security_protocol
    if "sasl_mechanism" in provided:
        db_conn.sasl_mechanism = update.sasl_mechanism
    if "sasl_username" in provided:
        db_conn.sasl_username = update.sasl_username
    if "schema_registry_url" in provided:
        db_conn.schema_registry_url = update.schema_registry_url
    if "ssl_ca_location" in provided:
        db_conn.ssl_ca_location = update.ssl_ca_location
    if "ssl_cert_location" in provided:
        db_conn.ssl_cert_location = update.ssl_cert_location
    if "extra_config" in provided:
        db_conn.extra_config = json.dumps(update.extra_config) if update.extra_config else None

    # Update secrets if provided
    if update.sasl_password is not None:
        password_value = update.sasl_password.get_secret_value()
        if password_value:
            if db_conn.sasl_password_id:
                secret = db.query(Secret).filter(Secret.id == db_conn.sasl_password_id).first()
                if secret:
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

    # Delete secrets first, then the connection
    secret_ids = [db_conn.sasl_password_id, db_conn.ssl_key_id]
    for sid in secret_ids:
        if sid:
            secret = db.query(Secret).filter(Secret.id == sid).first()
            if secret:
                db.delete(secret)
    db.delete(db_conn)
    db.commit()


def build_consumer_config(db: Session, db_conn: KafkaConnection, user_id: int) -> dict:
    """Build a confluent-kafka consumer config dict from a stored connection.

    Secret values (``sasl.password``, ``ssl.key.pem``) are returned in their
    **encrypted** form (``$ffsec$`` format).  They are only decrypted at
    point-of-use via ``KafkaReadSettings.to_consumer_config(decrypt_fn=...)``.
    """
    config: dict[str, str] = {
        "bootstrap.servers": db_conn.bootstrap_servers,
        "enable.auto.commit": "false",
    }
    config["security.protocol"] = db_conn.security_protocol
    if db_conn.sasl_mechanism:
        config["sasl.mechanism"] = db_conn.sasl_mechanism
    if db_conn.sasl_username:
        config["sasl.username"] = db_conn.sasl_username
    if db_conn.sasl_password_id:
        secret = db.query(Secret).filter(Secret.id == db_conn.sasl_password_id).first()
        if secret:
            config["sasl.password"] = secret.encrypted_value
    if db_conn.ssl_ca_location:
        config["ssl.ca.location"] = db_conn.ssl_ca_location
    if db_conn.ssl_cert_location:
        config["ssl.certificate.location"] = db_conn.ssl_cert_location
    if db_conn.ssl_key_id:
        secret = db.query(Secret).filter(Secret.id == db_conn.ssl_key_id).first()
        if secret:
            config["ssl.key.pem"] = secret.encrypted_value
    if db_conn.extra_config:
        extra = json.loads(db_conn.extra_config)
        blocked_prefixes = ("sasl.", "ssl.", "security.protocol")
        blocked_exact = {"bootstrap.servers", "group.id", "enable.auto.commit", "enable.partition.eof"}
        extra = {
            k: v for k, v in extra.items() if not k.startswith(blocked_prefixes) and k not in blocked_exact
        }
        config.update(extra)
    return config


def _decrypt_config_secrets(config: dict, user_id: int) -> dict:
    """Return a copy of *config* with encrypted secret values decrypted in-place.

    Used by helpers that create a confluent-kafka ``Consumer`` / ``AdminClient``
    directly (connection test, topic discovery, consumer group management).
    """
    out = dict(config)
    for key in ("sasl.password", "ssl.key.pem"):
        val = out.get(key)
        if val and val.startswith("$ffsec$"):
            out[key] = decrypt_secret(val, user_id).get_secret_value()
    return out


# ---------------------------------------------------------------------------
# Connection testing & topic discovery
# ---------------------------------------------------------------------------


def test_kafka_connection(db: Session, connection_id: int, user_id: int) -> KafkaConnectionTestResult:
    db_conn = get_kafka_connection(db, connection_id, user_id)
    if db_conn is None:
        return KafkaConnectionTestResult(success=False, message="Connection not found")

    try:
        config = _decrypt_config_secrets(build_consumer_config(db, db_conn, user_id), user_id)
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

    config = _decrypt_config_secrets(build_consumer_config(db, db_conn, user_id), user_id)
    config["group.id"] = "flowfile-topic-discovery"
    consumer = Consumer(config)
    try:
        metadata = consumer.list_topics(timeout=10.0)
        topics = []
        for name, topic_meta in metadata.topics.items():
            if not name.startswith("__"):  # Skip internal topics (e.g. __consumer_offsets)
                topics.append(KafkaTopicInfo(name=name, partition_count=len(topic_meta.partitions)))
        return sorted(topics, key=lambda t: t.name)
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# Consumer group management (offset tracking is handled by Kafka broker)
# ---------------------------------------------------------------------------


def reset_consumer_group(
    db: Session, group_id: str, connection_id: int, user_id: int, topic: str
) -> None:
    """Reset a consumer group's offsets to the beginning of a topic.

    Sets committed offsets to 0 for every partition of *topic*, so the next
    consume re-reads all messages regardless of ``auto.offset.reset``.

    The consumer group must not have active members (i.e., no flow currently
    running).
    """
    from confluent_kafka import TopicPartition

    db_conn = get_kafka_connection(db, connection_id, user_id)
    if db_conn is None:
        raise ValueError(f"Kafka connection {connection_id} not found for user {user_id}.")

    config = _decrypt_config_secrets(build_consumer_config(db, db_conn, user_id), user_id)

    # Discover partition count for the topic
    config["group.id"] = group_id
    consumer = Consumer(config)
    try:
        metadata = consumer.list_topics(topic, timeout=10.0)
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            raise ValueError(f"Topic '{topic}' not found on the broker.")
        partition_count = len(topic_meta.partitions)
    finally:
        consumer.close()

    # Set committed offsets to 0 (beginning) for every partition
    tps = [TopicPartition(topic, p, 0) for p in range(partition_count)]
    admin = AdminClient(config)
    futures = admin.alter_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group_id, tps)]
    )
    for gid, future in futures.items():
        try:
            future.result()
            logger.info(
                "Reset consumer group %s to beginning of topic %s (%d partitions)",
                gid, topic, partition_count,
            )
        except KafkaException as e:
            logger.warning("Could not reset consumer group %s: %s", gid, e)
            raise ValueError(f"Could not reset consumer group '{gid}': {e}") from e


def get_consumer_group_offsets(
    db: Session, group_id: str, connection_id: int, user_id: int
) -> dict[int, int]:
    """Query committed offsets for a consumer group from the broker.

    Returns {partition: offset} dict, or empty dict if the group has no commits.
    """
    db_conn = get_kafka_connection(db, connection_id, user_id)
    if db_conn is None:
        raise ValueError(f"Kafka connection {connection_id} not found for user {user_id}.")

    config = _decrypt_config_secrets(build_consumer_config(db, db_conn, user_id), user_id)
    admin = AdminClient(config)

    try:
        futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group_id)])
        for _gid, future in futures.items():
            result = future.result()
            return {tp.partition: tp.offset for tp in result if tp.offset >= 0}
    except Exception as e:
        logger.warning("Could not query offsets for group %s: %s", group_id, e)
    return {}


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
