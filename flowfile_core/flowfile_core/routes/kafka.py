"""API routes for Kafka connection management and consumer group operations."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db
from flowfile_core.kafka.connection_manager import (
    build_consumer_config,
    delete_kafka_connection,
    get_consumer_group_offsets,
    get_kafka_connection,
    list_kafka_connections,
    list_topics,
    reset_consumer_group,
    store_kafka_connection,
    test_kafka_connection,
    update_kafka_connection,
)
from flowfile_core.schemas.kafka_schemas import (
    KafkaConnectionCreate,
    KafkaConnectionOut,
    KafkaConnectionTestResult,
    KafkaConnectionUpdate,
    KafkaTopicInfo,
)
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.kafka.consumer import infer_topic_schema
from shared.kafka.models import KafkaReadSettings

router = APIRouter(prefix="/kafka", tags=["kafka"])


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


@router.post("/connections", response_model=KafkaConnectionOut)
def create_kafka_connection(
    connection: KafkaConnectionCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Create a new Kafka/Redpanda connection."""
    logger.info("Creating Kafka connection: %s", connection.connection_name)
    try:
        return store_kafka_connection(db, connection, current_user.id)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None


@router.get("/connections", response_model=list[KafkaConnectionOut])
def get_kafka_connections(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """List all Kafka connections for the current user."""
    return list_kafka_connections(db, current_user.id)


@router.put("/connections/{connection_id}", response_model=KafkaConnectionOut)
def update_kafka_connection_route(
    connection_id: int,
    update: KafkaConnectionUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Update an existing Kafka connection."""
    try:
        return update_kafka_connection(db, connection_id, update, current_user.id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from None


@router.delete("/connections/{connection_id}")
def delete_kafka_connection_route(
    connection_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Delete a Kafka connection."""
    try:
        delete_kafka_connection(db, connection_id, current_user.id)
        return {"message": "Kafka connection deleted successfully"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from None


@router.post("/connections/{connection_id}/test", response_model=KafkaConnectionTestResult)
def test_kafka_connection_route(
    connection_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Test connectivity to a Kafka/Redpanda cluster."""
    return test_kafka_connection(db, connection_id, current_user.id)


@router.get("/connections/{connection_id}/topics", response_model=list[KafkaTopicInfo])
def list_kafka_topics(
    connection_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """List available topics for a Kafka connection."""
    try:
        return list_topics(db, connection_id, current_user.id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from None


@router.get("/connections/{connection_id}/topics/{topic_name}/schema")
def infer_kafka_topic_schema(
    connection_id: int,
    topic_name: str,
    sample_size: int = Query(default=10, ge=1, le=1000),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Infer the schema of a Kafka topic by sampling messages.

    This is an explicit action — it connects to the broker, consumes a small
    sample, and returns the inferred column names and types. Not called
    automatically during flow setup.
    """
    db_conn = get_kafka_connection(db, connection_id, current_user.id)
    if db_conn is None:
        raise HTTPException(status_code=404, detail="Kafka connection not found")

    consumer_config = build_consumer_config(db, db_conn, current_user.id)
    settings = KafkaReadSettings.from_consumer_config(
        consumer_config,
        topic=topic_name,
        group_id=f"flowfile-schema-probe-{topic_name}",
        start_offset="earliest",
    )

    decrypt_fn = lambda v: decrypt_secret(v).get_secret_value()

    try:
        schema_pairs = infer_topic_schema(settings, sample_size=sample_size, decrypt_fn=decrypt_fn)
        return [{"name": name, "dtype": str(dtype)} for name, dtype in schema_pairs]
    except Exception as e:
        logger.error("Schema inference failed for topic %s on connection %d: %s", topic_name, connection_id, e)
        raise HTTPException(status_code=400, detail="Schema inference failed. Check server logs for details.") from None


# ---------------------------------------------------------------------------
# Consumer group management (offsets tracked by broker)
# ---------------------------------------------------------------------------


@router.get("/sync/{sync_name}/offsets", response_model=dict[int, int])
def get_sync_offsets_route(
    sync_name: str,
    connection_id: int = Query(..., description="Kafka connection ID to query the broker"),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """View committed offsets for a consumer group from the broker."""
    try:
        return get_consumer_group_offsets(db, sync_name, connection_id, current_user.id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from None


@router.post("/sync/{sync_name}/reset")
def reset_sync_offsets_route(
    sync_name: str,
    connection_id: int = Query(..., description="Kafka connection ID to access the broker"),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Reset offsets by deleting the consumer group (re-reads from start_offset)."""
    try:
        reset_consumer_group(db, sync_name, connection_id, current_user.id)
        return {"message": f"Consumer group '{sync_name}' deleted. Next run will use auto.offset.reset."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
