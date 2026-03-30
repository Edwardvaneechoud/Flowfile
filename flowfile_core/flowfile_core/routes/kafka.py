"""API routes for Kafka connection management and sync setup."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db
from flowfile_core.kafka.connection_manager import (
    delete_kafka_connection,
    get_sync_offset_info,
    list_kafka_connections,
    list_topics,
    reset_sync_offsets,
    store_kafka_connection,
    test_kafka_connection,
    update_kafka_connection,
)
from flowfile_core.schemas.kafka_schemas import (
    KafkaConnectionCreate,
    KafkaConnectionOut,
    KafkaConnectionTestResult,
    KafkaConnectionUpdate,
    KafkaSyncOffsetOut,
    KafkaTopicInfo,
)

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


# ---------------------------------------------------------------------------
# Sync offset management
# ---------------------------------------------------------------------------


@router.get("/sync/{sync_name}/offsets", response_model=KafkaSyncOffsetOut | None)
def get_sync_offsets_route(
    sync_name: str,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """View current committed offsets for a sync."""
    return get_sync_offset_info(db, sync_name)


@router.post("/sync/{sync_name}/reset")
def reset_sync_offsets_route(
    sync_name: str,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Reset offsets for a sync (forces re-read from start_offset on next run)."""
    reset_sync_offsets(db, sync_name)
    return {"message": f"Offsets reset for sync '{sync_name}'"}
