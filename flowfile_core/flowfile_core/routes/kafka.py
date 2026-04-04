"""API routes for Kafka connection management and consumer group operations."""

from __future__ import annotations

import os
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import yaml
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.service import CatalogService
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
from flowfile_core.schemas.catalog_schema import FlowRegistrationOut
from flowfile_core.schemas.kafka_schemas import (
    KafkaConnectionCreate,
    KafkaConnectionOut,
    KafkaConnectionTestResult,
    KafkaConnectionUpdate,
    KafkaSyncCreate,
    KafkaTopicInfo,
)
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.kafka.consumer import infer_topic_schema
from shared.kafka.models import KafkaReadSettings
from shared.storage_config import storage

try:
    _flowfile_version = version("Flowfile")
except PackageNotFoundError:
    _flowfile_version = "0.5.0"

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
    topic: str = Query(..., description="Topic to reset offsets for"),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Reset committed offsets to the beginning so the next run re-reads all messages."""
    try:
        reset_consumer_group(db, sync_name, connection_id, current_user.id, topic=topic)
        return {"message": f"Consumer group '{sync_name}' reset to beginning of topic '{topic}'."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None


# ---------------------------------------------------------------------------
# Kafka sync flow creation
# ---------------------------------------------------------------------------


def _ensure_sync_namespace(service: CatalogService, owner_id: int) -> int:
    """Return the namespace ID for ``General / sync``, creating both if needed."""
    repo = service.repo
    general = repo.get_namespace_by_name("General", parent_id=None)
    if general is None:
        general = service.create_namespace("General", owner_id=owner_id)
    sync_ns = repo.get_namespace_by_name("sync", parent_id=general.id)
    if sync_ns is None:
        sync_ns = service.create_namespace("sync", owner_id=owner_id, parent_id=general.id)
    return sync_ns.id


def _build_sync_flow_data(
    body: KafkaSyncCreate,
    connection_name: str,
    fields: list[dict[str, str]],
    registration_id: int,
) -> dict:
    """Build the YAML-serializable dict for a kafka_source → catalog_writer flow."""
    flow_name = f"sync-{body.sync_name}"
    return {
        "flowfile_version": _flowfile_version,
        "flowfile_id": 0,  # placeholder, overwritten on load
        "flowfile_name": flow_name,
        "flowfile_settings": {
            "description": f"Kafka sync: {body.topic_name} → {body.table_name}",
            "execution_mode": "Performance",
            "execution_location": "remote",
            "auto_save": False,
            "show_detailed_progress": True,
            "max_parallel_workers": 4,
            "source_registration_id": registration_id,
            "parameters": [],
        },
        "nodes": [
            {
                "id": 1,
                "type": "kafka_source",
                "is_start_node": True,
                "description": f"Kafka: {body.topic_name}",
                "node_reference": None,
                "x_position": 312,
                "y_position": 269,
                "left_input_id": None,
                "right_input_id": None,
                "input_ids": None,
                "outputs": [2],
                "setting_input": {
                    "cache_results": False,
                    "output_field_config": None,
                    "kafka_settings": {
                        "kafka_connection_id": body.kafka_connection_id,
                        "kafka_connection_name": connection_name,
                        "topic_name": body.topic_name,
                        "value_format": "json",
                        "sync_name": f"sync-{body.sync_name}",
                        "start_offset": body.start_offset,
                        "max_messages": 100000,
                        "poll_timeout_seconds": 30.0,
                    },
                    "fields": fields,
                },
            },
            {
                "id": 2,
                "type": "catalog_writer",
                "is_start_node": False,
                "description": f"Catalog: {body.table_name}",
                "node_reference": None,
                "x_position": 529,
                "y_position": 211,
                "left_input_id": None,
                "right_input_id": None,
                "input_ids": [1],
                "outputs": [],
                "setting_input": {
                    "cache_results": False,
                    "output_field_config": None,
                    "catalog_write_settings": {
                        "table_name": body.table_name,
                        "namespace_id": body.namespace_id,
                        "description": None,
                        "write_mode": body.write_mode,
                        "merge_keys": [],
                    },
                },
            },
        ],
    }


@router.post("/sync", response_model=FlowRegistrationOut, status_code=201)
def create_kafka_sync(
    body: KafkaSyncCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Create a Kafka-to-catalog sync flow.

    Generates a flow YAML with a ``kafka_source`` → ``catalog_writer`` pipeline,
    saves it to disk, and registers it in the catalog.
    """
    # Validate connection
    db_conn = get_kafka_connection(db, body.kafka_connection_id, current_user.id)
    if db_conn is None:
        raise HTTPException(status_code=404, detail="Kafka connection not found")

    # Infer topic schema for the fields list
    consumer_config = build_consumer_config(db, db_conn, current_user.id)
    settings = KafkaReadSettings.from_consumer_config(
        consumer_config,
        topic=body.topic_name,
        group_id=f"flowfile-schema-probe-{body.topic_name}",
        start_offset="earliest",
    )
    decrypt_fn = lambda v: decrypt_secret(v).get_secret_value()

    try:
        schema_pairs = infer_topic_schema(settings, sample_size=10, decrypt_fn=decrypt_fn)
        fields = [{"name": name, "data_type": str(dtype)} for name, dtype in schema_pairs]
    except Exception:
        logger.warning("Schema inference failed for topic %s; creating sync with empty fields", body.topic_name)
        fields = []

    # Reset any stale consumer group so the new sync starts fresh
    sync_group_id = f"sync-{body.sync_name}"
    try:
        reset_consumer_group(db, sync_group_id, body.kafka_connection_id, current_user.id, topic=body.topic_name)
        logger.info("Reset existing consumer group '%s' for clean sync start", sync_group_id)
    except Exception:
        # Group may not exist yet — that's fine, first run will create it
        logger.debug("No existing consumer group '%s' to reset (new group will be created on first run)", sync_group_id)

    # Save path
    flow_name = f"sync-{body.sync_name}"
    flow_path = storage.flows_directory / f"{flow_name}.yaml"
    if flow_path.exists():
        raise HTTPException(status_code=409, detail=f"A sync flow named '{flow_name}' already exists")

    # Register in catalog first to get the registration ID
    repo = SQLAlchemyCatalogRepository(db)
    service = CatalogService(repo)

    # Default to "General / sync" namespace when not specified
    namespace_id = body.namespace_id
    if namespace_id is None:
        namespace_id = _ensure_sync_namespace(service, current_user.id)
        body.namespace_id = namespace_id

    try:
        registration = service.register_flow(
            name=flow_name,
            flow_path=str(flow_path),
            owner_id=current_user.id,
            namespace_id=namespace_id,
            description=f"Kafka sync: {body.topic_name} → {body.table_name}",
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from None

    # Build and write YAML
    flow_data = _build_sync_flow_data(
        body,
        connection_name=db_conn.connection_name,
        fields=fields,
        registration_id=registration.id,
    )
    os.makedirs(flow_path.parent, exist_ok=True)
    with open(flow_path, "w", encoding="utf-8") as f:
        yaml.dump(flow_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    logger.info("Created Kafka sync flow '%s' at %s (registration %d)", flow_name, flow_path, registration.id)
    return registration
