"""Kafka connection and sync schemas for the API layer."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, SecretStr

# ---------------------------------------------------------------------------
# Kafka Connection schemas
# ---------------------------------------------------------------------------


class KafkaConnectionCreate(BaseModel):
    """Request body for creating a Kafka connection."""

    connection_name: str
    bootstrap_servers: str
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None
    ssl_ca_location: str | None = None
    ssl_cert_location: str | None = None
    ssl_key_pem: SecretStr | None = None
    schema_registry_url: str | None = None
    extra_config: dict[str, str] | None = None


class KafkaConnectionUpdate(BaseModel):
    """Request body for updating a Kafka connection."""

    connection_name: str | None = None
    bootstrap_servers: str | None = None
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None
    ssl_ca_location: str | None = None
    ssl_cert_location: str | None = None
    ssl_key_pem: SecretStr | None = None
    schema_registry_url: str | None = None
    extra_config: dict[str, str] | None = None


class KafkaConnectionOut(BaseModel):
    """Response model for a Kafka connection (no secrets exposed)."""

    id: int
    connection_name: str
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    schema_registry_url: str | None = None
    created_at: datetime
    updated_at: datetime


class KafkaTopicInfo(BaseModel):
    """Information about a Kafka topic."""

    name: str
    partition_count: int


class KafkaConnectionTestResult(BaseModel):
    """Result of testing a Kafka connection."""

    success: bool
    message: str
    topics_found: int = 0


# ---------------------------------------------------------------------------
# Kafka Sync schemas (one-click flow generation)
# ---------------------------------------------------------------------------


class KafkaSyncCreate(BaseModel):
    """One-click setup: specify source + target, auto-generates a flow."""

    name: str
    kafka_connection_id: int
    topic_name: str
    target_namespace_id: int
    target_table_name: str
    value_format: Literal["json"] = "json"
    schedule_interval_seconds: int = 300  # default: every 5 minutes
    start_offset: Literal["earliest", "latest"] = "latest"
    max_messages: int = 100_000


class KafkaSyncOut(BaseModel):
    """Response model for a Kafka sync configuration."""

    name: str
    kafka_connection_id: int
    topic_name: str
    target_table_name: str
    flow_registration_id: int
    schedule_id: int


