"""Pydantic models for Kafka read settings shared between core and worker."""

from __future__ import annotations

from pydantic import BaseModel


class KafkaReadSettings(BaseModel):
    """Settings sent from core to worker to consume from a Kafka topic."""

    bootstrap_servers: str
    topic: str
    value_format: str = "json"
    group_id: str = "flowfile-kafka-source"
    start_offset: str = "latest"  # "earliest" or "latest" — used on first-ever consume
    max_messages: int = 100_000
    poll_timeout_seconds: float = 30.0

    # Security / auth (optional)
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None  # decrypted, never stored
    ssl_ca_location: str | None = None
    ssl_cert_location: str | None = None
    ssl_key_pem: str | None = None  # decrypted, never stored

    # Extra confluent-kafka config overrides
    extra_config: dict[str, str] | None = None

    def to_consumer_config(self) -> dict:
        """Build a confluent-kafka Consumer config dict."""
        config: dict[str, str | int] = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": "false",
            "auto.offset.reset": self.start_offset,
            "session.timeout.ms": "6000",
            "heartbeat.interval.ms": "2000",
            "fetch.wait.max.ms": "500",
            "enable.partition.eof": "true",
        }

        if self.security_protocol != "PLAINTEXT":
            config["security.protocol"] = self.security_protocol

        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            config["sasl.username"] = self.sasl_username
        if self.sasl_password:
            config["sasl.password"] = self.sasl_password

        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location
        if self.ssl_cert_location:
            config["ssl.certificate.location"] = self.ssl_cert_location
        if self.ssl_key_pem:
            config["ssl.key.pem"] = self.ssl_key_pem

        if self.extra_config:
            config.update(self.extra_config)

        return config


class KafkaReadResult(BaseModel):
    """Metadata returned after a Kafka consume operation.

    The ``new_offsets`` field is informational only — offsets are committed
    broker-side via consumer groups, not tracked in a DB.
    """

    new_offsets: dict[int, int]  # {partition: reached_offset} for logging
    messages_consumed: int
    partitions_read: int
