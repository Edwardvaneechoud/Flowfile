"""Pydantic models for Kafka read settings shared between core and worker."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    pass

_BLOCKED_EXTRA_CONFIG_PREFIXES = ("sasl.", "ssl.", "security.protocol")
_BLOCKED_EXTRA_CONFIG_EXACT = frozenset(
    {
        "bootstrap.servers",
        "group.id",
        "enable.auto.commit",
        "enable.partition.eof",
    }
)


class KafkaReadSettings(BaseModel):
    """Settings sent from core to worker to consume from a Kafka topic.

    Secrets (``sasl_password``, ``ssl_key_pem``) are stored as **encrypted**
    strings (``$ffsec$1$…`` format).  They travel encrypted over the wire and
    are only decrypted at point-of-use via a ``decrypt_fn`` passed to
    :meth:`to_consumer_config`.
    """

    bootstrap_servers: str
    topic: str
    value_format: str = "json"
    group_id: str = "flowfile-kafka-source"
    start_offset: str = "latest"  # "earliest" or "latest" — used on first-ever consume
    max_messages: int = 100_000
    poll_timeout_seconds: float = 30.0

    # Security / auth (optional) — values are encrypted, not plaintext
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None  # encrypted ($ffsec$ format)
    ssl_ca_location: str | None = None
    ssl_cert_location: str | None = None
    ssl_key_pem: str | None = None  # encrypted ($ffsec$ format)

    # Flow context (for cache directory isolation)
    flowfile_flow_id: int = 1
    flowfile_node_id: int = -1

    # Extra confluent-kafka config overrides
    extra_config: dict[str, str] | None = None

    def to_consumer_config(self, decrypt_fn: Callable[[str], str] | None = None) -> dict:
        """Build a confluent-kafka Consumer config dict.

        Args:
            decrypt_fn: A callable that decrypts an encrypted string and returns
                the plaintext value.  Required when ``sasl_password`` or
                ``ssl_key_pem`` are set.
        """
        config: dict[str, str | int] = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": "false",
            "auto.offset.reset": self.start_offset,
            "session.timeout.ms": "6000",
            "heartbeat.interval.ms": "2000",
            "fetch.wait.max.ms": "500",
            "enable.partition.eof": "true",
            "security.protocol": self.security_protocol,
        }

        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            config["sasl.username"] = self.sasl_username
        if self.sasl_password:
            if decrypt_fn is None:
                raise ValueError("decrypt_fn is required when sasl_password is set")
            config["sasl.password"] = decrypt_fn(self.sasl_password)

        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location
        if self.ssl_cert_location:
            config["ssl.certificate.location"] = self.ssl_cert_location
        if self.ssl_key_pem:
            if decrypt_fn is None:
                raise ValueError("decrypt_fn is required when ssl_key_pem is set")
            config["ssl.key.pem"] = decrypt_fn(self.ssl_key_pem)

        if self.extra_config:
            filtered = {
                k: v
                for k, v in self.extra_config.items()
                if not k.startswith(_BLOCKED_EXTRA_CONFIG_PREFIXES) and k not in _BLOCKED_EXTRA_CONFIG_EXACT
            }
            config.update(filtered)

        return config

    @classmethod
    def from_consumer_config(
        cls,
        config: dict,
        topic: str,
        *,
        value_format: str = "json",
        group_id: str = "flowfile-kafka-source",
        start_offset: str = "latest",
        max_messages: int = 100_000,
        poll_timeout_seconds: float = 30.0,
        flowfile_flow_id: int = 1,
        flowfile_node_id: int = -1,
    ) -> KafkaReadSettings:
        """Create KafkaReadSettings from a confluent-kafka consumer config dict.

        Secret values (``sasl.password``, ``ssl.key.pem``) should be in their
        encrypted form so they stay encrypted in transit.
        """
        return cls(
            bootstrap_servers=config.get("bootstrap.servers", ""),
            topic=topic,
            value_format=value_format,
            group_id=group_id,
            start_offset=start_offset,
            max_messages=max_messages,
            poll_timeout_seconds=poll_timeout_seconds,
            security_protocol=config.get("security.protocol", "PLAINTEXT"),
            sasl_mechanism=config.get("sasl.mechanism"),
            sasl_username=config.get("sasl.username"),
            sasl_password=config.get("sasl.password"),
            ssl_ca_location=config.get("ssl.ca.location"),
            ssl_cert_location=config.get("ssl.certificate.location"),
            ssl_key_pem=config.get("ssl.key.pem"),
            flowfile_flow_id=flowfile_flow_id,
            flowfile_node_id=flowfile_node_id,
        )


class KafkaReadResult(BaseModel):
    """Metadata returned after a Kafka consume operation.

    The ``new_offsets`` field is informational only — offsets are committed
    broker-side via consumer groups, not tracked in a DB.
    """

    new_offsets: dict[int, int]  # {partition: reached_offset} for logging
    messages_consumed: int
    partitions_read: int


class DeferredKafkaCommit(BaseModel):
    """Holds the information needed to commit Kafka offsets after downstream success.

    Created during consumption with ``commit=False`` and stored on the
    FlowNode.  After all downstream dependents succeed, ``commit_offsets()``
    is called with these settings + offsets.
    """

    settings: KafkaReadSettings
    offsets: dict[int, int]  # {partition: next_offset_to_commit}
