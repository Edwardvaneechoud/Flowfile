"""Tests for Kafka shared models."""

import json

import pytest

from shared.kafka.models import KafkaReadSettings


class TestKafkaReadSettings:
    def test_default_values(self):
        settings = KafkaReadSettings(bootstrap_servers="localhost:9092", topic="test-topic")
        assert settings.value_format == "json"
        assert settings.group_id == "flowfile-kafka-source"
        assert settings.start_offset == "latest"
        assert settings.max_messages == 100_000
        assert settings.poll_timeout_seconds == 30.0
        assert settings.security_protocol == "PLAINTEXT"

    def test_custom_group_id(self):
        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092", topic="test", group_id="my-sync"
        )
        assert settings.group_id == "my-sync"

    def test_to_consumer_config_plaintext(self):
        settings = KafkaReadSettings(
            bootstrap_servers="broker1:9092,broker2:9092", topic="events", group_id="my-group"
        )
        config = settings.to_consumer_config()
        assert config["bootstrap.servers"] == "broker1:9092,broker2:9092"
        assert config["group.id"] == "my-group"
        assert config["enable.auto.commit"] == "false"
        assert config["auto.offset.reset"] == "latest"
        assert config["security.protocol"] == "PLAINTEXT"

    def test_to_consumer_config_sasl(self):
        settings = KafkaReadSettings(
            bootstrap_servers="kafka:9093",
            topic="secure-topic",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="user",
            sasl_password="encrypted-secret",
            ssl_ca_location="/etc/ssl/ca.pem",
        )
        decrypt_fn = lambda v: f"decrypted({v})"
        config = settings.to_consumer_config(decrypt_fn=decrypt_fn)
        assert config["security.protocol"] == "SASL_SSL"
        assert config["sasl.mechanism"] == "SCRAM-SHA-256"
        assert config["sasl.username"] == "user"
        assert config["sasl.password"] == "decrypted(encrypted-secret)"
        assert config["ssl.ca.location"] == "/etc/ssl/ca.pem"

    def test_to_consumer_config_requires_decrypt_fn(self):
        settings = KafkaReadSettings(
            bootstrap_servers="kafka:9093",
            topic="secure-topic",
            sasl_password="encrypted-secret",
        )
        with pytest.raises(ValueError, match="decrypt_fn is required"):
            settings.to_consumer_config()

    def test_to_consumer_config_ssl_key_requires_decrypt_fn(self):
        settings = KafkaReadSettings(
            bootstrap_servers="kafka:9093",
            topic="secure-topic",
            ssl_key_pem="encrypted-key",
        )
        with pytest.raises(ValueError, match="decrypt_fn is required"):
            settings.to_consumer_config()

    def test_to_consumer_config_extra(self):
        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test",
            extra_config={"fetch.min.bytes": "1024"},
        )
        config = settings.to_consumer_config()
        assert config["fetch.min.bytes"] == "1024"

    def test_extra_config_blocks_security_prefixes(self):
        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test",
            extra_config={
                "fetch.min.bytes": "1024",
                "sasl.password": "hacked",
                "ssl.ca.location": "/evil",
                "security.protocol": "PLAINTEXT",
            },
        )
        config = settings.to_consumer_config()
        assert config["fetch.min.bytes"] == "1024"
        assert config.get("sasl.password") is None
        assert config.get("ssl.ca.location") is None

    def test_extra_config_blocks_exact_keys(self):
        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test",
            extra_config={
                "bootstrap.servers": "evil:9092",
                "group.id": "hijacked",
                "enable.auto.commit": "true",
                "enable.partition.eof": "false",
                "fetch.min.bytes": "1024",
            },
        )
        config = settings.to_consumer_config()
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["group.id"] == "flowfile-kafka-source"
        assert config["enable.auto.commit"] == "false"
        assert config["enable.partition.eof"] == "true"
        assert config["fetch.min.bytes"] == "1024"

    def test_from_consumer_config(self):
        consumer_config = {
            "bootstrap.servers": "kafka:9093",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "user",
            "sasl.password": "$ffsec$1$encrypted-pass",
            "ssl.ca.location": "/ca.pem",
            "ssl.certificate.location": "/cert.pem",
            "ssl.key.pem": "$ffsec$1$encrypted-key",
        }
        settings = KafkaReadSettings.from_consumer_config(
            consumer_config, topic="my-topic", group_id="my-group", start_offset="earliest"
        )
        assert settings.bootstrap_servers == "kafka:9093"
        assert settings.topic == "my-topic"
        assert settings.group_id == "my-group"
        assert settings.start_offset == "earliest"
        assert settings.security_protocol == "SASL_SSL"
        assert settings.sasl_mechanism == "PLAIN"
        assert settings.sasl_username == "user"
        assert settings.sasl_password == "$ffsec$1$encrypted-pass"
        assert settings.ssl_ca_location == "/ca.pem"
        assert settings.ssl_cert_location == "/cert.pem"
        assert settings.ssl_key_pem == "$ffsec$1$encrypted-key"

    def test_model_dump_json_includes_encrypted_secrets(self):
        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test",
            sasl_password="$ffsec$1$encrypted",
            ssl_key_pem="$ffsec$1$encrypted-key",
        )
        data = json.loads(settings.model_dump_json())
        assert data["sasl_password"] == "$ffsec$1$encrypted"
        assert data["ssl_key_pem"] == "$ffsec$1$encrypted-key"

    def test_session_timeout_in_config(self):
        settings = KafkaReadSettings(bootstrap_servers="localhost:9092", topic="test")
        config = settings.to_consumer_config()
        assert config["session.timeout.ms"] == "6000"
        assert config["heartbeat.interval.ms"] == "2000"
