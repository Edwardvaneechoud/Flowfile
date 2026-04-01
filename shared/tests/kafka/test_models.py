"""Tests for Kafka shared models."""

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
        assert "security.protocol" not in config

    def test_to_consumer_config_sasl(self):
        settings = KafkaReadSettings(
            bootstrap_servers="kafka:9093",
            topic="secure-topic",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="user",
            sasl_password="secret",
            ssl_ca_location="/etc/ssl/ca.pem",
        )
        config = settings.to_consumer_config()
        assert config["security.protocol"] == "SASL_SSL"
        assert config["sasl.mechanism"] == "SCRAM-SHA-256"
        assert config["sasl.username"] == "user"
        assert config["sasl.password"] == "secret"
        assert config["ssl.ca.location"] == "/etc/ssl/ca.pem"

    def test_to_consumer_config_extra(self):
        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test",
            extra_config={"fetch.min.bytes": "1024"},
        )
        config = settings.to_consumer_config()
        assert config["fetch.min.bytes"] == "1024"

    def test_session_timeout_in_config(self):
        settings = KafkaReadSettings(bootstrap_servers="localhost:9092", topic="test")
        config = settings.to_consumer_config()
        assert config["session.timeout.ms"] == "6000"
        assert config["heartbeat.interval.ms"] == "2000"
