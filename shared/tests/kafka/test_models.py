"""Tests for Kafka shared models."""

from shared.kafka.models import KafkaReadSettings


class TestKafkaReadSettings:
    def test_default_values(self):
        settings = KafkaReadSettings(bootstrap_servers="localhost:9092", topic="test-topic")
        assert settings.value_format == "json"
        assert settings.start_offset == "latest"
        assert settings.max_messages == 100_000
        assert settings.poll_timeout_seconds == 30.0
        assert settings.security_protocol == "PLAINTEXT"
        assert settings.offsets == {}

    def test_to_consumer_config_plaintext(self):
        settings = KafkaReadSettings(bootstrap_servers="broker1:9092,broker2:9092", topic="events")
        config = settings.to_consumer_config(group_id="my-group")
        assert config["bootstrap.servers"] == "broker1:9092,broker2:9092"
        assert config["group.id"] == "my-group"
        assert config["enable.auto.commit"] == "false"
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

    def test_with_offsets(self):
        settings = KafkaReadSettings(
            bootstrap_servers="localhost:9092",
            topic="test",
            offsets={0: 100, 1: 200, 2: 150},
        )
        assert settings.offsets[0] == 100
        assert settings.offsets[1] == 200
        assert settings.offsets[2] == 150
