"""Webhook URL validation (the SSRF guard) and per-channel payload formatting."""

from __future__ import annotations

import pytest

from shared.notifications import senders
from shared.notifications.senders import send_test_notification, send_webhook, validate_webhook_url

PAYLOAD = {
    "event_type": "run_failed",
    "flow_name": "nightly etl",
    "schedule_name": "nightly 2am",
    "duration_seconds": 12.5,
    "nodes_completed": 1,
    "number_of_nodes": 3,
    "failed_nodes": [{"node_id": 2, "node_name": "join", "error": "column 'id' not found"}],
    "run_id": 7,
}


class _Response:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


@pytest.fixture
def posts(monkeypatch):
    sent: list[tuple[str, dict]] = []
    status = {"code": 200, "text": ""}

    def _post(url, json):
        sent.append((url, json))
        return _Response(status["code"], status["text"])

    monkeypatch.setattr(senders, "_post", _post)
    return type("Posts", (), {"sent": sent, "status": status})()


@pytest.fixture(autouse=True)
def _no_private_hosts(monkeypatch):
    monkeypatch.delenv(senders.ALLOW_PRIVATE_ENV, raising=False)


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/hook",
        "https://10.0.0.5/hook",
        "http://169.254.169.254/latest/meta-data/",
        "http://192.168.1.10:8080/hook",
        # CGNAT: ip.is_private misses 100.64.0.0/10, and metadata services live behind it.
        "https://100.64.0.1/hook",
        "https://100.127.255.254/hook",
    ],
)
def test_private_and_link_local_targets_are_rejected(url):
    assert validate_webhook_url(url) is not None


def test_a_cgnat_target_reports_the_standard_message():
    assert validate_webhook_url("https://100.64.0.1/hook") == (
        "Webhook host resolves to a non-public address (100.64.0.1)"
    )


def test_the_address_just_past_cgnat_is_allowed():
    assert validate_webhook_url("https://100.128.0.1/hook") is None


@pytest.mark.parametrize("url", ["ftp://example.com/hook", "file:///etc/passwd", "not-a-url", ""])
def test_non_http_schemes_are_rejected(url):
    assert "http" in validate_webhook_url(url)


def test_unresolvable_host_is_rejected():
    error = validate_webhook_url("https://no-such-host.invalid/hook")
    assert "resolve" in error


@pytest.mark.parametrize("url", ["https://93.184.216.34/hook", "https://8.8.8.8/hook"])
def test_public_ip_literal_is_allowed(url):
    assert validate_webhook_url(url) is None


def test_private_hosts_can_be_opted_in(monkeypatch):
    monkeypatch.setenv(senders.ALLOW_PRIVATE_ENV, "true")
    assert validate_webhook_url("http://localhost:9000/hook") is None


def test_slack_and_discord_carry_the_same_body(posts):
    """Same lines both ways — only the bold marker differs (single ``*`` is italic on Discord)."""
    send_webhook("slack", "https://93.184.216.34/hook", PAYLOAD)
    send_webhook("discord", "https://93.184.216.34/hook", PAYLOAD)

    slack, discord = posts.sent[0][1], posts.sent[1][1]
    assert "*nightly etl*" in slack["text"]
    assert "**nightly etl**" in discord["content"]
    assert slack["text"].splitlines()[1:] == discord["content"].splitlines()[1:]
    assert "nightly 2am" in slack["text"]
    assert "join: column 'id' not found" in slack["text"]
    assert "Run id: 7" in slack["text"]
    assert "❌" in slack["text"]


def test_teams_uses_a_message_card(posts):
    send_webhook("teams", "https://93.184.216.34/hook", PAYLOAD)
    body = posts.sent[0][1]
    assert body["@type"] == "MessageCard"
    assert body["themeColor"] == "D93025"
    assert "nightly etl" in body["title"]
    # Teams renders a single asterisk as italic too.
    assert "**nightly etl**" in body["text"]


def test_generic_ships_the_whole_payload(posts):
    send_webhook("generic", "https://93.184.216.34/hook", PAYLOAD)
    body = posts.sent[0][1]
    assert body == {"event": "run_failed", "data": PAYLOAD}


def test_unknown_channel_type_falls_back_to_generic(posts):
    send_webhook("carrier-pigeon", "https://93.184.216.34/hook", PAYLOAD)
    assert posts.sent[0][1]["event"] == "run_failed"


def test_non_2xx_raises_with_the_truncated_body(posts):
    posts.status["code"] = 502
    posts.status["text"] = "x" * 500

    with pytest.raises(RuntimeError) as excinfo:
        send_webhook("slack", "https://93.184.216.34/hook", PAYLOAD)

    assert "502" in str(excinfo.value)
    assert len(str(excinfo.value)) < 300


def test_send_test_notification_validates_before_sending(posts):
    with pytest.raises(ValueError):
        send_test_notification("slack", "http://127.0.0.1/hook")
    assert posts.sent == []


def test_send_test_notification_delivers_a_sample(posts):
    send_test_notification("slack", "https://93.184.216.34/hook")
    assert "test notification" in posts.sent[0][1]["text"]
