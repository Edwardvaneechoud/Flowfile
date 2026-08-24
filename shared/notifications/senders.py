"""Webhook URL validation, per-channel payload formatting, and delivery."""

from __future__ import annotations

import ipaddress
import logging
import os
import socket
from typing import Any
from urllib.parse import urlparse

import httpx

logger = logging.getLogger("flowfile.notifications")

ALLOW_PRIVATE_ENV = "FLOWFILE_NOTIFY_ALLOW_PRIVATE_HOSTS"
TRUTHY = ("true", "1", "yes")

STATUS_EMOJI = {
    "run_failed": "❌",
    "run_success": "✅",
    "run_recovered": "🔄",
    "run_orphaned": "💀",
    "test": "🔔",
}

EVENT_DESCRIPTION = {
    "run_failed": "Run failed",
    "run_success": "Run succeeded",
    "run_recovered": "Run recovered after a failure",
    "run_orphaned": "Run was closed as orphaned",
    "test": "Test notification",
}

REQUEST_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
MAX_RESPONSE_CHARS = 200

# ``ip.is_private`` does not cover the CGNAT range on 3.11, and some cloud metadata
# services sit behind it — so it is checked explicitly.
CGNAT_NETWORK = ipaddress.ip_network("100.64.0.0/10")


def _allow_private_hosts() -> bool:
    return os.environ.get(ALLOW_PRIVATE_ENV, "").strip().lower() in TRUTHY


def validate_webhook_url(url: str) -> str | None:
    """SSRF guard for a user-supplied webhook URL. Returns an error message, or None if allowed.

    Every address the hostname resolves to must be publicly routable, so a DNS name
    pointing at the loopback/metadata/RFC1918/CGNAT ranges is rejected. Validation and the
    send are separate steps, so a DNS rebind between them is an accepted v1 risk:
    redirects are not followed and the body carries only run metadata.
    """
    parsed = urlparse(url or "")
    if parsed.scheme not in ("http", "https"):
        return "Webhook URL must use http or https"
    if not parsed.hostname:
        return "Webhook URL has no host"
    if _allow_private_hosts():
        return None

    try:
        infos = socket.getaddrinfo(parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80))
    except socket.gaierror as e:
        return f"Could not resolve webhook host {parsed.hostname!r}: {e}"

    for info in infos:
        address = info[4][0]
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            return f"Webhook host resolved to an unusable address {address!r}"
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            return f"Webhook host resolves to a non-public address ({address})"
        if ip.version == 4 and ip in CGNAT_NETWORK:
            return f"Webhook host resolves to a non-public address ({address})"
    return None


def _message_text(payload: dict[str, Any], bold: str = "*") -> str:
    """The shared message body. ``bold`` is the flavour's bold marker: Slack mrkdwn uses
    a single asterisk, while Discord and Teams read that as italic and need two."""
    event_type = payload.get("event_type", "")
    emoji = STATUS_EMOJI.get(event_type, "🔔")
    description = EVENT_DESCRIPTION.get(event_type, event_type)

    lines = [f"{emoji} {bold}{payload.get('flow_name') or 'Flow'}{bold} — {description}"]
    if payload.get("schedule_name"):
        lines.append(f"Schedule: {payload['schedule_name']}")
    if payload.get("duration_seconds") is not None:
        lines.append(f"Duration: {payload['duration_seconds']:.1f}s")
    if payload.get("number_of_nodes"):
        lines.append(f"Nodes: {payload.get('nodes_completed') or 0}/{payload['number_of_nodes']}")
    if payload.get("reason"):
        lines.append(f"Reason: {payload['reason']}")
    for node in payload.get("failed_nodes") or []:
        name = node.get("node_name") or f"Node {node.get('node_id')}"
        lines.append(f"    {name}: {node.get('error')}")
    if payload.get("run_id") is not None:
        lines.append(f"Run id: {payload['run_id']}")
    return "\n".join(lines)


def _format_slack(payload: dict[str, Any]) -> dict[str, Any]:
    return {"text": _message_text(payload)}


def _format_discord(payload: dict[str, Any]) -> dict[str, Any]:
    return {"content": _message_text(payload, bold="**")}


def _format_teams(payload: dict[str, Any]) -> dict[str, Any]:
    failed = payload.get("event_type") in ("run_failed", "run_orphaned")
    title = f"{payload.get('flow_name') or 'Flow'} — {EVENT_DESCRIPTION.get(payload.get('event_type', ''), '')}"
    return {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "D93025" if failed else "1E8E3E",
        "summary": title,
        "title": title,
        "text": _message_text(payload, bold="**"),
    }


def _format_generic(payload: dict[str, Any]) -> dict[str, Any]:
    return {"event": payload.get("event_type"), "data": payload}


FORMATTERS = {
    "slack": _format_slack,
    "discord": _format_discord,
    "teams": _format_teams,
    "generic": _format_generic,
}


def _post(url: str, json: dict[str, Any]) -> httpx.Response:
    """Single seam for the outbound request — tests monkeypatch this."""
    return httpx.post(url, json=json, timeout=REQUEST_TIMEOUT, follow_redirects=False)


def send_webhook(channel_type: str, url: str, payload: dict[str, Any]) -> None:
    """Deliver one event. Raises ``RuntimeError`` on any non-2xx or transport failure."""
    body = FORMATTERS.get(channel_type, _format_generic)(payload)
    response = _post(url, body)
    if response.status_code >= 300:
        text = (response.text or "")[:MAX_RESPONSE_CHARS]
        raise RuntimeError(f"Webhook returned HTTP {response.status_code}: {text}")


def send_test_notification(channel_type: str, url: str) -> None:
    """Validate the URL and deliver a sample event, so a channel can be verified on save."""
    error = validate_webhook_url(url)
    if error:
        raise ValueError(error)

    send_webhook(
        channel_type,
        url,
        {
            "event_type": "test",
            "flow_name": "This is a test notification from Flowfile",
            "run_id": None,
            "failed_nodes": [],
        },
    )
