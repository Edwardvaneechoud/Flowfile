"""Minimal in-process publish/subscribe seam for neutral domain events.

Product code publishes facts about what the application did; observers
(telemetry today, anything a composition root wires up tomorrow) subscribe to
them. Publishers know nothing about their subscribers, and a subscriber can
never raise into the publisher: every handler call is isolated.

Handlers run synchronously on the publishing thread, so they must be fast and
non-blocking — anything slow belongs on a queue the handler feeds.

This module imports nothing from the rest of ``flowfile_core``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

_handlers: dict[str, list[Callable[..., None]]] = {}


def subscribe(event: str, handler: Callable[..., None]) -> None:
    """Register *handler* for *event*; handlers fire in registration order."""
    _handlers.setdefault(event, []).append(handler)


def publish(event: str, **payload: Any) -> None:
    """Fire *event*. Never raises: a failing subscriber is logged and skipped."""
    for handler in list(_handlers.get(event, ())):
        try:
            handler(**payload)
        except Exception:
            logger.debug("event subscriber failed for %r", event, exc_info=True)


def _reset_for_tests() -> None:
    _handlers.clear()
