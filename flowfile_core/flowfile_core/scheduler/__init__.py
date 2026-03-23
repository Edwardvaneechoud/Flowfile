"""Flow scheduling engine.

Provides ``FlowScheduler`` which runs as an asyncio background task
inside the core application's lifespan, polling for due interval and
table-trigger schedules.
"""

from .engine import FlowScheduler

__all__ = ["FlowScheduler"]
