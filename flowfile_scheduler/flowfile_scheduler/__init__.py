"""Lightweight flow scheduling engine for Flowfile.

This package has no dependencies on flowfile_core. Core imports *from* this
package, not the other way around.
"""

from flowfile_scheduler.engine import FlowScheduler

__all__ = ["FlowScheduler"]
