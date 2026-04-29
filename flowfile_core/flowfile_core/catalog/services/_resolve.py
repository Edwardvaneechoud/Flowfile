"""Shared helper: turn an exception during virtual-table resolution into
an empty result so route handlers do not break the page.

Used by TablePreviewService, VirtualTableService, SqlService,
VisualizationService. Broad catch is intentional (see docstring).
"""

from __future__ import annotations

import logging
from typing import Callable, TypeVar

logger = logging.getLogger(__name__)
T = TypeVar("T")


def resolve_or_log(resolver: Callable[[], T], *, kind: str, identifier: object) -> T | None:
    """Run ``resolver`` and return ``None`` on any exception.

    Broad catch is deliberate: virtual-table resolution can fail for
    any number of reasons (corrupt serialized lazy frame, missing
    producer flow file, polars eval error, recursion bug). The route
    contract for previews and ad-hoc visualizations is "never break
    the page; show an empty result." This helper enforces that.
    """
    try:
        return resolver()
    except Exception:
        logger.warning("Could not resolve %s %s", kind, identifier, exc_info=True)
        return None
