"""Engine logging: what ran, when, how long — and when a node fails, why.

Handlers write to stdout (Pyodide routes stdout to console.log; stderr renders
as errors in the browser console). Default level is INFO; call
`set_log_level("DEBUG")` from the JS bridge or the devtools console for
per-step detail (cache hits, schema-propagation reasons, start markers).
"""

import functools
import logging
import sys
import time

logger = logging.getLogger("flowfile.engine")
logger.propagate = False
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("[engine] %(levelname)s %(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)


def set_log_level(level: str) -> str:
    """Set the engine log level by name ('DEBUG', 'INFO', ...). Returns the active level."""
    logger.setLevel(level.upper())
    return logging.getLevelName(logger.level)


def log_node(fn):
    """Wrap an `execute_*` node function with outcome logging.

    Node functions report failures by returning {"success": False, "error": ...}
    rather than raising, so the wrapper inspects the result dict: success logs
    at INFO with the duration, failure logs the node's error at WARNING.
    """
    name = fn.__name__

    @functools.wraps(fn)
    def wrapper(node_id, *args, **kwargs):
        logger.debug("%s node=%s start", name, node_id)
        t0 = time.perf_counter()
        try:
            result = fn(node_id, *args, **kwargs)
        except Exception:
            logger.exception("%s node=%s raised after %.0fms", name, node_id, (time.perf_counter() - t0) * 1000)
            raise
        ms = (time.perf_counter() - t0) * 1000
        if isinstance(result, dict) and result.get("success") is False:
            logger.warning("%s node=%s failed (%.0fms): %s", name, node_id, ms, result.get("error"))
        else:
            logger.info("%s node=%s ok (%.0fms)", name, node_id, ms)
        return result

    return wrapper
