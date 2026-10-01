"""Keep credentials passed in a query string out of uvicorn's access log."""

import logging
import re

_QUERY_TOKEN = re.compile(r"([?&]access_token=)[^&\s]+")


class RedactQueryTokenFilter(logging.Filter):
    """Mask ``access_token`` query values in uvicorn access-log records.

    uvicorn logs the request line with the full query string as one of ``record.args``
    (``'%s - "%s %s HTTP/%s" %d'``), so the args are rewritten in place and keep their arity.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, tuple):
            record.args = tuple(
                _QUERY_TOKEN.sub(r"\1[REDACTED]", arg) if isinstance(arg, str) else arg for arg in record.args
            )
        return True


def install_access_log_redaction() -> None:
    """Attach the filter to ``uvicorn.access``; a logger filter survives uvicorn's dictConfig."""
    logging.getLogger("uvicorn.access").addFilter(RedactQueryTokenFilter())
