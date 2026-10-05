"""uvicorn access-log lines never carry an ``access_token`` query value."""

import logging

from uvicorn.logging import AccessFormatter

from flowfile_core.configs.access_log import RedactQueryTokenFilter


def _access_line(path: str) -> str:
    record = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        __file__,
        0,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:51234", "GET", path, "1.1", 200),
        None,
    )
    assert RedactQueryTokenFilter().filter(record)
    return AccessFormatter(fmt="%(client_addr)s %(request_line)s %(status_code)s", use_colors=False).format(record)


def test_access_token_value_is_redacted():
    line = _access_line("/logs/3?access_token=eyJhbGciOi.eyJzdWIiOi.sig-value&idle_timeout=5")
    assert "eyJhbGciOi" not in line
    assert "sig-value" not in line
    assert "/logs/3?access_token=[REDACTED]&idle_timeout=5" in line


def test_token_after_another_parameter_is_redacted():
    line = _access_line("/logs/3?idle_timeout=5&access_token=secret")
    assert "secret" not in line
    assert "idle_timeout=5&access_token=[REDACTED]" in line


def test_lines_without_a_token_are_untouched():
    assert "/editor/share_link?flow_id=1" in _access_line("/editor/share_link?flow_id=1")
    assert "/logs/3?my_access_token=keep" in _access_line("/logs/3?my_access_token=keep")
