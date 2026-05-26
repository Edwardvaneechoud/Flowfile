"""Unit tests for cron schedule validators (pure, no DB)."""

from __future__ import annotations

import pytest

from flowfile_core.catalog.validators import (
    validate_cron_expression,
    validate_cron_timezone,
    validate_schedule_create,
    validate_schedule_update,
)


def _always(_table_id: int) -> bool:
    return True


class TestCronValidators:
    def test_valid_cron_create(self):
        # Should not raise.
        validate_schedule_create(
            "cron",
            None,
            None,
            None,
            _always,
            cron_expression="0 9 * * 1-5",
            cron_timezone="Europe/Amsterdam",
        )

    def test_cron_default_timezone_ok(self):
        # A missing timezone is allowed — the scheduler defaults it to UTC.
        validate_schedule_create("cron", None, None, None, _always, cron_expression="*/15 * * * *")

    @pytest.mark.parametrize(
        "expr",
        ["", "   ", "nonsense", "0 9 * *", "60 9 * * *", "0 0 9 * * *", "@daily"],
    )
    def test_invalid_cron_expression(self, expr):
        with pytest.raises(ValueError):
            validate_cron_expression(expr)

    def test_missing_cron_expression_in_create(self):
        with pytest.raises(ValueError):
            validate_schedule_create("cron", None, None, None, _always, cron_expression=None)

    def test_invalid_timezone(self):
        with pytest.raises(ValueError):
            validate_cron_timezone("Not/AZone")

    def test_blank_timezone_ok(self):
        validate_cron_timezone(None)
        validate_cron_timezone("")

    def test_unknown_schedule_type_rejected(self):
        with pytest.raises(ValueError):
            validate_schedule_create("weekly", None, None, None, _always)

    def test_update_accepts_cron(self):
        validate_schedule_update(None, cron_expression="0 0 * * *", cron_timezone="UTC")

    def test_update_rejects_bad_cron(self):
        with pytest.raises(ValueError):
            validate_schedule_update(None, cron_expression="nope")

    def test_update_rejects_bad_timezone(self):
        with pytest.raises(ValueError):
            validate_schedule_update(None, cron_timezone="Mars/Phobos")
