"""The consent file: round-trip, install-id lifecycle, and every unreadable shape.

Mirrors flowfile_worker/tests/test_pool.py::TestPersistence — same commented-YAML
contract, same "corrupt settings fall back to the safe default" expectation, with
the extra privacy rule that declining forgets the install id.
"""

from __future__ import annotations

import os
import uuid

import pytest

from shared import telemetry


def _read() -> str:
    return telemetry._settings_file().read_text(encoding="utf-8")


def _is_uuid4(value: str) -> bool:
    parsed = uuid.UUID(value)
    return parsed.version == 4 and str(parsed) == value


class TestRoundTrip:
    def test_granting_consent_persists_and_loads_back(self):
        assert telemetry.load_state() is None, "nothing is stored before the user is asked"
        result = telemetry.set_consent(True)
        assert result.status.consent is True
        assert result.persisted is True

        state = telemetry.load_state()
        assert state is not None
        assert state.consent is True
        assert _is_uuid4(state.install_id)
        assert telemetry.install_id() == state.install_id

    def test_saved_file_is_commented_yaml(self):
        telemetry.set_consent(True)
        content = _read()
        assert content.startswith("#"), "the file must carry its explanatory header"
        assert "# FLOWFILE_TELEMETRY=0 disables telemetry regardless of this file." in content
        assert "consent: true" in content
        assert telemetry._settings_file().suffix == ".yaml"

    def test_no_temp_file_is_left_behind(self):
        telemetry.set_consent(True)
        leftovers = list(telemetry._settings_file().parent.glob("*.tmp"))
        assert leftovers == [], f"the atomic write leaked a temp file: {leftovers}"

    def test_a_failed_replace_leaves_no_temp_file_holding_the_install_id(self, monkeypatch):
        def _boom(src, dst):
            raise OSError("read-only file system")

        monkeypatch.setattr(os, "replace", _boom)
        assert telemetry.set_consent(True).persisted is False

        leftovers = list(telemetry._settings_file().parent.glob("*.tmp"))
        assert leftovers == [], f"a write that did not stick left an on-disk artifact: {leftovers}"

    def test_regranting_keeps_the_same_install_id(self):
        telemetry.set_consent(True)
        first = telemetry.install_id()
        telemetry._reset_for_tests()
        telemetry.set_consent(True)
        assert telemetry.install_id() == first


class TestInstallIdLifecycle:
    def test_declining_drops_the_install_id(self):
        telemetry.set_consent(True)
        assert telemetry.install_id() is not None

        telemetry.set_consent(False)
        content = _read()
        assert "consent: false" in content
        assert "install_id" not in content, "declining must forget the identifier, not park it"

        state = telemetry.load_state()
        assert state is not None
        assert state.consent is False
        assert state.install_id is None
        assert telemetry.install_id() is None

    def test_regranting_after_declining_mints_a_new_id(self):
        telemetry.set_consent(True)
        first = telemetry.install_id()
        telemetry.set_consent(False)
        telemetry.set_consent(True)
        second = telemetry.install_id()

        assert _is_uuid4(second)
        assert second != first, "an opt-out/opt-in cycle must not resurrect the old identifier"

    def test_declining_first_stores_no_id_at_all(self):
        telemetry.set_consent(False)
        assert "install_id" not in _read()
        assert telemetry.install_id() is None


class TestUnreadableFiles:
    @pytest.mark.parametrize(
        "content",
        [
            "[broken",  # YAML parse error
            "just a string",  # valid YAML, wrong shape
            "- a\n- b\n",  # valid YAML list
            "",  # empty file -> None document
            "install_id: abc\n",  # dict without the consent key
        ],
    )
    def test_unusable_content_loads_as_none(self, content):
        telemetry._settings_file().write_text(content, encoding="utf-8")
        assert telemetry.load_state() is None
        assert telemetry.consent() is None, "an unreadable file must read as 'never asked'"

    def test_missing_file_loads_as_none(self):
        assert not telemetry._settings_file().exists()
        assert telemetry.load_state() is None

    def test_non_string_install_id_is_ignored(self):
        telemetry._settings_file().write_text("consent: true\ninstall_id: 42\n", encoding="utf-8")
        state = telemetry.load_state()
        assert state is not None
        assert state.consent is True
        assert state.install_id is None


class TestWriteFailures:
    def test_unwritable_target_returns_false_without_raising(self, monkeypatch):
        def _boom(src, dst):
            raise OSError("read-only file system")

        monkeypatch.setattr(os, "replace", _boom)
        assert telemetry.persist_state(True, str(uuid.uuid4())) is False

    def test_set_consent_survives_a_failed_write(self, monkeypatch):
        def _boom(src, dst):
            raise OSError("read-only file system")

        monkeypatch.setattr(os, "replace", _boom)
        result = telemetry.set_consent(True)
        assert result.status.consent is None, "a write that failed must not be reported as consent"
        assert result.persisted is False, "the caller must be able to see the write did not stick"
        assert telemetry.is_enabled() is False

    def test_a_stored_state_that_does_not_match_the_request_is_not_persisted(self, monkeypatch):
        telemetry.set_consent(True)

        def _silently_drop(consent, install_id):
            return True  # claims success while leaving the old file in place

        monkeypatch.setattr(telemetry, "persist_state", _silently_drop)
        result = telemetry.set_consent(False)
        assert result.persisted is False, "a write that claims success but did not land is still a failure"
        assert result.status.consent is True
