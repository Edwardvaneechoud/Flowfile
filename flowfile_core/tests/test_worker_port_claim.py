"""The suite never silently reuses whatever answers on the default worker port.

A developer's live worker on 63579 may run other code than the checkout under test, so conftest moves
the session to a free port unless reuse is asked for explicitly.
"""

import os

import pytest

from tests import conftest


@pytest.fixture
def clean_env(monkeypatch):
    monkeypatch.delenv("FLOWFILE_WORKER_PORT", raising=False)
    monkeypatch.delenv("FLOWFILE_TEST_REUSE_WORKER", raising=False)
    return monkeypatch


def _default_port_taken(monkeypatch, taken: bool):
    monkeypatch.setattr(conftest, "worker_is_listening", lambda port=None: taken and port == 63579)


def test_taken_default_port_moves_the_session_to_a_free_port(clean_env):
    _default_port_taken(clean_env, True)

    note = conftest._claim_worker_port()

    port = int(os.environ["FLOWFILE_WORKER_PORT"])
    assert port != conftest.DEFAULT_WORKER_PORT
    assert f"uses {port}" in note


def test_free_default_port_is_kept(clean_env):
    _default_port_taken(clean_env, False)

    assert conftest._claim_worker_port() is None
    assert "FLOWFILE_WORKER_PORT" not in os.environ


def test_reuse_opt_in_keeps_the_running_worker(clean_env):
    _default_port_taken(clean_env, True)
    clean_env.setenv("FLOWFILE_TEST_REUSE_WORKER", "1")

    assert conftest._claim_worker_port() is None
    assert "FLOWFILE_WORKER_PORT" not in os.environ


def test_explicit_worker_port_is_used_as_is(clean_env):
    _default_port_taken(clean_env, True)
    clean_env.setenv("FLOWFILE_WORKER_PORT", "63579")

    assert conftest._claim_worker_port() is None
    assert os.environ["FLOWFILE_WORKER_PORT"] == "63579"
