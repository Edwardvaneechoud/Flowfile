"""``refuse_in_process_on_server``: a process that holds a server's secrets never runs Polars Code."""

import pytest

from shared.polars_code_parser import IN_PROCESS_REFUSED_MESSAGE, refuse_in_process_on_server


@pytest.mark.parametrize("mode", ["docker", "server"])
def test_it_refuses_on_a_multi_user_server(mode, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", mode)

    with pytest.raises(PermissionError, match=IN_PROCESS_REFUSED_MESSAGE):
        refuse_in_process_on_server()


@pytest.mark.parametrize("mode", ["electron", "package", None])
def test_it_lets_one_persons_machine_through(mode, monkeypatch):
    if mode is None:
        monkeypatch.delenv("FLOWFILE_MODE", raising=False)
    else:
        monkeypatch.setenv("FLOWFILE_MODE", mode)

    refuse_in_process_on_server()
