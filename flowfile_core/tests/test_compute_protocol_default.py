"""The compute protocol flag is on unless the environment turns it off.

``settings.FLOWFILE_COMPUTE_PROTOCOL`` reads ``FLOWFILE_COMPUTE_PROTOCOL`` when the module is imported, so each case
imports it in a fresh process given the variable as the case sets it.
"""

import os
import subprocess
import sys

import pytest

READ_THE_FLAG = "from flowfile_core.configs import settings; print('flag:', bool(settings.FLOWFILE_COMPUTE_PROTOCOL))"


def flag_in_a_fresh_process(value: str | None) -> bool:
    """The flag's value in a new interpreter whose environment has ``FLOWFILE_COMPUTE_PROTOCOL`` unset or ``value``."""
    env = {name: setting for name, setting in os.environ.items() if name != "FLOWFILE_COMPUTE_PROTOCOL"}
    if value is not None:
        env["FLOWFILE_COMPUTE_PROTOCOL"] = value
    result = subprocess.run(
        [sys.executable, "-c", READ_THE_FLAG], env=env, capture_output=True, text=True, timeout=300, check=True
    )
    (line,) = [line for line in result.stdout.splitlines() if line.startswith("flag: ")]
    return line == "flag: True"


def test_the_flag_is_on_when_the_variable_is_unset():
    assert flag_in_a_fresh_process(None) is True


@pytest.mark.parametrize("value", ["0", "false"])
def test_the_flag_is_off_when_the_variable_turns_it_off(value):
    assert flag_in_a_fresh_process(value) is False
