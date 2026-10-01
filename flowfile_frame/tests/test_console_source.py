"""Console source recovery: when the hook is installed, what it keeps, and which fragment answers for a function."""

import code
import json
import os
import subprocess
import sys
from typing import Any

import pytest

from flowfile_frame import _console_source
from flowfile_frame._console_source import console_function_source, install_hook


def _console(*fragments: str) -> dict[str, Any]:
    """Run each fragment through ``code.InteractiveConsole`` as the ``code`` module's REPL does."""
    install_hook()  # pytest is not a console, so importing flowfile_frame left it out
    namespace: dict[str, Any] = {}
    interpreter = code.InteractiveConsole(namespace)
    for fragment in fragments:
        assert interpreter.runsource(fragment, "<console>", "exec") is False
    return namespace


def test_a_stale_lambda_keeps_its_own_default_when_a_newer_one_differs_only_there():
    namespace = _console(
        "dbl = lambda x, k=1: x * 2 * k\n",
        "old = dbl\n",
        "dbl = lambda x, k=5: x * 2 * k\n",
        "import flowfile_frame as ff\nimport polars as pl\n"
        'out = ff.from_dict({"a": [1.0, 2.5]}).with_columns('
        'ff.col("a").map_elements(old, return_dtype=pl.Float64).alias("b"))\n',
    )
    assert namespace["old"].__code__ == namespace["dbl"].__code__

    polars_code = namespace["out"].get_node_settings().setting_input.polars_code_input.polars_code
    assert "serialized_value" not in polars_code
    assert "k=1" in polars_code and "k=5" not in polars_code
    assert namespace["out"].collect()["b"].to_list() == [2.0, 5.0]


@pytest.mark.parametrize(
    "header, newer_header",
    [("(x, k=1)", "(x, k=True)"), ("(x, *, how=None)", "(x, *, how='left')"), ("(x, k=(1, -2.5))", "(x, k=(1, 2))")],
)
def test_a_function_is_not_answered_by_a_redefinition_that_differs_only_in_a_default(header, newer_header):
    namespace = _console(
        f"def pick{header}:\n    return x\n", "older = pick\n", f"def pick{newer_header}:\n    return x\n"
    )
    assert namespace["older"].__code__ == namespace["pick"].__code__

    assert console_function_source(namespace["older"]) == f"def pick{header}:\n    return x\n"
    assert console_function_source(namespace["pick"]) == f"def pick{newer_header}:\n    return x\n"


def test_a_default_that_is_not_a_literal_never_matches():
    namespace = _console("scale = 3\nscaled = lambda x, k=scale: x * k\n")
    assert console_function_source(namespace["scaled"]) is None


def test_a_mutated_default_no_longer_matches_its_text():
    namespace = _console("def collect_into(x, acc=[]):\n    return acc\n")
    namespace["collect_into"].__defaults__[0].append(1)
    assert console_function_source(namespace["collect_into"]) is None


def test_only_fragments_that_could_define_a_function_are_kept():
    _console("unkept_console_marker = 1\n", "kept_console_marker = lambda x: x\n", "def kept_def_marker():\n    pass\n")
    kept = _console_source._FRAGMENTS["<console>"]
    assert not any(b"unkept_console_marker" in fragment for fragment in kept)
    assert any(b"kept_console_marker" in fragment for fragment in kept)
    assert any(b"kept_def_marker" in fragment for fragment in kept)


_HOOK_PROBE = """
import json, sys, types
{prelude}
from flowfile_frame import _console_source


def define(name):
    namespace = {{}}
    exec(compile(f"def {{name}}(x):\\n    return x\\n", "<console>", "exec"), namespace)
    return _console_source.console_function_source(namespace[name])


hooked = _console_source._hooked
before = define("made_at_import")
_console_source.install_hook()
_console_source.install_hook()
print(json.dumps({{"hooked": hooked, "before": before, "after": define("made_after_install")}}))
"""


@pytest.mark.parametrize(
    "env_value, prelude, hooked",
    [
        (None, "", False),
        ("0", "", False),
        ("1", "", True),
        ("On", "", True),
        (None, "sys.modules['pydevconsole'] = types.ModuleType('pydevconsole')", True),
    ],
)
def test_importing_installs_the_hook_only_in_a_console_or_when_asked(env_value, prelude, hooked):
    env = {key: value for key, value in os.environ.items() if key != "FLOWFILE_CONSOLE_SOURCE"}
    if env_value is not None:
        env["FLOWFILE_CONSOLE_SOURCE"] = env_value
    script = _HOOK_PROBE.format(prelude=prelude)
    run = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, env=env, timeout=300)

    assert run.returncode == 0, run.stderr[-3000:]
    result = json.loads(run.stdout.splitlines()[-1])
    assert result["hooked"] is hooked
    assert result["before"] == ("def made_at_import(x):\n    return x\n" if hooked else None)
    assert result["after"] == "def made_after_install(x):\n    return x\n"
