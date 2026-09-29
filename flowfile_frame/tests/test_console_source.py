"""Console source recovery: when the hook is installed, what it keeps, and which fragment answers for a function."""

import json
import os
import subprocess
import sys

import pytest

from .utils import console_namespace

from flowfile_frame import _console_source
from flowfile_frame._console_source import (
    console_class_source,
    console_function_source,
    console_import_for,
    install_hook,
)


_console = console_namespace  # the ``code`` module's REPL compiles under <console>


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


def test_only_fragments_that_could_define_a_function_or_class_or_import_are_kept():
    _console(
        "unkept_console_marker = 1\n",
        "kept_console_marker = lambda x: x\n",
        "def kept_def_marker():\n    pass\n",
        "class KeptClassMarker:\n    pass\n",
        "import json as kept_import_marker\n",
    )
    kept = _console_source._FRAGMENTS["<console>"]
    assert not any(b"unkept_console_marker" in fragment for fragment in kept)
    assert any(b"kept_console_marker" in fragment for fragment in kept)
    assert any(b"kept_def_marker" in fragment for fragment in kept)
    assert any(b"KeptClassMarker" in fragment for fragment in kept)
    assert any(b"kept_import_marker" in fragment for fragment in kept)


def test_a_class_with_methods_is_answered_by_the_selection_that_compiled_them():
    older = "import math\n\nclass Scaler:\n    factor = 2\n\n    def scale(self, x):\n        return x * self.factor\n"
    newer = "class Scaler:\n    factor = 3\n\n    def scale(self, x):\n        return x * self.factor * 1\n"
    namespace = _console(older, "kept = Scaler\n", newer)

    assert console_class_source(namespace["kept"]) == older
    assert console_class_source(namespace["Scaler"]) == newer
    assert console_class_source(int) is None  # not a console class


def test_a_class_without_methods_is_answered_by_the_newest_selection_defining_it():
    namespace = _console("class Plain:\n    factor = 2\n", "class Plain:\n    factor = 3\n")
    assert console_class_source(namespace["Plain"]) == "class Plain:\n    factor = 3\n"


@pytest.mark.parametrize(
    "source",
    [
        "class Named:\n    label = '{label}'\n\n    def run(self):\n        return 1\n",
        "from pydantic import BaseModel\n\nclass Named(BaseModel):\n    label: str = '{label}'\n\n    def run(self):\n        return 1\n",
    ],
    ids=["plain", "pydantic"],
)
def test_a_class_is_not_answered_by_a_redefinition_that_differs_only_in_a_literal(source):
    older, newer = source.format(label="old"), source.format(label="new")
    namespace = _console(older, "kept_named = Named\n", newer)
    assert namespace["kept_named"].run.__code__ == namespace["Named"].run.__code__

    assert console_class_source(namespace["kept_named"]) == older
    assert console_class_source(namespace["Named"]) == newer


def test_an_import_is_answered_by_the_console_statement_that_bound_it():
    namespace = _console("import json as kept_json\nfrom os import path as kept_path, sep\n")

    assert console_import_for("kept_json", namespace["kept_json"]) == "import json as kept_json"
    assert console_import_for("kept_path", namespace["kept_path"]) == "from os import path as kept_path"
    assert console_import_for("kept_json", namespace["kept_path"]) is None  # bound, but to something else


_SELECTION_PROBE = """
import code, json
namespace = {"__name__": "__main__"}
console = code.InteractiveConsole(namespace)
console.runsource(
    "from flowfile_frame import _console_source\\n\\nclass Probe:\\n    def run(self):\\n        return 1\\n",
    "<input>",
    "exec",
)
console.runsource("found = _console_source.console_class_source(Probe)\\n", "<input>", "exec")
print(json.dumps(namespace["found"]))
"""


def test_the_selection_running_when_the_hook_installs_stays_readable():
    env = {**os.environ, "FLOWFILE_CONSOLE_SOURCE": "1"}
    run = subprocess.run([sys.executable, "-c", _SELECTION_PROBE], capture_output=True, text=True, env=env, timeout=300)

    assert run.returncode == 0, run.stderr[-3000:]
    assert json.loads(run.stdout.splitlines()[-1]) == (
        "from flowfile_frame import _console_source\n\nclass Probe:\n    def run(self):\n        return 1\n"
    )


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
