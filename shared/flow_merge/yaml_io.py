"""The YAML styles Flowfile writes, without importing core.

A saved flow and a project file both write a list of at most ten scalars inline and every other list as a block.
A flow saved by the editor gets that style from a representer core registers on ``yaml.Dumper`` when it imports
``flow_graph.persistence``; a process that never imports core (a git merge driver) has no such representer, so
both styles here name their own dumper. A project file is also written without line wrapping.
"""

from __future__ import annotations

from typing import Any, Literal

import yaml


class ProjectDumper(yaml.Dumper):
    """PyYAML's dumper with the list style of flow and project files, so their bytes do not depend on what
    another module registered on ``yaml.Dumper``."""


def _represent_list(dumper: yaml.Dumper, data: list):
    flow = len(data) <= 10 and all(isinstance(item, int | str | float | bool | type(None)) for item in data)
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=flow)


ProjectDumper.add_representer(list, _represent_list)


def dump_yaml(data: Any) -> str:
    """The single canonical dumper used for every project file."""
    return yaml.dump(
        data, Dumper=ProjectDumper, default_flow_style=False, sort_keys=False, allow_unicode=True, width=4096
    )


def dump(data: Any, style: Literal["flow", "project"] = "flow") -> str:
    """``data`` as YAML in the style of a flow the editor saves (``flow``) or of a project file (``project``)."""
    if style == "project":
        return dump_yaml(data)
    return yaml.dump(data, Dumper=ProjectDumper, default_flow_style=False, sort_keys=False, allow_unicode=True)


def load(text: str) -> Any:
    """A YAML or JSON document as plain data; JSON is YAML. Text that does not parse is a ``ValueError``."""
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ValueError(f"not YAML: {e}") from None
