"""Exec-free palette template construction from AST node manifests.

Mirrors ``CustomNodeBase.to_node_template`` field-for-field so the palette can
be built at registry scan time without executing user code. Parity is enforced
by ``tests/flowfile/node_designer/test_manifest_template_parity.py``.
"""

from typing import TYPE_CHECKING

from flowfile_core.flowfile.node_designer.state import NodeManifest
from shared.node_designer.custom_node import palette_group

if TYPE_CHECKING:
    from flowfile_core.schemas.schemas import NodeTemplate


def manifest_to_template(manifest: NodeManifest, node_key: str) -> "NodeTemplate":
    from flowfile_core.schemas.schemas import NodeTemplate

    node_group, node_group_label = palette_group(manifest.node_category, manifest.node_group)
    return NodeTemplate(
        name=manifest.node_name or node_key,
        item=node_key,
        input=manifest.number_of_inputs,
        output=manifest.number_of_outputs,
        image=manifest.node_icon,
        node_group=node_group,
        node_group_label=node_group_label,
        drawer_title=manifest.title or "Custom Node",
        drawer_intro=manifest.intro or "A custom node for data processing",
        node_type=manifest.node_type,
        transform_type=manifest.transform_type,
        custom_node=True,
        execution_environment=manifest.environment.kind,
        dependencies=list(manifest.environment.dependencies) or None,
    )
