"""Built-in node specs, grouped by domain."""

from flowfile_core.flowfile.node_registry.builtin import (
    database,
    io_nodes,
    ml,
    scripting,
    simple,
    special,
    streaming_sources,
)
from flowfile_core.flowfile.node_registry.spec import NodeSpec

ALL_SPECS: list[NodeSpec] = [
    *simple.SPECS,
    *io_nodes.SPECS,
    *database.SPECS,
    *streaming_sources.SPECS,
    *scripting.SPECS,
    *ml.SPECS,
    *special.SPECS,
]
