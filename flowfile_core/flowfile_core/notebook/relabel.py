"""Relabel a serialized flow onto provenance node ids: moved to ``shared.flow_merge.relabel``, re-exported here.

A notebook push runs every cell into a fresh graph, so its node ids are arbitrary. Before reconciling with the
canvas, the save-format payload is rewritten so each node carries the canvas id of the node its cell rendered.
"""

from shared.flow_merge.relabel import (  # noqa: F401
    _SETTING_ID_KEYS,
    _SETTING_ID_LIST_KEYS,
    provenance_mapping,
    relabel,
)
