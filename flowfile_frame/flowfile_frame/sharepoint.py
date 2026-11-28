"""Build SharePoint source nodes using saved connections."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame


def read_sharepoint(
    connection_id: int,
    *,
    drive_id: str | None = None,
    file_path: str | None = None,
    list_source: dict | None = None,
    sheet_name: str | None = None,
    csv_delimiter: str = ",",
    delta_generation: int | None = None,
    flow_graph=None,
) -> FlowFrame:
    """Read a document-library file or a SharePoint list using a saved connection.

    ``connection_id`` is the ID in Flowfile; ``file_path`` is relative to
    the document library (``drive_id``). Alternatively pass ``list_source``
    with ``site_id``, ``list_id``, internal ``columns`` and optional ``odata_filter``.
    Set ``delta_generation=0`` to opt in to incremental reads for a registered
    flow. Change the generation to start again with a full file read. No
    credentials are accepted here.
    """
    from flowfile_core.schemas.input_schema import NodeSharePointReader
    from flowfile_frame._identity import current_user_id
    from flowfile_frame.native import source_frame
    from flowfile_frame.utils import _implicit_graph, generate_node_id

    if flow_graph is None:
        flow_graph = _implicit_graph()
    node_id = generate_node_id()
    flow_graph.add_sharepoint_reader(
        NodeSharePointReader(
            flow_id=flow_graph.flow_id,
            node_id=node_id,
            user_id=current_user_id(),
            connection_id=connection_id,
            drive_id=drive_id,
            file_path=file_path,
            list_source=list_source,
            sheet_name=sheet_name,
            csv_delimiter=csv_delimiter,
            delta_generation=delta_generation,
        )
    )
    return source_frame(flow_graph, node_id)
