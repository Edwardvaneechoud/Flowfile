"""Flow documents saved before a hub table was named by its name: the hub's row ids, converted at open.

Until this release a catalog reader on a hub table carried ``hub_table_id`` and ``hub_namespace_id``, and a catalog
writer into a hub schema ``hub_namespace_id``: ids of one hub's database, which mean nothing elsewhere and break when
a row is renumbered. A step now names a hub table by ``source: "hub"`` and its qualified name, resolved on the hub
when the flow runs. A document that still carries the ids is converted here while it loads, by asking the hub what
the ids name through the opening user's hub session; the next save writes the names. A hub that cannot be asked (no
user, no hub session, the hub down, the row gone) leaves the step on the hub with its bare table name and no qualified
name, after one warning, which the reader says at the node (``builders.catalog.unnamed_hub_table``) and a writer as a
write with no schema picked. This module goes after one public release.
"""

from flowfile_core.configs import logger
from flowfile_core.hub_client import catalog

READER_ID_FIELDS = ("hub_table_id", "hub_namespace_id")
WRITER_ID_FIELDS = ("hub_namespace_id",)


def convert_node_settings(node_type: str, setting_data: dict, user_id: int | None) -> None:
    """Rewrite a loaded node's raw ``setting_data`` from hub ids to names, in place; the id fields are dropped.

    Only ``catalog_reader`` and ``catalog_writer`` settings carry them; every other node type is left as it is.
    """
    if node_type == "catalog_reader":
        _convert_reader(setting_data, user_id)
    elif node_type == "catalog_writer":
        _convert_writer(setting_data, user_id)


def _convert_reader(settings: dict, user_id: int | None) -> None:
    table_id = settings.pop("hub_table_id", None)
    settings.pop("hub_namespace_id", None)
    if table_id is None:
        return
    settings.update({"source": "hub", "catalog_table_id": None, "catalog_namespace_id": None})
    try:
        table = catalog.table(_signed_in(user_id), table_id)
        qualified = table.get("qualified_name") or (
            f"{catalog.locate(user_id, table['namespace_id']).full_name}.{table['name']}"
        )
        settings["catalog_full_table_name"] = qualified
        settings["catalog_table_name"] = table["name"]
    except LookupError as e:
        logger.warning(f"A catalog reader names the hub's table {table_id} by an id that was not converted: {e}")
        settings["catalog_full_table_name"] = None


def _convert_writer(settings: dict, user_id: int | None) -> None:
    write_settings = settings.get("catalog_write_settings")
    if not isinstance(write_settings, dict):
        return
    namespace_id = write_settings.pop("hub_namespace_id", None)
    if namespace_id is None:
        return
    write_settings.update({"source": "hub", "namespace_id": None})
    try:
        write_settings["namespace_full_name"] = catalog.locate(_signed_in(user_id), namespace_id).full_name
    except LookupError as e:
        logger.warning(f"A catalog writer names the hub's schema {namespace_id} by an id that was not converted: {e}")
        write_settings["namespace_full_name"] = None


def _signed_in(user_id: int | None) -> int:
    if user_id is None:
        raise LookupError("the flow was opened for no user, so no hub session could be asked")
    return user_id
