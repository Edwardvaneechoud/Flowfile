"""Serialisation of a `FlowGraph`: the YAML file, `FlowfileData`, frontend payloads and code export."""

import datetime
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.graph_tree.graph_tree import render_flow
from flowfile_core.schemas import schemas
from flowfile_core.schemas.output_model import NodeData
from shared._version import get_version

if TYPE_CHECKING:
    pass


__version__ = get_version()


def represent_list_json(dumper, data):
    """Use inline style for short simple lists, block style for complex ones."""
    if len(data) <= 10 and all(isinstance(item, int | str | float | bool | type(None)) for item in data):
        return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=True)
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=False)


yaml.add_representer(list, represent_list_json)


class PersistenceMixin(GraphMixinBase):
    def print_tree(self):
        """Print the graph top to bottom, one node per line, with lanes where it branches and merges."""
        if not self._node_db:
            self.flow_logger.info("Empty flow graph")
            return
        print(render_flow(self.nodes))

    def get_node_data(self, node_id: int, include_example: bool = True) -> NodeData:
        """Retrieves all data needed to render a node in the UI.

        Args:
            node_id: The ID of the node.
            include_example: Whether to include data samples in the result.

        Returns:
            A NodeData object, or None if the node is not found.
        """
        node = self._node_db[node_id]
        return node.get_node_data(flow_id=self.flow_id, include_example=include_example)

    def get_flowfile_data(self) -> schemas.FlowfileData:
        start_node_ids = {v.node_id for v in self._flow_starts}

        nodes = []
        for node in self.nodes:
            node_info = node.get_node_information()
            flowfile_node = schemas.FlowfileNode(
                id=node_info.id,
                type=node_info.type,
                is_start_node=node.node_id in start_node_ids,
                description=node_info.description,
                description_is_auto_generated=not getattr(node.setting_input, "description", ""),
                node_reference=node_info.node_reference,
                x_position=int(node_info.x_position),
                y_position=int(node_info.y_position),
                group_id=node_info.group_id,
                left_input_id=node_info.left_input_id,
                right_input_id=node_info.right_input_id,
                # A node whose last main input was removed holds [] where a never-connected one holds None.
                input_ids=node_info.input_ids or None,
                outputs=node_info.outputs,
                output_handles=node_info.output_handles,
                input_connections=node_info.input_connections,
                setting_input=node_info.setting_input,
            )
            nodes.append(flowfile_node)

        settings = schemas.FlowfileSettings(
            description=self.flow_settings.description,
            execution_mode=self.flow_settings.execution_mode,
            execution_location=self.flow_settings.execution_location,
            auto_save=self.flow_settings.auto_save,
            show_detailed_progress=self.flow_settings.show_detailed_progress,
            validate_settings=self.flow_settings.validate_settings,
            max_parallel_workers=self.flow_settings.max_parallel_workers,
            source_registration_id=self.flow_settings.source_registration_id,
            parameters=self.flow_settings.parameters,
        )
        # Persist only groups that still have members (prune orphans).
        groups = [
            schemas.FlowfileGroup(**self._groups[group_id].model_dump())
            for group_id in self._groups
            if self._member_node_ids(group_id) or self._child_group_ids(group_id)
        ]
        return schemas.FlowfileData(
            flowfile_version=__version__,
            flowfile_id=self.flow_id,
            flowfile_name=self.__name__,
            flowfile_settings=settings,
            nodes=nodes,
            groups=groups,
            comments=self._serialized_comments(),
        )

    def get_node_storage(self) -> schemas.FlowInformation:
        """Serializes the entire graph's state into a storable format.

        Returns:
            A FlowInformation object representing the complete graph.
        """
        node_information = {
            node.node_id: node.get_node_information() for node in self.nodes if node.is_setup and node.is_correct
        }

        return schemas.FlowInformation(
            flow_id=self.flow_id,
            flow_name=self.__name__,
            flow_settings=self.flow_settings,
            data=node_information,
            node_starts=[v.node_id for v in self._flow_starts],
            node_connections=self.node_connections,
        )

    def _handle_flow_renaming(self, new_name: str, new_path: Path):
        """Adopt the target file's stem as the flow name, but only when a save relocates the flow.

        A graph without a path (``FlowGraph()``, a Python-built flow) has never been saved, so
        its first save relocates it and adopts the stem. A same-path save must never rename —
        the name can have been set from the catalog (``POST /editor/rename_flow/``) and the
        file stem is not authoritative.
        """
        if not self.flow_settings:
            return
        current_path = self.flow_settings.path
        if not current_path or Path(current_path).absolute() != new_path.absolute():
            self.__name__ = new_name
            self.flow_settings.save_location = str(new_path.absolute())
            self.flow_settings.name = new_name
        elif not self.flow_settings.save_location:
            # Back-fill where the flow lives, never what it is called.
            self.flow_settings.save_location = str(new_path.absolute())
            if not self.flow_settings.name:
                self.__name__ = new_name
                self.flow_settings.name = new_name

    def save_flow(self, flow_path: str):
        """Saves the current state of the flow graph to a file.

        Supports multiple formats based on file extension:
        - .yaml / .yml: New YAML format
        - .json: JSON format

        Args:
            flow_path: The path where the flow file will be saved.
        """
        with self.edit_lock(bounded=False):
            self._save_flow(flow_path)

    def _save_flow(self, flow_path: str):
        logger.info("Saving flow to %s", flow_path)
        path = Path(flow_path)
        os.makedirs(path.parent, exist_ok=True)
        suffix = path.suffix.lower()
        new_flow_name = path.name.replace(suffix, "")
        self._handle_flow_renaming(new_flow_name, path)
        self.flow_settings.modified_on = datetime.datetime.now().timestamp()
        self._validate_registration_ownership(flow_path)
        try:
            if suffix == ".flowfile":
                raise DeprecationWarning(
                    "The .flowfile format is deprecated. Please use .yaml or .json formats.\n\n"
                    "Or stay on.1 if you still need .flowfile support.\n\n"
                )
            elif suffix in (".yaml", ".yml"):
                flowfile_data = self.get_flowfile_data()
                data = flowfile_data.model_dump(mode="json")
                with open(flow_path, "w", encoding="utf-8") as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            elif suffix == ".json":
                flowfile_data = self.get_flowfile_data()
                data = flowfile_data.model_dump(mode="json")
                with open(flow_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

            else:
                flowfile_data = self.get_flowfile_data()
                logger.warning(f"Unknown file extension {suffix}. Defaulting to YAML format.")
                data = flowfile_data.model_dump(mode="json")
                with open(flow_path, "w", encoding="utf-8") as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        except Exception as e:
            logger.error(f"Error saving flow: {e}")
            raise

        self.flow_settings.path = flow_path
        self._sync_catalog_read_links()
        # Record the current state as the clean baseline for dirty tracking
        self.mark_as_saved()

    def _owns_registration(self, repo: SQLAlchemyCatalogRepository, registration_id: int, own_path: str | None) -> bool:
        """Whether ``registration_id`` really points at ``own_path``, this flow's file.

        Registration ids are machine-local and can alias another flow: SQLite reuses
        rowids after a registration is deleted, and a copied YAML carries the original's
        id. Since the read-link sync prunes, syncing under an aliased id would wipe the
        other flow's lineage. Renames never move ``flow_path``, so path identity is the
        authoritative check.
        """
        registration = repo.get_flow(registration_id)
        if registration is None or not registration.flow_path or not own_path:
            return False
        return os.path.realpath(registration.flow_path) == os.path.realpath(own_path)

    def _validate_registration_ownership(self, flow_path: str) -> None:
        """Re-point (or drop) a ``source_registration_id`` that isn't this file's own.

        Runs before the save serializes settings, so the corrected id is what lands in the
        file. Clearing it after the write would be resurrected on the next open —
        ``resolve_source_registration_id`` keeps any non-None stored id — leaving the flow's
        read-link sync permanently wedged. A transient DB failure never drops the id.
        """
        registration_id = getattr(self._flow_settings, "source_registration_id", None)
        if not registration_id:
            return
        try:
            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                if self._owns_registration(repo, registration_id, flow_path):
                    return
                own_id = CatalogService(repo).resolve_registration_id(flow_path)
            logger.warning(
                "Registration %s does not belong to flow '%s' (reused id or copied flow file); re-resolved to %s",
                registration_id,
                flow_path,
                own_id,
            )
            self._flow_settings.source_registration_id = own_id
        except Exception:
            logger.warning("Could not validate the catalog registration of flow '%s'", flow_path, exc_info=True)

    def _sync_catalog_read_links(self):
        """Record which catalog tables this flow reads from.

        Scans all nodes for catalog_reader types and replaces the flow's read
        links with exactly that set, so removing a reader drops its link. Runs
        at save time so that source_registration_id is guaranteed to be set.
        """
        registration_id = self._flow_settings.source_registration_id
        logger.debug("Found registration_id %s", registration_id)
        if not registration_id:
            return

        table_ids = set()
        for node in self.nodes:
            if node.node_type != "catalog_reader":
                continue
            setting = node.setting_input
            table_id = getattr(setting, "catalog_table_id", None)
            if table_id:
                table_ids.add(table_id)

        try:
            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                own_path = self._flow_settings.path or self._flow_settings.save_location
                if not self._owns_registration(repo, registration_id, own_path):
                    # Backstop for callers that set the id without going through save_flow.
                    logger.warning(
                        "Registration %s does not belong to flow '%s' (reused id or copied flow file); "
                        "clearing it and skipping the catalog read-link sync",
                        registration_id,
                        own_path,
                    )
                    self._flow_settings.source_registration_id = None
                    return
                repo.replace_read_links(registration_id, table_ids)
        except Exception:
            logger.warning(
                "Failed to record catalog read links for tables %s",
                table_ids,
                exc_info=True,
            )

    def get_frontend_data(self) -> dict:
        """Formats the graph structure into a JSON-like dictionary for a specific legacy frontend.

        This method transforms the graph's state into a format compatible with the
        Drawflow.js library.

        Returns:
            A dictionary representing the graph in Drawflow format.
        """
        result = {"Home": {"data": {}}}
        flow_info: schemas.FlowInformation = self.get_node_storage()

        for node_id, node_info in flow_info.data.items():
            if node_info.is_setup:
                try:
                    pos_x = node_info.data.pos_x
                    pos_y = node_info.data.pos_y
                    result["Home"]["data"][str(node_id)] = {
                        "id": node_info.id,
                        "name": node_info.type,
                        "data": {},
                        "class": node_info.type,
                        "html": node_info.type,
                        "typenode": "vue",
                        "inputs": {},
                        "outputs": {},
                        "pos_x": pos_x,
                        "pos_y": pos_y,
                    }
                except Exception as e:
                    logger.error(e)
            if node_info.outputs:
                outputs = {o: 0 for o in node_info.outputs}
                for o in node_info.outputs:
                    outputs[o] += 1
                connections = []
                for output_node_id, _n_connections in outputs.items():
                    leading_to_node = self.get_node(output_node_id)
                    input_types = leading_to_node.get_input_type(node_info.id)
                    for input_type in input_types:
                        if input_type == "main":
                            input_frontend_id = "input_1"
                        elif input_type == "right":
                            input_frontend_id = "input_2"
                        elif input_type == "left":
                            input_frontend_id = "input_3"
                        else:
                            input_frontend_id = "input_1"
                        connection = {"node": str(output_node_id), "input": input_frontend_id}
                        connections.append(connection)

                result["Home"]["data"][str(node_id)]["outputs"]["output_1"] = {"connections": connections}
            else:
                result["Home"]["data"][str(node_id)]["outputs"] = {"output_1": {"connections": []}}

            if (
                node_info.left_input_id is not None
                or node_info.right_input_id is not None
                or node_info.input_ids is not None
            ):
                main_inputs = node_info.main_input_ids
                result["Home"]["data"][str(node_id)]["inputs"]["input_1"] = {
                    "connections": [{"node": str(main_node_id), "input": "output_1"} for main_node_id in main_inputs]
                }
                if node_info.right_input_id is not None:
                    result["Home"]["data"][str(node_id)]["inputs"]["input_2"] = {
                        "connections": [{"node": str(node_info.right_input_id), "input": "output_1"}]
                    }
                if node_info.left_input_id is not None:
                    result["Home"]["data"][str(node_id)]["inputs"]["input_3"] = {
                        "connections": [{"node": str(node_info.left_input_id), "input": "output_1"}]
                    }
        return result

    def get_vue_flow_input(self) -> schemas.VueFlowInput:
        """Formats the graph's nodes and edges into a schema suitable for the VueFlow frontend.

        Returns:
            A VueFlowInput object.
        """
        edges: list[schemas.NodeEdge] = []
        nodes: list[schemas.NodeInput] = []
        for node in self.nodes:
            nodes.append(node.get_node_input())
            edges.extend(node.get_edge_input())
        groups = [
            schemas.FlowfileGroup(**self._groups[group_id].model_dump())
            for group_id in self._groups
            if self._member_node_ids(group_id) or self._child_group_ids(group_id)
        ]
        return schemas.VueFlowInput(
            node_edges=edges, node_inputs=nodes, groups=groups, comments=self._serialized_comments()
        )

    def generate_code(self):
        """Generates code for the flow graph.
        This method exports the flow graph to a Polars-compatible format.
        """
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

        print(export_flow_to_polars(self))
