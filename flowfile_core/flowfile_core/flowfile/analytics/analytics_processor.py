from flowfile_core.configs import logger
from flowfile_core.flowfile.analytics.graphic_walker import convert_ff_columns_to_gw_fields
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas.analysis_schemas.graphic_walker_schemas import (
    DataModel,
    GraphicWalkerInput,
    MutField,
    ViewField,
)
from flowfile_core.schemas.input_schema import NodeExploreData

GW_PSEUDO_FIDS = frozenset({"gw_count_fid", "gw_mea_key_fid", "gw_mea_val_fid"})


class AnalyticsProcessor:
    @staticmethod
    def process_graphic_walker_input(node_step: FlowNode) -> NodeExploreData:
        node_explore_data: NodeExploreData = node_step.setting_input
        if hasattr(node_explore_data, "graphic_walker_input"):
            graphic_walker_input = node_explore_data.graphic_walker_input
        else:
            logger.error("NodeExploreData is not an instance of GraphicWalkerInput.")
            raise ValueError("NodeExploreData is not an instance of GraphicWalkerInput.")
        graphic_walker_input = AnalyticsProcessor.create_graphic_walker_input(node_step, graphic_walker_input)
        node_explore_data.is_setup = True
        node_explore_data.graphic_walker_input = graphic_walker_input
        return node_explore_data

    @staticmethod
    def create_graphic_walker_input(
        node_step: FlowNode, graphic_walker_input: GraphicWalkerInput = None
    ) -> GraphicWalkerInput:
        """Build the explorer's setup payload: fields and specs, never rows.

        Rows are not shipped to the browser — Graphic Walker aggregates on the
        worker via ``/analysis_data/compute``, and takes its authoritative field
        schema from ``/analysis_data/fields`` (``polars_gw.get_fields``). The
        fields here exist only so saved specs can be reconciled against the
        node's current schema.
        """
        fields = convert_ff_columns_to_gw_fields(node_step.get_predicted_schema())
        data_model = DataModel(data=[], fields=fields)
        if graphic_walker_input:
            if graphic_walker_input.specList:
                validate_spec_lists_with_data_model(graphic_walker_input.specList, data_model)
            graphic_walker_input.dataModel = data_model
        else:
            graphic_walker_input = GraphicWalkerInput(dataModel=data_model)
        return graphic_walker_input


def check_if_field_in_spec_list_encodings(spec_list: dict, field_name: str) -> bool:
    for encoding in spec_list["encodings"]["dimensions"].values():
        if field_name in encoding["fid"]:
            return True
    for encoding in spec_list["encodings"]["measures"].values():
        if field_name in encoding["fid"]:
            return True
    return False


def get_existing_encoding_fields(spec_list: dict) -> set[str]:
    """
    Get the existing encoding fields from the spec_list.

    Args:
        spec_list (Dict): The spec list to check.

    Returns:
        Set[str]: A set of existing encoding fields.
    """
    dimensions = {encoding["fid"] for encoding in spec_list["encodings"]["dimensions"]}
    measures = {encoding["fid"] for encoding in spec_list["encodings"]["measures"]}
    return dimensions.union(measures)


def transform_mut_field_to_view_field(mut_field: MutField) -> ViewField:
    view_field = ViewField(**mut_field.model_dump())
    return view_field


def add_field_to_spec_list(spec_list: dict, mut_field: MutField) -> None:
    """
    Add a field to the spec_list.

    Args:
        spec_list (Dict): The spec list to modify.
        mut_field (MutField): The field to add.

    Returns:
        None
    """
    view_field = transform_mut_field_to_view_field(mut_field)
    if mut_field.analyticType == "measure":
        spec_list["encodings"]["measures"].append(view_field.model_dump_dict())
    else:
        spec_list["encodings"]["dimensions"].append(view_field.model_dump_dict())


def is_data_column_field(spec_field: dict) -> bool:
    """A spec entry backed by a real column, not a Graphic Walker pseudo or computed field."""
    return not spec_field.get("computed") and spec_field.get("fid") not in GW_PSEUDO_FIDS


def sync_spec_field_with_mut_field(spec_field: dict, mut_field: MutField) -> None:
    """Retype a spec entry only when the column's dtype class changed.

    Only ``semanticType`` is compared, because the analytic type and the shelf a field
    sits on are the user's choice (Graphic Walker's "to dimension" / "to measure" move an
    entry without touching its semantic type). ``ordinal`` is likewise always a manual
    override — the backend only ever derives nominal, quantitative or temporal.
    """
    spec_semantic_type = spec_field.get("semanticType")
    if spec_semantic_type == mut_field.semanticType or spec_semantic_type == "ordinal":
        return
    spec_field["semanticType"] = mut_field.semanticType
    spec_field["analyticType"] = mut_field.analyticType
    if mut_field.analyticType == "measure":
        if not spec_field.get("aggName"):
            spec_field["aggName"] = "sum"
    else:
        spec_field.pop("aggName", None)


def reconcile_spec_list_with_data_model(spec_list: dict, data_model: DataModel) -> None:
    """Make the saved encodings follow the data: retype what changed, drop what is gone."""
    fields_by_fid = {field.fid: field for field in data_model.fields}
    encodings = spec_list["encodings"]
    for channel, spec_fields in encodings.items():
        kept = []
        for spec_field in spec_fields:
            if not is_data_column_field(spec_field):
                kept.append(spec_field)
                continue
            mut_field = fields_by_fid.get(spec_field.get("fid"))
            if mut_field is None:
                continue
            sync_spec_field_with_mut_field(spec_field, mut_field)
            kept.append(spec_field)
        encodings[channel] = kept
    shelved = encodings["dimensions"] + encodings["measures"]
    encodings["dimensions"] = [f for f in shelved if f.get("analyticType") != "measure"]
    encodings["measures"] = [f for f in shelved if f.get("analyticType") == "measure"]


def validate_spec_list_with_data_model_types(spec_list: dict, data_model: DataModel) -> None:
    """
    Validate the spec_list with the data model types.

    Existing entries are reconciled with the current fields (a retyped column updates
    everywhere it appears and lands on the matching shelf, a dropped column is removed
    from every encoding list) and new columns are appended.

    Args:
        spec_list (Dict): The spec list to validate.
        data_model (DataModel): The data model to validate against.

    Returns:
        bool: True if the spec list is valid, False otherwise.
    """
    reconcile_spec_list_with_data_model(spec_list, data_model)
    existing_encoding_fields = get_existing_encoding_fields(spec_list)
    for field in data_model.fields:
        if field.fid not in existing_encoding_fields:
            add_field_to_spec_list(spec_list, field)


def validate_spec_lists_with_data_model(spec_lists: list[dict], data_model: DataModel) -> None:
    """
    Validate the spec lists with the data model.

    Args:
        spec_lists (List[Dict]): The list of spec lists to validate.
        data_model (DataModel): The data model to validate against.

    Returns:
        List[Dict]: The validated spec lists.
    """
    for spec_list in spec_lists:
        validate_spec_list_with_data_model_types(spec_list, data_model)
