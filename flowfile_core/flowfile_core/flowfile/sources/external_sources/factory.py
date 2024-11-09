from flowfile_core.flowfile.sources.external_sources.custom_external_sources.external_source import CustomExternalSource
from flowfile_core.flowfile.sources.external_sources.airbyte_sources.airbyte import AirbyteSource


def data_source_factory(source_type: str, **kwargs) -> CustomExternalSource | AirbyteSource:
    """
    Factory function to generate either CustomExternalSource or AirbyteSource.

    Args:
        source_type (str): The type of source to create ("custom" or "airbyte").
        **kwargs: The keyword arguments required for the specific source type.

    Returns:
        Union[CustomExternalSource, AirbyteSource]: An instance of the selected data source type.
    """
    if source_type == "custom":
        return CustomExternalSource(**kwargs)
    elif source_type == "airbyte":
        return AirbyteSource(**kwargs)
    else:
        raise ValueError(f"Unknown source type: {source_type}")

