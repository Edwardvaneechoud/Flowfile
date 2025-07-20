from flowfile_core.flowfile.sources.external_sources.custom_external_sources.external_source import CustomExternalSource


def data_source_factory(source_type: str, **kwargs) -> CustomExternalSource:
    """
    Factory function to generate either CustomExternalSource .

    Args:
        source_type (str): The type of source to create ("custom").
        **kwargs: The keyword arguments required for the specific source type.

    Returns:
        Union[CustomExternalSource]: An instance of the selected data source type.
    """
    if source_type == "custom":
        return CustomExternalSource(**kwargs)
    else:
        raise ValueError(f"Unknown source type: {source_type}")

