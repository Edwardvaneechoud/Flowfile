from flowfile_core.flowfile.sources.external_sources.airbyte_sources.settings import AirbyteSettings
from flowfile_core.flowfile.sources.external_sources.airbyte_sources.airbyte import AirbyteSource
import polars as pl


def test_read_airbyte():
    try:
        airbyte_settings = AirbyteSettings(**{'source_name': 'source-faker', 'stream': 'products', 'config_ref': None,
                                              'config': {'count': 1000, 'seed': -1, 'records_per_slice': 1000,
                                                         'always_updated': True, 'parallelism': 4}, 'fields': None,
                                              'enforce_full_refresh': True})
        s = AirbyteSource(airbyte_settings)
        data = s.get_pl_df()
        assert isinstance(data, pl.DataFrame)
    except Exception as e:
        print(e)
        assert False
