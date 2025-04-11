
from flowfile_core.flowfile.sources.external_sources.airbyte_sources.settings import airbyte_settings_from_config
import polars as pl
from flowfile_core.schemas import input_schema
from flowfile_core.flowfile.sources.external_sources.factory import data_source_factory

import pytest

try:
    from tests.flowfile_core_test_utils import (is_docker_available)
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    # noinspection PyUnresolvedReferences
    from flowfile_core_test_utils import (is_docker_available)


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_read_airbyte():
    try:
        settings = {'flow_id': 1, 'node_id': 1, 'cache_results': False, 'pos_x': 110.87272727272727, 'pos_y': 298.4,
                    'is_setup': True, 'description': '', 'node_type': 'airbyte_reader', 'source_settings': {
                'parsed_config': [
                    {'title': 'Count', 'type': 'integer', 'key': 'count', 'properties': [], 'required': False,
                     'description': 'How many users should be generated in total. The purchases table will be scaled to match, with 10 purchases created per 10 users. This setting does not apply to the products stream.',
                     'isOpen': False, 'airbyte_secret': False, 'input_value': 1000, 'default': 1000},
                    {'title': 'Seed', 'type': 'integer', 'key': 'seed', 'properties': [], 'required': False,
                     'description': 'Manually control the faker random seed to return the same values on subsequent runs (leave -1 for random)',
                     'isOpen': False, 'airbyte_secret': False, 'input_value': -1, 'default': -1},
                    {'title': 'Records Per Stream Slice', 'type': 'integer', 'key': 'records_per_slice',
                     'properties': [], 'required': False,
                     'description': 'How many fake records will be in each page (stream slice), before a state message is emitted?',
                     'isOpen': False, 'airbyte_secret': False, 'input_value': 1000, 'default': 1000},
                    {'title': 'Always Updated', 'type': 'boolean', 'key': 'always_updated', 'properties': [],
                     'required': False,
                     'description': 'Should the updated_at values for every record be new each sync?  Setting this to false will case the source to stop emitting records after COUNT records have been emitted.',
                     'isOpen': False, 'airbyte_secret': False, 'input_value': True, 'default': True},
                    {'title': 'Parallelism', 'type': 'integer', 'key': 'parallelism', 'properties': [],
                     'required': False,
                     'description': 'How many parallel workers should we use to generate fake data?  Choose a value equal to the number of CPUs you will allocate to this source.',
                     'isOpen': False, 'airbyte_secret': False, 'input_value': 4, 'default': 4}],
                'mapped_config_spec': {'count': 1000, 'seed': -1, 'records_per_slice': 1000, 'always_updated': True,
                                       'parallelism': 4}, 'config_mode': 'in_line', 'selected_stream': 'products',
                'source_name': 'faker', 'fields': [], 'version': '6.2.21'}}
        external_source_input = input_schema.NodeAirbyteReader(**settings)

        airbyte_settings = airbyte_settings_from_config(external_source_input.source_settings, flow_id=1,
                                                        node_id=1)

        airbyte_source = data_source_factory(source_type='airbyte', airbyte_settings=airbyte_settings)
        data = airbyte_source.get_pl_df()
        assert isinstance(data, pl.DataFrame)
    except Exception as e:
        print(e)
        assert False
