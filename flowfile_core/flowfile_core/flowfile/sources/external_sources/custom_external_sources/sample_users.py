from typing import Dict, Any, Generator
from time import sleep
from flowfile_core.schemas.input_schema import SampleUsers
import requests


def getter(data: SampleUsers) -> Generator[Dict[str, Any], None, None]:
    """
    Sample users generator function. This is a minimal example of a generator function that yields user data and can
    be used in a flowfile. The function simulates a delay to mimic the behavior of an external data source.
    Args:
        data ():

    Returns:

    """
    index_pos = 0
    for i in range(data.size):
        sleep(0.01)
        headers = {
            'x-api-key': 'reqres-free-v1'
        }

        response = requests.get("https://reqres.in/api/users", headers=headers).json()
        for v in response['data']:
            v['index'] = index_pos
            index_pos += 1
            yield v

