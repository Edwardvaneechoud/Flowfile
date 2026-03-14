from collections.abc import Generator
from time import sleep
from typing import Any

import requests

from flowfile_core.schemas.input_schema import SampleUsers


def getter(data: SampleUsers) -> Generator[dict[str, Any], None, None]:
    """
    Sample users generator function. This is a minimal example of a generator function that yields user data and can
    be used in a flowfile. The function simulates a delay to mimic the behavior of an external data source.
    Args:
        data ():

    Returns:

    """
    index_pos = 0
    for _i in range(data.size):
        sleep(0.01)
        headers = {"x-api-key": "reqres-free-v1"}

        response = requests.get("https://reqres.in/api/users", headers=headers).json()
        for v in response["data"]:
            v["index"] = index_pos
            index_pos += 1
            yield v
