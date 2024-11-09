from typing import Dict, Any, Generator
from time import sleep
from flowfile_core.schemas.input_schema import SampleUsers
import requests


def getter(data: SampleUsers) -> Generator[Dict[str, Any], None, None]:
    index_pos = 0
    for i in range(data.size):
        sleep(0.01)
        response = requests.get("https://reqres.in/api/users").json()
        for v in response['data']:
            v['index'] = index_pos
            index_pos+=1
            yield v

