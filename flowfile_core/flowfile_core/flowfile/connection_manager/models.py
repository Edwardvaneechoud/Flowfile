from typing import Any
from dataclasses import dataclass


@dataclass
class Connection:
    group: str  # e.g. source-faker
    name: str  # e.g. source-faker-100000
    config_setting: Any
    type: str = None  # e.g. airbyte
