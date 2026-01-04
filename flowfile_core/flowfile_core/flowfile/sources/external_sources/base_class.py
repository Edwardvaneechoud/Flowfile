from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from typing import Any

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn


class ExternalDataSource(ABC):
    schema: list[FlowfileColumn] | None
    data_getter: Callable | None
    is_collected: bool
    cache_store: Any
    _type: str
    initial_data_getter: Callable | None

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_initial_data(self) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def get_iter(self) -> Generator[dict[str, Any], None, None]:
        pass

    @abstractmethod
    def get_sample(self, n: int = 10000) -> Generator[dict[str, Any], None, None]:
        pass

    @abstractmethod
    def get_pl_df(self) -> pl.DataFrame:
        pass

    @staticmethod
    @abstractmethod
    def parse_schema(*args, **kwargs) -> list[FlowfileColumn]:
        pass
