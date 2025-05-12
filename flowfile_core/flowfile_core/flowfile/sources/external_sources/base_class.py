from abc import ABC, abstractmethod
from typing import Generator, Callable, List, Any, Optional, Dict
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
import polars as pl


class ExternalDataSource(ABC):
    schema: Optional[List[FlowfileColumn]]
    data_getter: Optional[Callable]
    is_collected: bool
    cache_store: Any
    _type: str
    initial_data_getter: Optional[Callable]

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_initial_data(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_iter(self) -> Generator[Dict[str, Any], None, None]:
        pass

    @abstractmethod
    def get_sample(self, n: int = 10000) -> Generator[Dict[str, Any], None, None]:
        pass

    @abstractmethod
    def get_pl_df(self) -> pl.DataFrame:
        pass

    @staticmethod
    @abstractmethod
    def parse_schema(*args, **kwargs) -> List[FlowfileColumn]:
        pass

