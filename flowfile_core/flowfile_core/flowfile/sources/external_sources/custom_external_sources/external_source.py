from typing import Any, Dict, Generator, List, Optional, Callable
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas import input_schema
from flowfile_core.flowfile.sources.external_sources.base_class import ExternalDataSource
import polars as pl


class CustomExternalSourceSettings:
    data_getter: Generator
    initial_data_getter: Optional[Callable] = None
    orientation: str = 'row'

    def __init__(self, data_getter: Generator, initial_data_getter: Optional[Callable] = None, orientation: str = 'row'):
        self.data_getter = data_getter
        self.initial_data_getter = initial_data_getter
        self.orientation = orientation


class CustomExternalSource(ExternalDataSource):
    data_getter: Generator = None
    schema: Optional[List[FlowfileColumn]] = None
    cache_store: List = None
    is_collected: bool = False

    def __init__(self, data_getter: Generator[Any, None, None],
                 initial_data_getter: Callable = None,
                 orientation: str = 'row',
                 schema: List = None,
                 **kwargs):
        self.cache_store = list()
        self.data_getter = data_getter
        self.collected = False
        if schema is not None:
            try:
                self.schema = self.parse_schema(schema)
            except ValueError:
                self.schema = None
        else:
            self.schema = None

        if not initial_data_getter and orientation == 'row':
            def initial_data_getter():
                if len(self.cache_store) == 0:
                    self.cache_store.append(next(data_getter, None))
                return self.cache_store
            self.initial_data_getter = initial_data_getter
        elif initial_data_getter:
            self.initial_data_getter = initial_data_getter
        elif self.schema:
            def initial_data_getter():
                return [{d.column_name: None for d in self.schema}]
            self.initial_data_getter = initial_data_getter
        else:
            self.initial_data_getter = None

    @staticmethod
    def parse_schema(schema: List[Any]) -> List[FlowfileColumn]:
        if len(schema) == 0:
            return []
        first_col = schema[0]
        if isinstance(first_col, dict):
            return [FlowfileColumn(**col) for col in schema]
        elif isinstance(first_col, (list, tuple)):
            return [FlowfileColumn.from_input(column_name=col[0], data_type=col[1]) for col in schema]
        elif isinstance(first_col, str):
            return [FlowfileColumn.from_input(column_name=col, data_type='varchar') for col in schema]
        elif isinstance(first_col, input_schema.MinimalFieldInfo):
            return [FlowfileColumn.from_input(column_name=col.name, data_type=col.data_type) for col in schema]
        elif isinstance(first_col, FlowfileColumn):
            return schema
        else:
            raise ValueError("Schema is not a valid type")

    def get_initial_data(self):
        if self.initial_data_getter:
            return self.initial_data_getter()
        return []

    def get_iter(self) -> Generator[Dict[str, Any], None, None]:
        if self.collected:
            return
        for data in self.cache_store:
            yield data
        for data in self.data_getter:
            self.cache_store.append(data)
            yield data
        self.is_collected = True
        return

    def get_sample(self, n: int = 10000):
        data = self.get_iter()
        for i in range(n):
            try:
                yield next(data)
            except StopIteration:
                break

    def get_pl_df(self) -> pl.DataFrame:
        data = self.get_iter()
        return pl.DataFrame(list(data))
