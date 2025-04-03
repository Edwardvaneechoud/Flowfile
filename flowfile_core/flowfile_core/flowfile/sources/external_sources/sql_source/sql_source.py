from typing import Any, Dict, Generator, List, Optional, Literal
from polars import DataFrame
import polars as pl
from flowfile_core.configs import logger

from flowfile_core.flowfile.sources.external_sources.base_class import ExternalDataSource


QueryMode = Literal['table', 'query']


class SqlSource(ExternalDataSource):
    table_name: Optional[str] = None
    connection_string: Optional[str]
    query: Optional[str] = None
    schema: Optional[List[Dict[str, Any]]] = None
    query_mode: QueryMode = 'sql'
    read_result: Optional[DataFrame] = None

    def __init__(self,
                 connection_string: str,
                 query: str = None,
                 table_name: str = None,
                 schema: List[Dict[str, Any]] = None):
        if query is not None and table_name is not None:
            raise ValueError("Only one of table_name or query can be provided")
        if query is None and table_name is None:
            raise ValueError("Either table_name or query must be provided")
        if query is not None:
            self.query_mode = 'query'
            self.query = query

        elif table_name is None and schema is None:
            raise ValueError("schema must be provided if table_name is not provided")
        else:
            self.query_mode = 'table'
            self.query = f"SELECT * FROM {table_name}"
            self.table_name = table_name
            self.schema = schema
        self.read_result = None

    def get_initial_data(self) -> List[Dict[str, Any]]:
        return []

    def get_iter(self) -> Generator[Dict[str, Any], None, None]:
        logger.warning('Getting data in iteration, this is suboptimal')
        data = self.data_getter()
        for row in data:
            yield row

    def get_df(self):
        df = self.get_pl_df()
        return df.to_pandas()

    def get_sample(self, n: int = 10000) -> Generator[Dict[str, Any], None, None]:
        if self.query_mode == 'table':
            query = f"SELECT * FROM {self.table_name} LIMIT {n}"
            try:
                df = pl.read_database_uri(query, self.connection_string)
                return (r for r in df.to_dicts())
            except Exception as e:
                logger.error(f"Error with query: {query}")
                raise e
        else:
            df = self.get_df()
            rows = df.head(n).to_dicts()
            return (r for r in rows)

    def data_getter(self) -> Generator[Dict[str, Any], None, None]:
        df = self.get_df()
        rows = df.to_dicts()
        return (r for r in rows)

    def get_pl_df(self) -> pl.DataFrame:
        if self.read_result is None:
            self.read_result = pl.read_database_uri(self.query, self.connection_string)
        return self.read_result

