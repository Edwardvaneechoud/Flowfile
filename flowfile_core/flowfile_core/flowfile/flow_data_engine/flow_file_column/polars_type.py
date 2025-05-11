from typing import Optional, Any
from pydantic import BaseModel


class ColumnInfo:
    pass


class PlType(BaseModel):
    column_name: str
    col_index: int = -1
    count: Optional[int] = -1
    null_count: Optional[int] = -1
    mean: Optional[str] = ""
    std: Optional[float] = -1
    min: Optional[str] = ""
    max: Optional[str] = ""
    median: Optional[str] = 0
    pl_datatype: Optional[Any]
    n_unique: Optional[int] = -1
    examples: Optional[str] = ""

    class Config:
        arbitrary_types_allowed = True
