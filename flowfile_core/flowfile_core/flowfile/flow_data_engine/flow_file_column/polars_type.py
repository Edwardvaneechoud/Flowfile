from typing import Any

from pydantic import BaseModel


class PlType(BaseModel):
    column_name: str
    col_index: int = -1
    count: int | None = -1
    null_count: int | None = -1
    mean: str | None = ""
    std: float | None = -1
    min: str | None = ""
    max: str | None = ""
    median: str | None = 0
    pl_datatype: Any | None
    n_unique: int | None = -1
    examples: str | None = ""

    class Config:
        arbitrary_types_allowed = True
