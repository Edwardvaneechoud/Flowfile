import os
import fastexcel
from typing import List
from functools import lru_cache

from flowfile_core.configs import logger


@lru_cache(maxsize=32)
def get_sheet_names(file_path: str) -> List[str] | None:
    if not os.path.exists(file_path):
        logger.error(f"File does not exist: {file_path}")
        return
    try:
        return fastexcel.read_excel(file_path).sheet_names
    except Exception as e:
        logger.error(e)
        return
