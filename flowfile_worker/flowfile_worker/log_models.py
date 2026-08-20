"""The one model ``flow_logger`` needs, kept free of pydantic.

Every spawned child builds a logger; importing it from ``models`` would drag pydantic,
``pl_fuzzy_frame_match`` and the external-source models into every task.
``models`` re-exports ``RawLogInput`` from here, so existing consumers are unaffected.
"""

from dataclasses import dataclass

LOG_TYPES = ("INFO", "WARNING", "ERROR")


@dataclass
class RawLogInput:
    flowfile_flow_id: int
    log_message: str
    log_type: str
    node_id: int | None = None
    extra: dict | None = None

    def __post_init__(self):
        # Stands in for the Literal[...] the pydantic model validated.
        if self.log_type not in LOG_TYPES:
            raise ValueError(f"log_type must be one of {LOG_TYPES}, got {self.log_type!r}")
