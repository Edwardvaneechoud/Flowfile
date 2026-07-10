import datetime
import math

import polars as pl

from flowfile import node_designer as nd

EPOCH = datetime.date(1970, 1, 1)
DEFAULT_PRECISION = 4


def _round_half_up(value: float, digits: int) -> float:
    factor = 10**digits
    return math.floor(value * factor + 0.5) / factor


class RounderSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Rounding",
        digits=nd.NumericInput(label="Digits", default=2, min_value=0, max_value=8),
    )


class RounderNode(nd.CustomNodeBase):
    node_name: str = "Rounder"
    settings_schema: RounderSettings = RounderSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        digits = int(self.settings_schema.main.digits.value or DEFAULT_PRECISION)
        return inputs[0].with_columns(
            pl.col(pl.Float64).map_elements(lambda v: self._apply(v, digits), return_dtype=pl.Float64)
        )

    def _apply(self, value: float, digits: int) -> float:
        return _round_half_up(value, digits)

    @staticmethod
    def days_since_epoch(d: datetime.date) -> int:
        return (d - EPOCH).days
