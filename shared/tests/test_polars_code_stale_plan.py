"""A Polars Code step holding a plan another Polars version serialised fails with a message that says so."""

import polars as pl
import pytest

from shared.polars_code_parser import STALE_PLAN_MESSAGE, polars_code_parser

# pl.LazyFrame({"a": [1]}).with_columns(b=pl.col("a") * 2), serialised by Polars 1.43.2.
PLAN_FROM_POLARS_1_43 = (
    "RFNMX1ZFUlNJT04YAAAANGM0MWFkZWQ3MTVmOGNhODUzMzdiYzljMjNkMzhhZmEwZTAzZDRkZmIyODAyYWMwN2UzYzdiM2IwYjRm"
    "ZWU5ZoOkcm9vdIKjaWR4Aqd2ZXJzaW9uAapkYXRhZnJhbWVzkoKldmFsdWXAp3ZlcnNpb24AgqV2YWx1ZZHcAZDM/8z/zP/M/8y4"
    "AAAABAAAAMzyzP/M/8z/FAAAAAQAAQAAAAoACwAIAAoABADM8sz/zP/M/0gAAAAQAAAAAAAKAAwAAAAEAAgAAQAAAAQAAADM9Mz/"
    "zP/M/xgAAAAMAAAACAAMAAQACAADAAAAWzVdAAkAAABfUExfRkxBR1MAAAABAAAABAAAAMzszP/M/8z/OAAAACAAAAAYAAAAAQIA"
    "ABAAEgAEABAAEQAIAAAADAAAAAAAzPTM/8z/zP9AAAAAAQAAAAgACQAEAAgAAQAAAGEAAAAAAAAAzP/M/8z/zP/MgAAAAAQAAADM"
    "7Mz/zP/M/0AAAAAAAAAAFAAAAAQAAwAMABMAEAASAAwABADM6sz/zP/M/wEAAAAAAAAAPAAAABAAAAAAAAoAFAAEAAwAEAACAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzP/M/8z/zP8AAAAAp3ZlcnNpb24BqWRzbF9wbGFu"
    "c5OCpXZhbHVlwKd2ZXJzaW9uAIKldmFsdWWBrURhdGFGcmFtZVNjYW6ComRmgqNpZHgBp3ZlcnNpb24BpnNjaGVtYYKmZmllbGRz"
    "gaFhpUludDY0qG1ldGFkYXRhwKd2ZXJzaW9uAYKldmFsdWWBpkhTdGFja4OlaW5wdXSCo2lkeAGndmVyc2lvbgGlZXhwcnORgaVB"
    "bGlhc5KBqkJpbmFyeUV4cHKDpGxlZnSBpkNvbHVtbqFhom9wqE11bHRpcGx5pXJpZ2h0gadMaXRlcmFsgaNEeW6Bo0ludMQQAAAA"
    "AAAAAAAAAAAAAAAAAqFip29wdGlvbnODrHJ1bl9wYXJhbGxlbMOvZHVwbGljYXRlX2NoZWNrw7BzaG91bGRfYnJvYWRjYXN0w6d2"
    "ZXJzaW9uAQ=="
)

FALLBACK_STEP = f"""
serialized_value = base64.b64decode('{PLAN_FROM_POLARS_1_43}')
buffer = BytesIO(serialized_value)
output_df = pl.LazyFrame.deserialize(buffer)
"""


def test_a_plan_from_another_polars_names_the_fix():
    run = polars_code_parser.get_executable(FALLBACK_STEP, 1)

    with pytest.raises(ValueError, match="rebuild the step from code") as caught:
        run(pl.LazyFrame({"a": [1]}))

    assert str(caught.value) == STALE_PLAN_MESSAGE
    assert isinstance(caught.value.__cause__, pl.exceptions.ComputeError)


def test_other_compute_errors_pass_through():
    run = polars_code_parser.get_executable("output_df = input_df.select(pl.col('a').str.to_uppercase())", 1)

    with pytest.raises(pl.exceptions.PolarsError):
        run(pl.LazyFrame({"a": [1]})).collect()
