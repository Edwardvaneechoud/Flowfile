from dataclasses import dataclass

import polars as pl
from polars.exceptions import PanicException

from shared.cloud_credential_provider import register_secret_decryptor


def _decrypt_plan_credentials(encrypted: str) -> str:
    """Decrypt the cloud credentials a core-built plan carries (``EncryptedCredentialProvider``)."""
    # Imported on first use: flowfile_worker.secrets pulls pydantic, which spawned children must not load eagerly.
    from flowfile_worker.secrets import decrypt_secret

    return decrypt_secret(encrypted).get_secret_value()


register_secret_decryptor(_decrypt_plan_credentials)


def collect_lazy_frame(lf: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lf.collect(engine="streaming")
    except PanicException:
        return lf.collect(engine="in-memory")


@dataclass
class CollectStreamingInfo:
    __slots__ = "df", "streaming_collect_available"
    df: pl.DataFrame
    streaming_collect_available: bool


def collect_lazy_frame_and_get_streaming_info(lf: pl.LazyFrame) -> CollectStreamingInfo:
    try:
        df = lf.collect(engine="streaming")
        return CollectStreamingInfo(df, True)
    except PanicException:
        return CollectStreamingInfo(lf.collect(), False)
