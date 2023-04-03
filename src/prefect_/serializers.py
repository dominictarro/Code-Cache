"""
Module for serializing and deserializing output.
"""
import io

import polars as pl
from prefect.serializers import Serializer


class PolarsSerializer(Serializer):
    """Serializes Polars DataFrames."""

    type = "polars"
    read_options: dict = {}
    write_options: dict = {}

    def dumps(self, obj: pl.DataFrame) -> bytes:
        """Converts a `polars.DataFrame` to parquet-formatted bytes."""
        buffer = io.BytesIO()
        obj.write_parquet(buffer, **self.write_options)
        return super().dumps(buffer.getvalue())

    def loads(self, blob: bytes) -> pl.DataFrame:
        """Loads a `polars.DataFrame` from parquet-formatted bytes."""
        buffer = io.BytesIO(blob)
        df = pl.read_parquet(buffer, **self.read_options)
        return df
