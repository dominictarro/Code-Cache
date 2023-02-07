"""
Tests for the src.aws_.s3.keys module.

"""
from __future__ import annotations

import pandas as pd
import pytest

from src.pandas_ import io_


@pytest.mark.parametrize(
    argnames=["text", "read_options"],
    argvalues=[
        ("a,b,c\n1,2,3\n4,5,6\n", None),
        ("a\tb\tc\n1\t2\t3\n4\t5\t6\n", {"sep": "\t"}),
        (b"a,b,c\n1,2,3\n4,5,6", None),
        (b"a\tb\tc\n1\t2\t3\n4\t5\t6\n", {"sep": "\t"}),
    ],
)
def test_text_to_dataframe_csv_tsv(
    text: bytes | str, read_options: dict | None
):
    """Tests `src.pandas_.io_.text_to_dataframe` with for CSV and TSV formats."""
    df = io_.text_to_dataframe(
        text, method=io_.PandasIOMethod.CSV, read_options=read_options
    )
    assert set(df.columns) == {"a", "b", "c"}


def test_dataframe_to_text_csv_tsv():
    """Tests `src.pandas_.io_.dataframe_to_bytes` with for CSV and TSV
    formats.
    """
    df = pd.DataFrame({"a": [1, 4], "b": [2, 5], "c": [3, 6]})

    result = io_.dataframe_to_bytes(df, method=io_.PandasIOMethod.CSV)
    assert result == b",a,b,c\n0,1,2,3\n1,4,5,6\n"

    result = io_.dataframe_to_bytes(
        df, method=io_.PandasIOMethod.CSV, write_options={"sep": "\t"}
    )
    assert result == b"\ta\tb\tc\n0\t1\t2\t3\n1\t4\t5\t6\n"
