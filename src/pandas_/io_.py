"""
Code for working with Pandas's read & write methods.
"""
from __future__ import annotations

import enum
import io

import pandas as pd


class PandasIOMethod(enum.Enum):
    """IO shorthand for Pandas IO capabilities."""

    # Read method, write method
    # NOTE the write method must be passed `self`
    CSV = pd.read_csv, pd.DataFrame.to_csv
    EXCEL = pd.read_excel, pd.DataFrame.to_excel
    JSON = pd.read_json, pd.DataFrame.to_json
    PARQUET = pd.read_parquet, pd.DataFrame.to_parquet
    STATA = pd.read_stata, pd.DataFrame.to_stata
    XML = pd.read_xml, pd.DataFrame.to_xml


def dataframe_to_bytes(
    df: pd.DataFrame,
    method: PandasIOMethod = PandasIOMethod.CSV,
    write_options: dict | None = None,
) -> bytes:
    """Converts a Pandas DataFrame to bytes.

    :param df:              String or bytes to load into the DataFrame.
    :param method:          Alias for the DataFrame loading method, defaults\
        to PandasIOMethod.CSV
    :param write_options:   Keyword arguments to pass to the DataFrame\
        writing method
    :raises TypeError:      Method is not a PandasIOMethod enumeration or\
        string
    :raises TypeError:      Write options are not None or a dictionary
    :return:                Bytes of the DataFrame
    """
    buffer_: io.BytesIO = io.BytesIO()

    if not isinstance(df, (pd.DataFrame, pd.Series)):
        raise TypeError(
            f"'df' must be a pandas.DataFrame, not {type(df).__name__}"
        )

    if isinstance(method, str):
        try:
            method = PandasIOMethod._member_map_[method.upper()]
        except KeyError:
            raise ValueError(
                "'method' must be a PandasIOMethod. Try "
                f"{PandasIOMethod._member_names_}"
            )
    elif not isinstance(method, PandasIOMethod):
        raise TypeError(
            "'method' must be a PandasIOMethod or string"
            f", not {type(method).__name__}"
        )

    if write_options is None:
        write_options = {}
    elif not isinstance(write_options, dict):
        raise TypeError(
            "'write_options' must be a dictionary or None"
            f", not {type(write_options).__name__}"
        )

    method.value[1](df, buffer_, **write_options)
    return buffer_.getvalue()


def text_to_dataframe(
    content: bytes | str,
    method: PandasIOMethod = PandasIOMethod.CSV,
    read_options: dict | None = None,
) -> pd.DataFrame:
    """Converts text to a Pandas DataFrame.

    :param content:         String or bytes to load into the DataFrame.
    :param method:          Alias for the DataFrame loading method, defaults\
        to PandasIOMethod.CSV
    :param read_options:    Keyword arguments to pass to the DataFrame\
        loading method
    :raises TypeError:      Content is not a string or bytes
    :raises TypeError:      Method is not a PandasIOMethod enumeration or\
        string
    :raises TypeError:      Read options are not None or a dictionary
    :return:                A DataFrame of the given content
    """
    buffer_: io.IOBase
    if isinstance(content, str):
        buffer_ = io.StringIO(content)
    elif isinstance(content, bytes):
        buffer_ = io.BytesIO(content)
    else:
        raise TypeError(
            "'content' must be a string or bytes"
            f", not {type(content).__name__}"
        )

    if isinstance(method, str):
        try:
            method = PandasIOMethod._member_map_[method.upper()]
        except KeyError:
            raise ValueError(
                "'method' must be a PandasIOMethod. Try "
                f"{PandasIOMethod._member_names_}"
            )
    elif not isinstance(method, PandasIOMethod):
        raise TypeError(
            "'method' must be a PandasIOMethod or string"
            f", not {type(method).__name__}"
        )

    if read_options is None:
        read_options = {}
    elif not isinstance(read_options, dict):
        raise TypeError(
            "'read_options' must be a dictionary or None"
            f", not {type(read_options).__name__}"
        )

    return method.value[0](buffer_, **read_options)
