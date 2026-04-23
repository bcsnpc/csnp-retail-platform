"""Parquet I/O — local filesystem and ABFSS stub."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(df: pd.DataFrame, path: Path | str, *, compression: str = "snappy") -> Path:
    """Write a DataFrame to Parquet. Creates parent directories as needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression=compression)
    return path


def read_parquet(path: Path | str) -> pd.DataFrame:
    return pd.read_parquet(path)
