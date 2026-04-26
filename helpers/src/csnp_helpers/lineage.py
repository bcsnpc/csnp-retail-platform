from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def add_lineage_columns(df: DataFrame, source_system: str) -> DataFrame:
    """Append ingestion_timestamp_utc and source_system to a DataFrame."""
    from pyspark.sql import functions as F

    return (
        df
        .withColumn("ingestion_timestamp_utc", F.current_timestamp())
        .withColumn("source_system", F.lit(source_system))
    )
