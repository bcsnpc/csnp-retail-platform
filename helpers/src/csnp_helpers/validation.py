from __future__ import annotations


def validate_silver(target_table_fqn: str, key_col: str) -> None:
    """Print row count and key-range summary for a silver Delta table."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.getActiveSession()
    df = spark.table(target_table_fqn)
    print(f"Silver row count: {df.count():,}")
    df.agg(
        F.min(key_col).alias(f"min_{key_col}"),
        F.max(key_col).alias(f"max_{key_col}"),
        F.countDistinct(key_col).alias(f"distinct_{key_col}"),
        F.max("ingestion_timestamp_utc").alias("latest_ingest_ts"),
    ).show(truncate=False)
