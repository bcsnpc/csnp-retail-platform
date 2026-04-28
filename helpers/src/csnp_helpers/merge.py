from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def merge_to_silver(
    source_df: DataFrame,
    target_table_fqn: str,
    business_keys: list[str],
    strategy: str = "scd1",
    tracking_cols: list[str] | None = None,
) -> dict:
    if strategy == "scd1":
        return _merge_scd1(source_df, target_table_fqn, business_keys)
    if strategy == "scd2":
        return _merge_scd2(source_df, target_table_fqn, business_keys, tracking_cols)
    raise ValueError(f"Unknown strategy: {strategy!r}. Supported: 'scd1', 'scd2'.")


def _merge_scd1(source_df: DataFrame, target_table_fqn: str, business_keys: list[str]) -> dict:
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()

    if not DeltaTable.isDeltaTable(spark, target_table_fqn):
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_fqn)
        return {"action": "initial_write", "rows_written": source_df.count()}

    target = DeltaTable.forName(spark, target_table_fqn)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in business_keys)
    (
        target.alias("t")
        .merge(source_df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"action": "merge", "business_keys": business_keys}


def _merge_scd2(
    source_df: DataFrame,
    target_table_fqn: str,
    business_keys: list[str],
    tracking_cols: list[str] | None,
) -> dict:
    """SCD2 merge for CDC-style sources that deliver only the current record.

    Adds valid_from / valid_to / is_current columns to the target.
    On each run:
      - Changed records: old row closed (is_current=False, valid_to=now), new row inserted.
      - New records: inserted with valid_from=now, valid_to=NULL, is_current=True.
      - Unchanged records: no-op.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.getActiveSession()

    src = (
        source_df
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if not DeltaTable.isDeltaTable(spark, target_table_fqn):
        src.write.format("delta").mode("overwrite").saveAsTable(target_table_fqn)
        return {"action": "initial_write", "rows_written": src.count()}

    _exclude = set(business_keys) | {
        "ingestion_timestamp_utc", "source_system",
        "valid_from", "valid_to", "is_current",
    }
    if tracking_cols is None:
        tracking_cols = [c for c in source_df.columns if c not in _exclude]

    target = DeltaTable.forName(spark, target_table_fqn)

    merge_cond = (
        " AND ".join(f"t.{k} = s.{k}" for k in business_keys)
        + " AND t.is_current = true"
    )
    # Simple <> is safe here: tracking cols with NULLs need IS NOT DISTINCT FROM instead.
    change_cond = " OR ".join(f"t.{c} <> s.{c}" for c in tracking_cols)

    # Step 1: close any records whose tracked attributes changed.
    (
        target.alias("t")
        .merge(src.alias("s"), merge_cond)
        .whenMatchedUpdate(
            condition=change_cond,
            set={"is_current": "false", "valid_to": "current_timestamp()"},
        )
        .execute()
    )

    # Step 2: insert new records + new versions of just-closed records.
    # After step 1, changed records have is_current=False so they won't appear
    # in current_keys — left_anti correctly selects them alongside net-new rows.
    current_keys = (
        spark.table(target_table_fqn)
        .filter("is_current = true")
        .select(business_keys)
    )
    to_insert = src.join(current_keys, on=business_keys, how="left_anti")
    to_insert.write.format("delta").mode("append").saveAsTable(target_table_fqn)

    return {
        "action": "scd2_merge",
        "business_keys": business_keys,
        "tracking_cols": tracking_cols,
    }
