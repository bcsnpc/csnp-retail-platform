from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def merge_to_silver(
    source_df: DataFrame,
    target_table_fqn: str,
    business_keys: list[str],
    strategy: str = "scd1",
) -> dict:
    """Upsert source_df into a Delta silver table.

    strategy='scd1'  — overwrite matched rows, insert unmatched.
    strategy='scd2'  — not yet implemented; raises NotImplementedError.
    """
    if strategy == "scd2":
        raise NotImplementedError(
            "SCD2 not yet implemented. "
            "Add open/close timestamp + is_current logic to "
            "helpers/src/csnp_helpers/merge.py before authoring "
            "dim_product or dim_customer silver notebooks."
        )
    if strategy != "scd1":
        raise ValueError(f"Unknown strategy: {strategy!r}. Supported: 'scd1', 'scd2'.")

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
