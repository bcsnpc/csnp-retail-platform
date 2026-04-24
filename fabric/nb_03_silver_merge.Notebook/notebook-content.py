# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

# dim_date arrives from the generator already in gold shape: date_key is DateType
# and the fiscal-calendar-derived columns are correct. The silver layer for dim_date
# is therefore a pass-through + lineage columns + upsert — no type casts and no UTC
# normalization on date_key. UTC timestamp handling is reserved for fact tables that
# carry event timestamps; dims flow through unchanged.

workspace_name: str = "CSNP Workspace Dev"
bronze_lakehouse: str = "CSNP_Bronze"
silver_lakehouse: str = "CSNP_Silver"
bronze_input_subpath: str = "bronze/dim_date/dim_date.parquet"
silver_table_name: str = "dim_date"
silver_table_fqn: str = "CSNP_Silver.silver_dim_date"
source_system: str = "generator"

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable


def onelake_files_path(workspace: str, lakehouse: str, subpath: str) -> str:
    return (
        f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse}.Lakehouse/Files/{subpath}"
    )


def onelake_tables_path(workspace: str, lakehouse: str, table_name: str) -> str:
    return (
        f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse}.Lakehouse/Tables/{table_name}"
    )


bronze_path = onelake_files_path(workspace_name, bronze_lakehouse, bronze_input_subpath)
silver_path = onelake_tables_path(workspace_name, silver_lakehouse, silver_table_name)

print(f"Bronze input : {bronze_path}")
print(f"Silver target: {silver_path}")

# CELL ********************

def merge_to_silver(
    source_df,
    silver_table_path: str,
    target_table_fqn: str,
    business_keys: list[str],
    strategy: str,
):
    if strategy == "scd2":
        raise NotImplementedError(
            "SCD2 merge is reserved for dim_product and dim_customer. "
            "Promote this helper to a shared location (sibling notebook via %run or "
            "wheel in csnp_env Custom Libraries) BEFORE implementing SCD2 — see "
            "project_fabric_deploy.md for the copy-paste-drift rationale."
        )
    if strategy != "scd1":
        raise ValueError(f"Unknown strategy: {strategy!r}. Supported: 'scd1', 'scd2'.")

    if not DeltaTable.isDeltaTable(spark, silver_table_path):
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_fqn)
        return {"action": "initial_write", "rows_written": source_df.count()}

    target = DeltaTable.forPath(spark, silver_table_path)
    match_condition = " AND ".join(f"t.{k} = s.{k}" for k in business_keys)
    (
        target.alias("t")
        .merge(source_df.alias("s"), match_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"action": "merge", "business_keys": business_keys}

# CELL ********************

bronze_df = spark.read.parquet(bronze_path)
print(f"Bronze rows: {bronze_df.count():,}")
bronze_df.printSchema()

# CELL ********************

silver_df = (
    bronze_df
    .withColumn("ingestion_timestamp_utc", F.current_timestamp())
    .withColumn("source_system", F.lit(source_system))
)

# CELL ********************

result = merge_to_silver(
    source_df=silver_df,
    silver_table_path=silver_path,
    target_table_fqn=silver_table_fqn,
    business_keys=["date_key"],
    strategy="scd1",
)
print(result)

# CELL ********************

silver_out = spark.read.format("delta").load(silver_path)
print(f"Silver row count: {silver_out.count():,}")

(
    silver_out.agg(
        F.min("date_key").alias("min_date_key"),
        F.max("date_key").alias("max_date_key"),
        F.countDistinct("date_key").alias("distinct_date_keys"),
        F.max("ingestion_timestamp_utc").alias("latest_ingest_ts"),
    ).show(truncate=False)
)
