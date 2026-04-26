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

workspace_name: str = "CSNP_Dev"
bronze_lakehouse: str = "CSNP_Bronze"
bronze_input_subpath: str = "bronze/dim_date/dim_date.parquet"
silver_table_fqn: str = "CSNP_Silver.silver_dim_date"
business_keys: list[str] = ["date_key"]
source_system: str = "generator"

# CELL ********************

from csnp_helpers import add_lineage_columns, merge_to_silver, onelake_files_path, validate_silver

bronze_path = onelake_files_path(workspace_name, bronze_lakehouse, bronze_input_subpath)
print(f"Bronze input : {bronze_path}")
print(f"Silver target: {silver_table_fqn}")

# CELL ********************

bronze_df = spark.read.parquet(bronze_path)
print(f"Bronze rows: {bronze_df.count():,}")
bronze_df.printSchema()

# CELL ********************

silver_df = add_lineage_columns(bronze_df, source_system)
result = merge_to_silver(silver_df, silver_table_fqn, business_keys, strategy="scd1")
print(result)

# CELL ********************

validate_silver(silver_table_fqn, key_col=business_keys[0])
