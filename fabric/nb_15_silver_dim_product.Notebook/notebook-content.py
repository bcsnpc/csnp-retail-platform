# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

workspace_name: str = "CSNP_Dev"
bronze_lakehouse: str = "CSNP_Bronze"
bronze_input_subpath: str = "bronze/dim_product/dim_product.parquet"
silver_table_fqn: str = "CSNP_Silver.silver_dim_product"
# product_key is the surrogate key — each row is one SCD2 version.
# The generator pre-builds all versions; SCD1 on the surrogate key
# preserves full history without re-implementing version logic here.
business_keys: list[str] = ["product_key"]
source_system: str = "generator"

# CELL ********************

from csnp_helpers import add_lineage_columns, merge_to_silver, onelake_files_path, validate_silver

bronze_path = onelake_files_path(workspace_name, bronze_lakehouse, bronze_input_subpath)
bronze_df = spark.read.parquet(bronze_path)
silver_df = add_lineage_columns(bronze_df, source_system)
result = merge_to_silver(silver_df, silver_table_fqn, business_keys, strategy="scd1")
print(result)

# CELL ********************

validate_silver(silver_table_fqn, key_col=business_keys[0])
