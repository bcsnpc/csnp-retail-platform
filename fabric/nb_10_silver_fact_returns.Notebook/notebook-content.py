# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5f1bda9e-8de7-4a7b-9c8d-2833ff44a86b",
# META       "default_lakehouse_name": "CSNP_Silver",
# META       "default_lakehouse_workspace_id": "23d7d6a1-7272-4793-9abd-5a31dc1f575a",
# META       "known_lakehouses": [
# META         {"id": "5f1bda9e-8de7-4a7b-9c8d-2833ff44a86b"}
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "8e13d8df-db60-445c-b2c3-fffa30de799c",
# META       "workspaceId": "23d7d6a1-7272-4793-9abd-5a31dc1f575a"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

workspace_name: str = "CSNP_Dev"
bronze_lakehouse: str = "CSNP_Bronze"
bronze_input_subpath: str = "bronze/fact_returns/fact_returns.parquet"
silver_table_fqn: str = "CSNP_Silver.silver_fact_returns"
business_keys: list[str] = ["return_key"]
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
