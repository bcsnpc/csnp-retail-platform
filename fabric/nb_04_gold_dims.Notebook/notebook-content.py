# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5d5f57cf-606b-4214-ab7f-5d34d30cfd70",
# META       "default_lakehouse_name": "CSNP_Gold",
# META       "default_lakehouse_workspace_id": "23d7d6a1-7272-4793-9abd-5a31dc1f575a",
# META       "known_lakehouses": [
# META         {"id": "5f1bda9e-8de7-4a7b-9c8d-2833ff44a86b"},
# META         {"id": "5d5f57cf-606b-4214-ab7f-5d34d30cfd70"}
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "8e13d8df-db60-445c-b2c3-fffa30de799c",
# META       "workspaceId": "23d7d6a1-7272-4793-9abd-5a31dc1f575a"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

silver_lakehouse: str = "CSNP_Silver"
gold_lakehouse: str = "CSNP_Gold"

# CELL ********************

from csnp_helpers import merge_to_silver, validate_silver

# (silver_table, gold_table, business_key)
DIMS = [
    ("silver_dim_date",          "dim_date",          "date_key"),
    ("silver_dim_geography",     "dim_geography",     "geo_key"),
    ("silver_dim_store",         "dim_store",         "store_key"),
    ("silver_dim_campaign",      "dim_campaign",      "campaign_key"),
    ("silver_dim_channel",       "dim_channel",       "channel_key"),
    ("silver_dim_return_reason", "dim_return_reason", "return_reason_key"),
    ("silver_dim_product",       "dim_product",       "product_key"),
    ("silver_dim_customer",      "dim_customer",      "customer_key"),
]

for silver_tbl, gold_tbl, bkey in DIMS:
    silver_fqn = f"{silver_lakehouse}.{silver_tbl}"
    gold_fqn   = f"{gold_lakehouse}.{gold_tbl}"
    df = spark.table(silver_fqn)
    result = merge_to_silver(df, gold_fqn, [bkey], strategy="scd1")
    print(f"{gold_tbl}: {result}")

# CELL ********************

for _, gold_tbl, bkey in DIMS:
    validate_silver(f"{gold_lakehouse}.{gold_tbl}", key_col=bkey)
