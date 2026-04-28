# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
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
