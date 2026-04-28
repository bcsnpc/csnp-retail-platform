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
FACTS = [
    ("silver_fact_sales",            "fact_sales",            "sale_key"),
    ("silver_fact_returns",          "fact_returns",          "return_key"),
    ("silver_fact_sessions",         "fact_sessions",         "session_key"),
    ("silver_fact_marketing_spend",  "fact_marketing_spend",  "spend_key"),
    ("silver_fact_loyalty_events",   "fact_loyalty_events",   "event_key"),
    ("silver_fact_inventory_daily",  "fact_inventory_daily",  "inventory_key"),
]

for silver_tbl, gold_tbl, bkey in FACTS:
    silver_fqn = f"{silver_lakehouse}.{silver_tbl}"
    gold_fqn   = f"{gold_lakehouse}.{gold_tbl}"
    df = spark.table(silver_fqn)
    result = merge_to_silver(df, gold_fqn, [bkey], strategy="scd1")
    print(f"{gold_tbl}: {result}")

# CELL ********************

for _, gold_tbl, bkey in FACTS:
    validate_silver(f"{gold_lakehouse}.{gold_tbl}", key_col=bkey)
