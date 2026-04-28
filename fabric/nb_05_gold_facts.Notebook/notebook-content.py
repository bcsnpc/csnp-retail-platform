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
