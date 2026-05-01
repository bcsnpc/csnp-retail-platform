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
# fail_on_errors=True causes the notebook (and pipeline) to raise on any FAIL result
fail_on_errors: bool = True

# CELL ********************

from pyspark.sql import functions as F

results = []   # list of dicts: layer, table, check, status, value, details

def _ok(layer, table, check, value="", details=""):
    results.append({"layer": layer, "table": table, "check": check,
                    "status": "PASS", "value": str(value), "details": str(details)})

def _fail(layer, table, check, value="", details=""):
    results.append({"layer": layer, "table": table, "check": check,
                    "status": "FAIL", "value": str(value), "details": str(details)})

def _warn(layer, table, check, value="", details=""):
    results.append({"layer": layer, "table": table, "check": check,
                    "status": "WARN", "value": str(value), "details": str(details)})

def _count(fqn):
    return spark.table(fqn).count()

def _null_count(fqn, col):
    return spark.table(fqn).filter(F.col(col).isNull()).count()

def _dup_count(fqn, key_col):
    return (spark.table(fqn)
            .groupBy(key_col).count()
            .filter(F.col("count") > 1)
            .count())

def _orphan_count(fact_fqn, fact_col, dim_fqn, dim_col, nullable=False):
    """Count fact rows whose FK has no match in the dim (ignoring NULLs if nullable)."""
    fact = spark.table(fact_fqn).select(F.col(fact_col).alias("fk"))
    if nullable:
        fact = fact.filter(F.col("fk").isNotNull())
    dim  = spark.table(dim_fqn).select(F.col(dim_col).alias("pk")).distinct()
    return fact.join(dim, fact["fk"] == dim["pk"], "left_anti").count()

print("Running DQ checks...")

# CELL ********************
# ── SILVER: row counts, null PKs, duplicate PKs ──────────────────────────────

SILVER_TABLES = {
    "silver_dim_date":             "date_key",
    "silver_dim_geography":        "geo_key",
    "silver_dim_store":            "store_key",
    "silver_dim_campaign":         "campaign_key",
    "silver_dim_channel":          "channel_key",
    "silver_dim_return_reason":    "return_reason_key",
    "silver_dim_product":          "product_key",
    "silver_dim_customer":         "customer_key",
    "silver_fact_sales":           "sale_key",
    "silver_fact_returns":         "return_key",
    "silver_fact_sessions":        "session_key",
    "silver_fact_marketing_spend": "spend_key",
    "silver_fact_loyalty_events":  "event_key",
    "silver_fact_inventory_daily": "inventory_key",
}

for tbl, pk in SILVER_TABLES.items():
    fqn = f"{silver_lakehouse}.{tbl}"
    try:
        n = _count(fqn)
        if n > 0:
            _ok("silver", tbl, "row_count", n)
        else:
            _fail("silver", tbl, "row_count", n, "table is empty")

        nulls = _null_count(fqn, pk)
        if nulls == 0:
            _ok("silver", tbl, "null_pk", 0)
        else:
            _fail("silver", tbl, "null_pk", nulls, f"{nulls} null {pk} values")

        dups = _dup_count(fqn, pk)
        if dups == 0:
            _ok("silver", tbl, "duplicate_pk", 0)
        else:
            _fail("silver", tbl, "duplicate_pk", dups, f"{dups} duplicate {pk} values")

    except Exception as e:
        _fail("silver", tbl, "accessible", details=str(e)[:200])

# CELL ********************
# ── GOLD: row counts, null PKs, duplicate PKs ────────────────────────────────

GOLD_DIMS = {
    "dim_date":          "date_key",
    "dim_geography":     "geo_key",
    "dim_store":         "store_key",
    "dim_campaign":      "campaign_key",
    "dim_channel":       "channel_key",
    "dim_return_reason": "return_reason_key",
    "dim_product":       "product_key",
    "dim_customer":      "customer_key",
}

GOLD_FACTS = {
    "fact_sales":            "sale_key",
    "fact_returns":          "return_key",
    "fact_sessions":         "session_key",
    "fact_marketing_spend":  "spend_key",
    "fact_loyalty_events":   "event_key",
    "fact_inventory_daily":  "inventory_key",
}

for tbl, pk in {**GOLD_DIMS, **GOLD_FACTS}.items():
    fqn = f"{gold_lakehouse}.{tbl}"
    try:
        n = _count(fqn)
        if n > 0:
            _ok("gold", tbl, "row_count", n)
        else:
            _fail("gold", tbl, "row_count", n, "table is empty")

        nulls = _null_count(fqn, pk)
        if nulls == 0:
            _ok("gold", tbl, "null_pk", 0)
        else:
            _fail("gold", tbl, "null_pk", nulls, f"{nulls} null {pk} values")

        dups = _dup_count(fqn, pk)
        if dups == 0:
            _ok("gold", tbl, "duplicate_pk", 0)
        else:
            _fail("gold", tbl, "duplicate_pk", dups, f"{dups} duplicate {pk} values")

    except Exception as e:
        _fail("gold", tbl, "accessible", details=str(e)[:200])

# CELL ********************
# ── SYNC: Silver → Gold row count match ──────────────────────────────────────

SILVER_TO_GOLD = {
    "silver_dim_date":             "dim_date",
    "silver_dim_geography":        "dim_geography",
    "silver_dim_store":            "dim_store",
    "silver_dim_campaign":         "dim_campaign",
    "silver_dim_channel":          "dim_channel",
    "silver_dim_return_reason":    "dim_return_reason",
    "silver_dim_product":          "dim_product",
    "silver_dim_customer":         "dim_customer",
    "silver_fact_sales":           "fact_sales",
    "silver_fact_returns":         "fact_returns",
    "silver_fact_sessions":        "fact_sessions",
    "silver_fact_marketing_spend": "fact_marketing_spend",
    "silver_fact_loyalty_events":  "fact_loyalty_events",
    "silver_fact_inventory_daily": "fact_inventory_daily",
}

for s_tbl, g_tbl in SILVER_TO_GOLD.items():
    try:
        s_n = _count(f"{silver_lakehouse}.{s_tbl}")
        g_n = _count(f"{gold_lakehouse}.{g_tbl}")
        if s_n == g_n:
            _ok("sync", f"{s_tbl}→{g_tbl}", "row_count_match", f"{s_n}={g_n}")
        else:
            _fail("sync", f"{s_tbl}→{g_tbl}", "row_count_match",
                  f"silver={s_n} gold={g_n}", f"delta={abs(s_n - g_n)}")
    except Exception as e:
        _fail("sync", f"{s_tbl}→{g_tbl}", "row_count_match", details=str(e)[:200])

# CELL ********************
# ── INTEGRITY: Fact → Dim FK checks ──────────────────────────────────────────

G = gold_lakehouse

FK_CHECKS = [
    # (fact_table, fact_col, dim_table, dim_col, nullable)
    ("fact_sales",           "date_key",          "dim_date",          "date_key",          False),
    ("fact_sales",           "product_key",        "dim_product",       "product_key",       False),
    ("fact_sales",           "channel_key",        "dim_channel",       "channel_key",       False),
    ("fact_sales",           "store_key",          "dim_store",         "store_key",         True),
    ("fact_sales",           "customer_key",       "dim_customer",      "customer_key",      True),
    ("fact_sales",           "campaign_key",       "dim_campaign",      "campaign_key",      True),
    ("fact_returns",         "date_key",           "dim_date",          "date_key",          False),
    ("fact_returns",         "product_key",        "dim_product",       "product_key",       False),
    ("fact_returns",         "return_reason_key",  "dim_return_reason", "return_reason_key", False),
    ("fact_sessions",        "date_key",           "dim_date",          "date_key",          False),
    ("fact_sessions",        "channel_key",        "dim_channel",       "channel_key",       False),
    ("fact_sessions",        "customer_key",       "dim_customer",      "customer_key",      True),
    ("fact_marketing_spend", "date_key",           "dim_date",          "date_key",          False),
    ("fact_marketing_spend", "campaign_key",       "dim_campaign",      "campaign_key",      False),
    ("fact_marketing_spend", "channel_key",        "dim_channel",       "channel_key",       False),
    ("fact_loyalty_events",  "date_key",           "dim_date",          "date_key",          False),
    ("fact_loyalty_events",  "customer_key",       "dim_customer",      "customer_key",      False),
    ("fact_loyalty_events",  "sale_key",           "fact_sales",        "sale_key",          True),
    ("fact_inventory_daily", "date_key",           "dim_date",          "date_key",          False),
    ("fact_inventory_daily", "product_key",        "dim_product",       "product_key",       False),
    ("fact_inventory_daily", "store_key",          "dim_store",         "store_key",         False),
]

for fact_tbl, fact_col, dim_tbl, dim_col, nullable in FK_CHECKS:
    check_name = f"{fact_col}→{dim_tbl}"
    try:
        orphans = _orphan_count(
            f"{G}.{fact_tbl}", fact_col,
            f"{G}.{dim_tbl}",  dim_col,
            nullable=nullable,
        )
        if orphans == 0:
            _ok("integrity", fact_tbl, check_name, 0)
        else:
            _fail("integrity", fact_tbl, check_name, orphans,
                  f"{orphans} {'non-null ' if nullable else ''}FK values with no matching dim row")
    except Exception as e:
        _fail("integrity", fact_tbl, check_name, details=str(e)[:200])

# CELL ********************
# ── FRESHNESS: Max date_key in key fact tables ────────────────────────────────

from datetime import date

for tbl in ["fact_sales", "fact_sessions", "fact_inventory_daily"]:
    try:
        max_dk = spark.sql(f"SELECT MAX(date_key) FROM {G}.{tbl}").collect()[0][0]
        if max_dk:
            dk_str = str(int(max_dk))
            max_date = date(int(dk_str[:4]), int(dk_str[4:6]), int(dk_str[6:8]))
            days_behind = (date.today() - max_date).days
            if days_behind <= 2:
                _ok("freshness", tbl, "max_date", max_date)
            elif days_behind <= 7:
                _warn("freshness", tbl, "max_date", max_date, f"{days_behind} days behind today")
            else:
                _fail("freshness", tbl, "max_date", max_date, f"{days_behind} days behind today")
        else:
            _fail("freshness", tbl, "max_date", details="no rows")
    except Exception as e:
        _fail("freshness", tbl, "max_date", details=str(e)[:200])

# CELL ********************
# ── Summary ───────────────────────────────────────────────────────────────────

import pandas as pd

df = pd.DataFrame(results)
fails  = df[df["status"] == "FAIL"]
warns  = df[df["status"] == "WARN"]
passes = df[df["status"] == "PASS"]

print(f"\n{'='*70}")
print(f"  DQ CHECKPOINT SUMMARY")
print(f"  PASS: {len(passes)}   WARN: {len(warns)}   FAIL: {len(fails)}   TOTAL: {len(df)}")
print(f"{'='*70}\n")

if len(warns) > 0:
    print("── WARNINGS ──────────────────────────────────────────────────────────")
    print(warns[["layer","table","check","value","details"]].to_string(index=False))
    print()

if len(fails) > 0:
    print("── FAILURES ──────────────────────────────────────────────────────────")
    print(fails[["layer","table","check","value","details"]].to_string(index=False))
    print()

display(spark.createDataFrame(df).orderBy("status", "layer", "table"))

if len(fails) > 0 and fail_on_errors:
    raise Exception(
        f"DQ checkpoint failed: {len(fails)} check(s) failed. See details above."
    )

print("DQ checkpoint passed." if len(fails) == 0 else "DQ complete (fail_on_errors=False).")
