# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b03116fd-1e49-4ab8-a6aa-d07dbf25441a",
# META       "default_lakehouse_name": "CSNP_Bronze",
# META       "default_lakehouse_workspace_id": "23d7d6a1-7272-4793-9abd-5a31dc1f575a",
# META       "known_lakehouses": [
# META         {"id": "b03116fd-1e49-4ab8-a6aa-d07dbf25441a"},
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

workspace_name: str = "CSNP_Dev"
gold_lakehouse: str = "CSNP_Gold"
scale: str = "xs"
seed: int = 42
# catchup=True: loop from fictional_date+1 up to catchup_until (inclusive)
# catchup=False: generate exactly one day (normal daily schedule mode)
catchup: bool = False
# ISO date string "YYYY-MM-DD"; empty string = yesterday
catchup_until: str = ""

# CELL ********************

import tempfile
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
from pathlib import Path

from csnp_retail import daily as _daily
from csnp_retail.config import GeneratorConfig, Scale, BACKFILL_START, BACKFILL_END
from csnp_retail.entities.dim_date import build_dim_date
from csnp_retail.manifest import IdWatermarks, Manifest, TimelineState
from csnp_retail.patterns import derive_seed, module_version

BRONZE   = Path("/lakehouse/default/Files/bronze")
MANIFEST = Path("/lakehouse/default/Files/manifest.json")

# CELL ********************

def _max_or_zero(table: str, col: str) -> int:
    try:
        val = spark.sql(f"SELECT MAX({col}) FROM {gold_lakehouse}.{table}").collect()[0][0]
        return int(val) if val is not None else 0
    except Exception:
        return 0

if MANIFEST.exists():
    manifest = Manifest.load(MANIFEST)
    print(f"Manifest loaded — fictional_date={manifest.timeline.fictional_date}, "
          f"runs_completed={manifest.timeline.daily_runs_completed}")
else:
    print("No manifest found — bootstrapping from Gold tables...")
    watermarks = IdWatermarks(
        sale_key        = _max_or_zero("fact_sales",           "sale_key"),
        order_seq       = _max_or_zero("fact_sales",           "sale_key"),
        return_key      = _max_or_zero("fact_returns",         "return_key"),
        session_key     = _max_or_zero("fact_sessions",        "session_key"),
        customer_key    = _max_or_zero("dim_customer",         "customer_key"),
        customer_id_seq = _max_or_zero("dim_customer",         "customer_key"),
        event_key       = _max_or_zero("fact_loyalty_events",  "event_key"),
        spend_key       = _max_or_zero("fact_marketing_spend", "spend_key"),
        inventory_key   = _max_or_zero("fact_inventory_daily", "inventory_key"),
    )
    manifest = Manifest(
        seed=seed,
        scale=Scale(scale),
        timeline=TimelineState(
            backfill_start=BACKFILL_START,
            backfill_end=BACKFILL_END,
            fictional_date=BACKFILL_END,
            daily_runs_completed=0,
        ),
        id_watermarks=watermarks,
        patterns_module_version=module_version(),
    )
    manifest.save(MANIFEST)
    print(f"Manifest bootstrapped — watermarks: {watermarks}")

# Resolve the end date for catchup
if catchup:
    end_date = date.fromisoformat(catchup_until) if catchup_until else date.today() - timedelta(days=1)
    days_behind = (end_date - manifest.timeline.fictional_date).days
    print(f"Catchup mode: {manifest.timeline.fictional_date} → {end_date}  ({days_behind} day(s) to generate)")
else:
    end_date = manifest.timeline.fictional_date + timedelta(days=1)
    print(f"Daily mode: generating {end_date}")

# CELL ********************

# Load gold dims once — used across all catchup days
dims = {}
for _name in ["dim_date", "dim_geography", "dim_store", "dim_campaign",
              "dim_channel", "dim_return_reason", "dim_product", "dim_customer"]:
    dims[_name] = spark.table(f"{gold_lakehouse}.{_name}").toPandas()
    print(f"  {_name}: {len(dims[_name])} rows")

# build_daily_inventory needs gold fact_sales as a local parquet file
_tmpdir = Path(tempfile.mkdtemp())
(spark.table(f"{gold_lakehouse}.fact_sales")
      .toPandas()
      .to_parquet(_tmpdir / "fact_sales.parquet", index=False))

# CELL ********************

def _append_bronze(new_df: pd.DataFrame, entity: str) -> int:
    """Concat new rows onto the existing bronze flat parquet file."""
    if len(new_df) == 0:
        return 0
    path = BRONZE / entity / f"{entity}.parquet"
    if path.exists():
        existing = pd.read_parquet(path)
        new_aligned = new_df.copy()
        for col in new_aligned.columns:
            if col in existing.columns and existing[col].dtype != new_aligned[col].dtype:
                try:
                    new_aligned[col] = new_aligned[col].astype(existing[col].dtype)
                except Exception:
                    if "date" in col:
                        new_aligned[col] = pd.to_datetime(new_aligned[col]).dt.date
        combined = pd.concat([existing, new_aligned], ignore_index=True)
    else:
        combined = new_df.copy()
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(combined, preserve_index=False), path)
    return len(new_df)


def _generate_one_day(manifest: Manifest, target_date: date, config: GeneratorConfig) -> Manifest:
    """Generate one day of bronze data, append to flat files, return updated manifest."""
    rng = np.random.default_rng(derive_seed(manifest.seed, "daily", target_date))

    sales_by_ch          = _daily.build_daily_sales(rng, target_date, manifest, dims, config)
    sessions             = _daily.build_daily_sessions(rng, target_date, manifest, dims, config)
    inventory            = _daily.build_daily_inventory(rng, target_date, manifest, dims, config, _tmpdir)
    cust_delta, loyalty  = _daily.build_daily_crm(rng, target_date, manifest, dims, config, sales_by_ch)
    _, spend             = _daily.build_daily_marketing(rng, target_date, manifest, dims)

    all_sales    = pd.concat(list(sales_by_ch.values()), ignore_index=True) if sales_by_ch else pd.DataFrame()
    all_sessions = pd.concat([df for _, df in sessions], ignore_index=True) if sessions else pd.DataFrame()

    # Convert date string columns in cust_delta to datetime.date objects so
    # PyArrow schema matches the existing bronze parquet (object dtype but date values)
    if len(cust_delta) > 0:
        for _col in ["signup_date", "effective_date", "expiry_date"]:
            if _col in cust_delta.columns:
                cust_delta[_col] = pd.to_datetime(cust_delta[_col], errors="coerce").dt.date

    _append_bronze(all_sales,    "fact_sales")
    _append_bronze(all_sessions, "fact_sessions")
    _append_bronze(inventory,    "fact_inventory_daily")
    _append_bronze(loyalty,      "fact_loyalty_events")
    _append_bronze(spend,        "fact_marketing_spend")
    _append_bronze(cust_delta,   "dim_customer")
    _append_bronze(build_dim_date(target_date, target_date), "dim_date")

    prev = manifest.id_watermarks
    updated = Manifest(
        seed=manifest.seed,
        scale=manifest.scale,
        timeline=TimelineState(
            backfill_start=manifest.timeline.backfill_start,
            backfill_end=manifest.timeline.backfill_end,
            fictional_date=target_date,
            daily_runs_completed=manifest.timeline.daily_runs_completed + 1,
        ),
        id_watermarks=IdWatermarks(
            sale_key        = int(all_sales["sale_key"].max())       if len(all_sales)    else prev.sale_key,
            order_seq       = int(all_sales["sale_key"].max())       if len(all_sales)    else prev.order_seq,
            return_key      = prev.return_key,
            session_key     = int(all_sessions["session_key"].max()) if len(all_sessions) else prev.session_key,
            customer_key    = int(cust_delta["customer_key"].max())  if len(cust_delta)   else prev.customer_key,
            customer_id_seq = prev.customer_id_seq + len(cust_delta),
            event_key       = int(loyalty["event_key"].max())        if len(loyalty)      else prev.event_key,
            spend_key       = int(spend["spend_key"].max())          if len(spend)        else prev.spend_key,
            inventory_key   = int(inventory["inventory_key"].max())  if len(inventory)    else prev.inventory_key,
        ),
        patterns_module_version=module_version(),
        tables_written=manifest.tables_written,
        row_counts=manifest.row_counts,
    )
    updated.save(MANIFEST)
    return updated

# CELL ********************

config = GeneratorConfig(
    scale=Scale(manifest.scale),
    seed=manifest.seed,
    start=manifest.timeline.backfill_start,
    end=manifest.timeline.backfill_end,
    out=Path("/lakehouse/default/Files"),
)

current_manifest = manifest
target = current_manifest.timeline.fictional_date + timedelta(days=1)
total_generated = 0

while target <= end_date:
    current_manifest = _generate_one_day(current_manifest, target, config)
    total_generated += 1
    print(f"  ✓ {target}  (run #{current_manifest.timeline.daily_runs_completed})")
    target = target + timedelta(days=1)

print(f"\nDone — {total_generated} day(s) generated. "
      f"fictional_date now {current_manifest.timeline.fictional_date}.")
