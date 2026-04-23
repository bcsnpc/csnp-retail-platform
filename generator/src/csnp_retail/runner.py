"""Orchestrates entity generators for backfill and daily modes."""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from csnp_retail.config import GeneratorConfig
from csnp_retail.entities.dim_campaign import build_dim_campaign
from csnp_retail.entities.dim_channel import build_dim_channel
from csnp_retail.entities.dim_customer import build_dim_customer
from csnp_retail.entities.dim_date import build_dim_date
from csnp_retail.entities.dim_geography import build_dim_geography
from csnp_retail.entities.dim_product import build_dim_product
from csnp_retail.entities.dim_return_reason import build_dim_return_reason
from csnp_retail.entities.dim_store import build_dim_store
from csnp_retail.entities.fact_inventory_daily import build_fact_inventory_daily
from csnp_retail.entities.fact_loyalty_events import build_fact_loyalty_events
from csnp_retail.entities.fact_marketing_spend import build_fact_marketing_spend
from csnp_retail.entities.fact_returns import build_fact_returns
from csnp_retail.entities.fact_sales import build_fact_sales
from csnp_retail.entities.fact_sessions import build_fact_sessions
from csnp_retail.io import write_parquet
from csnp_retail.manifest import IdWatermarks, Manifest, TimelineState
from csnp_retail.patterns import module_version

log = logging.getLogger(__name__)


def run_backfill(config: GeneratorConfig) -> Manifest:
    """Generate all dims and facts for the full backfill window."""
    rng = np.random.default_rng(config.seed)
    out = config.out
    out.mkdir(parents=True, exist_ok=True)

    tables_written: dict[str, int] = {}

    log.info("  Building dim_date (%s -> %s)...", config.start, config.end)
    dim_date = build_dim_date(config.start, config.end)
    write_parquet(dim_date, out / "dim_date.parquet")
    tables_written["dim_date"] = len(dim_date)

    log.info("  Building dim_geography...")
    dim_geo = build_dim_geography()
    write_parquet(dim_geo, out / "dim_geography.parquet")
    tables_written["dim_geography"] = len(dim_geo)

    log.info("  Building dim_store (n=%d)...", config.n_stores)
    dim_store = build_dim_store(config, rng, dim_geo)
    write_parquet(dim_store, out / "dim_store.parquet")
    tables_written["dim_store"] = len(dim_store)

    log.info("  Building dim_campaign...")
    dim_campaign = build_dim_campaign(config.start, config.end, rng)
    write_parquet(dim_campaign, out / "dim_campaign.parquet")
    tables_written["dim_campaign"] = len(dim_campaign)

    log.info("  Building dim_channel...")
    dim_channel = build_dim_channel()
    write_parquet(dim_channel, out / "dim_channel.parquet")
    tables_written["dim_channel"] = len(dim_channel)

    log.info("  Building dim_return_reason...")
    dim_return_reason = build_dim_return_reason()
    write_parquet(dim_return_reason, out / "dim_return_reason.parquet")
    tables_written["dim_return_reason"] = len(dim_return_reason)

    log.info("  Building dim_product (n=%d SKUs)...", config.n_products)
    dim_product = build_dim_product(config, rng)
    write_parquet(dim_product, out / "dim_product.parquet")
    tables_written["dim_product"] = len(dim_product)

    log.info("  Building dim_customer (n=%d)...", config.n_customers)
    dim_customer = build_dim_customer(config, rng)
    write_parquet(dim_customer, out / "dim_customer.parquet")
    tables_written["dim_customer"] = len(dim_customer)

    log.info("  Building fact_sales (n=%d rows)...", config.n_sales_rows)
    fact_sales = build_fact_sales(
        config, rng,
        dim_date, dim_store, dim_product, dim_customer, dim_channel, dim_campaign,
    )
    write_parquet(fact_sales, out / "fact_sales.parquet")
    tables_written["fact_sales"] = len(fact_sales)

    log.info("  Building fact_returns...")
    fact_returns = build_fact_returns(config, rng, fact_sales, dim_product)
    write_parquet(fact_returns, out / "fact_returns.parquet")
    tables_written["fact_returns"] = len(fact_returns)

    log.info("  Building fact_sessions (n=%d)...", config.n_sessions)
    fact_sessions = build_fact_sessions(config, rng, dim_date, dim_customer)
    write_parquet(fact_sessions, out / "fact_sessions.parquet")
    tables_written["fact_sessions"] = len(fact_sessions)

    log.info("  Building fact_marketing_spend...")
    fact_marketing_spend = build_fact_marketing_spend(config, rng, dim_campaign, dim_date)
    write_parquet(fact_marketing_spend, out / "fact_marketing_spend.parquet")
    tables_written["fact_marketing_spend"] = len(fact_marketing_spend)

    log.info("  Building fact_loyalty_events...")
    fact_loyalty_events = build_fact_loyalty_events(
        config, rng, fact_sales, dim_customer, dim_date
    )
    write_parquet(fact_loyalty_events, out / "fact_loyalty_events.parquet")
    tables_written["fact_loyalty_events"] = len(fact_loyalty_events)

    log.info("  Building fact_inventory_daily (n_skus=%d)...", config.n_inventory_skus)
    fact_inventory_daily = build_fact_inventory_daily(
        config, rng, fact_sales, dim_product, dim_store, dim_date
    )
    write_parquet(fact_inventory_daily, out / "fact_inventory_daily.parquet")
    tables_written["fact_inventory_daily"] = len(fact_inventory_daily)

    # ── Compute ID watermarks from built dataframes ───────────────────────────
    cust_id_seq = int(
        dim_customer["customer_id"].str.replace("CUST-", "").astype(int).max()
    )
    watermarks = IdWatermarks(
        sale_key=int(fact_sales["sale_key"].max()),
        order_seq=int(fact_sales["sale_key"].max()),  # proxy until order_id added
        return_key=int(fact_returns["return_key"].max()) if len(fact_returns) else 0,
        session_key=int(fact_sessions["session_key"].max()),
        customer_key=int(dim_customer["customer_key"].max()),
        customer_id_seq=cust_id_seq,
        event_key=int(fact_loyalty_events["event_key"].max()) if len(fact_loyalty_events) else 0,
        spend_key=int(fact_marketing_spend["spend_key"].max()) if len(fact_marketing_spend) else 0,
        inventory_key=int(fact_inventory_daily["inventory_key"].max()) if len(fact_inventory_daily) else 0,
    )

    manifest = Manifest(
        seed=config.seed,
        scale=config.scale,
        timeline=TimelineState(
            backfill_start=config.start,
            backfill_end=config.end,
            fictional_date=config.end,
            daily_runs_completed=0,
        ),
        id_watermarks=watermarks,
        patterns_module_version=module_version(),
        tables_written=list(tables_written.keys()),
        row_counts=tables_written,
    )
    manifest.save(out / "manifest.json")

    for name, count in tables_written.items():
        log.info("  + %-22s %8d rows", name, count)

    return manifest


def run_daily(config: GeneratorConfig, target_date: date) -> Manifest:
    """Generate one day of bronze-layer files and update the manifest."""
    from csnp_retail import daily as _daily
    from csnp_retail.patterns import derive_seed

    out = config.out
    manifest = Manifest.load(out / "manifest.json")
    _daily.validate_daily_date(manifest, target_date)

    rng = np.random.default_rng(derive_seed(config.seed, "daily", target_date))

    dims = _daily.load_gold_dims(out)

    sales_by_ch  = _daily.build_daily_sales(rng, target_date, manifest, dims, config)
    sessions     = _daily.build_daily_sessions(rng, target_date, manifest, dims, config)
    inventory    = _daily.build_daily_inventory(rng, target_date, manifest, dims, config, out)
    cust_delta, loyalty = _daily.build_daily_crm(
        rng, target_date, manifest, dims, config, sales_by_ch
    )
    campaigns, spend = _daily.build_daily_marketing(rng, target_date, manifest, dims)
    late_by_ch   = _daily.build_late_arrivals(rng, target_date, out, manifest)

    _daily.write_bronze_day(
        out=out,
        target_date=target_date,
        sales_by_channel=sales_by_ch,
        late_by_channel=late_by_ch,
        sessions_by_hour=sessions,
        inventory=inventory,
        customers_delta=cust_delta,
        loyalty_events=loyalty,
        campaigns=campaigns,
        spend=spend,
        dims=dims,
    )

    # Update watermarks
    fresh_sales = (
        pd.concat(list(sales_by_ch.values()), ignore_index=True)
        if sales_by_ch else pd.DataFrame()
    )
    all_sess = (
        pd.concat([df for _, df in sessions], ignore_index=True)
        if sessions else pd.DataFrame()
    )
    prev = manifest.id_watermarks
    new_wm = IdWatermarks(
        sale_key=int(fresh_sales["sale_key"].max()) if len(fresh_sales) else prev.sale_key,
        order_seq=int(fresh_sales["sale_key"].max()) if len(fresh_sales) else prev.order_seq,
        return_key=prev.return_key,
        session_key=int(all_sess["session_key"].max()) if len(all_sess) else prev.session_key,
        customer_key=int(cust_delta["customer_key"].max()) if len(cust_delta) else prev.customer_key,
        customer_id_seq=prev.customer_id_seq + len(cust_delta),
        event_key=int(loyalty["event_key"].max()) if len(loyalty) else prev.event_key,
        spend_key=int(spend["spend_key"].max()) if len(spend) else prev.spend_key,
        inventory_key=int(inventory["inventory_key"].max()) if len(inventory) else prev.inventory_key,
    )

    new_manifest = Manifest(
        seed=manifest.seed,
        scale=manifest.scale,
        timeline=TimelineState(
            backfill_start=manifest.timeline.backfill_start,
            backfill_end=manifest.timeline.backfill_end,
            fictional_date=target_date,
            daily_runs_completed=manifest.timeline.daily_runs_completed + 1,
        ),
        id_watermarks=new_wm,
        patterns_module_version=module_version(),
        tables_written=manifest.tables_written,
        row_counts=manifest.row_counts,
    )
    new_manifest.save(out / "manifest.json")

    n_s = len(fresh_sales)
    n_e = len(all_sess)
    log.info("  Daily %s | sales=%d sessions=%d inventory=%d", target_date, n_s, n_e, len(inventory))
    return new_manifest
