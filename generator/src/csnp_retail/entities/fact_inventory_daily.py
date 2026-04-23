"""fact_inventory_daily — daily inventory snapshot per SKU × store.

Planted insight patterns embedded here:
  §6.3  TX stores: elevated stockouts on Outerwear SKUs during
         Jun 15 – Aug 16 2025 (Texas heat event; demand collapsed,
         inventory already committed from pre-buys).

Grain: 1 row per (product_key, store_key, date_key).
Covers the top config.n_inventory_skus SKUs by sales volume.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from csnp_retail.config import GeneratorConfig

# ── Seasonal fill-rate multipliers by category and month (1=Jan … 12=Dec) ─────
# Values >1 mean product is more stocked relative to base; <1 means lighter stock
_SEASONAL_STOCK: dict[str, dict[int, float]] = {
    "outerwear":   {1:0.70, 2:0.60, 3:0.55, 4:0.50, 5:0.45, 6:0.40,
                    7:0.40, 8:0.50, 9:0.80, 10:1.10, 11:1.20, 12:1.00},
    "tops":        {1:0.80, 2:0.85, 3:0.95, 4:1.10, 5:1.10, 6:1.05,
                    7:1.00, 8:0.95, 9:1.00, 10:1.05, 11:1.10, 12:1.00},
    "bottoms":     {1:0.80, 2:0.85, 3:0.90, 4:1.00, 5:1.05, 6:1.00,
                    7:0.95, 8:0.95, 9:1.00, 10:1.05, 11:1.10, 12:1.00},
    "footwear":    {1:0.85, 2:0.85, 3:0.95, 4:1.05, 5:1.05, 6:1.00,
                    7:0.95, 8:0.95, 9:1.05, 10:1.05, 11:1.10, 12:1.00},
    "accessories": {1:0.85, 2:0.85, 3:0.95, 4:1.00, 5:1.05, 6:1.00,
                    7:0.95, 8:0.95, 9:1.00, 10:1.00, 11:1.10, 12:1.10},
    "home":        {1:0.80, 2:0.80, 3:0.90, 4:0.95, 5:1.00, 6:1.00,
                    7:0.95, 8:0.95, 9:1.00, 10:1.05, 11:1.15, 12:1.15},
    "beauty":      {1:0.85, 2:0.90, 3:0.95, 4:1.00, 5:1.10, 6:1.05,
                    7:1.00, 8:0.95, 9:1.00, 10:1.00, 11:1.10, 12:1.10},
}

# Base stock capacity per format type (units per SKU per store format)
_FORMAT_CAPACITY: dict[str, int] = {
    "Flagship": 120,
    "Standard": 60,
    "Outlet":   80,
    "Pop-up":   30,
}

# Base stockout probability by category (fraction of product-store-days with zero stock)
_STOCKOUT_PROB: dict[str, float] = {
    "outerwear":   0.12,
    "tops":        0.08,
    "bottoms":     0.07,
    "footwear":    0.09,
    "accessories": 0.05,
    "home":        0.04,
    "beauty":      0.04,
}

# §6.3 Texas heat event: outerwear stock in TX stores during Jun 15 – Aug 16 2025
_TX_HEAT_START = date(2025, 6, 15)
_TX_HEAT_END   = date(2025, 8, 16)
_TX_HEAT_STOCK_FACTOR = 0.05  # virtually out of stock during heat


def _date_key_to_date(dk: int) -> date:
    s = str(dk)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def build_fact_inventory_daily(
    config: GeneratorConfig,
    rng: np.random.Generator,
    fact_sales: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_store: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    n_inv_skus = config.n_inventory_skus

    # ── 1. Select top-N product_keys by sales volume ──────────────────────────
    sales_volume = (
        fact_sales.groupby("product_key")["quantity"]
        .sum()
        .sort_values(ascending=False)
    )
    top_prod_keys = sales_volume.index[:n_inv_skus].values.astype(np.int64)
    if len(top_prod_keys) == 0:
        return _empty_df()

    # ── 2. Product metadata: category, outerwear flag ─────────────────────────
    prod_meta = (
        dim_product[dim_product["is_current"]][["product_key", "category"]]
        .drop_duplicates("product_key")
        .set_index("product_key")
    )
    # Only keep product_keys that exist in current dim_product
    top_prod_keys = np.array(
        [pk for pk in top_prod_keys if pk in prod_meta.index], dtype=np.int64
    )
    if len(top_prod_keys) == 0:
        return _empty_df()

    prod_categories = np.array(
        [prod_meta.loc[pk, "category"] for pk in top_prod_keys], dtype=object
    )
    is_outerwear = prod_categories == "outerwear"

    # ── 3. Store metadata: format_type, TX flag ───────────────────────────────
    store_keys    = dim_store["store_key"].values.astype(np.int64)
    store_formats = dim_store["format_type"].values.astype(object)
    is_tx_store   = (dim_store["state_code"].values == "TX").astype(bool)

    # ── 4. Date metadata ───────────────────────────────────────────────────────
    date_keys = dim_date["date_key"].values.astype(np.int64)
    months    = dim_date["month"].values.astype(int)
    date_objs = np.array([_date_key_to_date(int(dk)) for dk in date_keys])

    tx_heat_mask = np.array(
        [_TX_HEAT_START <= d <= _TX_HEAT_END for d in date_objs], dtype=bool
    )

    n_prods  = len(top_prod_keys)
    n_stores = len(store_keys)
    n_dates  = len(date_keys)

    # ── 5. Build base capacity matrix (n_prods × n_stores) ────────────────────
    base_cap = np.array(
        [_FORMAT_CAPACITY.get(fmt, 60) for fmt in store_formats], dtype=float
    )  # shape (n_stores,)

    # Per-product capacity multiplier (outerwear typically stocked in higher quantities)
    prod_cap_mult = np.where(is_outerwear, 1.3, 1.0)  # shape (n_prods,)

    # ── 6. Build full cross-product and compute inventory ─────────────────────
    # Build index arrays for the full (n_prods × n_stores × n_dates) cross-product
    p_idx = np.repeat(np.arange(n_prods), n_stores * n_dates)
    s_idx = np.tile(np.repeat(np.arange(n_stores), n_dates), n_prods)
    d_idx = np.tile(np.arange(n_dates), n_prods * n_stores)

    n_total = n_prods * n_stores * n_dates

    # Category-based seasonal fill for each row
    row_cats   = prod_categories[p_idx]
    row_months = months[d_idx]
    seasonal   = np.array(
        [
            _SEASONAL_STOCK.get(cat, _SEASONAL_STOCK["tops"]).get(mo, 1.0)
            for cat, mo in zip(row_cats, row_months)
        ],
        dtype=float,
    )

    # Base stock per row = format_capacity × prod_mult × seasonal
    row_base_cap = (
        base_cap[s_idx] * prod_cap_mult[p_idx] * seasonal
    )

    # Safety stock = 20% of base capacity (before seasonal)
    safety_stock = np.round(base_cap[s_idx] * prod_cap_mult[p_idx] * 0.20).astype(int)

    # ── Stockout probability per row ──────────────────────────────────────────
    base_so_p = np.array(
        [_STOCKOUT_PROB.get(cat, 0.07) for cat in row_cats], dtype=float
    )
    # Out-of-season products have elevated stockout risk
    base_so_p *= np.where(seasonal < 0.65, 1.6, 1.0)

    # §6.3: TX stores × outerwear × heat window → near-certain stockout
    tx_heat_rows = is_outerwear[p_idx] & is_tx_store[s_idx] & tx_heat_mask[d_idx]
    base_so_p[tx_heat_rows] = 0.88

    base_so_p = np.clip(base_so_p, 0.0, 0.90)

    # Sample stockout flag, then fill non-stockout rows with Poisson inventory
    is_stockout  = rng.random(n_total) < base_so_p
    units_on_hand = np.where(
        is_stockout,
        0,
        np.clip(rng.poisson(row_base_cap * 0.65, n_total).astype(int), 1, None),
    )

    is_low_stock = (~is_stockout) & (units_on_hand < safety_stock)

    # units_on_order: placed when stock is low or out
    reorder_mask = is_stockout | is_low_stock
    units_on_order = np.where(
        reorder_mask,
        rng.poisson(row_base_cap * 0.4, n_total).astype(int),
        0,
    )

    # units_in_transit: fraction of on-order that's already shipped
    units_in_transit = np.where(
        units_on_order > 0,
        rng.binomial(units_on_order, 0.4),
        0,
    )

    # days_of_supply: how many days stock lasts at average daily sales rate
    avg_daily_sales = np.where(row_base_cap > 0, row_base_cap * seasonal * 0.02, 0.5)
    days_of_supply = np.where(
        is_stockout,
        0.0,
        np.round(units_on_hand / np.maximum(avg_daily_sales, 0.1), 1),
    )

    # ── 7. Assemble DataFrame ─────────────────────────────────────────────────
    df = pd.DataFrame({
        "date_key":          date_keys[d_idx],
        "product_key":       top_prod_keys[p_idx],
        "store_key":         store_keys[s_idx],
        "units_on_hand":     units_on_hand,
        "units_on_order":    units_on_order,
        "units_in_transit":  units_in_transit,
        "safety_stock_level": safety_stock,
        "days_of_supply":    days_of_supply,
        "is_stockout":       is_stockout,
        "is_low_stock":      is_low_stock,
    })
    df.insert(0, "inventory_key", range(1, len(df) + 1))
    return df


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "inventory_key", "date_key", "product_key", "store_key",
        "units_on_hand", "units_on_order", "units_in_transit",
        "safety_stock_level", "days_of_supply", "is_stockout", "is_low_stock",
    ])
