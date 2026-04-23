"""fact_sales — transaction grain sales fact table.

Planted insight patterns embedded here:
  §6.3  TX heat event: TX-store outerwear demand -55% from Jun 15 – Aug 16, 2025.
  §6.11 Meridian Cable Crew spike: 9x normal sales weight in Jul 2025.

Channel mix shifts linearly over the 3-year window:
  Store: 76% -> 59%  |  Web: 18% -> 30%  |  App: 5% -> 9%  |  Marketplace: 1% -> 2%
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from csnp_retail.config import GeneratorConfig
from csnp_retail.entities.dim_product import _CAT_WEIGHTS

# ── Date demand weights ────────────────────────────────────────────────────────

_MONTH_WEIGHTS = {
    1: 0.70, 2: 0.75, 3: 1.00,
    4: 1.10, 5: 1.00, 6: 0.90,
    7: 0.85, 8: 0.80, 9: 0.90,
    10: 1.10, 11: 1.40, 12: 1.60,
}

_WEEKDAY_WEIGHTS = {0: 0.95, 1: 0.90, 2: 0.90, 3: 1.00, 4: 1.20, 5: 1.40, 6: 1.10}

# ── Planted pattern constants ─────────────────────────────────────────────────

_TX_HEAT_START  = date(2025, 6, 15)
_TX_HEAT_END    = date(2025, 8, 16)
_MERIDIAN_MONTH = (2025, 7)      # (year, month)
_OUTERWEAR_MULTIPLIER = 0.45     # -55%
_MERIDIAN_MULTIPLIER  = 9.0


def _channel_probs(t: np.ndarray) -> np.ndarray:
    """Return (n,4) array of channel probabilities at each fractional time t."""
    store = 0.76 - 0.17 * t
    web   = 0.18 + 0.12 * t
    app_  = 0.05 + 0.04 * t
    mkt   = 0.01 + 0.01 * t
    return np.stack([store, web, app_, mkt], axis=1)


def build_fact_sales(
    config: GeneratorConfig,
    rng: np.random.Generator,
    dim_date: pd.DataFrame,
    dim_store: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_channel: pd.DataFrame,
    dim_campaign: pd.DataFrame,
) -> pd.DataFrame:
    n         = config.n_sales_rows
    start     = config.start
    end       = config.end
    span_days = (end - start).days

    # ── 1. Sample sale dates ──────────────────────────────────────────────────
    month_w   = dim_date["month"].map(_MONTH_WEIGHTS).values.astype(float)
    weekday_w = dim_date["day_of_week"].map(_WEEKDAY_WEIGHTS).values.astype(float)
    promo_w   = np.where(dim_date["is_promo_window"].values, 1.4, 1.0)
    date_w    = month_w * weekday_w * promo_w
    date_w   /= date_w.sum()

    date_idx   = rng.choice(len(dim_date), size=n, p=date_w)
    date_keys  = dim_date["date_key"].values[date_idx]
    dates_dt   = pd.to_datetime(dim_date["date"].values[date_idx])
    dates_py   = np.array([ts.date() for ts in dates_dt])

    # Promo window flag per sale
    is_promo = dim_date["is_promo_window"].values[date_idx]

    # fractional position in backfill [0,1]
    t_vals = np.array([(d - start).days for d in dates_py], dtype=float) / span_days

    # ── 2. Channel selection (vectorised) ─────────────────────────────────────
    ch_probs  = _channel_probs(t_vals)
    cum       = np.cumsum(ch_probs, axis=1)
    u_ch      = rng.random(n)
    channel_keys = np.ones(n, dtype=np.int64)
    channel_keys[u_ch >= cum[:, 0]] = 2
    channel_keys[(u_ch >= cum[:, 1]) & (channel_keys == 2)] = 3
    # fix: re-derive cleanly
    channel_keys = (
        np.where(u_ch < cum[:, 0], 1,
        np.where(u_ch < cum[:, 1], 2,
        np.where(u_ch < cum[:, 2], 3, 4)))
    ).astype(np.int64)

    is_store_ch = channel_keys == 1

    # ── 3. Store assignment (store-channel rows only) ─────────────────────────
    s_open_dt  = dim_store["open_date"].values   # datetime64
    s_close_dt = dim_store["close_date"].values  # datetime64, NaT if open-ended
    s_keys     = dim_store["store_key"].values
    s_states   = dim_store["state_code"].values
    s_currency = dim_store["currency_code"].values

    store_keys_out     = np.zeros(n, dtype=np.int64)
    store_states_out   = np.full(n, "", dtype=object)
    store_currency_out = np.full(n, "USD", dtype=object)

    store_indices = np.where(is_store_ch)[0]
    if len(store_indices) > 0:
        unique_dates = np.unique(dates_py[store_indices])
        open_by_date: dict[date, tuple] = {}
        for d in unique_dates:
            d_ts = pd.Timestamp(d)
            open_mask = (s_open_dt <= d_ts) & (pd.isna(s_close_dt) | (s_close_dt >= d_ts))
            if open_mask.any():
                open_by_date[d] = (
                    s_keys[open_mask],
                    s_states[open_mask],
                    s_currency[open_mask],
                )

        for d in unique_dates:
            if d not in open_by_date:
                continue
            ok, os, oc = open_by_date[d]
            m = is_store_ch & (dates_py == d)
            nm = int(m.sum())
            if nm == 0:
                continue
            picks = rng.integers(0, len(ok), size=nm)
            store_keys_out[m]     = ok[picks]
            store_states_out[m]   = os[picks]
            store_currency_out[m] = oc[picks]

    # ── 4. Product selection with planted patterns ────────────────────────────
    cur_prod = dim_product[dim_product["is_current"]].copy().reset_index(drop=True)
    cat_counts = cur_prod.groupby("category").size().to_dict()
    cat_wt_dict = dict(_CAT_WEIGHTS)
    base_w = np.array([
        cat_wt_dict.get(row["category"], 0.0) / cat_counts.get(row["category"], 1)
        for _, row in cur_prod.iterrows()
    ], dtype=float)
    base_w /= base_w.sum()

    is_outerwear = cur_prod["category"].values == "outerwear"
    is_mcc       = cur_prod["style_code"].str.startswith("TOP-MCC").values

    # Boolean masks for planted periods
    is_tx_heat = (
        is_store_ch
        & np.array([s == "TX" for s in store_states_out])
        & np.array([_TX_HEAT_START <= d <= _TX_HEAT_END for d in dates_py])
    )
    is_meridian = np.array(
        [d.year == _MERIDIAN_MONTH[0] and d.month == _MERIDIAN_MONTH[1] for d in dates_py]
    )

    # Build four product weight variants
    def _apply(w: np.ndarray, tx: bool, mer: bool) -> np.ndarray:
        w = w.copy()
        if tx:
            w[is_outerwear] *= _OUTERWEAR_MULTIPLIER
        if mer:
            w[is_mcc] *= _MERIDIAN_MULTIPLIER
        total = w.sum()
        return w / total if total > 0 else w

    w_normal = _apply(base_w, False, False)
    w_tx     = _apply(base_w, True,  False)
    w_mer    = _apply(base_w, False, True)
    w_both   = _apply(base_w, True,  True)

    prod_indices = np.zeros(n, dtype=np.int64)

    def _sample(mask: np.ndarray, w: np.ndarray) -> None:
        cnt = int(mask.sum())
        if cnt > 0:
            prod_indices[mask] = rng.choice(len(cur_prod), size=cnt, p=w)

    _sample(~is_tx_heat & ~is_meridian, w_normal)
    _sample(is_tx_heat  & ~is_meridian, w_tx)
    _sample(~is_tx_heat & is_meridian,  w_mer)
    _sample(is_tx_heat  & is_meridian,  w_both)

    sel_prod    = cur_prod.iloc[prod_indices]
    product_ids = sel_prod["product_id"].values

    # ── 5. SCD2 product_key lookup ────────────────────────────────────────────
    # Merge all SCD2 versions with sales on product_id, then keep the latest
    # version whose effective_date <= sale_date.
    prod_lookup = dim_product[
        ["product_id", "effective_date", "product_key", "list_price", "cost_price"]
    ].copy()

    sales_for_prod = pd.DataFrame({
        "_idx":       np.arange(n),
        "product_id": product_ids,
        "sale_date":  dates_dt,
    })

    merged = sales_for_prod.merge(prod_lookup, on="product_id", how="left")
    # Keep only versions active on or before the sale date
    merged = merged[merged["effective_date"] <= merged["sale_date"]]
    # For each sale (_idx) keep the latest (max effective_date) version
    merged = (
        merged.sort_values("effective_date")
        .groupby("_idx", as_index=False)
        .agg({"product_key": "last", "list_price": "last", "cost_price": "last"})
        .sort_values("_idx")
    )

    product_keys_out = merged["product_key"].values
    list_prices      = merged["list_price"].values.astype(float)
    cost_prices      = merged["cost_price"].values.astype(float)

    # ── 6. Customer assignment ────────────────────────────────────────────────
    # 85% of store sales logged; 90% of digital sales logged
    attach_p    = np.where(is_store_ch, 0.85, 0.90)
    has_cust    = rng.random(n) < attach_p

    n_cust      = config.n_customers
    cust_ids    = np.where(has_cust, rng.integers(1, n_cust + 1, size=n), 0)

    # SCD2 customer_key lookup
    cur_cust = dim_customer[dim_customer["is_current"]].copy()
    cust_id_to_key = dict(zip(
        cur_cust["customer_id"].str.replace("CUST-", "").astype(int),
        cur_cust["customer_key"].values,
    ))
    customer_keys_out = np.array(
        [cust_id_to_key.get(int(c), pd.NA) if c > 0 else pd.NA for c in cust_ids],
        dtype=object,
    )

    # ── 7. Campaign assignment ────────────────────────────────────────────────
    camp_start = pd.to_datetime(dim_campaign["start_date"]).dt.date.values
    camp_end   = pd.to_datetime(dim_campaign["end_date"]).dt.date.values
    camp_keys  = dim_campaign["campaign_key"].values

    # Build active-campaigns-per-date lookup for unique sale dates
    unique_sale_dates = np.unique(dates_py)
    active_by_date: dict[date, np.ndarray] = {}
    for d in unique_sale_dates:
        active = camp_keys[(camp_start <= d) & (camp_end >= d)]
        if len(active):
            active_by_date[d] = active

    is_promo_by_date: dict[date, bool] = {}
    for _, row in dim_date.iterrows():
        is_promo_by_date[row["date"].date()] = bool(row["is_promo_window"])

    campaign_keys_out = np.full(n, pd.NA, dtype=object)
    for d in unique_sale_dates:
        active = active_by_date.get(d)
        if active is None:
            continue
        p_assign = 0.40 if is_promo_by_date.get(d, False) else 0.10
        m     = dates_py == d
        nmask = int(m.sum())
        if nmask == 0:
            continue
        assigned = rng.random(nmask) < p_assign
        if assigned.any():
            chosen = rng.choice(active, size=int(assigned.sum()))
            idxs = np.where(m)[0][assigned]
            campaign_keys_out[idxs] = chosen

    # ── 8. Quantities, pricing, amounts ──────────────────────────────────────
    qty_choices = rng.choice([1, 2, 3], size=n, p=[0.82, 0.15, 0.03])

    # Discount from campaign or small organic markdown
    disc_pct = np.zeros(n, dtype=float)
    has_campaign = np.array([v is not pd.NA for v in campaign_keys_out])
    if has_campaign.any():
        camp_key_to_disc = dict(zip(
            dim_campaign["campaign_key"].values,
            dim_campaign["discount_pct"].values.astype(float) / 100.0,
        ))
        for i in np.where(has_campaign)[0]:
            ck = campaign_keys_out[i]
            if ck is not pd.NA:
                disc_pct[i] = camp_key_to_disc.get(int(ck), 0.0)

    # 5% organic markdown (5-15%) for non-campaign rows
    organic_sale = (~has_campaign) & (rng.random(n) < 0.05)
    disc_pct[organic_sale] = rng.uniform(0.05, 0.15, size=int(organic_sale.sum()))

    gross_amount    = list_prices * qty_choices
    discount_amount = gross_amount * disc_pct
    net_amount      = gross_amount - discount_amount
    cost_amount     = cost_prices * qty_choices
    gross_margin    = net_amount - cost_amount

    # ── 9. Currency ───────────────────────────────────────────────────────────
    currency = store_currency_out.copy()
    currency[~is_store_ch] = "USD"

    # ── 10. Assemble DataFrame ────────────────────────────────────────────────
    df = pd.DataFrame({
        "date_key":        date_keys,
        "customer_key":    customer_keys_out,
        "product_key":     product_keys_out,
        "store_key":       np.where(is_store_ch, store_keys_out, pd.NA),
        "channel_key":     channel_keys,
        "campaign_key":    campaign_keys_out,
        "quantity":        qty_choices,
        "unit_price":      list_prices,
        "discount_pct":    disc_pct,
        "gross_amount":    gross_amount.round(2),
        "discount_amount": discount_amount.round(2),
        "net_amount":      net_amount.round(2),
        "cost_amount":     cost_amount.round(2),
        "gross_margin":    gross_margin.round(2),
        "currency_code":   currency,
    })
    df.insert(0, "sale_key", range(1, n + 1))
    sale_years = date_keys // 10_000  # YYYYMMDD → YYYY
    df.insert(1, "order_id", [
        f"CSNP-{yr}-{seq:09d}" for yr, seq in zip(sale_years, range(1, n + 1))
    ])

    # Nullable integer columns
    for col in ["customer_key", "store_key", "campaign_key"]:
        df[col] = pd.array(df[col].values, dtype="Int64")

    return df
