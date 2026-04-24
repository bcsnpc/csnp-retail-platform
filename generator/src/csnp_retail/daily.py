"""Daily-mode generator — one day of bronze-layer files.

Each function is stateless: receives (rng, target_date, manifest, dims, config)
and returns data for that day only.  The rng is seeded with
derive_seed(config.seed, "daily", target_date) before the first call so
every re-run of the same (manifest, date) produces byte-identical output.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

from csnp_retail.config import GeneratorConfig
from csnp_retail.entities.dim_customer import (
    _ACQN_CHANNELS,
    _ACQN_WEIGHTS,
    _GENDER_WEIGHTS,
    _GENDERS,
    _INITIAL_TIER_WEIGHTS,
    _SEGMENTS,
    _TIERS,
)
from csnp_retail.entities.fact_inventory_daily import (
    _FORMAT_CAPACITY,
    _SEASONAL_STOCK,
    _STOCKOUT_PROB,
    _TX_HEAT_END,
    _TX_HEAT_START,
)
from csnp_retail.entities.fact_marketing_spend import (
    _CAMPAIGN_CHANNELS,
    _CTR,
    _IMPRESSIONS_PER_DOLLAR,
    _REVENUE_ROI,
    _SPEND_RANGE,
)
from csnp_retail.io import read_parquet, write_parquet
from csnp_retail.manifest import Manifest

# ── Demand weights (mirrors fact_sales.py and fact_sessions.py) ───────────────

_MONTH_WEIGHTS = {
    1: 0.70, 2: 0.75, 3: 1.00, 4: 1.10, 5: 1.00, 6: 0.90,
    7: 0.85, 8: 0.80, 9: 0.90, 10: 1.10, 11: 1.40, 12: 1.60,
}
_WEEKDAY_WEIGHTS = {0: 0.95, 1: 0.90, 2: 0.90, 3: 1.00, 4: 1.20, 5: 1.40, 6: 1.10}

_SESSION_MONTH_WEIGHTS = {
    1: 0.75, 2: 0.80, 3: 1.05, 4: 1.10, 5: 1.00, 6: 0.92,
    7: 0.88, 8: 0.85, 9: 0.92, 10: 1.08, 11: 1.38, 12: 1.50,
}
_SESSION_WEEKDAY_WEIGHTS = {0: 0.95, 1: 0.90, 2: 0.90, 3: 1.00, 4: 1.15, 5: 1.30, 6: 1.20}

# Hour probability distribution for clickstream partitioning (peaks 12-14, 19-21)
_HOUR_WEIGHTS = np.array([
    0.5, 0.3, 0.2, 0.2, 0.2, 0.3,   # 00-05  night
    0.5, 0.8, 1.0, 1.1, 1.2, 1.3,   # 06-11  morning
    1.4, 1.4, 1.3, 1.2, 1.1, 1.2,   # 12-17  afternoon
    1.4, 1.5, 1.4, 1.2, 0.9, 0.7,   # 18-23  evening
], dtype=float)
_HOUR_PROBS = _HOUR_WEIGHTS / _HOUR_WEIGHTS.sum()

# Late arrivals: ~0.5% of each of D-1, D-2, D-3 re-lands in today's bronze
_LATE_ARRIVAL_RATE = 0.005


# ── Weight helpers ────────────────────────────────────────────────────────────

def _day_weight(d: date) -> float:
    return _MONTH_WEIGHTS[d.month] * _WEEKDAY_WEIGHTS[d.weekday()]


def _session_weight(d: date) -> float:
    return _SESSION_MONTH_WEIGHTS[d.month] * _SESSION_WEEKDAY_WEIGHTS[d.weekday()]


def _backfill_avg_weights(config: GeneratorConfig) -> tuple[float, float]:
    """Average (sales, session) demand weight over the backfill window.

    Computed once per daily run so expected_daily_count stays proportional
    to the backfill distribution.
    """
    start, end = config.start, config.end
    n_days = (end - start).days + 1
    total_s = sum(_day_weight(start + timedelta(days=i)) for i in range(n_days))
    total_e = sum(_session_weight(start + timedelta(days=i)) for i in range(n_days))
    return total_s / n_days, total_e / n_days


def _channel_probs_at_t1() -> np.ndarray:
    """Channel probabilities at t=1 (end of linear mix-shift window)."""
    return np.array([
        0.76 - 0.17,   # Store  0.59
        0.18 + 0.12,   # Web    0.30
        0.05 + 0.04,   # App    0.09
        0.01 + 0.01,   # Mkt    0.02
    ])


# ── Public API ────────────────────────────────────────────────────────────────

def validate_daily_date(manifest: Manifest, target_date: date) -> None:
    """Raise ValueError unless target_date == manifest.fictional_date + 1 day."""
    expected = manifest.timeline.fictional_date + timedelta(days=1)
    if target_date != expected:
        raise ValueError(
            f"Expected next daily date {expected!r}, got {target_date!r}. "
            f"Manifest fictional_date={manifest.timeline.fictional_date!r}"
        )


def load_gold_dims(gold_dir: Path) -> dict[str, pd.DataFrame]:
    """Load all dimension Parquet files from the gold output directory."""
    dims: dict[str, pd.DataFrame] = {}
    for name in [
        "dim_date", "dim_geography", "dim_store", "dim_campaign",
        "dim_channel", "dim_return_reason", "dim_product", "dim_customer",
    ]:
        p = gold_dir / f"{name}.parquet"
        if p.exists():
            dims[name] = read_parquet(p)
    return dims


def build_daily_sales(
    rng: np.random.Generator,
    target_date: date,
    manifest: Manifest,
    dims: dict[str, pd.DataFrame],
    config: GeneratorConfig,
) -> dict[int, pd.DataFrame]:
    """Return {channel_key: DataFrame} of today's fresh sales.

    sale_key and order_seq continue from manifest watermarks.
    All rows have late_arrival_days=0.
    """
    avg_w, _ = _backfill_avg_weights(config)
    span = (config.end - config.start).days + 1
    expected = (config.n_sales_rows / span) * _day_weight(target_date) / avg_w
    n = int(rng.poisson(expected))
    if n == 0:
        return {}

    date_key = int(target_date.strftime("%Y%m%d"))
    start_sale_key = manifest.id_watermarks.sale_key + 1
    start_order_seq = manifest.id_watermarks.order_seq + 1

    # Channel assignment at t=1 (linear shift is complete)
    ch_probs = _channel_probs_at_t1()
    cum = np.cumsum(ch_probs)
    u_ch = rng.random(n)
    channel_keys = np.where(
        u_ch < cum[0], 1,
        np.where(u_ch < cum[1], 2,
        np.where(u_ch < cum[2], 3, 4))
    ).astype(np.int64)
    is_store_ch = channel_keys == 1

    # Store assignment
    dim_store = dims["dim_store"]
    target_ts = pd.Timestamp(target_date)
    s_open = pd.to_datetime(dim_store["open_date"]).values
    s_close_raw = dim_store["close_date"].values
    open_mask = (s_open <= target_ts) & (
        pd.isnull(s_close_raw) | (pd.to_datetime(pd.Series(s_close_raw)).values >= target_ts)
    )
    open_stores  = dim_store["store_key"].values[open_mask].astype(np.int64)
    open_curr    = dim_store["currency_code"].values[open_mask]

    store_keys_out = np.zeros(n, dtype=np.int64)
    store_curr_out = np.full(n, "USD", dtype=object)

    n_store = int(is_store_ch.sum())
    if n_store > 0 and len(open_stores) > 0:
        picks = rng.integers(0, len(open_stores), size=n_store)
        store_keys_out[is_store_ch] = open_stores[picks]
        store_curr_out[is_store_ch] = open_curr[picks]

    # Product assignment (no planted modifiers active after backfill window)
    from csnp_retail.entities.dim_product import _CAT_WEIGHTS
    dim_product = dims["dim_product"]
    cur_prod = dim_product[dim_product["is_current"]].copy().reset_index(drop=True)
    cat_counts = cur_prod.groupby("category").size().to_dict()
    cat_wt = dict(_CAT_WEIGHTS)
    base_w = np.array([
        cat_wt.get(r["category"], 0.0) / cat_counts.get(r["category"], 1)
        for _, r in cur_prod.iterrows()
    ], dtype=float)
    base_w /= base_w.sum()
    prod_idx = rng.choice(len(cur_prod), size=n, p=base_w)
    sel = cur_prod.iloc[prod_idx]
    product_keys_out = sel["product_key"].values.astype(np.int64)
    list_prices      = sel["list_price"].values.astype(float)
    cost_prices      = sel["cost_price"].values.astype(float)

    # Customer assignment (reference existing customers only)
    max_ck = manifest.id_watermarks.customer_key
    attach_p = np.where(is_store_ch, 0.85, 0.90)
    has_cust = rng.random(n) < attach_p
    ck_raw = np.where(has_cust & (max_ck > 0), rng.integers(1, max_ck + 1, size=n), 0)
    customer_keys_out = pd.array(np.where(ck_raw > 0, ck_raw, pd.NA), dtype="Int64")

    # Campaign assignment
    dim_campaign = dims.get("dim_campaign", pd.DataFrame())
    campaign_keys_out = np.full(n, pd.NA, dtype=object)
    if len(dim_campaign) > 0:
        cs = pd.to_datetime(dim_campaign["start_date"]).dt.date.values
        ce = pd.to_datetime(dim_campaign["end_date"]).dt.date.values
        active_ck = dim_campaign["campaign_key"].values[(cs <= target_date) & (ce >= target_date)]
        if len(active_ck) > 0:
            assigned = rng.random(n) < 0.10
            if assigned.any():
                campaign_keys_out[assigned] = rng.choice(active_ck, size=int(assigned.sum()))

    # Discount
    disc_pct = np.zeros(n, dtype=float)
    has_camp = np.array([v is not pd.NA for v in campaign_keys_out])
    if has_camp.any() and len(dim_campaign) > 0:
        ck_to_disc = dict(zip(
            dim_campaign["campaign_key"].values,
            dim_campaign["discount_pct"].values.astype(float) / 100.0,
        ))
        for i in np.where(has_camp)[0]:
            ck = campaign_keys_out[i]
            if ck is not pd.NA:
                disc_pct[i] = ck_to_disc.get(int(ck), 0.0)
    organic = (~has_camp) & (rng.random(n) < 0.05)
    disc_pct[organic] = rng.uniform(0.05, 0.15, size=int(organic.sum()))

    qty   = rng.choice([1, 2, 3], size=n, p=[0.82, 0.15, 0.03])
    gross = list_prices * qty
    disc_amt = gross * disc_pct
    net  = gross - disc_amt
    cost = cost_prices * qty
    margin = net - cost

    currency = store_curr_out.copy()
    currency[~is_store_ch] = "USD"

    sale_keys  = np.arange(start_sale_key,  start_sale_key  + n, dtype=np.int64)
    order_seqs = np.arange(start_order_seq, start_order_seq + n, dtype=np.int64)
    order_ids  = [f"CSNP-{target_date.year}-{s:09d}" for s in order_seqs]

    df = pd.DataFrame({
        "sale_key":         sale_keys,
        "order_id":         order_ids,
        "date_key":         np.full(n, date_key, dtype=np.int64),
        "customer_key":     customer_keys_out,
        "product_key":      product_keys_out,
        "store_key":        pd.array(np.where(is_store_ch, store_keys_out, pd.NA), dtype="Int64"),
        "channel_key":      channel_keys,
        "campaign_key":     pd.array(campaign_keys_out, dtype="Int64"),
        "quantity":         qty,
        "unit_price":       list_prices,
        "discount_pct":     disc_pct,
        "gross_amount":     gross.round(2),
        "discount_amount":  disc_amt.round(2),
        "net_amount":       net.round(2),
        "cost_amount":      cost.round(2),
        "gross_margin":     margin.round(2),
        "currency_code":    currency,
        "late_arrival_days": np.zeros(n, dtype=np.int64),
    })

    return {ch: df[df["channel_key"] == ch].copy() for ch in [1, 2, 3, 4] if (df["channel_key"] == ch).any()}


def build_daily_sessions(
    rng: np.random.Generator,
    target_date: date,
    manifest: Manifest,
    dims: dict[str, pd.DataFrame],
    config: GeneratorConfig,
) -> list[tuple[int, pd.DataFrame]]:
    """Return list of (hour, DataFrame) for clickstream bronze partitioning."""
    _, avg_sw = _backfill_avg_weights(config)
    span = (config.end - config.start).days + 1
    expected = (config.n_sessions / span) * _session_weight(target_date) / avg_sw
    n = int(rng.poisson(expected))
    if n == 0:
        return []

    date_key  = int(target_date.strftime("%Y%m%d"))
    start_key = manifest.id_watermarks.session_key + 1

    u_ch = rng.random(n)
    channel_keys = np.where(u_ch < 0.65, 2, 3).astype(np.int64)

    dev_by_ch: dict[int, dict] = {
        2: {"Mobile": 0.52, "Desktop": 0.37, "Tablet": 0.11},
        3: {"Mobile": 0.82, "Tablet": 0.14, "Desktop": 0.04},
    }
    devices = np.empty(n, dtype=object)
    for ch, dist in dev_by_ch.items():
        mask = channel_keys == ch
        names = list(dist.keys())
        probs = np.array(list(dist.values()), dtype=float)
        probs /= probs.sum()
        devices[mask] = rng.choice(names, size=int(mask.sum()), p=probs)

    conv = {"Desktop": 0.28, "Tablet": 0.20, "Mobile": 0.14}
    is_converted = rng.random(n) < np.array([conv[d] for d in devices])

    pages_cfg = {2: (5.2, 3.4), 3: (3.1, 1.8)}
    time_cfg = {
        "Desktop-2": (210, 130), "Mobile-2": (140, 100), "Tablet-2": (185, 120),
        "Desktop-3": (125, 80),  "Mobile-3": (95,  60),  "Tablet-3": (110, 70),
    }
    pages_viewed = np.zeros(n, dtype=np.int64)
    time_on_site = np.zeros(n, dtype=np.int64)
    for ch in (2, 3):
        mu_p, sd_p = pages_cfg[ch]
        for dev in ("Mobile", "Desktop", "Tablet"):
            dm = (channel_keys == ch) & (devices == dev)
            cnt = int(dm.sum())
            if cnt == 0:
                continue
            mu_t, sd_t = time_cfg[f"{dev}-{ch}"]
            pages_viewed[dm] = np.clip(rng.normal(mu_p, sd_p, cnt).round().astype(int), 1, 50)
            time_on_site[dm] = np.clip(rng.normal(mu_t, sd_t, cnt).round().astype(int), 5, 3600)

    max_ck = manifest.id_watermarks.customer_key
    has_cust = rng.random(n) < 0.65
    ck_raw = np.where(has_cust & (max_ck > 0), rng.integers(1, max_ck + 1, size=n), 0)
    customer_keys_out = pd.array(np.where(ck_raw > 0, ck_raw, pd.NA), dtype="Int64")

    hours = rng.choice(24, size=n, p=_HOUR_PROBS).astype(np.int64)
    session_keys = np.arange(start_key, start_key + n, dtype=np.int64)

    df = pd.DataFrame({
        "session_key":       session_keys,
        "date_key":          np.full(n, date_key, dtype=np.int64),
        "customer_key":      customer_keys_out,
        "channel_key":       channel_keys,
        "device_type":       devices,
        "pages_viewed":      pages_viewed,
        "time_on_site_secs": time_on_site,
        "is_converted":      is_converted,
        "hour":              hours,
    })

    return [
        (h, df[df["hour"] == h].drop(columns=["hour"]).copy())
        for h in range(24)
        if (df["hour"] == h).any()
    ]


def build_daily_inventory(
    rng: np.random.Generator,
    target_date: date,
    manifest: Manifest,
    dims: dict[str, pd.DataFrame],
    config: GeneratorConfig,
    gold_dir: Path,
) -> pd.DataFrame:
    """One inventory snapshot row per (top-SKU × store) for target_date."""
    gold_sales_path = gold_dir / "fact_sales.parquet"
    if not gold_sales_path.exists():
        return pd.DataFrame()
    gold_sales = read_parquet(gold_sales_path)

    volume = gold_sales.groupby("product_key")["quantity"].sum().sort_values(ascending=False)
    top_keys = volume.index[: config.n_inventory_skus].values.astype(np.int64)

    dim_product = dims["dim_product"]
    prod_meta = (
        dim_product[dim_product["is_current"]][["product_key", "category"]]
        .drop_duplicates("product_key")
        .set_index("product_key")
    )
    top_keys = np.array([pk for pk in top_keys if pk in prod_meta.index], dtype=np.int64)
    if len(top_keys) == 0:
        return pd.DataFrame()

    prod_cats = np.array([prod_meta.loc[pk, "category"] for pk in top_keys], dtype=object)

    dim_store    = dims["dim_store"]
    store_keys   = dim_store["store_key"].values.astype(np.int64)
    store_fmts   = dim_store["format_type"].values.astype(object)
    is_tx        = (dim_store["state_code"].values == "TX").astype(bool)

    month      = target_date.month
    date_key   = int(target_date.strftime("%Y%m%d"))
    is_tx_heat = _TX_HEAT_START <= target_date <= _TX_HEAT_END

    n_p, n_s  = len(top_keys), len(store_keys)
    n_total   = n_p * n_s
    p_idx     = np.repeat(np.arange(n_p), n_s)
    s_idx     = np.tile(np.arange(n_s), n_p)

    row_cats = prod_cats[p_idx]
    seasonal = np.array([
        _SEASONAL_STOCK.get(cat, _SEASONAL_STOCK["tops"]).get(month, 1.0)
        for cat in row_cats
    ], dtype=float)

    base_cap     = np.array([_FORMAT_CAPACITY.get(fmt, 60) for fmt in store_fmts], dtype=float)
    prod_mult    = np.where(prod_cats[p_idx] == "outerwear", 1.3, 1.0)
    row_base_cap = base_cap[s_idx] * prod_mult * seasonal
    safety       = np.round(base_cap[s_idx] * prod_mult * 0.20).astype(int)

    base_so_p = np.array([_STOCKOUT_PROB.get(cat, 0.07) for cat in row_cats], dtype=float)
    base_so_p *= np.where(seasonal < 0.65, 1.6, 1.0)
    if is_tx_heat:
        tx_rows = (prod_cats[p_idx] == "outerwear") & is_tx[s_idx]
        base_so_p[tx_rows] = 0.88
    base_so_p = np.clip(base_so_p, 0.0, 0.90)

    is_stockout   = rng.random(n_total) < base_so_p
    units_on_hand = np.where(
        is_stockout, 0,
        np.clip(rng.poisson(row_base_cap * 0.65, n_total).astype(int), 1, None),
    )
    is_low        = (~is_stockout) & (units_on_hand < safety)
    reorder       = is_stockout | is_low
    on_order      = np.where(reorder, rng.poisson(row_base_cap * 0.4, n_total).astype(int), 0)
    in_transit    = np.where(on_order > 0, rng.binomial(on_order, 0.4), 0)
    avg_daily     = np.where(row_base_cap > 0, row_base_cap * seasonal * 0.02, 0.5)
    dos           = np.where(
        is_stockout, 0.0,
        np.round(units_on_hand / np.maximum(avg_daily, 0.1), 1),
    )

    start_inv = manifest.id_watermarks.inventory_key + 1
    return pd.DataFrame({
        "inventory_key":      np.arange(start_inv, start_inv + n_total, dtype=np.int64),
        "date_key":           np.full(n_total, date_key, dtype=np.int64),
        "product_key":        top_keys[p_idx],
        "store_key":          store_keys[s_idx],
        "units_on_hand":      units_on_hand,
        "units_on_order":     on_order,
        "units_in_transit":   in_transit,
        "safety_stock_level": safety,
        "days_of_supply":     dos,
        "is_stockout":        is_stockout,
        "is_low_stock":       is_low,
    })


def build_daily_crm(
    rng: np.random.Generator,
    target_date: date,
    manifest: Manifest,
    dims: dict[str, pd.DataFrame],
    config: GeneratorConfig,
    daily_sales: dict[int, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (customers_delta, loyalty_events) DataFrames for target_date."""
    span = (config.end - config.start).days + 1
    n_new = int(rng.poisson(config.n_customers / span))

    start_ck  = manifest.id_watermarks.customer_key + 1
    start_seq = manifest.id_watermarks.customer_id_seq + 1
    start_ek  = manifest.id_watermarks.event_key + 1

    # New customer rows
    customers_delta = pd.DataFrame()
    if n_new > 0:
        fake = Faker()
        fake.seed_instance(int(rng.integers(0, 2**32)))

        seg_names = [s[0] for s in _SEGMENTS]
        seg_probs = np.array([s[1] for s in _SEGMENTS], dtype=float)
        seg_probs /= seg_probs.sum()

        cust_keys = np.arange(start_ck,  start_ck  + n_new, dtype=np.int64)
        cust_seqs = np.arange(start_seq, start_seq + n_new, dtype=np.int64)

        customers_delta = pd.DataFrame({
            "customer_key":        cust_keys,
            "customer_id":         [f"CUST-{s:05d}" for s in cust_seqs],
            "first_name":          [fake.first_name() for _ in range(n_new)],
            "last_name":           [fake.last_name() for _ in range(n_new)],
            "email":               [fake.email() for _ in range(n_new)],
            "segment":             rng.choice(seg_names, size=n_new, p=seg_probs),
            "acquisition_channel": rng.choice(_ACQN_CHANNELS, size=n_new, p=_ACQN_WEIGHTS),
            "gender":              rng.choice(_GENDERS, size=n_new, p=_GENDER_WEIGHTS),
            "loyalty_tier":        rng.choice(_TIERS, size=n_new, p=_INITIAL_TIER_WEIGHTS),
            "signup_date":         str(target_date),
            "country_code":        rng.choice(
                ["US", "CA", "GB", "MX"], size=n_new, p=[0.76, 0.155, 0.056, 0.029]
            ),
            "is_current":          True,
            "effective_date":      str(target_date),
            "expiry_date":         None,
        })

    # Loyalty events
    date_key = int(target_date.strftime("%Y%m%d"))
    event_rows: list[dict] = []
    ek = start_ek

    for i in range(n_new):
        event_rows.append({
            "event_key":    ek,
            "date_key":     date_key,
            "customer_key": int(start_ck + i),
            "event_type":   "enrollment",
            "points_delta": 100,
            "balance_after": 100,
            "sale_key":     pd.NA,
        })
        ek += 1

    all_sales = (
        pd.concat(list(daily_sales.values()), ignore_index=True)
        if daily_sales else pd.DataFrame()
    )
    if len(all_sales) > 0 and "customer_key" in all_sales.columns:
        loyal = all_sales.dropna(subset=["customer_key"]).copy()
        loyal["customer_key"] = loyal["customer_key"].astype(int)
        for _, row in loyal.iterrows():
            pts = max(1, int(row.get("net_amount", 0)))
            event_rows.append({
                "event_key":    ek,
                "date_key":     date_key,
                "customer_key": int(row["customer_key"]),
                "event_type":   "points_earned",
                "points_delta": pts,
                "balance_after": pts,
                "sale_key":     int(row["sale_key"]),
            })
            ek += 1

    cols = ["event_key", "date_key", "customer_key", "event_type",
            "points_delta", "balance_after", "sale_key"]
    loyalty_events = pd.DataFrame(event_rows) if event_rows else pd.DataFrame(columns=cols)
    if len(loyalty_events) > 0:
        loyalty_events["sale_key"] = pd.array(loyalty_events["sale_key"].values, dtype="Int64")

    return customers_delta, loyalty_events


def build_daily_marketing(
    rng: np.random.Generator,
    target_date: date,
    manifest: Manifest,
    dims: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (campaigns_active_today, spend_rows) for target_date."""
    spend_cols = [
        "spend_key", "campaign_key", "channel_key", "date_key",
        "planned_spend", "actual_spend", "impressions", "clicks", "revenue_attributed",
    ]
    dim_campaign = dims.get("dim_campaign", pd.DataFrame())
    if len(dim_campaign) == 0:
        return pd.DataFrame(), pd.DataFrame(columns=spend_cols)

    cs = pd.to_datetime(dim_campaign["start_date"]).dt.date.values
    ce = pd.to_datetime(dim_campaign["end_date"]).dt.date.values
    active = dim_campaign[(cs <= target_date) & (ce >= target_date)].copy()

    date_key = int(target_date.strftime("%Y%m%d"))
    start_sk = manifest.id_watermarks.spend_key + 1
    rows: list[dict] = []

    for _, camp in active.iterrows():
        ctype    = camp["campaign_type"]
        camp_key = int(camp["campaign_key"])
        channels = _CAMPAIGN_CHANNELS.get(ctype, [2])
        spend_cfg = _SPEND_RANGE.get(ctype, {2: (100, 500)})
        roi       = _REVENUE_ROI.get(ctype, 4.0)
        imp_rate  = _IMPRESSIONS_PER_DOLLAR.get(ctype, 0.0)
        ctr       = _CTR.get(ctype, 0.0)

        for ch_key in channels:
            lo, hi  = spend_cfg.get(ch_key, (100, 500))
            planned = float(rng.uniform(lo, hi))
            actual  = round(planned * float(rng.uniform(0.85, 1.15)), 2)
            revenue = round(actual * roi * float(rng.uniform(0.7, 1.3)), 2)
            if imp_rate > 0:
                imps   = int(actual * imp_rate * float(rng.uniform(0.8, 1.2)))
                clicks = int(imps * ctr * float(rng.uniform(0.7, 1.3)))
            else:
                imps, clicks = 0, 0
            rows.append({
                "campaign_key":       camp_key,
                "channel_key":        ch_key,
                "date_key":           date_key,
                "planned_spend":      round(planned, 2),
                "actual_spend":       actual,
                "impressions":        imps,
                "clicks":             clicks,
                "revenue_attributed": revenue,
            })

    if not rows:
        return active, pd.DataFrame(columns=spend_cols)

    spend_df = pd.DataFrame(rows)
    spend_df.insert(0, "spend_key", range(start_sk, start_sk + len(spend_df)))
    return active, spend_df


def _load_prior_day_sales(out_dir: Path, lag_date: date) -> pd.DataFrame:
    """Read sales for lag_date from bronze (preferred) or gold fallback."""
    yyyy = lag_date.strftime("%Y")
    mm   = lag_date.strftime("%m")
    dd   = lag_date.strftime("%d")
    date_key = int(lag_date.strftime("%Y%m%d"))

    parts: list[pd.DataFrame] = []

    pos_dir = out_dir / "bronze" / "pos" / yyyy / mm / dd
    if pos_dir.exists():
        for f in sorted(pos_dir.glob("*.parquet")):
            parts.append(read_parquet(f))

    for src in ("ecom", "app"):
        p = out_dir / "bronze" / src / yyyy / mm / dd / "orders.parquet"
        if p.exists():
            parts.append(read_parquet(p))

    if parts:
        combined = pd.concat(parts, ignore_index=True)
        if "late_arrival_days" in combined.columns:
            combined = combined[combined["late_arrival_days"] == 0].copy()
        return combined

    gold = out_dir / "fact_sales.parquet"
    if not gold.exists():
        return pd.DataFrame()
    df = read_parquet(gold)
    return df[df["date_key"] == date_key].copy()


def build_late_arrivals(
    rng: np.random.Generator,
    target_date: date,
    out_dir: Path,
    manifest: Manifest,
) -> dict[int, pd.DataFrame]:
    """Sample ~0.5% of each D-1/D-2/D-3 day's sales as today's late arrivals."""
    result: dict[int, pd.DataFrame] = {}

    for lag in (1, 2, 3):
        prior = _load_prior_day_sales(out_dir, target_date - timedelta(days=lag))
        if len(prior) == 0:
            continue
        mask = rng.random(len(prior)) < _LATE_ARRIVAL_RATE
        if not mask.any():
            continue
        late = prior[mask].copy()
        late["late_arrival_days"] = lag
        for ch in [1, 2, 3, 4]:
            ch_rows = late[late["channel_key"] == ch]
            if len(ch_rows) == 0:
                continue
            result[ch] = (
                pd.concat([result[ch], ch_rows], ignore_index=True)
                if ch in result else ch_rows.copy()
            )

    return result


def write_bronze_day(
    out: Path,
    target_date: date,
    sales_by_channel: dict[int, pd.DataFrame],
    late_by_channel: dict[int, pd.DataFrame],
    sessions_by_hour: list[tuple[int, pd.DataFrame]],
    inventory: pd.DataFrame,
    customers_delta: pd.DataFrame,
    loyalty_events: pd.DataFrame,
    campaigns: pd.DataFrame,
    spend: pd.DataFrame,
    dims: dict[str, pd.DataFrame],
) -> dict[str, int]:
    """Write all bronze Parquet files. Returns row count dict."""
    yyyy = target_date.strftime("%Y")
    mm   = target_date.strftime("%m")
    dd   = target_date.strftime("%d")
    base = out / "bronze"
    counts: dict[str, int] = {}

    # Merge fresh + late per channel
    all_ch: set[int] = set(sales_by_channel) | set(late_by_channel)
    merged: dict[int, pd.DataFrame] = {}
    for ch in all_ch:
        parts = [df for df in [sales_by_channel.get(ch), late_by_channel.get(ch)] if df is not None]
        merged[ch] = pd.concat(parts, ignore_index=True)

    # POS: one file per store (channel 1)
    pos_df = merged.get(1, pd.DataFrame())
    pos_dir = base / "pos" / yyyy / mm / dd
    pos_dir.mkdir(parents=True, exist_ok=True)
    if len(pos_df) > 0:
        for sk in pos_df["store_key"].dropna().unique():
            rows = pos_df[pos_df["store_key"] == sk].copy()
            write_parquet(rows, pos_dir / f"store_{int(sk):03d}.parquet")
    counts["bronze_pos"] = len(pos_df)

    # ecom orders (channels 2 and 4 combined)
    ecom_parts = [merged[ch] for ch in (2, 4) if ch in merged]
    ecom_df = pd.concat(ecom_parts, ignore_index=True) if ecom_parts else pd.DataFrame()
    ecom_path = base / "ecom" / yyyy / mm / dd / "orders.parquet"
    ecom_path.parent.mkdir(parents=True, exist_ok=True)
    if len(ecom_df) > 0:
        write_parquet(ecom_df, ecom_path)
    else:
        pd.DataFrame().to_parquet(ecom_path)
    counts["bronze_ecom"] = len(ecom_df)

    # App orders (channel 3)
    app_df = merged.get(3, pd.DataFrame())
    app_path = base / "app" / yyyy / mm / dd / "orders.parquet"
    app_path.parent.mkdir(parents=True, exist_ok=True)
    if len(app_df) > 0:
        write_parquet(app_df, app_path)
    else:
        pd.DataFrame().to_parquet(app_path)
    counts["bronze_app"] = len(app_df)

    # Clickstream: 24 hourly buckets
    total_sess = 0
    for hour, sess_df in sessions_by_hour:
        hh = f"{hour:02d}"
        write_parquet(sess_df, base / "clickstream" / yyyy / mm / dd / hh / "sessions.parquet")
        total_sess += len(sess_df)
    counts["bronze_clickstream"] = total_sess

    # Inventory snapshot
    inv_path = base / "inventory" / yyyy / mm / dd / "snapshot.parquet"
    inv_path.parent.mkdir(parents=True, exist_ok=True)
    if len(inventory) > 0:
        write_parquet(inventory, inv_path)
    counts["bronze_inventory"] = len(inventory)

    # CRM customers delta
    crm_dir = base / "crm" / yyyy / mm / dd
    crm_dir.mkdir(parents=True, exist_ok=True)
    if len(customers_delta) > 0:
        write_parquet(customers_delta, crm_dir / "customers_delta.parquet")
    counts["bronze_crm_customers"] = len(customers_delta)

    # CRM loyalty events
    if len(loyalty_events) > 0:
        write_parquet(loyalty_events, crm_dir / "loyalty_events.parquet")
    counts["bronze_crm_loyalty"] = len(loyalty_events)

    # Marketing campaigns + spend
    mkt_dir = base / "marketing" / yyyy / mm / dd
    mkt_dir.mkdir(parents=True, exist_ok=True)
    if len(campaigns) > 0:
        write_parquet(campaigns, mkt_dir / "campaigns.parquet")
    counts["bronze_marketing_campaigns"] = len(campaigns)
    if len(spend) > 0:
        write_parquet(spend, mkt_dir / "spend.parquet")
    counts["bronze_marketing_spend"] = len(spend)

    # Product master snapshot (current SKUs only)
    prod_master = dims.get("dim_product", pd.DataFrame())
    prod_dir = base / "products" / yyyy / mm / dd
    prod_dir.mkdir(parents=True, exist_ok=True)
    if len(prod_master) > 0:
        current = prod_master[prod_master["is_current"]].copy()
        write_parquet(current, prod_dir / "product_master.parquet")
        counts["bronze_product_master"] = len(current)

    return counts
