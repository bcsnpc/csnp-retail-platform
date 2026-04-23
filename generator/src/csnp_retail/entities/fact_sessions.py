"""fact_sessions — web and app browsing sessions.

Planted insight patterns embedded here:
  §6.7  Mobile conversion rate is materially lower than desktop:
         Desktop ~28%  |  Tablet ~20%  |  Mobile ~14%
  §6.8  Session volume spikes during promo windows (1.6x baseline traffic).
  §6.9  App sessions are shorter than web sessions but have higher add-to-cart rates
         (captured via pages_viewed and time_on_site_seconds distributions).

Only Web (channel_key=2) and App (channel_key=3) channels have sessions.
Marketplace sessions are not tracked directly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from csnp_retail.config import GeneratorConfig

# ── Session config ─────────────────────────────────────────────────────────────

_CHANNEL_SPLIT = {2: 0.65, 3: 0.35}          # Web / App

_DEVICE_BY_CHANNEL: dict[int, dict] = {
    2: {"Mobile": 0.52, "Desktop": 0.37, "Tablet": 0.11},   # Web
    3: {"Mobile": 0.82, "Tablet": 0.14, "Desktop": 0.04},   # App
}

# §6.7: conversion rates by device
_CONV_RATE_BY_DEVICE = {"Desktop": 0.28, "Tablet": 0.20, "Mobile": 0.14}

# Pages viewed distributions (mean, std) by channel
_PAGES_PARAMS: dict[int, tuple[float, float]] = {
    2: (5.2, 3.4),   # Web: wider browsing
    3: (3.1, 1.8),   # App: more direct, fewer pages
}

# Time on site (seconds) by device × channel
_TIME_PARAMS: dict[str, tuple[float, float]] = {
    "Desktop-2": (210, 130),
    "Mobile-2":  (140, 100),
    "Tablet-2":  (185, 120),
    "Desktop-3": (125, 80),
    "Mobile-3":  (95, 60),
    "Tablet-3":  (110, 70),
}

_MONTH_WEIGHTS = {
    1: 0.75, 2: 0.80, 3: 1.05,
    4: 1.10, 5: 1.00, 6: 0.92,
    7: 0.88, 8: 0.85, 9: 0.92,
    10: 1.08, 11: 1.38, 12: 1.50,
}

_WEEKDAY_WEIGHTS = {0: 0.95, 1: 0.90, 2: 0.90, 3: 1.00, 4: 1.15, 5: 1.30, 6: 1.20}


def build_fact_sessions(
    config: GeneratorConfig,
    rng: np.random.Generator,
    dim_date: pd.DataFrame,
    dim_customer: pd.DataFrame,
) -> pd.DataFrame:
    n = config.n_sessions

    # ── 1. Date selection ─────────────────────────────────────────────────────
    month_w   = dim_date["month"].map(_MONTH_WEIGHTS).values.astype(float)
    weekday_w = dim_date["day_of_week"].map(_WEEKDAY_WEIGHTS).values.astype(float)
    promo_w   = np.where(dim_date["is_promo_window"].values, 1.6, 1.0)  # §6.8
    date_w    = month_w * weekday_w * promo_w
    date_w   /= date_w.sum()

    date_idx  = rng.choice(len(dim_date), size=n, p=date_w)
    date_keys = dim_date["date_key"].values[date_idx].astype(np.int64)

    # ── 2. Channel assignment ─────────────────────────────────────────────────
    u_ch = rng.random(n)
    channel_keys = np.where(u_ch < _CHANNEL_SPLIT[2], 2, 3).astype(np.int64)

    # ── 3. Device assignment ──────────────────────────────────────────────────
    devices = np.empty(n, dtype=object)
    for ch, dist in _DEVICE_BY_CHANNEL.items():
        mask = channel_keys == ch
        names = list(dist.keys())
        probs = np.array(list(dist.values()), dtype=float)
        probs /= probs.sum()
        devices[mask] = rng.choice(names, size=int(mask.sum()), p=probs)

    # ── 4. Conversion (§6.7: mobile lags desktop) ────────────────────────────
    conv_p = np.array([_CONV_RATE_BY_DEVICE[d] for d in devices])
    is_converted = rng.random(n) < conv_p

    # ── 5. Engagement metrics ─────────────────────────────────────────────────
    pages_viewed = np.zeros(n, dtype=np.int64)
    time_on_site = np.zeros(n, dtype=np.int64)

    for ch in (2, 3):
        mu_p, sd_p = _PAGES_PARAMS[ch]
        ch_mask = channel_keys == ch
        for dev in ("Mobile", "Desktop", "Tablet"):
            dev_mask = ch_mask & (devices == dev)
            cnt = int(dev_mask.sum())
            if cnt == 0:
                continue
            key = f"{dev}-{ch}"
            mu_t, sd_t = _TIME_PARAMS[key]
            pages_viewed[dev_mask] = np.clip(
                rng.normal(mu_p, sd_p, cnt).round().astype(int), 1, 50
            )
            time_on_site[dev_mask] = np.clip(
                rng.normal(mu_t, sd_t, cnt).round().astype(int), 5, 3600
            )

    # ── 6. Customer assignment (~65% of sessions are authenticated) ───────────
    n_cust = config.n_customers
    has_cust = rng.random(n) < 0.65
    cust_ids_raw = np.where(has_cust, rng.integers(1, n_cust + 1, size=n), 0)

    cur_cust = dim_customer[dim_customer["is_current"]]
    cust_id_to_key = dict(zip(
        cur_cust["customer_id"].str.replace("CUST-", "").astype(int),
        cur_cust["customer_key"].values,
    ))
    customer_keys_out = np.array(
        [cust_id_to_key.get(int(c), pd.NA) if c > 0 else pd.NA for c in cust_ids_raw],
        dtype=object,
    )

    # ── 7. Assemble DataFrame ─────────────────────────────────────────────────
    df = pd.DataFrame({
        "date_key":           date_keys,
        "customer_key":       customer_keys_out,
        "channel_key":        channel_keys,
        "device_type":        devices,
        "pages_viewed":       pages_viewed,
        "time_on_site_secs":  time_on_site,
        "is_converted":       is_converted,
    })
    df.insert(0, "session_key", range(1, n + 1))
    df["customer_key"] = pd.array(df["customer_key"].values, dtype="Int64")

    return df
