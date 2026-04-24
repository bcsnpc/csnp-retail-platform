"""fact_marketing_spend — daily spend per campaign × sales channel.

Planted insight patterns embedded here:
  §6.7  Email campaigns have 3.4x higher ROI than Paid Social:
         Email: low actual_spend, high revenue_attributed.
         Paid Social: high actual_spend, lower revenue_attributed.

Grain: 1 row per campaign × channel_key × date (only active campaign-days).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from csnp_retail.config import GeneratorConfig

# ── Which sales channels each campaign type spends against ────────────────────
# channel_key: 1=Store, 2=Web, 3=App, 4=Marketplace
_CAMPAIGN_CHANNELS: dict[str, list[int]] = {
    "Promo":       [1, 2],
    "Email":       [2, 3],
    "Paid Search": [2],
    "Paid Social": [2, 3],
    "Affiliate":   [2],
    "Influencer":  [2, 3],
}

# ── Daily spend range (actual_spend) by campaign type and channel ─────────────
# (lo, hi) in USD
_SPEND_RANGE: dict[str, dict[int, tuple[float, float]]] = {
    "Promo":       {1: (200, 800),   2: (300, 1200)},
    "Email":       {2: (80, 250),    3: (50, 150)},
    "Paid Search": {2: (400, 2000)},
    "Paid Social": {2: (600, 2500),  3: (400, 1800)},
    "Affiliate":   {2: (150, 600)},
    "Influencer":  {2: (500, 1500),  3: (300, 900)},
}

# ── Revenue per dollar of spend (ROI multiplier) by campaign type ─────────────
# §6.7: Email ROI >> Paid Social ROI (3.4x gap)
_REVENUE_ROI: dict[str, float] = {
    "Promo":       4.5,
    "Email":       9.2,   # high: owned channel, intent-rich audience
    "Paid Search": 5.8,
    "Paid Social": 2.7,   # low: top-of-funnel, high CPM
    "Affiliate":   6.1,
    "Influencer":  3.4,
}

# ── Impression / click rates by campaign type and channel ─────────────────────
_IMPRESSIONS_PER_DOLLAR: dict[str, float] = {
    "Email":       0.0,    # not applicable
    "Promo":       0.0,
    "Paid Search": 120.0,
    "Paid Social": 500.0,
    "Affiliate":   200.0,
    "Influencer":  800.0,
}

_CTR: dict[str, float] = {  # click-through rate
    "Paid Search": 0.045,
    "Paid Social": 0.012,
    "Affiliate":   0.025,
    "Influencer":  0.008,
    "Email":       0.0,
    "Promo":       0.0,
}


def build_fact_marketing_spend(
    config: GeneratorConfig,
    rng: np.random.Generator,
    dim_campaign: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    date_key_arr = dim_date["date_key"].values

    def _date_range_keys(start: object, end: object) -> list[int]:
        s = int(start.strftime("%Y%m%d"))
        e = int(end.strftime("%Y%m%d"))
        return [dk for dk in date_key_arr if s <= dk <= e]

    rows: list[dict] = []

    for _, camp in dim_campaign.iterrows():
        ctype     = camp["campaign_type"]
        camp_key  = int(camp["campaign_key"])
        channels  = _CAMPAIGN_CHANNELS.get(ctype, [2])
        spend_cfg = _SPEND_RANGE.get(ctype, {2: (100, 500)})
        roi       = _REVENUE_ROI.get(ctype, 4.0)
        imp_rate  = _IMPRESSIONS_PER_DOLLAR.get(ctype, 0.0)
        ctr       = _CTR.get(ctype, 0.0)

        active_days = _date_range_keys(camp["start_date"], camp["end_date"])
        if not active_days:
            continue

        for ch_key in channels:
            lo, hi = spend_cfg.get(ch_key, (100, 500))
            n_days = len(active_days)

            # Planned spend: smooth daily target with small noise
            planned_daily = float(rng.uniform(lo, hi))
            planned = np.full(n_days, planned_daily)

            # Actual spend: planned ± 15% noise
            actual = planned * rng.uniform(0.85, 1.15, n_days)
            actual = actual.round(2)

            # Revenue attributed: actual × ROI + noise
            revenue = (actual * roi * rng.uniform(0.7, 1.3, n_days)).round(2)

            # Impressions / clicks (only for paid media)
            if imp_rate > 0:
                impressions = (actual * imp_rate * rng.uniform(0.8, 1.2, n_days)).astype(int)
                clicks = (impressions * ctr * rng.uniform(0.7, 1.3, n_days)).astype(int)
            else:
                impressions = np.zeros(n_days, dtype=int)
                clicks = np.zeros(n_days, dtype=int)

            for j, dk in enumerate(active_days):
                rows.append({
                    "campaign_key":       camp_key,
                    "channel_key":        ch_key,
                    "date_key":           dk,
                    "planned_spend":      round(float(planned[j]), 2),
                    "actual_spend":       round(float(actual[j]), 2),
                    "impressions":        int(impressions[j]),
                    "clicks":             int(clicks[j]),
                    "revenue_attributed": round(float(revenue[j]), 2),
                })

    if not rows:
        return pd.DataFrame(columns=[
            "spend_key", "campaign_key", "channel_key", "date_key",
            "planned_spend", "actual_spend", "impressions", "clicks",
            "revenue_attributed",
        ])

    df = pd.DataFrame(rows)
    df.insert(0, "spend_key", range(1, len(df) + 1))
    return df
