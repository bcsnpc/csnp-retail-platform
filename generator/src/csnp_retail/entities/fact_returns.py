"""fact_returns — returns of individual fact_sales rows.

Planted insight patterns embedded here:
  §6.4  Online return rate is materially higher than in-store:
         Store ~8%  |  Web ~18%  |  App ~22%  |  Marketplace ~15%
  §6.5  Fit issues (wrong size / didn't fit) dominate online tops returns.
  §6.6  Post-holiday (Jan) return spike: January return volume 2x December sales volume.

Return rate by category (baseline, applied before channel multiplier):
  outerwear 16%  |  tops 11%  |  footwear 14%  |  bottoms 9%
  accessories 7%  |  home 5%  |  beauty 2%
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from csnp_retail.config import GeneratorConfig

# ── Return-rate configuration ─────────────────────────────────────────────────

_CAT_BASE_RATE: dict[str, float] = {
    "tops":        0.11,
    "bottoms":     0.09,
    "outerwear":   0.16,
    "footwear":    0.14,
    "accessories": 0.07,
    "home":        0.05,
    "beauty":      0.02,
}

_CHANNEL_MULTIPLIER: dict[int, float] = {
    1: 0.73,   # Store — try-before-you-buy reduces returns
    2: 1.64,   # Web   — §6.4 planted
    3: 2.00,   # App   — §6.4 planted
    4: 1.36,   # Marketplace
}

# ── Return-reason probabilities per channel type ───────────────────────────────
# reason_keys: 1=Wrong size, 2=Didn't fit, 3=Poor quality, 4=Defective,
#              5=Not as described, 6=Changed mind, 7=Better price,
#              8=Ordered wrong, 9=Received wrong, 10=Gift return,
#              11=Too late, 12=Style not as expected

_STORE_REASON_WEIGHTS = np.array([
    0.10, 0.10, 0.12, 0.08, 0.06, 0.18, 0.06, 0.04, 0.02, 0.12, 0.05, 0.07
], dtype=float)

_ONLINE_REASON_WEIGHTS = np.array([
    0.22, 0.18, 0.08, 0.06, 0.12, 0.12, 0.05, 0.03, 0.05, 0.04, 0.02, 0.03
], dtype=float)

# §6.12: Field Straight-Leg Pant (BTM-FSL-*) sizing issue — 60% "Didn't fit"
_FSL_REASON_WEIGHTS = np.array([
    0.15, 0.60, 0.06, 0.03, 0.03, 0.06, 0.01, 0.01, 0.01, 0.01, 0.01, 0.02
], dtype=float)


def _date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def _date_from_key(dk: int) -> date:
    s = str(int(dk))
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def build_fact_returns(
    config: GeneratorConfig,
    rng: np.random.Generator,
    fact_sales: pd.DataFrame,
    dim_product: pd.DataFrame,
) -> pd.DataFrame:
    end = config.end

    # ── Join dim_product to get category and style_code ──────────────────────
    prod_info = (
        dim_product[["product_key", "category", "style_code"]]
        .drop_duplicates("product_key")
        .set_index("product_key")
    )
    categories  = fact_sales["product_key"].map(prod_info["category"]).fillna("tops")
    style_codes = fact_sales["product_key"].map(prod_info["style_code"]).fillna("")

    # ── Compute per-row return probability ────────────────────────────────────
    base_p  = categories.map(_CAT_BASE_RATE).fillna(0.10).values
    ch_mult = fact_sales["channel_key"].map(_CHANNEL_MULTIPLIER).fillna(1.0).values

    # §6.6: January return spike — sales in Dec get 2× return probability
    sale_months = (fact_sales["date_key"].values // 100) % 100  # YYYYMM -> MM
    jan_boost = np.where(sale_months == 12, 2.0, 1.0)

    # §6.12: BTM-FSL-001 Field Straight-Leg sizing issue — boost to ~23% return rate
    fsl_boost = np.where(
        style_codes.str.startswith("BTM-FSL-").values,
        23.0 / 9.0,  # scale from 9% bottom baseline to 23%
        1.0,
    )

    return_p = base_p * ch_mult * jan_boost * fsl_boost
    return_p = np.clip(return_p, 0.0, 0.90)

    # ── Sample which sales are returned ───────────────────────────────────────
    is_returned = rng.random(len(fact_sales)) < return_p
    returned_sales = fact_sales[is_returned].copy().reset_index(drop=True)
    n_returns = len(returned_sales)

    if n_returns == 0:
        return pd.DataFrame(columns=[
            "return_key", "sale_key", "date_key", "customer_key", "product_key",
            "store_key", "channel_key", "return_reason_key", "quantity_returned",
            "return_amount", "days_to_return",
        ])

    # ── Return dates: 1-30 days after sale, skewed early ─────────────────────
    sale_dates = np.array([_date_from_key(int(dk)) for dk in returned_sales["date_key"]])
    days_later = rng.integers(1, 31, size=n_returns)

    return_dates = np.array([
        min(sd + timedelta(days=int(d)), end)
        for sd, d in zip(sale_dates, days_later)
    ])
    return_date_keys = np.array([_date_key(d) for d in return_dates], dtype=np.int64)
    days_to_return = days_later

    # ── Return reasons ────────────────────────────────────────────────────────
    ret_cats      = categories[is_returned].values
    ret_channels  = returned_sales["channel_key"].values
    ret_styles    = style_codes[is_returned].values

    reason_keys = np.zeros(n_returns, dtype=np.int64)
    for i in range(n_returns):
        if str(ret_styles[i]).startswith("BTM-FSL-"):
            # §6.12: Field Straight-Leg sizing issue dominates reason mix
            w = _FSL_REASON_WEIGHTS.copy()
        elif ret_channels[i] == 1:
            w = _STORE_REASON_WEIGHTS.copy()
        else:
            w = _ONLINE_REASON_WEIGHTS.copy()
            if ret_cats[i] == "tops":
                w[0] *= 1.8   # boost "Wrong size"
                w[1] *= 1.8   # boost "Didn't fit"
                w /= w.sum()
        reason_keys[i] = rng.choice(np.arange(1, 13), p=w / w.sum())

    # ── Store key: online returns may be processed in-store (store_key set),
    #   but channel_key always reflects the original purchase channel (§6.4)
    orig_channels = returned_sales["channel_key"].values.copy()
    online_mask = orig_channels != 1
    return_at_store = online_mask & (rng.random(n_returns) < 0.50)

    ret_store_keys = returned_sales["store_key"].values.copy()
    ret_store_keys[online_mask & ~return_at_store] = pd.NA

    # ── Return amount: mostly full refund ─────────────────────────────────────
    net_amounts = returned_sales["net_amount"].values.astype(float)
    full_refund = rng.random(n_returns) < 0.90
    refund_pct  = np.where(full_refund, 1.0, rng.uniform(0.50, 0.90, n_returns))
    return_amounts = (net_amounts * refund_pct).round(2)

    # ── Assemble DataFrame ────────────────────────────────────────────────────
    df = pd.DataFrame({
        "sale_key":         returned_sales["sale_key"].values,
        "date_key":         return_date_keys,
        "customer_key":     returned_sales["customer_key"].values,
        "product_key":      returned_sales["product_key"].values,
        "store_key":        ret_store_keys,
        "channel_key":      orig_channels.astype(np.int64),
        "return_reason_key": reason_keys,
        "quantity_returned": returned_sales["quantity"].values,
        "return_amount":    return_amounts,
        "days_to_return":   days_to_return,
    })
    df.insert(0, "return_key", range(1, n_returns + 1))

    for col in ["customer_key", "store_key"]:
        df[col] = pd.array(df[col].values, dtype="Int64")

    return df
