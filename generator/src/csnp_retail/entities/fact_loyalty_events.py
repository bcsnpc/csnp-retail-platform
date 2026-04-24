"""fact_loyalty_events — one row per loyalty programme action.

Event types:
  enrollment    — customer joins loyalty; +100 bonus points
  points_earned — after each qualifying purchase; +round(net_amount) pts
  points_redeemed — periodic redemption; negative delta
  tier_upgrade  — informational; points_delta = 0

Grain: 1 row per event.  balance_after is the running account balance
after this event (computed via per-customer cumulative sum).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from csnp_retail.config import GeneratorConfig

_TIER_ORDER = {"None": 0, "Bronze": 1, "Silver": 2, "Gold": 3, "Platinum": 4}
_ENROLLMENT_BONUS = 100   # points given on enrolment
_EARN_RATE = 1            # 1 point per $1 of net_amount (rounded)
_MIN_REDEEM = 200         # minimum balance to trigger a redemption
_DATE_KEY_FMT = "%Y%m%d"


def _to_date_key(dt_series: pd.Series) -> pd.Series:
    return dt_series.dt.strftime(_DATE_KEY_FMT).astype(int)


def build_fact_loyalty_events(
    config: GeneratorConfig,
    rng: np.random.Generator,
    fact_sales: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    # ── 1. Identify loyalty members ───────────────────────────────────────────
    # Any customer who ever had a non-None tier is a member
    member_rows = dim_customer[dim_customer["loyalty_tier"] != "None"].copy()
    if len(member_rows) == 0:
        return _empty_df()

    loyal_keys = set(member_rows["customer_key"].unique())

    # ── 2. Enrollment events ──────────────────────────────────────────────────
    # Per customer_id: earliest SCD2 row with non-None tier
    first_loyal = (
        member_rows
        .sort_values("effective_date")
        .groupby("customer_id", as_index=False)
        .first()
    )
    enroll_dk = _to_date_key(first_loyal["effective_date"])

    enroll_events = pd.DataFrame({
        "date_key":    enroll_dk.values,
        "customer_key": first_loyal["customer_key"].values,
        "event_type":  "enrollment",
        "points_delta": _ENROLLMENT_BONUS,
        "sale_key":    pd.array([pd.NA] * len(first_loyal), dtype="Int64"),
    })

    # ── 3. Tier-upgrade events ────────────────────────────────────────────────
    sorted_cust = dim_customer.sort_values(["customer_id", "effective_date"])
    sorted_cust = sorted_cust.assign(
        prev_tier=sorted_cust.groupby("customer_id")["loyalty_tier"].shift(1)
    )
    upgrades = sorted_cust[
        sorted_cust["prev_tier"].notna()
        & sorted_cust.apply(
            lambda r: (
                _TIER_ORDER.get(r["loyalty_tier"], 0)
                > _TIER_ORDER.get(r["prev_tier"], 0)
            ),
            axis=1,
        )
    ]
    upgrade_events = pd.DataFrame({
        "date_key":    _to_date_key(upgrades["effective_date"]).values,
        "customer_key": upgrades["customer_key"].values,
        "event_type":  "tier_upgrade",
        "points_delta": 0,
        "sale_key":    pd.array([pd.NA] * len(upgrades), dtype="Int64"),
    })

    # ── 4. Points-earned events (per qualifying sale) ─────────────────────────
    loyal_sales = fact_sales[
        fact_sales["customer_key"].isin(loyal_keys)
        & fact_sales["customer_key"].notna()
    ].copy()
    points_earned = np.round(loyal_sales["net_amount"].values * _EARN_RATE).astype(int)
    points_earned = np.clip(points_earned, 1, None)

    earned_events = pd.DataFrame({
        "date_key":    loyal_sales["date_key"].values,
        "customer_key": loyal_sales["customer_key"].values.astype(int),
        "event_type":  "points_earned",
        "points_delta": points_earned,
        "sale_key":    pd.array(loyal_sales["sale_key"].values, dtype="Int64"),
    })

    # ── 5. Redemption events ──────────────────────────────────────────────────
    # For each customer with total earned > _MIN_REDEEM, generate 1-3 redemptions
    cust_totals = earned_events.groupby("customer_key")["points_delta"].sum()
    redeemers = cust_totals[cust_totals > _MIN_REDEEM].index

    redeem_rows: list[dict] = []

    for ck in redeemers:
        total = int(cust_totals[ck])
        n_red = int(rng.integers(1, 4))
        # Pick random dates from this customer's purchase dates
        cust_dks = loyal_sales.loc[
            loyal_sales["customer_key"] == ck, "date_key"
        ].unique()
        if len(cust_dks) == 0:
            continue
        n_pick = min(n_red, len(cust_dks))
        chosen_dks = rng.choice(cust_dks, size=n_pick, replace=False)
        per_redeem = max(100, int(total / n_red * float(rng.uniform(0.25, 0.60))))
        for dk in chosen_dks:
            redeem_rows.append({
                "date_key":    int(dk),
                "customer_key": int(ck),
                "event_type":  "points_redeemed",
                "points_delta": -per_redeem,
                "sale_key":    pd.NA,
            })

    redeem_events = (
        pd.DataFrame(redeem_rows)
        if redeem_rows
        else pd.DataFrame(columns=["date_key", "customer_key", "event_type",
                                    "points_delta", "sale_key"])
    )
    if len(redeem_events) > 0:
        redeem_events["sale_key"] = pd.array(
            redeem_events["sale_key"].values, dtype="Int64"
        )

    # ── 6. Combine and compute balance_after ──────────────────────────────────
    all_events = pd.concat(
        [enroll_events, earned_events, upgrade_events, redeem_events],
        ignore_index=True,
    )
    all_events = all_events.sort_values(
        ["customer_key", "date_key", "event_type"]
    ).reset_index(drop=True)

    all_events["balance_after"] = (
        all_events.groupby("customer_key")["points_delta"]
        .cumsum()
        .clip(lower=0)   # floor at 0 — redemption can't create negative balance
        .astype(int)
    )

    all_events.insert(0, "event_key", range(1, len(all_events) + 1))
    all_events["customer_key"] = pd.array(
        all_events["customer_key"].values, dtype="Int64"
    )

    return all_events[
        ["event_key", "date_key", "customer_key", "event_type",
         "points_delta", "balance_after", "sale_key"]
    ]


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "event_key", "date_key", "customer_key", "event_type",
        "points_delta", "balance_after", "sale_key",
    ])
