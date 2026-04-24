"""dim_customer — customer master with SCD2 loyalty-tier revisions.

~20% of customers receive one loyalty-tier upgrade during the 3-year window,
producing a second row for those customers.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd
from faker import Faker

from csnp_retail.config import GeneratorConfig

_SEGMENTS = [
    ("Style Loyalist", 0.04),
    ("Sale Seeker",    0.25),
    ("Core Shopper",   0.46),
    ("Gift & Occasion",0.16),
    ("Digital Native", 0.07),
    ("One-Timer",      0.02),
]

_TIERS         = ["None", "Bronze", "Silver", "Gold", "Platinum"]
_TIER_UPGRADES = {"None": "Bronze", "Bronze": "Silver", "Silver": "Gold", "Gold": "Platinum"}

_ACQN_CHANNELS = ["Store", "Web", "App", "Referral", "Campaign"]
_ACQN_WEIGHTS  = [0.40,    0.30,  0.15,  0.10,       0.05]

_GENDERS        = ["M",   "F",   "NB"]
_GENDER_WEIGHTS = [0.48,  0.48,  0.04]

_INITIAL_TIER_WEIGHTS = [0.50, 0.30, 0.15, 0.04, 0.01]

_UPGRADE_RATE = 0.20


def build_dim_customer(config: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    n     = config.n_customers
    start = config.start
    end   = config.end
    span  = (end - start).days

    fake = Faker()
    Faker.seed(config.seed)

    seg_names  = [s[0] for s in _SEGMENTS]
    seg_probs  = np.array([s[1] for s in _SEGMENTS], dtype=float)
    seg_probs /= seg_probs.sum()

    segments       = rng.choice(seg_names, size=n, p=seg_probs)
    genders        = rng.choice(_GENDERS, size=n, p=_GENDER_WEIGHTS)
    acqn_channels  = rng.choice(_ACQN_CHANNELS, size=n, p=_ACQN_WEIGHTS)
    initial_tiers  = rng.choice(_TIERS, size=n, p=_INITIAL_TIER_WEIGHTS)
    signup_offsets = rng.integers(0, span, size=n)
    birth_offsets  = rng.integers(18 * 365, 75 * 365, size=n)
    country_codes  = rng.choice(["US", "CA", "GB", "MX"], size=n, p=[0.76, 0.155, 0.056, 0.029])

    rows: list[dict] = []
    for i in range(n):
        signup_date = start + timedelta(days=int(signup_offsets[i]))
        birth_date  = start - timedelta(days=int(birth_offsets[i]))
        first_name  = fake.first_name()
        last_name   = fake.last_name()
        email       = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"

        rows.append({
            "customer_id":          f"CUST-{i + 1:06d}",
            "first_name":           first_name,
            "last_name":            last_name,
            "email":                email,
            "birth_date":           birth_date,
            "gender":               genders[i],
            "customer_segment":     segments[i],
            "acquisition_channel":  acqn_channels[i],
            "loyalty_tier":         initial_tiers[i],
            "signup_date":          signup_date,
            "country_code":         country_codes[i],
            "effective_date":       signup_date,
            "expiry_date":          None,
            "is_current":           True,
        })

    # ── SCD2 loyalty-tier upgrades (~20% of customers) ────────────────────────
    upgrade_mask    = rng.random(n) < _UPGRADE_RATE
    upgrade_indices = np.where(upgrade_mask)[0]

    upgrade_rows: list[dict] = []
    for idx in upgrade_indices:
        base = rows[idx]
        current_tier = base["loyalty_tier"]
        if current_tier not in _TIER_UPGRADES:
            continue

        earliest = base["signup_date"] + timedelta(days=90)
        latest   = end - timedelta(days=30)
        if earliest >= latest:
            continue

        upgrade_days = rng.integers(0, (latest - earliest).days)
        upgrade_date = earliest + timedelta(days=int(upgrade_days))

        base["expiry_date"] = upgrade_date - timedelta(days=1)
        base["is_current"]  = False

        upgrade_rows.append({
            **base,
            "loyalty_tier":   _TIER_UPGRADES[current_tier],
            "effective_date": upgrade_date,
            "expiry_date":    None,
            "is_current":     True,
        })

    all_rows = rows + upgrade_rows
    df = pd.DataFrame(all_rows)
    df.insert(0, "customer_key", range(1, len(df) + 1))
    df["effective_date"] = pd.to_datetime(df["effective_date"])
    df["expiry_date"]    = pd.to_datetime(df["expiry_date"])
    df["birth_date"]     = pd.to_datetime(df["birth_date"])
    df["signup_date"]    = pd.to_datetime(df["signup_date"])
    return df
