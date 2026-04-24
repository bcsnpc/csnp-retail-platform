"""Planted insight patterns — canonical definitions and seed utilities.

Pattern window constants live here so daily-mode runner and entity
generators share a single source of truth.  The manifest records
module_version() so any reproduction attempt can verify that the same
pattern logic was applied.

derive_seed() provides collision-safe RNG seeds for both backfill and
daily runs using blake2b — no arithmetic aliasing between modes or dates.
"""

from __future__ import annotations

import hashlib
from datetime import date as _date
from pathlib import Path

import pandas as pd

# ── Seed derivation ───────────────────────────────────────────────────────────

def derive_seed(base_seed: int, mode: str, target_date: _date) -> int:
    """Collision-safe RNG seed from (base_seed, mode, date).

    Uses blake2b so backfill and daily streams never share state even when
    base_seed + date ordinal arithmetic would collide.
    mode should be 'backfill' or 'daily'.
    """
    key = f"{base_seed}|{mode}|{target_date.isoformat()}".encode()
    return int.from_bytes(
        hashlib.blake2b(key, digest_size=8).digest(), "big"
    )


def module_version() -> str:
    """First 16 hex chars of SHA-256 of this file's content.

    Stored in manifest so reproductions can verify the same pattern
    windows and logic were applied.
    """
    content = Path(__file__).read_bytes()
    return hashlib.sha256(content).hexdigest()[:16]


# ── Pattern window constants ──────────────────────────────────────────────────

# §6.3  Texas heat event
TX_HEAT_START = _date(2025, 6, 15)
TX_HEAT_END   = _date(2025, 8, 16)

# §6.11 Meridian Cable Crew TikTok spike
MERIDIAN_SPIKE_START = _date(2025, 7, 1)
MERIDIAN_SPIKE_END   = _date(2025, 9, 30)

# §6.12 Field Straight-Leg sizing issue (applies for full backfill window)
FSL_STYLE_PREFIX = "BTM-FSL-"


# ── Pattern 6.3 ──────────────────────────────────────────────────────────────

def apply_tx_heat_event(sales: pd.DataFrame) -> pd.DataFrame:
    """Texas stores: Outerwear revenue drops 55% Jun 15 – Aug 16 FY25."""
    mask = (
        (sales["state_code"] == "TX")
        & (sales["order_date"] >= "2025-06-15")
        & (sales["order_date"] <= "2025-08-16")
        & (sales["department"] == "Outerwear")
    )
    sales = sales.copy()
    sales.loc[mask, "gross_revenue"] *= 0.45
    sales.loc[mask, "net_revenue"] *= 0.45
    return sales


# ── Pattern 6.11 ─────────────────────────────────────────────────────────────

def apply_meridian_cable_spike(sales: pd.DataFrame) -> pd.DataFrame:
    """Meridian Cable Crew cardigan: 9× volume spike in Jul 2025."""
    # Spike is injected at generation time via weighted sampling in fact_sales.
    return sales


# ── Pattern 6.12 ─────────────────────────────────────────────────────────────

def apply_signature_sizing_issue(returns: pd.DataFrame) -> pd.DataFrame:
    """Field Straight-Leg in Signature line: return rate boosted to 23%."""
    # Applied at fact_returns generation time via elevated return probability.
    return returns


# ── Stubs for remaining patterns (implemented with fact generators) ───────────

def apply_channel_mix_shift(sales: pd.DataFrame) -> pd.DataFrame:
    return sales


def apply_vip_concentration(customers: pd.DataFrame) -> pd.DataFrame:
    return customers


def apply_bfcm_cohort_degradation(sales: pd.DataFrame) -> pd.DataFrame:
    return sales


def apply_bopis_cannibalization(sales: pd.DataFrame) -> pd.DataFrame:
    return sales


def apply_promo_effectiveness(sales: pd.DataFrame) -> pd.DataFrame:
    return sales


def apply_price_elasticity(sales: pd.DataFrame) -> pd.DataFrame:
    return sales


def apply_weather_driven_demand(sales: pd.DataFrame) -> pd.DataFrame:
    return sales


def apply_conversion_funnel_by_device(sessions: pd.DataFrame) -> pd.DataFrame:
    return sessions


def apply_return_rate_bracketing(returns: pd.DataFrame) -> pd.DataFrame:
    return returns


def apply_cross_shop_pattern(customers: pd.DataFrame) -> pd.DataFrame:
    return customers
