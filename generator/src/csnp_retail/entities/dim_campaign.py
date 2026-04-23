"""dim_campaign builder.

Generates ~180 campaigns over 3 fiscal years (FY24–FY26 = Apr 2023 – Mar 2026).

Campaign types:
  Promo         — seasonal promotional discounts
  Paid Search   — always-on, renewed quarterly
  Paid Social   — quarterly always-on + seasonal boosts
  Email         — monthly newsletters + event campaigns
  Affiliate     — ongoing commissions-based
  Influencer    — periodic drops

Each campaign has planned_spend, actual_spend (±15% noise), and target_revenue.
"""

from __future__ import annotations

import calendar
import datetime
from dataclasses import dataclass

import numpy as np
import pandas as pd

from csnp_retail.config import BACKFILL_END, BACKFILL_START

_SEGMENTS = [
    "All Customers", "Style Loyalist", "Sale Seeker", "Core Shopper",
    "Gift & Occasion", "Digital Native", "One-Timer",
]

# ── Seasonal campaign templates ────────────────────────────────────────────────
# (name, type, start_mmdd, end_mmdd, discount_pct, planned_spend_usd, target_revenue_mult)
# start/end given as (month, day) offsets from calendar year

@dataclass
class _CampaignTemplate:
    name: str
    campaign_type: str
    start_mm: int
    start_dd: int
    end_mm: int
    end_dd: int
    discount_pct: float
    planned_spend: float
    target_revenue_mult: float       # target_revenue = planned_spend * this
    target_segment: str = "All Customers"


_SEASONAL_TEMPLATES: list[_CampaignTemplate] = [
    _CampaignTemplate("Spring New Arrivals", "Email", 4, 1, 4, 15, 0.0, 15_000, 8.0, "Style Loyalist"),
    _CampaignTemplate("Earth Day Sale", "Promo", 4, 19, 4, 22, 10.0, 8_000, 6.0, "All Customers"),
    _CampaignTemplate("Mother's Day Gift Guide", "Email", 4, 28, 5, 12, 15.0, 12_000, 7.0, "Gift & Occasion"),
    _CampaignTemplate("Mother's Day Promo", "Promo", 5, 1, 5, 14, 20.0, 20_000, 6.5, "Gift & Occasion"),
    _CampaignTemplate("Memorial Day Sale", "Promo", 5, 22, 5, 27, 25.0, 30_000, 5.5, "Sale Seeker"),
    _CampaignTemplate("Summer Collection Launch", "Paid Social", 6, 1, 6, 14, 0.0, 35_000, 7.0, "Style Loyalist"),
    _CampaignTemplate("Summer Sale", "Promo", 6, 20, 7, 10, 30.0, 40_000, 5.0, "Sale Seeker"),
    _CampaignTemplate("Independence Day Weekend", "Promo", 7, 1, 7, 6, 20.0, 18_000, 5.5, "Core Shopper"),
    _CampaignTemplate("Back-to-School Early Access", "Email", 7, 20, 8, 10, 10.0, 15_000, 7.5, "Style Loyalist"),
    _CampaignTemplate("Back-to-School Main", "Paid Search", 8, 1, 9, 10, 0.0, 60_000, 6.0, "All Customers"),
    _CampaignTemplate("Back-to-School Paid Social", "Paid Social", 8, 1, 9, 5, 0.0, 45_000, 5.5, "Digital Native"),
    _CampaignTemplate("Labor Day Sale", "Promo", 8, 29, 9, 2, 25.0, 28_000, 5.5, "Sale Seeker"),
    _CampaignTemplate("Fall New Arrivals", "Email", 9, 1, 9, 15, 0.0, 18_000, 8.5, "Style Loyalist"),
    _CampaignTemplate("Fall Collection Influencer Drop", "Influencer", 9, 15, 9, 30, 20.0, 55_000, 4.5, "Digital Native"),
    _CampaignTemplate("Fall Affiliate Push", "Affiliate", 10, 1, 10, 31, 15.0, 20_000, 6.0, "Core Shopper"),
    _CampaignTemplate("Halloween Weekend", "Email", 10, 20, 10, 31, 15.0, 10_000, 7.0, "All Customers"),
    _CampaignTemplate("Veterans Day Sale", "Promo", 11, 8, 11, 12, 20.0, 22_000, 6.0, "Core Shopper"),
    _CampaignTemplate("Black Friday", "Promo", 11, 28, 11, 28, 40.0, 80_000, 5.0, "Sale Seeker"),
    _CampaignTemplate("Cyber Monday", "Promo", 12, 2, 12, 2, 30.0, 50_000, 5.5, "Digital Native"),
    _CampaignTemplate("Cyber Week Email", "Email", 12, 1, 12, 7, 30.0, 15_000, 8.0, "Sale Seeker"),
    _CampaignTemplate("Holiday Gift Guide Email", "Email", 12, 1, 12, 20, 0.0, 18_000, 9.0, "Gift & Occasion"),
    _CampaignTemplate("Holiday Paid Search", "Paid Search", 12, 1, 12, 24, 0.0, 70_000, 6.5, "All Customers"),
    _CampaignTemplate("Last-Minute Gifts Influencer", "Influencer", 12, 15, 12, 22, 10.0, 40_000, 5.0, "Gift & Occasion"),
    _CampaignTemplate("New Year's Sale", "Promo", 12, 26, 1, 6, 35.0, 35_000, 5.0, "Sale Seeker"),
    _CampaignTemplate("Valentine's Day Email", "Email", 1, 28, 2, 14, 15.0, 12_000, 7.5, "Gift & Occasion"),
    _CampaignTemplate("Valentine's Day Promo", "Promo", 2, 7, 2, 14, 15.0, 22_000, 6.0, "Gift & Occasion"),
    _CampaignTemplate("Presidents' Day Sale", "Promo", 2, 14, 2, 17, 25.0, 25_000, 5.5, "Sale Seeker"),
    _CampaignTemplate("St. Patrick's Day Email", "Email", 3, 10, 3, 17, 10.0, 8_000, 6.5, "All Customers"),
    _CampaignTemplate("End-of-Season Clearance", "Promo", 3, 1, 3, 31, 40.0, 30_000, 4.5, "Sale Seeker"),
]

# BFCM dates vary year-to-year — compute dynamically
def _thanksgiving(year: int) -> datetime.date:
    """4th Thursday of November."""
    first = datetime.date(year, 11, 1)
    offset = (3 - first.weekday()) % 7  # Thursday = weekday 3
    return first + datetime.timedelta(days=offset + 21)


def _resolve_date(year: int, mm: int, dd: int) -> datetime.date:
    """Return date clamped to valid calendar date."""
    try:
        return datetime.date(year, mm, dd)
    except ValueError:
        # Clamp to last day of month (e.g. Feb 30)
        last = calendar.monthrange(year, mm)[1]
        return datetime.date(year, mm, min(dd, last))


def _build_seasonal_campaigns(
    fiscal_year: int, rng: np.random.Generator, start_key: int
) -> list[dict]:
    """Generate seasonal campaign rows for one fiscal year (April–March).

    fiscal_year = 2024 → covers April 2023 – March 2024.
    """
    cal_year_start = fiscal_year - 1  # April of this calendar year
    rows: list[dict] = []
    key = start_key

    for tmpl in _SEASONAL_TEMPLATES:
        # Resolve start/end dates
        # Jan–Mar belong to the calendar year that ends the fiscal year
        if tmpl.start_mm <= 3:
            start_year = fiscal_year
        else:
            start_year = cal_year_start

        if tmpl.end_mm <= 3:
            end_year = fiscal_year
        else:
            end_year = cal_year_start

        start_date = _resolve_date(start_year, tmpl.start_mm, tmpl.start_dd)
        end_date = _resolve_date(end_year, tmpl.end_mm, tmpl.end_dd)

        # For Black Friday, pin to actual Thanksgiving-derived date
        if "Black Friday" in tmpl.name:
            tg = _thanksgiving(start_year)
            start_date = tg + datetime.timedelta(1)
            end_date = start_date
        if "Cyber Monday" in tmpl.name:
            tg = _thanksgiving(start_year)
            start_date = tg + datetime.timedelta(4)
            end_date = start_date

        if end_date < start_date:
            end_date = start_date

        # Actual spend = planned ± 15%
        noise = float(rng.uniform(0.85, 1.15))
        actual_spend = round(tmpl.planned_spend * noise, 2)
        target_rev = round(tmpl.planned_spend * tmpl.target_revenue_mult, 2)

        rows.append({
            "campaign_key": key,
            "campaign_id": f"CAMP-{fiscal_year}-{key:04d}",
            "campaign_name": f"{tmpl.name} FY{fiscal_year}",
            "campaign_type": tmpl.campaign_type,
            "start_date": start_date,
            "end_date": end_date,
            "target_segment": tmpl.target_segment,
            "discount_pct": tmpl.discount_pct,
            "planned_spend": tmpl.planned_spend,
            "actual_spend": actual_spend,
            "target_revenue": target_rev,
        })
        key += 1

    return rows


def _build_always_on_campaigns(
    start: datetime.date, end: datetime.date, rng: np.random.Generator, start_key: int
) -> list[dict]:
    """Quarterly always-on Paid Search and monthly Affiliate campaigns."""
    rows: list[dict] = []
    key = start_key

    # Quarterly paid search: one campaign per quarter
    current = datetime.date(start.year, start.month, 1)
    while current <= end:
        q_end_month = ((current.month - 1) // 3) * 3 + 3
        q_end = datetime.date(current.year + (q_end_month // 13), q_end_month % 12 or 12, 1)
        q_end = datetime.date(
            q_end.year, q_end.month,
            calendar.monthrange(q_end.year, q_end.month)[1]
        )
        q_end = min(q_end, end)

        noise = float(rng.uniform(0.9, 1.1))
        planned = 55_000.0
        rows.append({
            "campaign_key": key,
            "campaign_id": f"CAMP-AO-{key:04d}",
            "campaign_name": f"Paid Search — Brand + Category {current.strftime('%b %Y')}",
            "campaign_type": "Paid Search",
            "start_date": current,
            "end_date": q_end,
            "target_segment": "All Customers",
            "discount_pct": 0.0,
            "planned_spend": planned,
            "actual_spend": round(planned * noise, 2),
            "target_revenue": round(planned * 6.5, 2),
        })
        key += 1

        # Next quarter
        next_month = q_end.month % 12 + 1
        next_year = q_end.year + (1 if next_month == 1 else 0)
        current = datetime.date(next_year, next_month, 1)

    # Monthly affiliate campaigns
    current = datetime.date(start.year, start.month, 1)
    while current <= end:
        month_end = datetime.date(
            current.year, current.month,
            calendar.monthrange(current.year, current.month)[1]
        )
        month_end = min(month_end, end)
        noise = float(rng.uniform(0.88, 1.12))
        planned = 18_000.0
        rows.append({
            "campaign_key": key,
            "campaign_id": f"CAMP-AFF-{key:04d}",
            "campaign_name": f"Affiliate Program {current.strftime('%b %Y')}",
            "campaign_type": "Affiliate",
            "start_date": current,
            "end_date": month_end,
            "target_segment": "Core Shopper",
            "discount_pct": 12.0,
            "planned_spend": planned,
            "actual_spend": round(planned * noise, 2),
            "target_revenue": round(planned * 5.5, 2),
        })
        key += 1

        next_month = current.month % 12 + 1
        next_year = current.year + (1 if next_month == 1 else 0)
        current = datetime.date(next_year, next_month, 1)

    return rows


def build_dim_campaign(
    start: datetime.date = BACKFILL_START,
    end: datetime.date = BACKFILL_END,
    rng: np.random.Generator | None = None,
) -> pd.DataFrame:
    """Build dim_campaign over the full backfill window (~180 campaigns).

    Generates seasonal campaigns per fiscal year plus always-on paid search
    and affiliate campaigns.
    """
    if rng is None:
        rng = np.random.default_rng(42)

    rows: list[dict] = []
    key = 1

    # Determine fiscal years in range
    # FY = year whose March 31 falls on or after start, and whose April 1 <= end
    fy_start = start.year + (1 if start.month >= 4 else 0)
    fy_end = end.year + (1 if end.month >= 4 else 0)

    for fy in range(fy_start, fy_end + 1):
        seasonal = _build_seasonal_campaigns(fy, rng, key)
        # Filter to only campaigns whose start_date is within [start, end]
        seasonal = [r for r in seasonal if start <= r["start_date"] <= end]
        rows.extend(seasonal)
        key += len(seasonal)

    # Always-on campaigns
    always_on = _build_always_on_campaigns(start, end, rng, key)
    rows.extend(always_on)

    df = pd.DataFrame(rows)
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    df["campaign_key"] = df["campaign_key"].astype("int32")
    df["discount_pct"] = df["discount_pct"].astype("float64")
    df["planned_spend"] = df["planned_spend"].astype("float64")
    df["actual_spend"] = df["actual_spend"].astype("float64")
    df["target_revenue"] = df["target_revenue"].astype("float64")

    # Re-sequence campaign_key cleanly
    df = df.reset_index(drop=True)
    df["campaign_key"] = (df.index + 1).astype("int32")

    return df
