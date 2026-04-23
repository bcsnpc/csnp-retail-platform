"""dim_store builder.

Store count per scale:
  XS=15, S=45, M=142, L=380

Country distribution (M scale): 108 US, 22 CA, 8 UK, 4 MX.
UK expansion: all UK stores opened in FY25 Q3–Q4 (Oct 2024 – Mar 2025).
Format mix: Flagship / Standard / Outlet / Pop-up.
"""

from __future__ import annotations

import calendar
import datetime

import numpy as np
import pandas as pd

from csnp_retail.config import GeneratorConfig, Scale
from csnp_retail.faker_pools import (
    CLIMATE_ZONES,
    DISTRICTS,
    GEO_COLUMNS,
    GEO_RECORDS,
    REGION_MANAGERS,
    STORE_NEIGHBORHOODS,
)

# Target country distribution fractions (M scale)
_COUNTRY_FRACTIONS = {"US": 108 / 142, "CA": 22 / 142, "GB": 8 / 142, "MX": 4 / 142}

# Format mix (M scale counts)
_FORMAT_COUNTS_M = {"Flagship": 8, "Standard": 118, "Outlet": 12, "Pop-up": 4}

# Square footage ranges per format
_SQFT_RANGES = {
    "Flagship": (14_000, 22_000),
    "Standard": (4_000, 8_000),
    "Outlet": (6_000, 10_000),
    "Pop-up": (1_200, 2_800),
}

# District assignment per state code (US)
_US_DISTRICT: dict[str, str] = {
    "ME": "Northeast", "VT": "Northeast", "NH": "Northeast", "MA": "Northeast",
    "RI": "Northeast", "CT": "Northeast", "NY": "Northeast", "NJ": "Northeast",
    "PA": "Mid-Atlantic", "MD": "Mid-Atlantic", "VA": "Mid-Atlantic",
    "WV": "Mid-Atlantic", "DE": "Mid-Atlantic",
    "NC": "Southeast", "SC": "Southeast", "GA": "Southeast",
    "AL": "Southeast", "MS": "Southeast",
    "FL": "Florida",
    "OH": "Great Lakes", "IN": "Great Lakes", "MI": "Great Lakes",
    "WI": "Great Lakes", "IL": "Great Lakes",
    "MN": "Midwest", "IA": "Midwest", "MO": "Midwest",
    "ND": "Midwest", "SD": "Midwest", "NE": "Midwest", "KS": "Midwest",
    "TN": "South Central", "KY": "South Central", "AR": "South Central",
    "LA": "South Central", "OK": "South Central",
    "TX": "Texas",
    "CO": "Mountain West", "UT": "Mountain West", "NM": "Mountain West",
    "WY": "Mountain West", "MT": "Mountain West", "ID": "Mountain West",
    "AZ": "Desert Southwest", "NV": "Desert Southwest",
    "WA": "Pacific Northwest", "OR": "Pacific Northwest", "AK": "Pacific Northwest",
    "CA": "California North",  # overridden below for SoCal
    "HI": "Hawaii",
}

# SoCal zip prefixes (for splitting CA district)
_SOCAL_ZIPS = {"90", "91", "92", "93"}

_UK_OPEN_WINDOWS = [
    # (open_date_earliest, open_date_latest) — random pick within window
    (datetime.date(2024, 10, 1), datetime.date(2024, 12, 31)),  # Q3 FY25
    (datetime.date(2025, 1, 1), datetime.date(2025, 3, 31)),   # Q4 FY25
]


def _scale_country_counts(n_stores: int) -> dict[str, int]:
    """Scale country distribution to requested store count."""
    counts: dict[str, int] = {}
    allocated = 0
    for country, frac in list(_COUNTRY_FRACTIONS.items())[:-1]:
        c = max(1, round(n_stores * frac))
        counts[country] = c
        allocated += c
    counts["MX"] = max(0, n_stores - allocated)
    return counts


def _scale_format_counts(n_stores: int) -> dict[str, int]:
    scale_f = n_stores / 142
    result: dict[str, int] = {}
    allocated = 0
    for fmt, cnt in list(_FORMAT_COUNTS_M.items())[:-1]:
        c = max(0, round(cnt * scale_f))
        result[fmt] = c
        allocated += c
    result["Pop-up"] = max(0, n_stores - allocated)
    return result


def _geo_pool_by_country(country_code: str) -> pd.DataFrame:
    all_geo = pd.DataFrame(GEO_RECORDS, columns=GEO_COLUMNS)
    return all_geo[all_geo["country_code"] == country_code].reset_index(drop=True)


def _pick_geos(pool: pd.DataFrame, n: int, rng: np.random.Generator) -> pd.DataFrame:
    if len(pool) == 0:
        raise ValueError("Empty geography pool for country")
    if n <= len(pool):
        idx = rng.choice(len(pool), size=n, replace=False)
    else:
        idx = rng.choice(len(pool), size=n, replace=True)
    return pool.iloc[idx].reset_index(drop=True)


def _make_store_name(city: str, fmt: str, neighborhood: str | None) -> str:
    if fmt == "Outlet":
        return f"CSNP & Co. {city} Outlet"
    if fmt == "Pop-up":
        return f"CSNP & Co. Pop-Up {city}"
    suffix = f" — {neighborhood}" if neighborhood else ""
    return f"CSNP & Co. {city}{suffix}"


def _district_for_geo(row: pd.Series) -> str:
    cc = row["country_code"]
    sc = row["state_code"]
    if cc == "US":
        base = _US_DISTRICT.get(sc, "Mountain West")
        if sc == "CA" and str(row["postal_code"])[:2] in _SOCAL_ZIPS:
            return "California South"
        return base
    return DISTRICTS.get(cc, ["International"])[0]


def _climate_for_geo(row: pd.Series) -> str:
    return CLIMATE_ZONES.get(row["state_code"], "Temperate")


def build_dim_store(
    config: GeneratorConfig,
    rng: np.random.Generator,
    dim_geography: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build dim_store for the requested scale."""
    n_stores = config.n_stores
    geo_counts = _scale_country_counts(n_stores)
    fmt_counts = _scale_format_counts(n_stores)

    # Build a flat list of (country_code, format_type) for each store slot
    # Distribute formats roughly uniformly across countries
    slots: list[tuple[str, str]] = []
    fmt_pool = []
    for fmt, cnt in fmt_counts.items():
        fmt_pool.extend([fmt] * cnt)
    rng.shuffle(fmt_pool)

    country_list: list[str] = []
    for cc, cnt in geo_counts.items():
        country_list.extend([cc] * cnt)

    assert len(fmt_pool) == n_stores
    assert len(country_list) == n_stores

    rows = []
    store_num = 1001
    mgr_idx = 0

    for i in range(n_stores):
        cc = country_list[i]
        fmt = fmt_pool[i]

        pool = _geo_pool_by_country(cc)
        if len(pool) == 0:
            continue
        geo_row = pool.iloc[int(rng.integers(0, len(pool)))]

        city = geo_row["city"]
        neighborhoods = STORE_NEIGHBORHOODS.get(city, [""])
        nbhd = neighborhoods[int(rng.integers(0, len(neighborhoods)))] if neighborhoods else None

        store_name = _make_store_name(city, fmt, nbhd)

        # Open date
        if cc == "GB":
            # UK expansion in FY25 Q3/Q4
            window = _UK_OPEN_WINDOWS[int(rng.integers(0, 2))]
            days_in_window = (window[1] - window[0]).days
            open_date = window[0] + datetime.timedelta(days=int(rng.integers(0, days_in_window + 1)))
        elif fmt == "Pop-up":
            # Pop-ups open within first 2 years and close within 90 days
            open_date = config.start + datetime.timedelta(days=int(rng.integers(0, 730)))
        else:
            # Most stores open before history starts (established brand)
            open_date = config.start - datetime.timedelta(days=int(rng.integers(365, 365 * 8)))

        # Close date (only pop-ups or very rare closures)
        if fmt == "Pop-up":
            close_date: datetime.date | None = open_date + datetime.timedelta(
                days=int(rng.integers(45, 91))
            )
        else:
            close_date = None

        lo, hi = _SQFT_RANGES[fmt]
        sqft = int(rng.integers(lo, hi))

        mgr = REGION_MANAGERS[mgr_idx % len(REGION_MANAGERS)]
        mgr_idx += 1

        district = _district_for_geo(geo_row)
        climate = _climate_for_geo(geo_row)

        rows.append({
            "store_key": store_num - 1000,  # 1-based
            "store_id": f"CSNP-{store_num}",
            "store_name": store_name,
            "format_type": fmt,
            "square_footage": sqft,
            "open_date": open_date,
            "close_date": close_date,
            "postal_code": geo_row["postal_code"],
            "city": city,
            "state_province": geo_row["state_province"],
            "state_code": geo_row["state_code"],
            "country": geo_row["country"],
            "country_code": cc,
            "currency_code": geo_row["currency_code"],
            "latitude": float(geo_row["latitude"]),
            "longitude": float(geo_row["longitude"]),
            "timezone": geo_row["timezone"],
            "district": district,
            "climate_zone": climate,
            "region_manager": mgr,
        })
        store_num += 1

    df = pd.DataFrame(rows)
    df["open_date"] = pd.to_datetime(df["open_date"])
    df["close_date"] = pd.to_datetime(df["close_date"])
    return df
