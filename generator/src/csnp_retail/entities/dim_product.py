"""dim_product — product catalogue with SCD2 price revisions.

Each row is one (SKU, effective_period) version.  Price changes create
additional rows with updated effective_date / expiry_date.
style_code groups colorways of the same style together.
Planted styles: TOP-MCC-001 (Meridian Cable Crew), BTM-FSL-001 (Field Straight-Leg Pant).
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import numpy as np
import pandas as pd

from csnp_retail.config import GeneratorConfig
from csnp_retail.faker_pools import COLORS, PRODUCT_ADJECTIVES, PRODUCT_NOUNS

# ── Category metadata ──────────────────────────────────────────────────────────

_CAT_WEIGHTS = [
    ("tops",        0.30),
    ("bottoms",     0.25),
    ("outerwear",   0.20),
    ("footwear",    0.12),
    ("accessories", 0.08),
    ("home",        0.03),
    ("beauty",      0.02),
]

_CAT_ABBREV = {
    "tops":        "TOP",
    "bottoms":     "BTM",
    "outerwear":   "OTW",
    "footwear":    "FTW",
    "accessories": "ACC",
    "home":        "HOM",
    "beauty":      "BTY",
}

_DEPARTMENTS: dict[str, list[str]] = {
    "tops":        ["Mens", "Womens"],
    "bottoms":     ["Mens", "Womens"],
    "outerwear":   ["Mens", "Womens", "Unisex"],
    "footwear":    ["Mens", "Womens", "Unisex"],
    "accessories": ["Unisex"],
    "home":        ["Unisex"],
    "beauty":      ["Unisex"],
}

_PRICE_RANGES: dict[str, tuple[float, float]] = {
    "tops":        (29.0,  89.0),
    "bottoms":     (39.0, 129.0),
    "outerwear":   (69.0, 289.0),
    "footwear":    (59.0, 189.0),
    "accessories": (19.0, 149.0),
    "home":        (15.0,  89.0),
    "beauty":      (12.0,  49.0),
}

_MARGIN_RATES: dict[str, float] = {
    "tops":        0.52,
    "bottoms":     0.50,
    "outerwear":   0.48,
    "footwear":    0.45,
    "accessories": 0.55,
    "home":        0.50,
    "beauty":      0.60,
}

# ── Planted styles ─────────────────────────────────────────────────────────────

_PLANTED: list[dict] = [
    {
        "category":    "tops",
        "name":        "Meridian Cable Crew",
        "style_code":  "TOP-MCC-001",
        "colors":      ["Navy", "Oat Heather", "Black"],
        "list_price":  79.0,
        "department":  "Unisex",
    },
    {
        "category":    "bottoms",
        "name":        "Field Straight-Leg Pant",
        "style_code":  "BTM-FSL-001",
        "colors":      ["Stone", "Navy", "Olive Branch"],
        "list_price":  89.0,
        "department":  "Mens",
    },
]


def _color_code(name: str) -> str:
    """3-char color code: first 3 chars of name stripped of spaces/hyphens."""
    clean = name.replace(" ", "").replace("-", "").upper()
    return clean[:3].ljust(3, "X")


def _style_abbrev(adj: str, noun: str, used: set[str]) -> str:
    """3-char style abbreviation, deduplicated against used set."""
    words = (adj + " " + noun).replace("-", " ").split()
    base = "".join(w[0].upper() for w in words[:3]).ljust(3, "X")[:3]
    if base not in used:
        used.add(base)
        return base
    for suffix in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        alt = base[:2] + suffix
        if alt not in used:
            used.add(alt)
            return alt
    used.add(base)
    return base


def _round_price(price: float) -> float:
    """Round to .99 pricing (or .00 for multiples of 5)."""
    base = int(price)
    return float(base) + 0.99 if base % 5 != 0 else float(base)


def _build_scd2_rows(
    product_id: str,
    style_code: str,
    product_name: str,
    category: str,
    department: str,
    color_name: str,
    color_family: str,
    initial_price: float,
    cost_rate: float,
    rng: np.random.Generator,
    start: date,
    end: date,
) -> list[dict]:
    """Return 1-3 SCD2 rows for a single SKU with possible price changes."""
    span_days = (end - start).days

    r = rng.random()
    if r < 0.40:
        n_versions = 2
    elif r < 0.55:
        n_versions = 3
    else:
        n_versions = 1

    prices: list[float] = [initial_price]
    for _ in range(n_versions - 1):
        direction = rng.choice([-1, 1])
        pct = float(rng.uniform(0.05, 0.20))
        raw = prices[-1] * (1 + direction * pct)
        lo, hi = _PRICE_RANGES[category]
        prices.append(_round_price(max(lo, min(hi, raw))))

    change_offsets = sorted(int(x) for x in rng.integers(180, span_days - 30, size=n_versions - 1))
    effective_dates = [start] + [start + timedelta(days=d) for d in change_offsets]
    expiry_dates = [effective_dates[i + 1] - timedelta(days=1) for i in range(n_versions - 1)] + [None]

    rows = []
    for i, (eff, exp, price) in enumerate(zip(effective_dates, expiry_dates, prices)):
        rows.append({
            "product_id":      product_id,
            "style_code":      style_code,
            "sku":             product_id,
            "product_name":    product_name,
            "category":        category,
            "department":      department,
            "color_name":      color_name,
            "color_family":    color_family,
            "list_price":      price,
            "cost_price":      round(price * (1 - cost_rate), 2),
            "effective_date":  eff,
            "expiry_date":     exp,
            "is_current":      i == n_versions - 1,
        })
    return rows


def build_dim_product(config: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    n_skus = config.n_products
    start = config.start
    end = config.end

    all_rows: list[dict] = []
    planted_cat_counts: dict[str, int] = {}

    # ── 1. Planted styles ──────────────────────────────────────────────────────
    for ps in _PLANTED:
        cat = ps["category"]
        cost_rate = _MARGIN_RATES[cat]
        for color_name in ps["colors"]:
            color_family = next((cf for cn, cf in COLORS if cn == color_name), "neutral")
            c_code = _color_code(color_name)
            product_id = f"{ps['style_code']}-{c_code}"
            scd2 = _build_scd2_rows(
                product_id, ps["style_code"], ps["name"], cat,
                ps["department"], color_name, color_family,
                ps["list_price"], cost_rate, rng, start, end,
            )
            all_rows.extend(scd2)
            planted_cat_counts[cat] = planted_cat_counts.get(cat, 0) + 1

    # ── 2. Per-category SKU targets ────────────────────────────────────────────
    cat_sku_targets: dict[str, int] = {}
    for cat, weight in _CAT_WEIGHTS:
        target = round(n_skus * weight)
        already = planted_cat_counts.get(cat, 0)
        cat_sku_targets[cat] = max(0, target - already)

    total_assigned = sum(cat_sku_targets.values())
    planted_total = sum(planted_cat_counts.values())
    remaining = n_skus - planted_total - total_assigned
    if remaining != 0:
        cat_sku_targets["tops"] = max(0, cat_sku_targets["tops"] + remaining)

    # ── 3. Generate remaining SKUs per category ────────────────────────────────
    planted_abbrevs: dict[str, set[str]] = {
        "tops":    {"MCC"},
        "bottoms": {"FSL"},
    }

    for cat, n_cat_skus in cat_sku_targets.items():
        if n_cat_skus <= 0:
            continue

        cat_abbrev = _CAT_ABBREV[cat]
        adjs = PRODUCT_ADJECTIVES[cat]
        nouns = PRODUCT_NOUNS[cat]
        departments = _DEPARTMENTS[cat]
        lo_price, hi_price = _PRICE_RANGES[cat]
        cost_rate = _MARGIN_RATES[cat]

        used_abbrevs: set[str] = set(planted_abbrevs.get(cat, set()))
        n_colors = 3
        n_styles = math.ceil(n_cat_skus / n_colors)
        seq = 2  # planted styles use 001

        sku_count = 0
        for s_idx in range(n_styles):
            if sku_count >= n_cat_skus:
                break
            adj = adjs[s_idx % len(adjs)]
            noun = nouns[s_idx % len(nouns)]
            abbrev = _style_abbrev(adj, noun, used_abbrevs)
            style_code = f"{cat_abbrev}-{abbrev}-{seq:03d}"
            seq += 1
            dept = departments[s_idx % len(departments)]
            product_name = f"{adj} {noun}"
            initial_price = _round_price(float(rng.uniform(lo_price, hi_price)))

            color_start = (s_idx * n_colors) % len(COLORS)
            for c_offset in range(n_colors):
                if sku_count >= n_cat_skus:
                    break
                color_name, color_family = COLORS[(color_start + c_offset) % len(COLORS)]
                c_code = _color_code(color_name)
                product_id = f"{style_code}-{c_code}"
                scd2 = _build_scd2_rows(
                    product_id, style_code, product_name, cat, dept,
                    color_name, color_family, initial_price, cost_rate,
                    rng, start, end,
                )
                all_rows.extend(scd2)
                sku_count += 1

    # ── 4. Assemble DataFrame ──────────────────────────────────────────────────
    df = pd.DataFrame(all_rows)
    df.insert(0, "product_key", range(1, len(df) + 1))
    df["effective_date"] = pd.to_datetime(df["effective_date"])
    df["expiry_date"] = pd.to_datetime(df["expiry_date"])
    return df
