"""Post-generation sanity checks."""

from __future__ import annotations

import pandas as pd


def validate_dim_date(df: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    if df["date_key"].duplicated().any():
        issues.append("duplicate date_keys")
    if df["fiscal_year"].isna().any():
        issues.append("null fiscal_year")
    return issues


def validate_no_orphan_keys(
    fact: pd.DataFrame, dim: pd.DataFrame, fact_col: str, dim_col: str, dim_name: str
) -> list[str]:
    valid = set(dim[dim_col])
    orphans = ~fact[fact_col].isin(valid) & fact[fact_col].notna()
    if orphans.any():
        return [f"{fact_col}: {orphans.sum()} orphan FK rows (not in {dim_name}.{dim_col})"]
    return []
