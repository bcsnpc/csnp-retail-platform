"""Integration test: full XS backfill run.

Runs the generator end-to-end at XS scale and checks that all expected
Parquet files are written with sensible row counts.

Skipped by default in unit test runs; run with:
    pytest generator/tests/integration -v
"""

from __future__ import annotations

import pytest
from pathlib import Path
import pandas as pd

from csnp_retail.config import GeneratorConfig, Scale
from csnp_retail.runner import run_backfill


@pytest.fixture(scope="module")
def backfill_output(tmp_path_factory):
    out = tmp_path_factory.mktemp("backfill_xs")
    config = GeneratorConfig(scale=Scale.xs, seed=42, out=out)
    run_backfill(config)
    return out


def test_dim_date_written(backfill_output):
    p = backfill_output / "dim_date.parquet"
    assert p.exists()
    df = pd.read_parquet(p)
    assert len(df) == 1096  # 2023-04-01 → 2026-03-31


def test_dim_geography_written(backfill_output):
    p = backfill_output / "dim_geography.parquet"
    assert p.exists()
    df = pd.read_parquet(p)
    assert len(df) >= 150


def test_dim_store_written(backfill_output):
    p = backfill_output / "dim_store.parquet"
    assert p.exists()
    df = pd.read_parquet(p)
    assert len(df) == 15  # XS scale


def test_dim_campaign_written(backfill_output):
    p = backfill_output / "dim_campaign.parquet"
    assert p.exists()
    df = pd.read_parquet(p)
    assert len(df) >= 100


def test_manifest_written(backfill_output):
    p = backfill_output / "manifest.json"
    assert p.exists()
    import json
    m = json.loads(p.read_text())
    assert m["scale"] == "xs"
    assert m["seed"] == 42
    assert m["schema_version"] == "1"
    assert "timeline" in m
    assert "id_watermarks" in m
    expected_tables = {
        "dim_date", "dim_geography", "dim_store", "dim_campaign",
        "dim_channel", "dim_return_reason", "dim_product", "dim_customer",
        "fact_sales", "fact_returns", "fact_sessions",
        "fact_marketing_spend", "fact_loyalty_events", "fact_inventory_daily",
    }
    assert set(m["tables_written"]) == expected_tables


def test_determinism(tmp_path):
    """Same seed → identical output."""
    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    run_backfill(GeneratorConfig(scale=Scale.xs, seed=42, out=out1))
    run_backfill(GeneratorConfig(scale=Scale.xs, seed=42, out=out2))
    for name in ["dim_date", "dim_geography", "dim_store", "dim_campaign"]:
        df1 = pd.read_parquet(out1 / f"{name}.parquet")
        df2 = pd.read_parquet(out2 / f"{name}.parquet")
        pd.testing.assert_frame_equal(df1, df2, check_like=False)
