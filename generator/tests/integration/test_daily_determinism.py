"""Integration: same seed + same date → identical bronze output.

Two fully independent runs (separate output directories) with identical
(scale, seed, backfill) must produce identical DataFrames for every
bronze file on the same target date.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from csnp_retail.config import GeneratorConfig, Scale
from csnp_retail.runner import run_backfill, run_daily

_TARGET = date(2026, 4, 1)


@pytest.fixture(scope="module")
def two_runs(tmp_path_factory):
    out1 = tmp_path_factory.mktemp("det_run1")
    out2 = tmp_path_factory.mktemp("det_run2")
    for out in (out1, out2):
        config = GeneratorConfig(scale=Scale.xs, seed=42, out=out)
        run_backfill(config)
        run_daily(config, _TARGET)
    return out1, out2


def _bronze_sales_dfs(out, d: date) -> dict[str, pd.DataFrame]:
    yyyy, mm, dd = d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")
    result: dict[str, pd.DataFrame] = {}

    pos_dir = out / "bronze" / "pos" / yyyy / mm / dd
    if pos_dir.exists():
        for f in sorted(pos_dir.glob("*.parquet")):
            result[f"pos/{f.name}"] = pd.read_parquet(f)

    for src in ("ecom", "app"):
        p = out / "bronze" / src / yyyy / mm / dd / "orders.parquet"
        if p.exists():
            result[src] = pd.read_parquet(p)

    return result


def _bronze_sessions_df(out, d: date) -> pd.DataFrame:
    yyyy, mm, dd = d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")
    parts = []
    cs_dir = out / "bronze" / "clickstream" / yyyy / mm / dd
    if cs_dir.exists():
        for f in sorted(cs_dir.glob("**/sessions.parquet")):
            parts.append(pd.read_parquet(f))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


class TestSalesDeterminism:
    def test_pos_files_identical(self, two_runs):
        out1, out2 = two_runs
        dfs1 = _bronze_sales_dfs(out1, _TARGET)
        dfs2 = _bronze_sales_dfs(out2, _TARGET)
        assert set(dfs1.keys()) == set(dfs2.keys()), "Different POS file names across runs"
        for key in dfs1:
            if key.startswith("pos/"):
                pd.testing.assert_frame_equal(
                    dfs1[key].reset_index(drop=True),
                    dfs2[key].reset_index(drop=True),
                    check_like=False,
                    obj=f"pos file {key}",
                )

    def test_ecom_orders_identical(self, two_runs):
        out1, out2 = two_runs
        dfs1 = _bronze_sales_dfs(out1, _TARGET)
        dfs2 = _bronze_sales_dfs(out2, _TARGET)
        if "ecom" not in dfs1 and "ecom" not in dfs2:
            pytest.skip("No ecom orders on target date")
        df1 = dfs1.get("ecom", pd.DataFrame())
        df2 = dfs2.get("ecom", pd.DataFrame())
        pd.testing.assert_frame_equal(
            df1.reset_index(drop=True), df2.reset_index(drop=True), check_like=False
        )

    def test_app_orders_identical(self, two_runs):
        out1, out2 = two_runs
        dfs1 = _bronze_sales_dfs(out1, _TARGET)
        dfs2 = _bronze_sales_dfs(out2, _TARGET)
        if "app" not in dfs1 and "app" not in dfs2:
            pytest.skip("No app orders on target date")
        df1 = dfs1.get("app", pd.DataFrame())
        df2 = dfs2.get("app", pd.DataFrame())
        pd.testing.assert_frame_equal(
            df1.reset_index(drop=True), df2.reset_index(drop=True), check_like=False
        )


class TestSessionsDeterminism:
    def test_sessions_identical(self, two_runs):
        out1, out2 = two_runs
        df1 = _bronze_sessions_df(out1, _TARGET)
        df2 = _bronze_sessions_df(out2, _TARGET)
        if len(df1) == 0 and len(df2) == 0:
            pytest.skip("No sessions on target date")
        pd.testing.assert_frame_equal(
            df1.sort_values("session_key").reset_index(drop=True),
            df2.sort_values("session_key").reset_index(drop=True),
            check_like=False,
        )


class TestInventoryDeterminism:
    def test_inventory_snapshot_identical(self, two_runs):
        out1, out2 = two_runs
        yyyy, mm, dd = _TARGET.strftime("%Y"), _TARGET.strftime("%m"), _TARGET.strftime("%d")
        p1 = out1 / "bronze" / "inventory" / yyyy / mm / dd / "snapshot.parquet"
        p2 = out2 / "bronze" / "inventory" / yyyy / mm / dd / "snapshot.parquet"
        if not p1.exists() or not p2.exists():
            pytest.skip("No inventory snapshot on target date")
        df1 = pd.read_parquet(p1)
        df2 = pd.read_parquet(p2)
        pd.testing.assert_frame_equal(
            df1.reset_index(drop=True), df2.reset_index(drop=True), check_like=False
        )


class TestManifestDeterminism:
    def test_watermarks_identical(self, two_runs):
        out1, out2 = two_runs
        from csnp_retail.manifest import Manifest
        m1 = Manifest.load(out1 / "manifest.json")
        m2 = Manifest.load(out2 / "manifest.json")
        assert m1.id_watermarks == m2.id_watermarks
        assert m1.timeline.fictional_date == m2.timeline.fictional_date
        assert m1.timeline.daily_runs_completed == m2.timeline.daily_runs_completed
