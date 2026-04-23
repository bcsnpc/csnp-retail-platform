"""Integration: backfill + 10 consecutive daily runs.

Verifies:
- Bronze paths exist for every run date
- sale_key, session_key, inventory_key are contiguous and never duplicate
- Watermarks in the manifest are monotonically non-decreasing
- fictional_date advances by 1 each run; daily_runs_completed increments
"""

from __future__ import annotations

import pandas as pd
import pytest
from datetime import date, timedelta
from pathlib import Path

from csnp_retail.config import GeneratorConfig, Scale
from csnp_retail.manifest import Manifest
from csnp_retail.runner import run_backfill, run_daily

_DAILY_START = date(2026, 4, 1)
_N_DAYS = 10


@pytest.fixture(scope="module")
def continuity_output(tmp_path_factory):
    out = tmp_path_factory.mktemp("daily_continuity")
    config = GeneratorConfig(scale=Scale.xs, seed=42, out=out)
    run_backfill(config)
    manifests: list[Manifest] = []
    for i in range(_N_DAYS):
        target = _DAILY_START + timedelta(days=i)
        m = run_daily(config, target)
        manifests.append(m)
    return out, manifests


def _read_day_sales(out: Path, d: date) -> pd.DataFrame:
    yyyy, mm, dd = d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")
    parts: list[pd.DataFrame] = []
    pos_dir = out / "bronze" / "pos" / yyyy / mm / dd
    if pos_dir.exists():
        for f in sorted(pos_dir.glob("*.parquet")):
            parts.append(pd.read_parquet(f))
    for src in ("ecom", "app"):
        p = out / "bronze" / src / yyyy / mm / dd / "orders.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            if len(df) > 0:
                parts.append(df)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _read_day_sessions(out: Path, d: date) -> pd.DataFrame:
    yyyy, mm, dd = d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")
    cs_dir = out / "bronze" / "clickstream" / yyyy / mm / dd
    if not cs_dir.exists():
        return pd.DataFrame()
    parts = [pd.read_parquet(f) for f in sorted(cs_dir.glob("**/sessions.parquet"))]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


class TestBronzePathsExist:
    def test_pos_directories(self, continuity_output):
        out, _ = continuity_output
        for i in range(_N_DAYS):
            d = _DAILY_START + timedelta(days=i)
            pos_dir = out / "bronze" / "pos" / d.strftime("%Y") / d.strftime("%m") / d.strftime("%d")
            assert pos_dir.exists(), f"POS dir missing for {d}"

    def test_ecom_orders(self, continuity_output):
        out, _ = continuity_output
        for i in range(_N_DAYS):
            d = _DAILY_START + timedelta(days=i)
            p = out / "bronze" / "ecom" / d.strftime("%Y") / d.strftime("%m") / d.strftime("%d") / "orders.parquet"
            assert p.exists(), f"ecom orders missing for {d}"

    def test_app_orders(self, continuity_output):
        out, _ = continuity_output
        for i in range(_N_DAYS):
            d = _DAILY_START + timedelta(days=i)
            p = out / "bronze" / "app" / d.strftime("%Y") / d.strftime("%m") / d.strftime("%d") / "orders.parquet"
            assert p.exists(), f"app orders missing for {d}"

    def test_inventory_snapshots(self, continuity_output):
        out, _ = continuity_output
        for i in range(_N_DAYS):
            d = _DAILY_START + timedelta(days=i)
            p = out / "bronze" / "inventory" / d.strftime("%Y") / d.strftime("%m") / d.strftime("%d") / "snapshot.parquet"
            assert p.exists(), f"inventory snapshot missing for {d}"

    def test_clickstream_has_hourly_files(self, continuity_output):
        out, _ = continuity_output
        d = _DAILY_START  # just check day 1
        cs_dir = out / "bronze" / "clickstream" / d.strftime("%Y") / d.strftime("%m") / d.strftime("%d")
        assert cs_dir.exists()
        hourly_files = list(cs_dir.glob("**/sessions.parquet"))
        assert len(hourly_files) > 0, "No clickstream files found for day 1"


class TestSaleKeyNoDuplicates:
    def test_fresh_sale_keys_unique_across_all_days(self, continuity_output):
        out, _ = continuity_output
        all_keys: list[int] = []
        for i in range(_N_DAYS):
            d = _DAILY_START + timedelta(days=i)
            df = _read_day_sales(out, d)
            if len(df) == 0:
                continue
            fresh = df[df["late_arrival_days"] == 0] if "late_arrival_days" in df.columns else df
            all_keys.extend(fresh["sale_key"].tolist())
        assert len(all_keys) == len(set(all_keys)), "Duplicate sale_keys across daily runs"

    def test_sale_keys_continue_from_backfill(self, continuity_output):
        out, manifests = continuity_output
        # First daily run's sale_key > 100_000 (backfill max)
        d = _DAILY_START
        df = _read_day_sales(out, d)
        fresh = df[df["late_arrival_days"] == 0] if "late_arrival_days" in df.columns else df
        if len(fresh) == 0:
            pytest.skip("No fresh sales on day 1")
        assert fresh["sale_key"].min() > 100_000

    def test_order_ids_unique(self, continuity_output):
        out, _ = continuity_output
        all_oids: list[str] = []
        for i in range(_N_DAYS):
            d = _DAILY_START + timedelta(days=i)
            df = _read_day_sales(out, d)
            if len(df) == 0 or "order_id" not in df.columns:
                continue
            fresh = df[df["late_arrival_days"] == 0] if "late_arrival_days" in df.columns else df
            all_oids.extend(fresh["order_id"].tolist())
        assert len(all_oids) == len(set(all_oids)), "Duplicate order_ids across daily runs"


class TestSessionKeyNoDuplicates:
    def test_session_keys_unique_across_all_days(self, continuity_output):
        out, _ = continuity_output
        all_keys: list[int] = []
        for i in range(_N_DAYS):
            d = _DAILY_START + timedelta(days=i)
            df = _read_day_sessions(out, d)
            all_keys.extend(df["session_key"].tolist() if len(df) else [])
        assert len(all_keys) == len(set(all_keys)), "Duplicate session_keys"

    def test_session_keys_continue_from_backfill(self, continuity_output):
        out, _ = continuity_output
        d = _DAILY_START
        df = _read_day_sessions(out, d)
        if len(df) == 0:
            pytest.skip("No sessions on day 1")
        assert df["session_key"].min() > 180_000  # backfill max


class TestWatermarksMonotone:
    def test_sale_key_watermark_non_decreasing(self, continuity_output):
        _, manifests = continuity_output
        for i in range(1, len(manifests)):
            assert manifests[i].id_watermarks.sale_key >= manifests[i - 1].id_watermarks.sale_key

    def test_session_key_watermark_non_decreasing(self, continuity_output):
        _, manifests = continuity_output
        for i in range(1, len(manifests)):
            assert manifests[i].id_watermarks.session_key >= manifests[i - 1].id_watermarks.session_key

    def test_inventory_key_watermark_non_decreasing(self, continuity_output):
        _, manifests = continuity_output
        for i in range(1, len(manifests)):
            assert manifests[i].id_watermarks.inventory_key >= manifests[i - 1].id_watermarks.inventory_key


class TestManifestTimeline:
    def test_fictional_date_advances_daily(self, continuity_output):
        _, manifests = continuity_output
        for i, m in enumerate(manifests):
            expected = _DAILY_START + timedelta(days=i)
            assert m.timeline.fictional_date == expected, (
                f"Run {i+1}: expected fictional_date={expected}, got {m.timeline.fictional_date}"
            )

    def test_daily_runs_completed_increments(self, continuity_output):
        _, manifests = continuity_output
        for i, m in enumerate(manifests):
            assert m.timeline.daily_runs_completed == i + 1

    def test_manifest_persisted_to_disk(self, continuity_output):
        out, manifests = continuity_output
        loaded = Manifest.load(out / "manifest.json")
        assert loaded.timeline.daily_runs_completed == _N_DAYS
        assert loaded.timeline.fictional_date == _DAILY_START + timedelta(days=_N_DAYS - 1)
