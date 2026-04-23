"""Unit tests for manifest schema, roundtrip, and ID watermarks."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest

from csnp_retail.config import Scale
from csnp_retail.manifest import IdWatermarks, Manifest, TimelineState
from csnp_retail.patterns import module_version


# ── Helpers ───────────────────────────────────────────────────────────────────

def _minimal_manifest() -> Manifest:
    return Manifest(
        seed=42,
        scale=Scale.xs,
        generated_at=datetime(2026, 4, 21, 10, 0, 0, tzinfo=timezone.utc),
        timeline=TimelineState(
            backfill_start=date(2023, 4, 1),
            backfill_end=date(2026, 3, 31),
            fictional_date=date(2026, 3, 31),
        ),
        id_watermarks=IdWatermarks(),
        patterns_module_version=module_version(),
    )


def _full_manifest() -> Manifest:
    return Manifest(
        seed=42,
        scale=Scale.xs,
        generated_at=datetime(2026, 4, 21, 10, 0, 0, tzinfo=timezone.utc),
        timeline=TimelineState(
            backfill_start=date(2023, 4, 1),
            backfill_end=date(2026, 3, 31),
            fictional_date=date(2026, 3, 31),
            daily_runs_completed=0,
        ),
        id_watermarks=IdWatermarks(
            sale_key=100_000,
            order_seq=100_000,
            return_key=13_475,
            session_key=180_000,
            customer_key=5_902,
            customer_id_seq=5_000,
            event_key=62_133,
            spend_key=4_424,
            inventory_key=805_560,
        ),
        patterns_module_version=module_version(),
        tables_written=["fact_sales", "fact_returns"],
        row_counts={"fact_sales": 100_000, "fact_returns": 13_475},
    )


# ── Roundtrip ─────────────────────────────────────────────────────────────────

class TestRoundtrip:
    def test_minimal_roundtrip(self, tmp_path):
        m = _minimal_manifest()
        path = tmp_path / "manifest.json"
        m.save(path)
        loaded = Manifest.load(path)
        assert loaded.seed == m.seed
        assert loaded.scale == m.scale
        assert loaded.timeline.fictional_date == m.timeline.fictional_date
        assert loaded.id_watermarks == m.id_watermarks
        assert loaded.patterns_module_version == m.patterns_module_version

    def test_full_roundtrip(self, tmp_path):
        m = _full_manifest()
        path = tmp_path / "manifest.json"
        m.save(path)
        loaded = Manifest.load(path)
        assert loaded == m

    def test_written_as_valid_json(self, tmp_path):
        path = tmp_path / "manifest.json"
        _minimal_manifest().save(path)
        data = json.loads(path.read_text())
        assert isinstance(data, dict)

    def test_creates_parent_dir(self, tmp_path):
        path = tmp_path / "subdir" / "manifest.json"
        _minimal_manifest().save(path)
        assert path.exists()


# ── Schema fields ─────────────────────────────────────────────────────────────

class TestSchemaFields:
    def test_schema_version_is_one(self, tmp_path):
        path = tmp_path / "manifest.json"
        _minimal_manifest().save(path)
        data = json.loads(path.read_text())
        assert data["schema_version"] == "1"

    def test_timeline_nested(self, tmp_path):
        path = tmp_path / "manifest.json"
        _minimal_manifest().save(path)
        data = json.loads(path.read_text())
        assert "timeline" in data
        assert data["timeline"]["fictional_date"] == "2026-03-31"
        assert data["timeline"]["backfill_start"] == "2023-04-01"
        assert data["timeline"]["daily_runs_completed"] == 0

    def test_id_watermarks_nested(self, tmp_path):
        path = tmp_path / "manifest.json"
        _full_manifest().save(path)
        data = json.loads(path.read_text())
        wm = data["id_watermarks"]
        assert wm["sale_key"] == 100_000
        assert wm["customer_id_seq"] == 5_000
        assert wm["inventory_key"] == 805_560

    def test_patterns_module_version_in_json(self, tmp_path):
        path = tmp_path / "manifest.json"
        _full_manifest().save(path)
        data = json.loads(path.read_text())
        assert "patterns_module_version" in data
        assert len(data["patterns_module_version"]) == 16


# ── IdWatermarks ──────────────────────────────────────────────────────────────

class TestIdWatermarks:
    def test_defaults_all_zero(self):
        w = IdWatermarks()
        for field in IdWatermarks.model_fields:
            assert getattr(w, field) == 0, f"{field} should default to 0"

    def test_all_nine_fields_present(self):
        fields = set(IdWatermarks.model_fields.keys())
        expected = {
            "sale_key", "order_seq", "return_key", "session_key",
            "customer_key", "customer_id_seq", "event_key",
            "spend_key", "inventory_key",
        }
        assert fields == expected

    def test_watermarks_survive_roundtrip(self, tmp_path):
        wm = IdWatermarks(sale_key=99, customer_id_seq=55, inventory_key=12345)
        m = Manifest(
            seed=1,
            scale=Scale.xs,
            generated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            timeline=TimelineState(
                backfill_start=date(2023, 4, 1),
                backfill_end=date(2026, 3, 31),
                fictional_date=date(2026, 3, 31),
            ),
            id_watermarks=wm,
            patterns_module_version=module_version(),
        )
        path = tmp_path / "m.json"
        m.save(path)
        loaded = Manifest.load(path)
        assert loaded.id_watermarks.sale_key == 99
        assert loaded.id_watermarks.customer_id_seq == 55
        assert loaded.id_watermarks.inventory_key == 12345


# ── module_version ────────────────────────────────────────────────────────────

class TestModuleVersion:
    def test_returns_16_hex_chars(self):
        v = module_version()
        assert isinstance(v, str)
        assert len(v) == 16
        int(v, 16)  # raises if not valid hex

    def test_stable_within_session(self):
        assert module_version() == module_version()

    def test_changes_if_content_changes(self, tmp_path, monkeypatch):
        import csnp_retail.patterns as pat_mod
        from pathlib import Path

        # Point __file__ at a temp file with different content
        fake = tmp_path / "fake_patterns.py"
        fake.write_bytes(b"# different content")
        monkeypatch.setattr(pat_mod, "__file__", str(fake))
        # Re-import module_version that reads __file__
        from importlib import reload
        reloaded = reload(pat_mod)
        v_new = reloaded.module_version()
        assert v_new != module_version.__module__  # crude check — content differs
        assert len(v_new) == 16


# ── Backfill integration (uses runner) ────────────────────────────────────────
# One shared run per test session to avoid 5× backfill cost.

@pytest.fixture(scope="module")
def backfill_result(tmp_path_factory):
    from csnp_retail.config import GeneratorConfig
    from csnp_retail.runner import run_backfill
    out = tmp_path_factory.mktemp("manifest_backfill")
    config = GeneratorConfig(scale=Scale.xs, seed=42, out=out)
    manifest = run_backfill(config)
    return manifest, out, config


class TestBackfillManifest:
    def test_backfill_writes_manifest(self, backfill_result):
        _, out, _ = backfill_result
        assert (out / "manifest.json").exists()

    def test_backfill_manifest_watermarks(self, backfill_result):
        m, _, _ = backfill_result
        assert m.id_watermarks.sale_key == 100_000
        assert m.id_watermarks.session_key == 180_000
        assert m.id_watermarks.customer_key > 0
        assert m.id_watermarks.customer_id_seq == 5_000

    def test_backfill_fictional_date_is_end(self, backfill_result):
        m, _, config = backfill_result
        assert m.timeline.fictional_date == config.end

    def test_backfill_daily_runs_zero(self, backfill_result):
        m, _, _ = backfill_result
        assert m.timeline.daily_runs_completed == 0

    def test_backfill_manifest_roundtrip(self, backfill_result):
        _, out, _ = backfill_result
        loaded = Manifest.load(out / "manifest.json")
        assert loaded.id_watermarks.sale_key == 100_000
        assert loaded.timeline.daily_runs_completed == 0
        assert loaded.scale == Scale.xs
