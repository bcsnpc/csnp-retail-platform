"""Unit tests for derive_seed() in patterns.py."""

from __future__ import annotations

from datetime import date

from csnp_retail.patterns import derive_seed


class TestDeriveSeed:
    def test_deterministic(self):
        d = date(2026, 4, 21)
        assert derive_seed(42, "daily", d) == derive_seed(42, "daily", d)

    def test_different_dates_differ(self):
        assert derive_seed(42, "daily", date(2026, 4, 21)) != derive_seed(
            42, "daily", date(2026, 4, 22)
        )

    def test_different_modes_differ(self):
        d = date(2026, 4, 21)
        assert derive_seed(42, "backfill", d) != derive_seed(42, "daily", d)

    def test_different_base_seeds_differ(self):
        d = date(2026, 4, 21)
        assert derive_seed(42, "daily", d) != derive_seed(43, "daily", d)

    def test_result_is_non_negative_int(self):
        seed = derive_seed(42, "daily", date(2026, 4, 21))
        assert isinstance(seed, int)
        assert seed >= 0

    def test_fits_in_uint64(self):
        seed = derive_seed(42, "daily", date(2026, 4, 21))
        assert seed < 2**64

    def test_no_arithmetic_collision(self):
        # Arithmetic seed = base * K + date.toordinal() can collide when
        # base1 * K + d1 == base2 * K + d2.  Blake2b avoids this class.
        d1 = date(2026, 4, 21)
        d2 = date(2026, 4, 11)
        assert derive_seed(1, "daily", d1) != derive_seed(2, "daily", d2)

    def test_all_months_unique(self):
        seeds = [derive_seed(42, "daily", date(2026, m, 1)) for m in range(1, 13)]
        assert len(set(seeds)) == 12

    def test_backfill_stream_distinct_from_daily(self):
        # Every date in a year: backfill and daily seeds must not share any value
        backfill = {derive_seed(42, "backfill", date(2026, 1, d)) for d in range(1, 32)}
        daily    = {derive_seed(42, "daily",    date(2026, 1, d)) for d in range(1, 32)}
        assert backfill.isdisjoint(daily)

    def test_usable_as_numpy_seed(self):
        import numpy as np
        seed = derive_seed(42, "daily", date(2026, 4, 21))
        rng = np.random.default_rng(seed)
        vals = rng.integers(0, 100, size=5)
        assert len(vals) == 5
