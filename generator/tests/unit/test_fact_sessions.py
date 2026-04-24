"""Unit tests for fact_sessions, including planted conversion-rate patterns."""

from __future__ import annotations

import pytest


class TestRowCount:
    def test_exact_row_count(self, fact_sessions_df, xs_config):
        assert len(fact_sessions_df) == xs_config.n_sessions

    def test_session_key_sequential(self, fact_sessions_df):
        assert list(fact_sessions_df["session_key"]) == list(range(1, len(fact_sessions_df) + 1))


class TestColumns:
    _required = [
        "session_key", "date_key", "customer_key", "channel_key",
        "device_type", "pages_viewed", "time_on_site_secs", "is_converted",
    ]

    def test_required_columns(self, fact_sessions_df):
        for col in self._required:
            assert col in fact_sessions_df.columns, f"Missing: {col}"

    def test_no_nulls_in_non_nullable(self, fact_sessions_df):
        for col in ["session_key", "date_key", "channel_key",
                    "device_type", "pages_viewed", "time_on_site_secs"]:
            assert fact_sessions_df[col].notna().all(), f"Nulls in {col}"

    def test_pages_viewed_positive(self, fact_sessions_df):
        assert (fact_sessions_df["pages_viewed"] >= 1).all()

    def test_time_on_site_positive(self, fact_sessions_df):
        assert (fact_sessions_df["time_on_site_secs"] >= 5).all()


class TestChannelAndDevice:
    def test_only_digital_channels(self, fact_sessions_df):
        assert fact_sessions_df["channel_key"].isin([2, 3]).all()

    def test_both_channels_present(self, fact_sessions_df):
        assert set(fact_sessions_df["channel_key"].unique()) == {2, 3}

    def test_device_types_valid(self, fact_sessions_df):
        valid = {"Mobile", "Desktop", "Tablet"}
        assert set(fact_sessions_df["device_type"].unique()).issubset(valid)

    def test_all_device_types_present(self, fact_sessions_df):
        assert {"Mobile", "Desktop", "Tablet"}.issubset(
            set(fact_sessions_df["device_type"].unique())
        )

    def test_web_has_desktop_sessions(self, fact_sessions_df):
        web_devices = fact_sessions_df[fact_sessions_df["channel_key"] == 2]["device_type"]
        assert (web_devices == "Desktop").any()

    def test_app_dominated_by_mobile(self, fact_sessions_df):
        app = fact_sessions_df[fact_sessions_df["channel_key"] == 3]
        mobile_share = (app["device_type"] == "Mobile").mean()
        assert mobile_share > 0.60, f"App mobile share {mobile_share:.1%} should be >60%"


class TestConversionRate:
    def test_overall_conversion_reasonable(self, fact_sessions_df):
        rate = fact_sessions_df["is_converted"].mean()
        assert 0.10 <= rate <= 0.35, f"Conversion rate {rate:.1%} outside 10-35% band"

    def test_desktop_converts_better_than_mobile(self, fact_sessions_df):
        """§6.7: Mobile conversion rate is lower than desktop."""
        desktop_rate = fact_sessions_df[
            fact_sessions_df["device_type"] == "Desktop"
        ]["is_converted"].mean()
        mobile_rate = fact_sessions_df[
            fact_sessions_df["device_type"] == "Mobile"
        ]["is_converted"].mean()
        assert desktop_rate > mobile_rate, (
            f"Desktop conv ({desktop_rate:.1%}) should exceed mobile ({mobile_rate:.1%})"
        )

    def test_promo_session_volume_higher(self, fact_sessions_df, dim_date_df):
        """§6.8: Session volume spikes during promo windows."""
        promo_dates = set(
            dim_date_df[dim_date_df["is_promo_window"]]["date_key"].values
        )
        non_promo_dates = set(
            dim_date_df[~dim_date_df["is_promo_window"]]["date_key"].values
        )
        n_promo_days = len(promo_dates)
        n_non_promo_days = len(non_promo_dates)
        if n_promo_days == 0 or n_non_promo_days == 0:
            pytest.skip("No promo / non-promo days")

        promo_daily = (
            fact_sessions_df["date_key"].isin(promo_dates).sum() / n_promo_days
        )
        non_promo_daily = (
            fact_sessions_df["date_key"].isin(non_promo_dates).sum() / n_non_promo_days
        )
        assert promo_daily > non_promo_daily * 1.2, (
            f"Promo daily sessions ({promo_daily:.0f}) should be >1.2x non-promo ({non_promo_daily:.0f})"
        )

    def test_app_shorter_time_than_web(self, fact_sessions_df):
        """§6.9: App sessions are shorter than web sessions."""
        app_time = fact_sessions_df[fact_sessions_df["channel_key"] == 3]["time_on_site_secs"].mean()
        web_time = fact_sessions_df[fact_sessions_df["channel_key"] == 2]["time_on_site_secs"].mean()
        assert app_time < web_time, (
            f"App mean time ({app_time:.0f}s) should be < web ({web_time:.0f}s)"
        )
