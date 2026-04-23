"""Unit tests for fact_loyalty_events."""

from __future__ import annotations

import pytest


class TestRowCount:
    def test_has_rows(self, fact_loyalty_events_df):
        assert len(fact_loyalty_events_df) > 0

    def test_event_key_sequential(self, fact_loyalty_events_df):
        assert list(fact_loyalty_events_df["event_key"]) == list(
            range(1, len(fact_loyalty_events_df) + 1)
        )


class TestColumns:
    _required = [
        "event_key", "date_key", "customer_key", "event_type",
        "points_delta", "balance_after", "sale_key",
    ]

    def test_required_columns(self, fact_loyalty_events_df):
        for col in self._required:
            assert col in fact_loyalty_events_df.columns, f"Missing: {col}"

    def test_no_nulls_in_non_nullable(self, fact_loyalty_events_df):
        for col in ["event_key", "date_key", "event_type", "points_delta", "balance_after"]:
            assert fact_loyalty_events_df[col].notna().all(), f"Nulls in {col}"

    def test_balance_after_non_negative(self, fact_loyalty_events_df):
        assert (fact_loyalty_events_df["balance_after"] >= 0).all()

    def test_points_earned_positive(self, fact_loyalty_events_df):
        earned = fact_loyalty_events_df[
            fact_loyalty_events_df["event_type"] == "points_earned"
        ]
        assert (earned["points_delta"] > 0).all()

    def test_redeemed_negative(self, fact_loyalty_events_df):
        redeemed = fact_loyalty_events_df[
            fact_loyalty_events_df["event_type"] == "points_redeemed"
        ]
        if len(redeemed) == 0:
            pytest.skip("No redemption events")
        assert (redeemed["points_delta"] < 0).all()

    def test_tier_upgrade_zero_delta(self, fact_loyalty_events_df):
        upgrades = fact_loyalty_events_df[
            fact_loyalty_events_df["event_type"] == "tier_upgrade"
        ]
        if len(upgrades) == 0:
            pytest.skip("No tier-upgrade events")
        assert (upgrades["points_delta"] == 0).all()


class TestEventTypes:
    def test_valid_event_types(self, fact_loyalty_events_df):
        valid = {"enrollment", "points_earned", "points_redeemed", "tier_upgrade"}
        assert set(fact_loyalty_events_df["event_type"].unique()).issubset(valid)

    def test_enrollment_events_present(self, fact_loyalty_events_df):
        assert (fact_loyalty_events_df["event_type"] == "enrollment").any()

    def test_points_earned_events_present(self, fact_loyalty_events_df):
        assert (fact_loyalty_events_df["event_type"] == "points_earned").any()

    def test_sale_key_set_for_earned(self, fact_loyalty_events_df):
        earned = fact_loyalty_events_df[
            fact_loyalty_events_df["event_type"] == "points_earned"
        ]
        assert earned["sale_key"].notna().all()

    def test_sale_key_null_for_enrollment(self, fact_loyalty_events_df):
        enroll = fact_loyalty_events_df[
            fact_loyalty_events_df["event_type"] == "enrollment"
        ]
        assert enroll["sale_key"].isna().all()


class TestFKValidity:
    def test_customer_key_in_dim_customer(
        self, fact_loyalty_events_df, dim_customer_df
    ):
        valid_keys = set(dim_customer_df["customer_key"].values)
        non_null = fact_loyalty_events_df["customer_key"].dropna()
        assert non_null.isin(valid_keys).all()

    def test_date_key_in_dim_date(self, fact_loyalty_events_df, dim_date_df):
        valid_keys = set(dim_date_df["date_key"].values)
        assert fact_loyalty_events_df["date_key"].isin(valid_keys).all()
