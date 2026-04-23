"""Unit tests for fact_marketing_spend, including §6.7 email ROI pattern."""

from __future__ import annotations

import pytest


class TestRowCount:
    def test_has_rows(self, fact_marketing_spend_df):
        assert len(fact_marketing_spend_df) > 0

    def test_spend_key_sequential(self, fact_marketing_spend_df):
        assert list(fact_marketing_spend_df["spend_key"]) == list(
            range(1, len(fact_marketing_spend_df) + 1)
        )


class TestColumns:
    _required = [
        "spend_key", "campaign_key", "channel_key", "date_key",
        "planned_spend", "actual_spend", "impressions", "clicks",
        "revenue_attributed",
    ]

    def test_required_columns(self, fact_marketing_spend_df):
        for col in self._required:
            assert col in fact_marketing_spend_df.columns, f"Missing: {col}"

    def test_no_nulls_in_key_columns(self, fact_marketing_spend_df):
        for col in ["spend_key", "campaign_key", "channel_key", "date_key"]:
            assert fact_marketing_spend_df[col].notna().all(), f"Nulls in {col}"

    def test_actual_spend_positive(self, fact_marketing_spend_df):
        assert (fact_marketing_spend_df["actual_spend"] > 0).all()

    def test_revenue_attributed_positive(self, fact_marketing_spend_df):
        assert (fact_marketing_spend_df["revenue_attributed"] > 0).all()

    def test_impressions_non_negative(self, fact_marketing_spend_df):
        assert (fact_marketing_spend_df["impressions"] >= 0).all()

    def test_clicks_non_negative(self, fact_marketing_spend_df):
        assert (fact_marketing_spend_df["clicks"] >= 0).all()

    def test_clicks_lte_impressions(self, fact_marketing_spend_df):
        df = fact_marketing_spend_df
        paid_rows = df[df["impressions"] > 0]
        if len(paid_rows) == 0:
            pytest.skip("No paid-media rows")
        assert (paid_rows["clicks"] <= paid_rows["impressions"]).all()


class TestFKValidity:
    def test_campaign_key_in_dim_campaign(self, fact_marketing_spend_df, dim_campaign_df):
        valid_keys = set(dim_campaign_df["campaign_key"].values)
        assert fact_marketing_spend_df["campaign_key"].isin(valid_keys).all()

    def test_channel_key_valid(self, fact_marketing_spend_df):
        assert fact_marketing_spend_df["channel_key"].isin([1, 2, 3, 4]).all()

    def test_date_key_in_dim_date(self, fact_marketing_spend_df, dim_date_df):
        valid_keys = set(dim_date_df["date_key"].values)
        assert fact_marketing_spend_df["date_key"].isin(valid_keys).all()


class TestPlantedPatterns:
    def test_email_roi_higher_than_paid_social(
        self, fact_marketing_spend_df, dim_campaign_df
    ):
        """§6.7: Email ROI materially higher than Paid Social."""
        spend = fact_marketing_spend_df.merge(
            dim_campaign_df[["campaign_key", "campaign_type"]], on="campaign_key"
        )
        email  = spend[spend["campaign_type"] == "Email"]
        social = spend[spend["campaign_type"] == "Paid Social"]
        if len(email) == 0 or len(social) == 0:
            pytest.skip("No email or paid-social campaigns")
        if social["actual_spend"].sum() == 0:
            pytest.skip("Zero paid-social spend")

        email_roi  = email["revenue_attributed"].sum() / email["actual_spend"].sum()
        social_roi = social["revenue_attributed"].sum() / social["actual_spend"].sum()
        assert email_roi > social_roi * 2.5, (
            f"Email ROI ({email_roi:.1f}x) should be >2.5x paid-social ({social_roi:.1f}x)"
        )

    def test_paid_search_higher_roi_than_social(
        self, fact_marketing_spend_df, dim_campaign_df
    ):
        spend = fact_marketing_spend_df.merge(
            dim_campaign_df[["campaign_key", "campaign_type"]], on="campaign_key"
        )
        search = spend[spend["campaign_type"] == "Paid Search"]
        social = spend[spend["campaign_type"] == "Paid Social"]
        if len(search) == 0 or len(social) == 0:
            pytest.skip("Insufficient campaign data")
        if social["actual_spend"].sum() == 0 or search["actual_spend"].sum() == 0:
            pytest.skip("Zero spend")

        search_roi = search["revenue_attributed"].sum() / search["actual_spend"].sum()
        social_roi = social["revenue_attributed"].sum() / social["actual_spend"].sum()
        assert search_roi > social_roi, (
            f"Paid search ROI ({search_roi:.1f}x) should exceed social ({social_roi:.1f}x)"
        )
