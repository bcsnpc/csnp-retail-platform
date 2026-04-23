"""Unit tests for dim_campaign."""

from __future__ import annotations

import pandas as pd
import pytest

from csnp_retail.config import BACKFILL_END, BACKFILL_START


class TestCampaignCount:
    def test_minimum_campaigns(self, dim_campaign_df):
        assert len(dim_campaign_df) >= 100, "Expected at least 100 campaigns over 3 fiscal years"

    def test_maximum_campaigns(self, dim_campaign_df):
        assert len(dim_campaign_df) <= 300, "Sanity cap: not more than 300 campaigns"

    def test_campaign_key_unique(self, dim_campaign_df):
        assert dim_campaign_df["campaign_key"].is_unique

    def test_campaign_id_unique(self, dim_campaign_df):
        assert dim_campaign_df["campaign_id"].is_unique


class TestCampaignTypes:
    _valid_types = {"Promo", "Paid Search", "Paid Social", "Email", "Affiliate", "Influencer"}

    def test_campaign_types_valid(self, dim_campaign_df):
        actual = set(dim_campaign_df["campaign_type"].unique())
        assert actual.issubset(self._valid_types), f"Unknown types: {actual - self._valid_types}"

    def test_all_types_present(self, dim_campaign_df):
        actual = set(dim_campaign_df["campaign_type"].unique())
        # At minimum these four must be present
        assert {"Promo", "Paid Search", "Email", "Affiliate"}.issubset(actual)

    def test_promo_campaigns_have_discount(self, dim_campaign_df):
        promos = dim_campaign_df[dim_campaign_df["campaign_type"] == "Promo"]
        assert (promos["discount_pct"] > 0).all()

    def test_paid_search_has_zero_discount(self, dim_campaign_df):
        ps = dim_campaign_df[dim_campaign_df["campaign_type"] == "Paid Search"]
        assert (ps["discount_pct"] == 0.0).all()


class TestDateRanges:
    def test_start_dates_within_backfill(self, dim_campaign_df):
        assert (dim_campaign_df["start_date"] >= pd.Timestamp(BACKFILL_START)).all()

    def test_end_dates_within_backfill(self, dim_campaign_df):
        assert (dim_campaign_df["end_date"] <= pd.Timestamp(BACKFILL_END)).all()

    def test_end_date_gte_start_date(self, dim_campaign_df):
        assert (dim_campaign_df["end_date"] >= dim_campaign_df["start_date"]).all()


class TestBFCMCoverage:
    def test_black_friday_in_each_year(self, dim_campaign_df):
        """One Black Friday campaign for each of the 3 years in range."""
        bfcm = dim_campaign_df[
            dim_campaign_df["campaign_name"].str.contains("Black Friday", na=False)
        ]
        assert len(bfcm) >= 3, f"Expected ≥3 Black Friday campaigns, got {len(bfcm)}"

    def test_new_years_sale_present(self, dim_campaign_df):
        nys = dim_campaign_df[
            dim_campaign_df["campaign_name"].str.contains("New Year", na=False)
        ]
        assert len(nys) >= 2

    def test_back_to_school_present(self, dim_campaign_df):
        bts = dim_campaign_df[
            dim_campaign_df["campaign_name"].str.contains("Back-to-School", na=False)
        ]
        assert len(bts) >= 3


class TestSpend:
    def test_planned_spend_positive(self, dim_campaign_df):
        assert (dim_campaign_df["planned_spend"] > 0).all()

    def test_actual_spend_positive(self, dim_campaign_df):
        assert (dim_campaign_df["actual_spend"] > 0).all()

    def test_actual_spend_within_20pct_of_planned(self, dim_campaign_df):
        ratio = dim_campaign_df["actual_spend"] / dim_campaign_df["planned_spend"]
        assert (ratio >= 0.80).all()
        assert (ratio <= 1.20).all()

    def test_target_revenue_exceeds_spend(self, dim_campaign_df):
        assert (dim_campaign_df["target_revenue"] > dim_campaign_df["planned_spend"]).all()


class TestColumns:
    _required = [
        "campaign_key", "campaign_id", "campaign_name", "campaign_type",
        "start_date", "end_date", "target_segment", "discount_pct",
        "planned_spend", "actual_spend", "target_revenue",
    ]

    def test_required_columns_present(self, dim_campaign_df):
        for col in self._required:
            assert col in dim_campaign_df.columns, f"Missing: {col}"

    def test_no_nulls_in_required(self, dim_campaign_df):
        for col in self._required:
            assert dim_campaign_df[col].notna().all(), f"Nulls in {col}"

    def test_discount_pct_range(self, dim_campaign_df):
        assert (dim_campaign_df["discount_pct"] >= 0).all()
        assert (dim_campaign_df["discount_pct"] <= 50).all()
