"""Unit tests for fact_sales, including planted pattern checks."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest


class TestRowCount:
    def test_exact_row_count(self, fact_sales_df, xs_config):
        assert len(fact_sales_df) == xs_config.n_sales_rows

    def test_sale_key_sequential(self, fact_sales_df):
        assert list(fact_sales_df["sale_key"]) == list(range(1, len(fact_sales_df) + 1))


class TestColumns:
    _required = [
        "sale_key", "date_key", "customer_key", "product_key",
        "store_key", "channel_key", "campaign_key",
        "quantity", "unit_price", "discount_pct",
        "gross_amount", "discount_amount", "net_amount",
        "cost_amount", "gross_margin", "currency_code",
    ]

    def test_required_columns(self, fact_sales_df):
        for col in self._required:
            assert col in fact_sales_df.columns, f"Missing: {col}"

    def test_no_nulls_in_required_non_nullable(self, fact_sales_df):
        for col in ["sale_key", "date_key", "product_key", "channel_key", "quantity", "unit_price"]:
            assert fact_sales_df[col].notna().all(), f"Nulls in {col}"


class TestAmounts:
    def test_gross_amount_equals_price_times_qty(self, fact_sales_df):
        expected = (fact_sales_df["unit_price"] * fact_sales_df["quantity"]).round(2)
        assert (fact_sales_df["gross_amount"].round(2) == expected).all()

    def test_net_amount_leq_gross(self, fact_sales_df):
        assert (fact_sales_df["net_amount"] <= fact_sales_df["gross_amount"] + 0.01).all()

    def test_discount_pct_in_range(self, fact_sales_df):
        assert (fact_sales_df["discount_pct"] >= 0).all()
        assert (fact_sales_df["discount_pct"] <= 1).all()

    def test_unit_price_positive(self, fact_sales_df):
        assert (fact_sales_df["unit_price"] > 0).all()


class TestChannelMix:
    def test_store_dominant_early(self, fact_sales_df):
        fy24 = fact_sales_df[fact_sales_df["date_key"].between(20230401, 20240331)]
        store_share = (fy24["channel_key"] == 1).mean()
        assert store_share > 0.65, f"Store share in FY24 should be >65%, got {store_share:.1%}"

    def test_digital_growing_late(self, fact_sales_df):
        fy26 = fact_sales_df[fact_sales_df["date_key"].between(20250401, 20260331)]
        digital_share = (fy26["channel_key"] != 1).mean()
        assert digital_share > 0.30, f"Digital share in FY26 should be >30%, got {digital_share:.1%}"

    def test_all_channels_present(self, fact_sales_df):
        assert set(fact_sales_df["channel_key"].unique()) == {1, 2, 3, 4}


class TestFKValidity:
    def test_product_key_in_range(self, fact_sales_df, dim_product_df):
        max_pk = dim_product_df["product_key"].max()
        assert (fact_sales_df["product_key"] >= 1).all()
        assert (fact_sales_df["product_key"] <= max_pk).all()

    def test_channel_key_valid(self, fact_sales_df):
        assert fact_sales_df["channel_key"].isin([1, 2, 3, 4]).all()

    def test_store_key_only_for_store_channel(self, fact_sales_df):
        digital = fact_sales_df[fact_sales_df["channel_key"] != 1]
        assert digital["store_key"].isna().all()

    def test_store_channel_has_store_key(self, fact_sales_df):
        store_rows = fact_sales_df[fact_sales_df["channel_key"] == 1]
        assert store_rows["store_key"].notna().all()


class TestPlantedPatterns:
    def test_tx_heat_depresses_outerwear(self, fact_sales_df, dim_product_df, dim_store_df):
        """§6.3: TX stores outerwear -55% during Jun 15 – Aug 16, 2025."""
        tx_store_keys = set(
            dim_store_df[dim_store_df["state_code"] == "TX"]["store_key"].values
        )
        if not tx_store_keys:
            pytest.skip("No TX stores at this scale")

        otw_keys = set(
            dim_product_df[dim_product_df["category"] == "outerwear"]["product_key"].values
        )

        heat_mask = (
            fact_sales_df["date_key"].between(20250615, 20250816)
            & fact_sales_df["store_key"].isin(tx_store_keys)
        )
        baseline_mask = (
            fact_sales_df["date_key"].between(20250401, 20250614)
            & fact_sales_df["store_key"].isin(tx_store_keys)
        )

        heat_n    = len(fact_sales_df[heat_mask])
        base_n    = len(fact_sales_df[baseline_mask])
        if heat_n == 0 or base_n == 0:
            pytest.skip("Not enough TX store sales to verify heat event")

        heat_otw_share    = fact_sales_df[heat_mask]["product_key"].isin(otw_keys).mean()
        baseline_otw_share = fact_sales_df[baseline_mask]["product_key"].isin(otw_keys).mean()

        if baseline_otw_share == 0:
            pytest.skip("No baseline outerwear share to compare against")

        ratio = heat_otw_share / baseline_otw_share
        assert ratio < 0.70, (
            f"TX outerwear share should be <70% of baseline during heat event, got {ratio:.2f}"
        )

    def test_meridian_spike_in_jul_2025(self, fact_sales_df, dim_product_df):
        """§6.11: Meridian Cable Crew 9x normal in Jul 2025."""
        mcc_keys = set(
            dim_product_df[dim_product_df["style_code"].str.startswith("TOP-MCC")]["product_key"].values
        )
        assert mcc_keys, "MCC product keys not found in dim_product"

        jul_mask      = fact_sales_df["date_key"].between(20250701, 20250731)
        baseline_mask = fact_sales_df["date_key"].between(20250401, 20250630)

        jul_mcc_share  = fact_sales_df[jul_mask]["product_key"].isin(mcc_keys).mean()
        base_mcc_share = fact_sales_df[baseline_mask]["product_key"].isin(mcc_keys).mean()

        if base_mcc_share == 0:
            pytest.skip("No MCC baseline share to compare")

        ratio = jul_mcc_share / base_mcc_share
        assert ratio > 3.0, (
            f"MCC share in Jul 2025 should be >3x baseline (target 9x), got {ratio:.1f}x"
        )
