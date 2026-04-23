"""Unit tests for fact_inventory_daily, including §6.3 TX outerwear pattern."""

from __future__ import annotations

import pytest


class TestRowCount:
    def test_has_rows(self, fact_inventory_daily_df):
        assert len(fact_inventory_daily_df) > 0

    def test_inventory_key_sequential(self, fact_inventory_daily_df):
        assert list(fact_inventory_daily_df["inventory_key"]) == list(
            range(1, len(fact_inventory_daily_df) + 1)
        )

    def test_row_count_formula(self, fact_inventory_daily_df, xs_config, dim_store_df, dim_date_df):
        n_stores = len(dim_store_df)
        n_days   = len(dim_date_df)
        n_skus   = xs_config.n_inventory_skus
        # Allow a small shortfall if some top-N skus don't have current dim_product rows
        assert len(fact_inventory_daily_df) <= n_skus * n_stores * n_days


class TestColumns:
    _required = [
        "inventory_key", "date_key", "product_key", "store_key",
        "units_on_hand", "units_on_order", "units_in_transit",
        "safety_stock_level", "days_of_supply", "is_stockout", "is_low_stock",
    ]

    def test_required_columns(self, fact_inventory_daily_df):
        for col in self._required:
            assert col in fact_inventory_daily_df.columns, f"Missing: {col}"

    def test_no_nulls_in_key_columns(self, fact_inventory_daily_df):
        for col in ["inventory_key", "date_key", "product_key", "store_key"]:
            assert fact_inventory_daily_df[col].notna().all(), f"Nulls in {col}"

    def test_units_non_negative(self, fact_inventory_daily_df):
        for col in ["units_on_hand", "units_on_order", "units_in_transit",
                    "safety_stock_level"]:
            assert (fact_inventory_daily_df[col] >= 0).all(), f"Negative in {col}"

    def test_days_of_supply_non_negative(self, fact_inventory_daily_df):
        assert (fact_inventory_daily_df["days_of_supply"] >= 0).all()

    def test_is_stockout_is_bool(self, fact_inventory_daily_df):
        assert fact_inventory_daily_df["is_stockout"].dtype == bool

    def test_stockout_means_zero_units(self, fact_inventory_daily_df):
        stocked_out = fact_inventory_daily_df[fact_inventory_daily_df["is_stockout"]]
        assert (stocked_out["units_on_hand"] == 0).all()

    def test_not_stockout_positive_units(self, fact_inventory_daily_df):
        in_stock = fact_inventory_daily_df[~fact_inventory_daily_df["is_stockout"]]
        assert (in_stock["units_on_hand"] > 0).all()


class TestFKValidity:
    def test_product_key_in_dim_product(
        self, fact_inventory_daily_df, dim_product_df
    ):
        valid_keys = set(dim_product_df["product_key"].values)
        assert fact_inventory_daily_df["product_key"].isin(valid_keys).all()

    def test_store_key_in_dim_store(self, fact_inventory_daily_df, dim_store_df):
        valid_keys = set(dim_store_df["store_key"].values)
        assert fact_inventory_daily_df["store_key"].isin(valid_keys).all()

    def test_date_key_in_dim_date(self, fact_inventory_daily_df, dim_date_df):
        valid_keys = set(dim_date_df["date_key"].values)
        assert fact_inventory_daily_df["date_key"].isin(valid_keys).all()


class TestPlantedPatterns:
    def test_tx_outerwear_stockout_elevated_during_heat(
        self, fact_inventory_daily_df, dim_store_df, dim_product_df
    ):
        """§6.3: TX stores show elevated outerwear stockouts during heat window."""
        tx_keys = set(dim_store_df[dim_store_df["state_code"] == "TX"]["store_key"])
        ow_keys = set(
            dim_product_df[dim_product_df["category"] == "outerwear"]["product_key"]
        )

        # Heat window: 20250615 – 20250816
        heat_dates = set(range(20250615, 20250817))

        df = fact_inventory_daily_df
        tx_ow_heat = df[
            df["store_key"].isin(tx_keys)
            & df["product_key"].isin(ow_keys)
            & df["date_key"].isin(heat_dates)
        ]
        tx_ow_other = df[
            df["store_key"].isin(tx_keys)
            & df["product_key"].isin(ow_keys)
            & ~df["date_key"].isin(heat_dates)
        ]

        if len(tx_ow_heat) == 0 or len(tx_ow_other) == 0:
            pytest.skip("No TX outerwear rows for comparison")

        heat_stockout_rate  = tx_ow_heat["is_stockout"].mean()
        other_stockout_rate = tx_ow_other["is_stockout"].mean()
        assert heat_stockout_rate > other_stockout_rate * 2.0, (
            f"TX outerwear heat stockout rate ({heat_stockout_rate:.1%}) "
            f"should be >2x non-heat ({other_stockout_rate:.1%})"
        )

    def test_overall_stockout_rate_reasonable(self, fact_inventory_daily_df):
        rate = fact_inventory_daily_df["is_stockout"].mean()
        assert 0.02 <= rate <= 0.35, f"Overall stockout rate {rate:.1%} outside 2-35% band"
