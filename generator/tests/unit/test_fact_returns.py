"""Unit tests for fact_returns, including planted return-rate patterns."""

from __future__ import annotations

import pytest


class TestRowCount:
    def test_has_rows(self, fact_returns_df):
        assert len(fact_returns_df) > 0

    def test_return_key_sequential(self, fact_returns_df):
        assert list(fact_returns_df["return_key"]) == list(range(1, len(fact_returns_df) + 1))

    def test_overall_return_rate_reasonable(self, fact_returns_df, fact_sales_df):
        rate = len(fact_returns_df) / len(fact_sales_df)
        assert 0.05 <= rate <= 0.30, f"Overall return rate {rate:.1%} outside 5-30% band"


class TestColumns:
    _required = [
        "return_key", "sale_key", "date_key", "customer_key", "product_key",
        "store_key", "channel_key", "return_reason_key",
        "quantity_returned", "return_amount", "days_to_return",
    ]

    def test_required_columns(self, fact_returns_df):
        for col in self._required:
            assert col in fact_returns_df.columns, f"Missing: {col}"

    def test_no_nulls_in_key_columns(self, fact_returns_df):
        for col in ["return_key", "sale_key", "date_key", "product_key",
                    "channel_key", "return_reason_key"]:
            assert fact_returns_df[col].notna().all(), f"Nulls in {col}"

    def test_return_amount_positive(self, fact_returns_df):
        assert (fact_returns_df["return_amount"] > 0).all()

    def test_days_to_return_positive(self, fact_returns_df):
        assert (fact_returns_df["days_to_return"] >= 1).all()

    def test_days_to_return_max_30(self, fact_returns_df):
        assert (fact_returns_df["days_to_return"] <= 30).all()

    def test_reason_key_valid(self, fact_returns_df):
        assert fact_returns_df["return_reason_key"].between(1, 12).all()


class TestFKValidity:
    def test_sale_key_in_fact_sales(self, fact_returns_df, fact_sales_df):
        valid_keys = set(fact_sales_df["sale_key"].values)
        assert fact_returns_df["sale_key"].isin(valid_keys).all()

    def test_product_key_in_dim_product(self, fact_returns_df, dim_product_df):
        valid_keys = set(dim_product_df["product_key"].values)
        assert fact_returns_df["product_key"].isin(valid_keys).all()

    def test_channel_key_valid(self, fact_returns_df):
        assert fact_returns_df["channel_key"].isin([1, 2, 3, 4]).all()


class TestPlantedPatterns:
    def test_online_return_rate_higher_than_store(self, fact_returns_df, fact_sales_df):
        """§6.4: Online channels have materially higher return rates than store."""
        store_sales  = len(fact_sales_df[fact_sales_df["channel_key"] == 1])
        online_sales = len(fact_sales_df[fact_sales_df["channel_key"] != 1])

        store_returns  = len(fact_returns_df[fact_returns_df["channel_key"] == 1])
        online_returns = len(fact_returns_df[fact_returns_df["channel_key"] != 1])

        if store_sales == 0 or online_sales == 0:
            pytest.skip("Insufficient sales data")

        store_rate  = store_returns  / store_sales
        online_rate = online_returns / online_sales

        assert online_rate > store_rate * 1.5, (
            f"Online return rate ({online_rate:.1%}) should be >1.5x store ({store_rate:.1%})"
        )

    def test_january_return_spike(self, fact_returns_df):
        """§6.6: January has elevated returns (holiday gifts returned)."""
        jan_returns = (fact_returns_df["date_key"] // 100 % 100 == 1).sum()
        jun_returns = (fact_returns_df["date_key"] // 100 % 100 == 6).sum()
        if jun_returns == 0:
            pytest.skip("No June returns to compare against")
        ratio = jan_returns / jun_returns
        assert ratio > 1.2, f"January returns should be >1.2x June; got {ratio:.2f}x"
