"""Unit tests for dim_store."""

from __future__ import annotations

import pandas as pd
import pytest


class TestStoreCount:
    def test_xs_store_count(self, dim_store_df, xs_config):
        assert len(dim_store_df) == xs_config.n_stores  # 15

    def test_store_key_unique(self, dim_store_df):
        assert dim_store_df["store_key"].is_unique

    def test_store_id_unique(self, dim_store_df):
        assert dim_store_df["store_id"].is_unique


class TestStoreId:
    def test_store_id_format(self, dim_store_df):
        assert dim_store_df["store_id"].str.match(r"^CSNP-\d{4}$").all()

    def test_store_key_positive(self, dim_store_df):
        assert (dim_store_df["store_key"] > 0).all()


class TestCountryDistribution:
    def test_us_stores_present(self, dim_store_df):
        assert (dim_store_df["country_code"] == "US").sum() >= 1

    def test_at_least_one_non_us(self, dim_store_df):
        assert (dim_store_df["country_code"] != "US").sum() >= 1

    def test_country_codes_valid(self, dim_store_df):
        valid = {"US", "CA", "GB", "MX"}
        assert set(dim_store_df["country_code"].unique()).issubset(valid)


class TestUKExpansion:
    def test_uk_stores_open_after_oct_2024(self, dim_store_df):
        uk = dim_store_df[dim_store_df["country_code"] == "GB"]
        if len(uk) > 0:
            assert (uk["open_date"] >= pd.Timestamp("2024-10-01")).all(), (
                "UK stores must open no earlier than Oct 2024 (FY25 Q3)"
            )

    def test_uk_stores_open_before_apr_2025(self, dim_store_df):
        uk = dim_store_df[dim_store_df["country_code"] == "GB"]
        if len(uk) > 0:
            assert (uk["open_date"] <= pd.Timestamp("2025-03-31")).all(), (
                "UK stores must open by end of FY26 Q4 (Mar 2025)"
            )


class TestFormatType:
    _valid_formats = {"Flagship", "Standard", "Outlet", "Pop-up"}

    def test_valid_format_types(self, dim_store_df):
        assert set(dim_store_df["format_type"].unique()).issubset(self._valid_formats)

    def test_at_least_one_standard(self, dim_store_df):
        assert (dim_store_df["format_type"] == "Standard").sum() >= 1

    def test_square_footage_positive(self, dim_store_df):
        assert (dim_store_df["square_footage"] > 0).all()

    def test_flagship_square_footage(self, dim_store_df):
        flagship = dim_store_df[dim_store_df["format_type"] == "Flagship"]
        if len(flagship) > 0:
            assert (flagship["square_footage"] >= 10_000).all()

    def test_popup_has_close_date(self, dim_store_df):
        popup = dim_store_df[dim_store_df["format_type"] == "Pop-up"]
        if len(popup) > 0:
            assert popup["close_date"].notna().all()


class TestDates:
    def test_non_popup_open_date_before_history(self, dim_store_df):
        from csnp_retail.config import BACKFILL_START
        non_popup = dim_store_df[
            (dim_store_df["format_type"] != "Pop-up") &
            (dim_store_df["country_code"] != "GB")
        ]
        assert (non_popup["open_date"] <= pd.Timestamp(BACKFILL_START)).all()

    def test_close_date_after_open_date(self, dim_store_df):
        closed = dim_store_df[dim_store_df["close_date"].notna()]
        if len(closed) > 0:
            assert (closed["close_date"] > closed["open_date"]).all()


class TestGeoCoordinates:
    def test_latitude_range(self, dim_store_df):
        assert dim_store_df["latitude"].between(-90, 90).all()

    def test_longitude_range(self, dim_store_df):
        assert dim_store_df["longitude"].between(-180, 180).all()


class TestColumns:
    _required = [
        "store_key", "store_id", "store_name", "format_type", "square_footage",
        "open_date", "city", "country_code", "latitude", "longitude",
        "district", "climate_zone", "region_manager",
    ]

    def test_required_columns_present(self, dim_store_df):
        for col in self._required:
            assert col in dim_store_df.columns, f"Missing column: {col}"

    def test_climate_zone_valid(self, dim_store_df):
        valid = {"Cold", "Temperate", "Warm", "Hot"}
        assert set(dim_store_df["climate_zone"].unique()).issubset(valid)

    def test_store_name_contains_csnp(self, dim_store_df):
        assert dim_store_df["store_name"].str.contains("CSNP").all()

    def test_no_nulls_in_required(self, dim_store_df):
        for col in ["store_key", "store_id", "format_type", "open_date", "country_code"]:
            assert dim_store_df[col].notna().all(), f"Null in {col}"
