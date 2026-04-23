"""Unit tests for dim_geography."""

from __future__ import annotations

import pytest


class TestRowCount:
    def test_minimum_rows(self, dim_geo_df):
        assert len(dim_geo_df) >= 150

    def test_unique_geo_key(self, dim_geo_df):
        assert dim_geo_df["geo_key"].is_unique

    def test_geo_key_starts_at_1(self, dim_geo_df):
        assert dim_geo_df["geo_key"].min() == 1

    def test_geo_key_is_sequential(self, dim_geo_df):
        expected = list(range(1, len(dim_geo_df) + 1))
        assert list(dim_geo_df["geo_key"]) == expected


class TestCountryDistribution:
    def test_all_four_countries_present(self, dim_geo_df):
        codes = set(dim_geo_df["country_code"].unique())
        assert {"US", "CA", "GB", "MX"}.issubset(codes)

    def test_us_is_largest_country(self, dim_geo_df):
        counts = dim_geo_df.groupby("country_code").size()
        assert counts["US"] > counts["CA"]
        assert counts["US"] > counts["GB"]
        assert counts["US"] > counts["MX"]

    def test_us_count(self, dim_geo_df):
        us_count = (dim_geo_df["country_code"] == "US").sum()
        assert us_count >= 100

    def test_ca_count(self, dim_geo_df):
        assert (dim_geo_df["country_code"] == "CA").sum() >= 15

    def test_gb_count(self, dim_geo_df):
        assert (dim_geo_df["country_code"] == "GB").sum() >= 8

    def test_mx_count(self, dim_geo_df):
        assert (dim_geo_df["country_code"] == "MX").sum() >= 5


class TestLatLon:
    def test_latitude_range(self, dim_geo_df):
        assert dim_geo_df["latitude"].between(-90, 90).all()

    def test_longitude_range(self, dim_geo_df):
        assert dim_geo_df["longitude"].between(-180, 180).all()

    def test_us_latitudes_north_of_24(self, dim_geo_df):
        us = dim_geo_df[dim_geo_df["country_code"] == "US"]
        assert (us["latitude"] >= 18).all()   # Hawaii southernmost point ~18°

    def test_uk_latitudes(self, dim_geo_df):
        uk = dim_geo_df[dim_geo_df["country_code"] == "GB"]
        assert (uk["latitude"] > 50).all()


class TestCurrencies:
    def test_us_uses_usd(self, dim_geo_df):
        us = dim_geo_df[dim_geo_df["country_code"] == "US"]
        assert (us["currency_code"] == "USD").all()

    def test_ca_uses_cad(self, dim_geo_df):
        ca = dim_geo_df[dim_geo_df["country_code"] == "CA"]
        assert (ca["currency_code"] == "CAD").all()

    def test_gb_uses_gbp(self, dim_geo_df):
        gb = dim_geo_df[dim_geo_df["country_code"] == "GB"]
        assert (gb["currency_code"] == "GBP").all()

    def test_mx_uses_mxn(self, dim_geo_df):
        mx = dim_geo_df[dim_geo_df["country_code"] == "MX"]
        assert (mx["currency_code"] == "MXN").all()


class TestColumns:
    _required = [
        "geo_key", "postal_code", "city", "metro", "state_province",
        "state_code", "country", "country_code", "currency_code",
        "latitude", "longitude", "timezone", "tax_rate",
    ]

    def test_required_columns_present(self, dim_geo_df):
        for col in self._required:
            assert col in dim_geo_df.columns, f"Missing column: {col}"

    def test_no_nulls_in_key_columns(self, dim_geo_df):
        for col in ["geo_key", "postal_code", "city", "country_code", "latitude", "longitude"]:
            assert dim_geo_df[col].notna().all(), f"Nulls found in {col}"

    def test_tax_rate_non_negative(self, dim_geo_df):
        assert (dim_geo_df["tax_rate"] >= 0).all()

    def test_us_tax_rates_reasonable(self, dim_geo_df):
        us = dim_geo_df[dim_geo_df["country_code"] == "US"]
        assert (us["tax_rate"] <= 0.15).all()
        # OR/AK/MT/NH have 0% sales tax
        zero_tax = us[us["tax_rate"] == 0.0]
        if len(zero_tax) > 0:
            assert zero_tax["state_code"].isin(["OR", "AK", "MT", "NH"]).all()
