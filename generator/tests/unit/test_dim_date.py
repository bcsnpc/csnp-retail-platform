"""Unit tests for dim_date."""

from __future__ import annotations

import datetime

import pandas as pd
import pytest

from csnp_retail.config import BACKFILL_END, BACKFILL_START
from csnp_retail.entities.dim_date import (
    _fiscal_month,
    _fiscal_quarter,
    _fiscal_year,
    _season,
)

_EXPECTED_DAYS = (BACKFILL_END - BACKFILL_START).days + 1  # 1096


class TestRowCount:
    def test_total_rows(self, dim_date_df):
        assert len(dim_date_df) == _EXPECTED_DAYS

    def test_no_duplicate_date_keys(self, dim_date_df):
        assert dim_date_df["date_key"].is_unique

    def test_date_key_format(self, dim_date_df):
        assert dim_date_df["date_key"].iloc[0] == 20230401
        assert dim_date_df["date_key"].iloc[-1] == 20260331


class TestFiscalCalendar:
    """April 2023 = FY2024 Q1, March 2024 = FY2024 Q4, April 2024 = FY2025 Q1."""

    def test_april_2023_is_fy2024_q1(self):
        assert _fiscal_year(datetime.date(2023, 4, 1)) == 2024
        assert _fiscal_month(datetime.date(2023, 4, 1)) == 1
        assert _fiscal_quarter(1) == 1

    def test_june_2023_is_fy2024_q1(self):
        assert _fiscal_year(datetime.date(2023, 6, 30)) == 2024
        assert _fiscal_month(datetime.date(2023, 6, 30)) == 3
        assert _fiscal_quarter(3) == 1

    def test_july_2023_is_fy2024_q2(self):
        assert _fiscal_year(datetime.date(2023, 7, 1)) == 2024
        assert _fiscal_month(datetime.date(2023, 7, 1)) == 4
        assert _fiscal_quarter(4) == 2

    def test_march_2024_is_fy2024_q4(self):
        assert _fiscal_year(datetime.date(2024, 3, 31)) == 2024
        assert _fiscal_month(datetime.date(2024, 3, 31)) == 12
        assert _fiscal_quarter(12) == 4

    def test_april_2024_is_fy2025_q1(self):
        assert _fiscal_year(datetime.date(2024, 4, 1)) == 2025
        assert _fiscal_month(datetime.date(2024, 4, 1)) == 1

    def test_fiscal_year_column(self, dim_date_df):
        row = dim_date_df[dim_date_df["date_key"] == 20230401].iloc[0]
        assert row["fiscal_year"] == 2024
        assert row["fiscal_quarter"] == 1
        assert row["fiscal_month"] == 1

    def test_fiscal_year_end_march_2024(self, dim_date_df):
        row = dim_date_df[dim_date_df["date_key"] == 20240331].iloc[0]
        assert row["fiscal_year"] == 2024
        assert row["is_fiscal_year_end"]
        assert row["fiscal_quarter"] == 4

    def test_fiscal_week_positive(self, dim_date_df):
        assert (dim_date_df["fiscal_week"] > 0).all()
        assert (dim_date_df["fiscal_week"] <= 54).all()

    def test_day_of_fiscal_year_range(self, dim_date_df):
        assert (dim_date_df["day_of_fiscal_year"] > 0).all()
        assert (dim_date_df["day_of_fiscal_year"] <= 366).all()


class TestHolidaysAndPromos:
    def test_black_friday_2023_is_promo(self, dim_date_df):
        # Thanksgiving 2023 = Nov 23 → Black Friday = Nov 24
        bf = dim_date_df[dim_date_df["date_key"] == 20231124].iloc[0]
        assert bf["is_promo_window"]
        assert "Black Friday" in str(bf["promo_name"])

    def test_christmas_is_holiday(self, dim_date_df):
        xmas = dim_date_df[dim_date_df["date_key"] == 20231225].iloc[0]
        assert xmas["is_holiday"]
        assert xmas["holiday_name"] is not None

    def test_independence_day_is_holiday(self, dim_date_df):
        july4 = dim_date_df[dim_date_df["date_key"] == 20240704].iloc[0]
        assert july4["is_holiday"]

    def test_promo_nulls_on_non_promo_day(self, dim_date_df):
        # Jun 10 sits between Spring New Arrivals (ends Apr 15) and Summer Sale (starts Jun 15)
        row = dim_date_df[dim_date_df["date_key"] == 20240610].iloc[0]
        if not row["is_promo_window"]:
            assert pd.isna(row["promo_name"])

    def test_holiday_name_null_on_non_holiday(self, dim_date_df):
        row = dim_date_df[dim_date_df["date_key"] == 20230405].iloc[0]
        if not row["is_holiday"]:
            assert pd.isna(row["holiday_name"])

    def test_summer_sale_is_promo(self, dim_date_df):
        row = dim_date_df[dim_date_df["date_key"] == 20230625].iloc[0]
        assert row["is_promo_window"]

    def test_spring_new_arrivals_is_promo(self, dim_date_df):
        row = dim_date_df[dim_date_df["date_key"] == 20230405].iloc[0]
        assert row["is_promo_window"]
        assert "Spring" in str(row["promo_name"])


class TestSeasonAndWeekend:
    @pytest.mark.parametrize("month,expected", [
        (4, "Spring"), (5, "Spring"), (6, "Summer"),
        (7, "Summer"), (8, "Summer"), (9, "Fall"),
        (10, "Fall"), (11, "Fall"), (12, "Winter"),
        (1, "Winter"), (2, "Winter"), (3, "Spring"),
    ])
    def test_season(self, month, expected):
        d = datetime.date(2024, month, 15)
        assert _season(d) == expected

    def test_weekend_saturday(self, dim_date_df):
        # 2023-04-01 is a Saturday
        row = dim_date_df[dim_date_df["date_key"] == 20230401].iloc[0]
        assert row["is_weekend"]
        assert not row["is_business_day"]

    def test_weekday_is_not_weekend(self, dim_date_df):
        # 2023-04-03 is a Monday
        row = dim_date_df[dim_date_df["date_key"] == 20230403].iloc[0]
        assert not row["is_weekend"]

    def test_post_payday_1st(self, dim_date_df):
        row = dim_date_df[dim_date_df["date_key"] == 20230501].iloc[0]
        assert row["is_post_payday"]

    def test_post_payday_15th(self, dim_date_df):
        row = dim_date_df[dim_date_df["date_key"] == 20230515].iloc[0]
        assert row["is_post_payday"]

    def test_non_payday(self, dim_date_df):
        row = dim_date_df[dim_date_df["date_key"] == 20230510].iloc[0]
        assert not row["is_post_payday"]


class TestNulls:
    _required = [
        "date_key", "date", "year", "month", "day",
        "fiscal_year", "fiscal_quarter", "fiscal_month", "fiscal_week",
        "is_weekend", "is_holiday", "is_business_day", "is_promo_window",
        "season", "day_of_fiscal_year",
    ]

    def test_no_nulls_in_required_columns(self, dim_date_df):
        for col in self._required:
            assert dim_date_df[col].notna().all(), f"Nulls found in {col}"
