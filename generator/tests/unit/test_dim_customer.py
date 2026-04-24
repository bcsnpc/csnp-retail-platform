"""Unit tests for dim_customer."""

from __future__ import annotations


class TestRowCount:
    def test_current_rows_equal_n_customers(self, dim_customer_df, xs_config):
        current = dim_customer_df[dim_customer_df["is_current"]]
        assert len(current) == xs_config.n_customers

    def test_total_rows_geq_n_customers(self, dim_customer_df, xs_config):
        assert len(dim_customer_df) >= xs_config.n_customers

    def test_customer_key_sequential(self, dim_customer_df):
        assert list(dim_customer_df["customer_key"]) == list(range(1, len(dim_customer_df) + 1))


class TestSCD2:
    def test_one_current_row_per_customer(self, dim_customer_df):
        current_count = dim_customer_df.groupby("customer_id")["is_current"].sum()
        assert (current_count == 1).all()

    def test_expiry_null_on_current(self, dim_customer_df):
        current = dim_customer_df[dim_customer_df["is_current"]]
        assert current["expiry_date"].isna().all()

    def test_expiry_set_on_non_current(self, dim_customer_df):
        non_current = dim_customer_df[~dim_customer_df["is_current"]]
        if len(non_current) > 0:
            assert non_current["expiry_date"].notna().all()

    def test_upgraded_customers_have_two_rows(self, dim_customer_df):
        counts = dim_customer_df.groupby("customer_id").size()
        upgraded = counts[counts == 2]
        assert len(upgraded) > 0, "Expected some customers to have 2 SCD2 rows"


class TestSegments:
    _expected = {
        "Style Loyalist", "Sale Seeker", "Core Shopper",
        "Gift & Occasion", "Digital Native", "One-Timer",
    }

    def test_all_segments_present(self, dim_customer_df):
        current = dim_customer_df[dim_customer_df["is_current"]]
        segs = set(current["customer_segment"].unique())
        assert self._expected.issubset(segs)

    def test_core_shopper_is_largest(self, dim_customer_df):
        current = dim_customer_df[dim_customer_df["is_current"]]
        counts = current["customer_segment"].value_counts()
        assert counts.index[0] == "Core Shopper"

    def test_sale_seeker_is_second(self, dim_customer_df):
        current = dim_customer_df[dim_customer_df["is_current"]]
        counts = current["customer_segment"].value_counts()
        assert counts.index[1] == "Sale Seeker"


class TestColumns:
    _required = [
        "customer_key", "customer_id", "first_name", "last_name", "email",
        "birth_date", "gender", "customer_segment", "acquisition_channel",
        "loyalty_tier", "signup_date", "country_code",
        "effective_date", "expiry_date", "is_current",
    ]

    def test_required_columns(self, dim_customer_df):
        for col in self._required:
            assert col in dim_customer_df.columns, f"Missing: {col}"

    def test_no_nulls_in_key_columns(self, dim_customer_df):
        for col in ["customer_key", "customer_id", "email", "customer_segment"]:
            assert dim_customer_df[col].notna().all(), f"Nulls in {col}"

    def test_gender_values(self, dim_customer_df):
        assert set(dim_customer_df["gender"].unique()).issubset({"M", "F", "NB"})

    def test_loyalty_tiers_valid(self, dim_customer_df):
        valid = {"None", "Bronze", "Silver", "Gold", "Platinum"}
        assert set(dim_customer_df["loyalty_tier"].unique()).issubset(valid)
