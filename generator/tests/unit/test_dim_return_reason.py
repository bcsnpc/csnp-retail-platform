"""Unit tests for dim_return_reason."""

from __future__ import annotations


class TestStructure:
    def test_row_count(self, dim_return_reason_df):
        assert len(dim_return_reason_df) == 12

    def test_key_sequential(self, dim_return_reason_df):
        assert list(dim_return_reason_df["return_reason_key"]) == list(range(1, 13))

    def test_required_columns(self, dim_return_reason_df):
        for col in ["return_reason_key", "return_reason", "return_reason_group", "is_controllable"]:
            assert col in dim_return_reason_df.columns

    def test_no_nulls(self, dim_return_reason_df):
        assert dim_return_reason_df.notna().all().all()


class TestValues:
    def test_reason_names_unique(self, dim_return_reason_df):
        assert dim_return_reason_df["return_reason"].is_unique

    def test_controllable_is_bool(self, dim_return_reason_df):
        assert dim_return_reason_df["is_controllable"].dtype == bool

    def test_fit_group_exists(self, dim_return_reason_df):
        assert "Fit" in dim_return_reason_df["return_reason_group"].values

    def test_some_controllable_true(self, dim_return_reason_df):
        assert dim_return_reason_df["is_controllable"].any()

    def test_some_controllable_false(self, dim_return_reason_df):
        assert (~dim_return_reason_df["is_controllable"]).any()
