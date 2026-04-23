"""Unit tests for dim_channel."""

from __future__ import annotations


class TestStructure:
    def test_row_count(self, dim_channel_df):
        assert len(dim_channel_df) == 4

    def test_channel_key_sequential(self, dim_channel_df):
        assert list(dim_channel_df["channel_key"]) == [1, 2, 3, 4]

    def test_required_columns(self, dim_channel_df):
        for col in ["channel_key", "channel_name", "channel_type", "is_digital"]:
            assert col in dim_channel_df.columns

    def test_no_nulls(self, dim_channel_df):
        assert dim_channel_df.notna().all().all()


class TestValues:
    def test_store_is_physical(self, dim_channel_df):
        store = dim_channel_df[dim_channel_df["channel_name"] == "Store"]
        assert len(store) == 1
        assert store.iloc[0]["channel_type"] == "Physical"
        assert store.iloc[0]["is_digital"] is False or store.iloc[0]["is_digital"] == False

    def test_digital_channels(self, dim_channel_df):
        digital = dim_channel_df[dim_channel_df["is_digital"] == True]
        assert set(digital["channel_name"]) == {"Web", "App", "Marketplace"}

    def test_channel_names_unique(self, dim_channel_df):
        assert dim_channel_df["channel_name"].is_unique
