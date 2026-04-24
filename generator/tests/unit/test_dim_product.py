"""Unit tests for dim_product."""

from __future__ import annotations


class TestRowCount:
    def test_at_least_n_products_skus(self, dim_product_df, xs_config):
        unique_skus = dim_product_df["product_id"].nunique()
        assert unique_skus == xs_config.n_products

    def test_total_rows_geq_n_products(self, dim_product_df, xs_config):
        assert len(dim_product_df) >= xs_config.n_products

    def test_product_key_sequential(self, dim_product_df):
        assert list(dim_product_df["product_key"]) == list(range(1, len(dim_product_df) + 1))


class TestSCD2:
    def test_current_row_per_sku(self, dim_product_df):
        current_count = dim_product_df.groupby("product_id")["is_current"].sum()
        assert (current_count == 1).all(), "Every SKU must have exactly one is_current=True row"

    def test_effective_dates_ordered(self, dim_product_df):
        for pid, grp in dim_product_df.groupby("product_id"):
            dates = grp.sort_values("effective_date")["effective_date"].values
            assert (dates[:-1] <= dates[1:]).all(), f"Effective dates out of order for {pid}"

    def test_expiry_null_only_on_current(self, dim_product_df):
        non_current = dim_product_df[~dim_product_df["is_current"]]
        assert non_current["expiry_date"].notna().all()

    def test_current_expiry_is_null(self, dim_product_df):
        current = dim_product_df[dim_product_df["is_current"]]
        assert current["expiry_date"].isna().all()


class TestPlantedProducts:
    def test_meridian_cable_crew_exists(self, dim_product_df):
        mcc = dim_product_df[dim_product_df["style_code"] == "TOP-MCC-001"]
        assert len(mcc) >= 3, "Meridian Cable Crew must have at least 3 colorway rows"

    def test_meridian_is_tops(self, dim_product_df):
        mcc = dim_product_df[dim_product_df["style_code"] == "TOP-MCC-001"]
        assert (mcc["category"] == "tops").all()

    def test_field_straight_leg_exists(self, dim_product_df):
        fsl = dim_product_df[dim_product_df["style_code"] == "BTM-FSL-001"]
        assert len(fsl) >= 3

    def test_field_straight_leg_is_bottoms(self, dim_product_df):
        fsl = dim_product_df[dim_product_df["style_code"] == "BTM-FSL-001"]
        assert (fsl["category"] == "bottoms").all()


class TestPricing:
    def test_list_price_positive(self, dim_product_df):
        assert (dim_product_df["list_price"] > 0).all()

    def test_cost_price_positive(self, dim_product_df):
        assert (dim_product_df["cost_price"] > 0).all()

    def test_cost_less_than_list(self, dim_product_df):
        assert (dim_product_df["cost_price"] < dim_product_df["list_price"]).all()


class TestColumns:
    _required = [
        "product_key", "product_id", "style_code", "sku", "product_name",
        "category", "department", "color_name", "color_family",
        "list_price", "cost_price", "effective_date", "expiry_date", "is_current",
    ]

    def test_required_columns(self, dim_product_df):
        for col in self._required:
            assert col in dim_product_df.columns, f"Missing column: {col}"

    def test_no_nulls_in_key_columns(self, dim_product_df):
        for col in ["product_key", "product_id", "style_code", "category", "list_price"]:
            assert dim_product_df[col].notna().all(), f"Nulls in {col}"

    def test_categories_valid(self, dim_product_df):
        valid = {"tops", "bottoms", "outerwear", "footwear", "accessories", "home", "beauty"}
        assert set(dim_product_df["category"].unique()).issubset(valid)
