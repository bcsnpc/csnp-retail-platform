"""Session-scoped fixtures shared across all test modules."""

from __future__ import annotations

import numpy as np
import pytest

from csnp_retail.config import BACKFILL_END, BACKFILL_START, GeneratorConfig, Scale
from csnp_retail.entities.dim_campaign import build_dim_campaign
from csnp_retail.entities.dim_channel import build_dim_channel
from csnp_retail.entities.dim_customer import build_dim_customer
from csnp_retail.entities.dim_date import build_dim_date
from csnp_retail.entities.dim_geography import build_dim_geography
from csnp_retail.entities.dim_product import build_dim_product
from csnp_retail.entities.dim_return_reason import build_dim_return_reason
from csnp_retail.entities.dim_store import build_dim_store
from csnp_retail.entities.fact_inventory_daily import build_fact_inventory_daily
from csnp_retail.entities.fact_loyalty_events import build_fact_loyalty_events
from csnp_retail.entities.fact_marketing_spend import build_fact_marketing_spend
from csnp_retail.entities.fact_returns import build_fact_returns
from csnp_retail.entities.fact_sales import build_fact_sales
from csnp_retail.entities.fact_sessions import build_fact_sessions


@pytest.fixture(scope="session")
def xs_config() -> GeneratorConfig:
    return GeneratorConfig(scale=Scale.xs, seed=42)


@pytest.fixture(scope="session")
def xs_rng() -> np.random.Generator:
    return np.random.default_rng(42)


@pytest.fixture(scope="session")
def dim_date_df():
    return build_dim_date(BACKFILL_START, BACKFILL_END)


@pytest.fixture(scope="session")
def dim_geo_df():
    return build_dim_geography()


@pytest.fixture(scope="session")
def dim_store_df(xs_config, xs_rng, dim_geo_df):
    return build_dim_store(xs_config, xs_rng, dim_geo_df)


@pytest.fixture(scope="session")
def dim_campaign_df():
    rng = np.random.default_rng(42)
    return build_dim_campaign(BACKFILL_START, BACKFILL_END, rng)


@pytest.fixture(scope="session")
def dim_channel_df():
    return build_dim_channel()


@pytest.fixture(scope="session")
def dim_return_reason_df():
    return build_dim_return_reason()


@pytest.fixture(scope="session")
def dim_product_df(xs_config):
    rng = np.random.default_rng(xs_config.seed + 10)
    return build_dim_product(xs_config, rng)


@pytest.fixture(scope="session")
def dim_customer_df(xs_config):
    rng = np.random.default_rng(xs_config.seed + 20)
    return build_dim_customer(xs_config, rng)


@pytest.fixture(scope="session")
def fact_sales_df(
    xs_config,
    dim_date_df,
    dim_store_df,
    dim_product_df,
    dim_customer_df,
    dim_channel_df,
    dim_campaign_df,
):
    rng = np.random.default_rng(xs_config.seed + 50)
    return build_fact_sales(
        xs_config, rng,
        dim_date_df, dim_store_df, dim_product_df,
        dim_customer_df, dim_channel_df, dim_campaign_df,
    )


@pytest.fixture(scope="session")
def fact_returns_df(xs_config, fact_sales_df, dim_product_df):
    rng = np.random.default_rng(xs_config.seed + 60)
    return build_fact_returns(xs_config, rng, fact_sales_df, dim_product_df)


@pytest.fixture(scope="session")
def fact_sessions_df(xs_config, dim_date_df, dim_customer_df):
    rng = np.random.default_rng(xs_config.seed + 70)
    return build_fact_sessions(xs_config, rng, dim_date_df, dim_customer_df)


@pytest.fixture(scope="session")
def fact_marketing_spend_df(xs_config, dim_campaign_df, dim_date_df):
    rng = np.random.default_rng(xs_config.seed + 80)
    return build_fact_marketing_spend(xs_config, rng, dim_campaign_df, dim_date_df)


@pytest.fixture(scope="session")
def fact_loyalty_events_df(xs_config, fact_sales_df, dim_customer_df, dim_date_df):
    rng = np.random.default_rng(xs_config.seed + 90)
    return build_fact_loyalty_events(
        xs_config, rng, fact_sales_df, dim_customer_df, dim_date_df
    )


@pytest.fixture(scope="session")
def fact_inventory_daily_df(
    xs_config, fact_sales_df, dim_product_df, dim_store_df, dim_date_df
):
    rng = np.random.default_rng(xs_config.seed + 100)
    return build_fact_inventory_daily(
        xs_config, rng, fact_sales_df, dim_product_df, dim_store_df, dim_date_df
    )
