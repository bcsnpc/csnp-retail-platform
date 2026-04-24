"""Generator configuration — scale profiles and run parameters."""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field

BACKFILL_START = date(2023, 4, 1)
BACKFILL_END = date(2026, 3, 31)


class Scale(StrEnum):
    xs = "xs"
    s = "s"
    m = "m"
    l = "l"  # noqa: E741


class Mode(StrEnum):
    backfill = "backfill"
    daily = "daily"


class ScaleProfile(BaseModel):
    sales_rows: int
    customers: int
    products: int
    stores: int
    sessions: int
    inventory_skus: int


SCALE_PROFILES: dict[Scale, ScaleProfile] = {
    Scale.xs: ScaleProfile(
        sales_rows=100_000, customers=5_000, products=600, stores=15,
        sessions=180_000, inventory_skus=50,
    ),
    Scale.s: ScaleProfile(
        sales_rows=1_000_000, customers=50_000, products=1_800, stores=45,
        sessions=1_800_000, inventory_skus=150,
    ),
    Scale.m: ScaleProfile(
        sales_rows=8_000_000, customers=320_000, products=3_200, stores=142,
        sessions=14_000_000, inventory_skus=600,
    ),
    Scale.l: ScaleProfile(
        sales_rows=50_000_000, customers=1_500_000, products=6_000, stores=380,
        sessions=90_000_000, inventory_skus=2_000,
    ),
}


class GeneratorConfig(BaseModel):
    mode: Mode = Mode.backfill
    scale: Scale = Scale.m
    seed: int = 42
    start: date = BACKFILL_START
    end: date = BACKFILL_END
    out: Path = Field(default_factory=lambda: Path("./data"))
    seed_file: Path | None = None

    @property
    def profile(self) -> ScaleProfile:
        return SCALE_PROFILES[self.scale]

    @property
    def n_stores(self) -> int:
        return self.profile.stores

    @property
    def n_customers(self) -> int:
        return self.profile.customers

    @property
    def n_products(self) -> int:
        return self.profile.products

    @property
    def n_sales_rows(self) -> int:
        return self.profile.sales_rows

    @property
    def n_sessions(self) -> int:
        return self.profile.sessions

    @property
    def n_inventory_skus(self) -> int:
        return self.profile.inventory_skus
