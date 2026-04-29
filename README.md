# CSNP & Co. — Retail Platform

Microsoft Fabric proof-of-concept using a synthetic retail dataset.

CSNP & Co. is a fictional mid-market omnichannel apparel retailer. Three years of
deterministic, pattern-rich transactional history across 142 stores, 320K customers,
and 3.2K SKUs — purpose-built to exercise the full Fabric medallion pipeline and surface
14 planted insight stories.

## Quick start

```bash
# Install uv (if needed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the generator
uv sync

# Generate 3 years of XS-scale data locally
uv run python -m csnp_retail generate --mode backfill --scale xs --out ./data/

# Run tests
uv run pytest generator/tests/unit -v
```

## Scale profiles

| Profile | Sales rows | Customers | Stores | Use case |
|---------|-----------|-----------|--------|----------|
| `xs`    | 100K      | 5K        | 15     | Dev / unit tests |
| `s`     | 1M        | 50K       | 45     | Demo on laptop |
| `m`     | 8M        | 320K      | 142    | Realistic Fabric test |
| `l`     | 50M       | 1.5M      | 380    | Perf / capacity stress |

## Structure

```
generator/          Python package: csnp-retail
fabric/             Fabric notebooks, pipelines, lakehouses (Bronze → Silver → Gold)
dq/                 Great Expectations suites (Week 3+)
deploy/             fabric-cicd deployment scripts (Week 3+)
```

## Design

See [csnp_dataset_design.md](csnp_dataset_design.md) and [csnp_production_build_plan.md](csnp_production_build_plan.md).
