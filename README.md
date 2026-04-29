# CSNP & Co. — Retail Platform

Microsoft Fabric proof-of-concept using a synthetic retail dataset.

CSNP & Co. is a fictional mid-market omnichannel apparel retailer. Three years of
deterministic, pattern-rich transactional history across 142 stores, 320K customers,
and 3.2K SKUs — purpose-built to exercise the full Fabric medallion pipeline and surface
14 planted insight stories.

## Architecture

Full medallion pipeline orchestrated by a single Fabric Data Pipeline (`pl_daily_load`):

```
Bronze (raw Parquet files)
  └─ nb_01_generate          synthetic daily data → Bronze lakehouse Files
  └─ nb_02_bronze_validate   row-count + schema checks

Silver (Delta tables, SCD1/SCD2 merges)
  └─ 8 dim notebooks         dim_date, geography, store, campaign, channel,
                             return_reason, product (SCD1), customer (SCD1)
  └─ 6 fact notebooks        fact_sales, returns, sessions, marketing_spend,
                             loyalty_events, inventory_daily

Gold (star schema, reporting-ready Delta tables)
  └─ nb_04_gold_dims         all 8 dims silver → gold (SCD1 on surrogate key)
  └─ nb_05_gold_facts        all 6 facts silver → gold (SCD1 on surrogate key)
  └─ nb_06_dq_checkpoint     data quality gate

Pipeline: Generate → Validate_Bronze → Silver (14 sequential) → Gold (2 parallel) → DQ
Schedule: daily at 06:00 UTC
```

## Structure

```
generator/          Python package: csnp-retail (synthetic data generator)
helpers/            Python wheel: csnp_helpers (shared PySpark merge/lineage/validation logic)
fabric/             Fabric items: 3 lakehouses, 1 environment, 20 notebooks, 1 pipeline
deploy/             fabric-cicd deployment scripts (DEV / TEST / PROD)
scripts/            build_helpers.sh, upload_to_bronze.py
tests/              Fabric integration tests
docs/               helpers_workflow.md, bronze_path_convention.md
```

## Quick start

```bash
# Install uv (if needed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install all packages
uv sync

# Generate 3 years of XS-scale data locally
uv run python -m csnp_retail generate --mode backfill --scale xs --out ./data/

# Upload generated data to Fabric Bronze lakehouse
uv run python scripts/upload_to_bronze.py --env dev

# Deploy all Fabric items (lakehouses, notebooks, pipeline, environment)
uv run csnp-deploy --environment dev

# Run tests
uv run pytest
```

## Scale profiles

| Profile | Sales rows | Customers | Stores | Use case |
|---------|-----------|-----------|--------|----------|
| `xs`    | 100K      | 5K        | 15     | Dev / unit tests |
| `s`     | 1M        | 50K       | 45     | Demo on laptop |
| `m`     | 8M        | 320K      | 142    | Realistic Fabric test |
| `l`     | 50M       | 1.5M      | 380    | Perf / capacity stress |

## csnp_helpers wheel

Shared PySpark logic packaged as a Python wheel and installed into the `csnp_env`
Fabric Environment. All notebooks import from here — no duplicated logic.

| Function | Description |
|----------|-------------|
| `merge_to_silver(df, table, keys, strategy)` | Delta MERGE with SCD1 or SCD2 |
| `add_lineage_columns(df, source_system)` | Adds `ingestion_timestamp_utc`, `source_system` |
| `validate_silver(table, key_col)` | Null-key and duplicate checks |
| `onelake_files_path(workspace, lakehouse, subpath)` | Builds `abfss://` path for Bronze files |

To update the wheel after a helpers change:

```bash
bash scripts/build_helpers.sh   # bumps patch version, builds, copies .whl
# then commit and deploy
```

See [docs/helpers_workflow.md](docs/helpers_workflow.md) for full details.
