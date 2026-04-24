# Medallion path conventions

Canonical path patterns for OneLake storage across the bronze, silver, and gold
layers of the CSNP retail platform.

---

## 1. Bronze — OneLake Files/

Bronze data lives under `Files/` in the CSNP_Bronze lakehouse.  Two layouts exist
depending on how data arrives.

### Backfill / static dims and facts

Generated once by the generator's `--mode backfill` run and uploaded with
`scripts/upload_to_bronze.py`.

```
Files/bronze/{entity_name}/{filename}
```

Examples:
```
Files/bronze/dim_date/dim_date.parquet
Files/bronze/dim_product/dim_product.parquet
Files/bronze/fact_sales/fact_sales.parquet
```

`upload_to_bronze.py` batch mode uses the template `Files/bronze/{stem}/{name}`,
where `{stem}` is the filename without extension and `{name}` is the full filename.
For a file named `dim_date.parquet`, `{stem}` = `dim_date` and `{name}` =
`dim_date.parquet`, expanding to `Files/bronze/dim_date/dim_date.parquet`.

### Daily incremental (source-system conformant)

Written by the daily runner and partitioned by source system and date.  The layout
mirrors the generator's local output structure (`out/bronze/...`) so that the
source-system folder acts as a logical topic and the date partitions enable
incremental processing.

```
Files/bronze/{source}/{yyyy}/{mm}/{dd}/{file}
```

Sources and their files:

| Source | File(s) |
|--------|---------|
| `pos` | `store_{n}.parquet` |
| `ecom` | `orders.parquet` |
| `app` | `orders.parquet` |
| `clickstream/{HH}` | `sessions.parquet` |
| `inventory` | `snapshot.parquet` |
| `crm` | `customers_delta.parquet`, `loyalty_events.parquet` |
| `marketing` | `campaigns.parquet`, `spend.parquet` |
| `products` | `product_master.parquet` |

Example:
```
Files/bronze/pos/2025/06/15/store_3.parquet
Files/bronze/clickstream/14/sessions.parquet
```

---

## 2. Silver — OneLake Tables/

Silver tables are registered Delta tables in the CSNP_Silver lakehouse metastore.

### Table name convention

```
silver_{entity_name}
```

Examples: `silver_dim_date`, `silver_dim_product`, `silver_fact_sales`.

### Registration

Tables must be written with `.saveAsTable()` using the fully-qualified name so
Fabric recognises them as lakehouse-managed tables (visible under **Tables/** in
the lakehouse explorer, with schema inference and SQL endpoint access).

```python
source_df.write.format("delta").mode("overwrite").saveAsTable("CSNP_Silver.silver_dim_date")
```

The `merge_to_silver` helper accepts a `target_table_fqn` parameter for both
the initial `saveAsTable` write and all subsequent MERGE operations:

```python
merge_to_silver(
    source_df=silver_df,
    target_table_fqn="CSNP_Silver.silver_dim_date",
    business_keys=["date_key"],
    strategy="scd1",
)
```

### Portal setup — required before first notebook run

`saveAsTable` requires the target lakehouse to be accessible in the Spark session
catalog.  **This cannot be declared in the notebook source file or `.platform`
JSON** — fabric-cicd does not support it.  Each silver notebook needs the following
lakehouses attached before its first run:

| Role | Lakehouse | How |
|------|-----------|-----|
| Read source (bronze parquet) | **CSNP_Bronze** | Attach as additional lakehouse |
| Write target (saveAsTable) | **CSNP_Silver** | Attach as additional lakehouse |

Steps in the Fabric portal:
1. Open the notebook (`nb_03_silver_merge` or equivalent).
2. In the **Explorer** panel → **Lakehouses** → **Add lakehouse**.
3. Add **CSNP_Bronze** and **CSNP_Silver** (additional, not default is fine).
4. Save and re-run.

---

## 3. Gold — OneLake Tables/

Gold tables follow the same FQN + `saveAsTable` pattern as silver.

### Table name convention

```
dim_{entity_name}    — dimension tables
fact_{entity_name}   — fact tables
```

The `silver_` prefix is dropped.  Examples: `dim_date`, `dim_product`,
`fact_sales`, `fact_returns`.

### FQN pattern

```python
saveAsTable("CSNP_Gold.dim_date")
saveAsTable("CSNP_Gold.fact_sales")
```

Gold notebooks will need **CSNP_Silver** (read) and **CSNP_Gold** (write) attached
in the same way as silver notebooks need CSNP_Bronze + CSNP_Silver.

---

## 4. Multi-environment portability

### Lakehouse naming convention — identical across all environments

Lakehouse display names (`CSNP_Bronze`, `CSNP_Silver`, `CSNP_Gold`) are kept
**identical across CSNP_Dev, CSNP_Test, and CSNP_Prod workspaces**.  This is a
deliberate design choice: catalog FQNs (e.g. `CSNP_Silver.silver_dim_date`) and
ABFSS lakehouse paths (e.g. `.../CSNP_Bronze.Lakehouse/Files/...`) do not need
per-environment substitution rules in `parameter.yml`.

**Future Lakehouse additions must follow this convention** — create the lakehouse
with the same `displayName` in all three workspaces before deploying notebooks that
reference it.

### Workspace name substitution via parameter.yml

The notebook `workspace_name` parameter is used in ABFSS bronze file paths and
therefore is environment-specific.  `fabric/parameter.yml` handles this with a
`find_replace` rule scoped to Notebook items:

```yaml
find_replace:
  - find_value: 'workspace_name: str = "CSNP_Dev"'
    replace_value:
      DEV:  'workspace_name: str = "CSNP_Dev"'
      TEST: 'workspace_name: str = "CSNP_Test"'
      PROD: 'workspace_name: str = "CSNP_Prod"'
    item_type: "Notebook"
```

The `find_value` matches only the exact parameter cell assignment line, preventing
accidental substitution in cell comments or variable names.  The `DEV`/`TEST`/`PROD`
keys are uppercase to match `args.environment.upper()` in `deploy/deploy.py`.

`silver_table_fqn` (e.g. `CSNP_Silver.silver_dim_date`) uses only lakehouse display
names and requires no substitution — it is correct in all three environments as-is.

### Workspace display names

| Environment | Workspace display name |
|-------------|----------------------|
| Dev | `CSNP_Dev` |
| Test | `CSNP_Test` |
| Prod | `CSNP_Prod` |

These names are confirmed by `fab ls` and `tests/fabric/test_auth.py`.  No spaces,
no suffixes.  The `parameter.yml` rule above uses them verbatim.

---

## Quick reference

| Layer | Storage type | Path / name pattern | Registration |
|-------|-------------|---------------------|--------------|
| Bronze (static) | Files | `Files/bronze/{entity}/{filename}` | Not registered — raw parquet |
| Bronze (daily) | Files | `Files/bronze/{source}/{yyyy}/{mm}/{dd}/{file}` | Not registered — raw parquet |
| Silver | Tables | `silver_{entity}` in CSNP_Silver | `saveAsTable("CSNP_Silver.silver_{entity}")` |
| Gold | Tables | `dim_{entity}` / `fact_{entity}` in CSNP_Gold | `saveAsTable("CSNP_Gold.dim_{entity}")` |
