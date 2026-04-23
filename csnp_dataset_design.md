# CSNP & Co. — Fabric POC Test Dataset & Pipeline Design

**Version:** 1.0 · **Author:** Chiranjeevi Bhogireddy · **Date:** 21 Apr 2026
**Brand:** CSNP & Co. (fictional apparel retailer)
**Purpose:** End-to-end synthetic but realistic retail dataset for Fabric POC testing. Covers domain, schema, planted patterns, Fabric medallion pipeline, and daily incremental load orchestrated via Data Factory + Notebook.

---

## 0. About the brand

**CSNP & Co.** is a fictional mid-market omnichannel apparel retailer. Think J.Crew meets Madewell meets Everlane — elevated-casual clothing, footwear, accessories, and small home goods, with a strong DTC backbone and a retail footprint across North America plus a recent UK expansion.

Founded (fictionally) in 2014. 142 stores today. 3 years of detailed transactional history (FY23 Q2 through FY26 Q1). Apparel-first, with enough category breadth to make mix-analysis stories interesting.

**Why apparel.** Universally relatable — LinkedIn demos need zero industry context. Generates the right shape of data: seasonality, sizing complexity, returns, inventory turnover, trend-driven demand, heavy DTC/store balance, cohort stories.

**Brand aesthetic for demos.** Heritage-editorial — serif masthead, warm neutrals, editorial layouts. Pairs with the dashboard direction mocked earlier. In every Fabric POC screenshot, "CSNP & Co." appears as the data source or brand header, making demos unmistakably personal to you.

---

## 1. Design principles

Five goals the dataset must hit:

1. **Exercise every visual type** Fabric POC supports — line, bar, treemap, Sankey, funnel, cohort heatmap, map, waterfall, scatter, gauge, small multiples, Pareto, distribution, KPI cards.
2. **Contain planted insight stories** (§6) the multi-agent LLM pipeline should surface — anomalies, mix shifts, cohort degradation, price elasticity, promotion lift, cannibalization, seasonality breaks. Each is a demo script.
3. **Run end-to-end in Fabric** — bronze landing in OneLake, medallion transformation, Direct Lake reporting layer, daily incremental append via Data Factory pipeline. Not a static CSV dump.
4. **Be plausible** — real-sounding product names, believable prices, realistic geography, no obvious synthetic fingerprints.
5. **Scale configurably** — XS / S / M / L profiles from the same generator, so dev loop runs in seconds and stress tests run against 50M+ rows.

**Non-goals.** Not a training dataset. Correlations are engineered, not learned.

---

## 2. End-to-end flow at a glance

```
┌─────────────────────────────────────────────────────────────────────┐
│            csnp-retail Python generator                             │
│                                                                     │
│   mode=backfill  →  3 years of historical data (one-time)           │
│   mode=daily     →  1 day append (runs every morning)               │
└────────────────────────────┬────────────────────────────────────────┘
                             │ writes Parquet to ADLS Gen2 staging
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                OneLake — Bronze layer                               │
│   Files/bronze/{source}/{yyyy}/{mm}/{dd}/                           │
│   pos  ·  ecom  ·  clickstream  ·  inventory  ·  crm  ·  marketing  │
└────────────────────────────┬────────────────────────────────────────┘
                             │ notebook: bronze → silver
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│               OneLake — Silver (Delta tables)                       │
│   conformed · deduped · type-cast · UTC-normalized                  │
│   partitioned by date                                               │
└────────────────────────────┬────────────────────────────────────────┘
                             │ notebook: silver → gold (MERGE/SCD2)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│               OneLake — Gold (star schema)                          │
│   fact_* partitioned by date_key                                    │
│   dim_* with SCD2 for product, customer                             │
│   Z-ORDER on high-cardinality filter columns                        │
└────────────────────────────┬────────────────────────────────────────┘
                             │ Direct Lake
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│          Fabric Semantic Model (TMDL-versioned)                     │
│   CSNP_Retail_Model · measures · hierarchies · RLS                  │
└────────────────────────────┬────────────────────────────────────────┘
                             │ executeQueries REST API
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Fabric POC                                   │
│   Multi-agent pipeline · visual renderer · narrative generator      │
└─────────────────────────────────────────────────────────────────────┘

         Orchestration: Fabric Data Factory pipeline
         Schedule: daily at 02:00 UTC (post-close)
         Sequence: generate → bronze → silver → gold → refresh model → validate
```

---

## 3. Star schema — gold layer

### 3.1 Fact tables

| Table | Grain | Rows (M scale) | Primary use |
|---|---|---|---|
| `fact_sales` | 1 row per sales line item | ~8M | Revenue, units, margin, discount |
| `fact_returns` | 1 row per returned line item | ~480K (≈6% of sales, apparel runs hotter) | Return rate, reason mix |
| `fact_sessions` | 1 row per web/app session | ~14M | Funnel, conversion, bounce |
| `fact_inventory_daily` | 1 row per SKU × store × day | ~2M | Stockouts, days-of-supply |
| `fact_loyalty_events` | 1 row per loyalty action | ~700K | Program engagement |
| `fact_marketing_spend` | 1 row per campaign × channel × day | ~35K | Spend, CAC, attribution |

### 3.2 Dimension tables

| Table | Rows | Notes |
|---|---|---|
| `dim_date` | ~1,100 | Full fiscal + Gregorian attrs, holiday/promo flags |
| `dim_product` | ~3,200 SKUs | SCD2 — apparel needs size/color variants, so SKU count is higher |
| `dim_customer` | ~320K | SCD2 for loyalty tier and segment changes |
| `dim_store` | 142 stores | Geographic, format type, open/close dates |
| `dim_channel` | 4 | Store / Web / App / Marketplace |
| `dim_campaign` | ~180 | Promo and paid media campaigns |
| `dim_geography` | ~400 | Postal → city → metro → state → country |
| `dim_employee` | ~3,800 | Store staff (SCD2) |
| `dim_return_reason` | ~12 | Did not fit / Quality / Changed mind / etc. |

### 3.3 ERD (conceptual)

```
                  ┌──────────────┐
                  │  dim_date    │
                  └──────┬───────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
┌───────▼──────┐  ┌──────▼──────┐  ┌──────▼────────┐
│ fact_sales   │  │fact_sessions│  │fact_inventory │
└─┬──┬──┬──┬───┘  └──┬──┬───────┘  └────┬──┬───────┘
  │  │  │  │         │  │               │  │
  │  │  │  └──────┐  │  │               │  │
  │  │  │         │  │  │               │  │
  │  │  ▼         ▼  │  ▼               ▼  ▼
┌─▼──┐│┌───┐   ┌────┐│┌──────┐        ┌────┐┌────┐
│cust│││prd│   │chan│││campgn│        │store││prod│
└──┬─┘│└───┘   └────┘│└──────┘        └─────┘└────┘
   │  │              │
   │  │   ┌──────────▼───┐
   │  │   │ fact_returns │ (same dim keys as sales)
   │  │   └──────────────┘
   │  │
   │  │   ┌──────────────┐
   │  └──→│ fact_loyalty │
   │      └──────────────┘
   │
┌──▼────────┐
│ dim_geo   │ (chained from customer and store)
└───────────┘
```

Relationships: single-direction dim → fact, single-column keys. `dim_date` is referenced by every fact. `dim_geography` chains from both `dim_customer` and `dim_store` — tests how Fabric POC handles shared dimensions.

---

## 4. Detailed column specs

### 4.1 `fact_sales`

| Column | Type | Notes |
|---|---|---|
| `sales_line_key` | BIGINT PK | Surrogate, monotonic |
| `order_id` | VARCHAR(20) | Format: `CSNP-{YYYY}-{9 digits}` |
| `order_date_key` | INT FK → dim_date | YYYYMMDD |
| `customer_key` | BIGINT FK | Nullable for guest checkout (~22% of web — apparel skews higher) |
| `product_key` | BIGINT FK | SCD2 effective-dated lookup |
| `store_key` | INT FK | Nullable for pure-online orders |
| `channel_key` | TINYINT FK | |
| `campaign_key` | INT FK | Nullable; promotional orders only |
| `quantity` | INT | 1–4 typical, long tail to 12 |
| `size` | VARCHAR(10) | XS / S / M / L / XL / XXL / numeric |
| `color` | VARCHAR(30) | Marketing name (e.g., "Oat Heather") |
| `unit_price` | DECIMAL(10,2) | Price at time of sale |
| `list_price` | DECIMAL(10,2) | Undiscounted MSRP |
| `discount_amount` | DECIMAL(10,2) | Promo + loyalty discount |
| `tax_amount` | DECIMAL(10,2) | Computed from geography |
| `gross_revenue` | DECIMAL(12,2) | quantity × unit_price |
| `net_revenue` | DECIMAL(12,2) | gross − discount |
| `cost_of_goods` | DECIMAL(10,2) | ~38–58% of list (apparel margins) |
| `gross_margin` | DECIMAL(12,2) | net_revenue − cost_of_goods |
| `payment_method` | VARCHAR(20) | card / paypal / apple_pay / gift_card / bnpl / klarna |
| `fulfillment_type` | VARCHAR(20) | in_store / ship_to_home / bopis / sfs / locker |
| `loyalty_points_earned` | INT | Nullable if non-member |
| `is_first_purchase` | BIT | For acquisition cohorts |
| `source_system` | VARCHAR(20) | pos / ecom / app / marketplace |
| `source_created_at` | TIMESTAMP UTC | For late-arriving data tests |
| `ingestion_date_key` | INT | When the record landed in bronze |

**Grain:** one line. An order with three SKUs = three rows with the same `order_id`. `order_id` is a natural key, not PK.

**Partitioning (gold):** by `order_date_key`, one partition per month. Z-ORDER on `customer_key`, `product_key`, `store_key`.

**Late-arriving data behavior:** ~1.5% of sales from any given day will appear in the bronze drop 1–3 days late (replicates real POS reconciliation delay). The daily job MERGEs on `order_id + product_key + size` to handle this correctly.

### 4.2 `fact_sessions` (web + app)

Apparel browse-to-buy has long session counts — you can stress funnel/Sankey visuals heavily here.

| Column | Type | Notes |
|---|---|---|
| `session_key` | BIGINT PK | Surrogate |
| `session_id` | VARCHAR(36) | UUID |
| `session_date_key` | INT FK | |
| `session_start_time` | TIMESTAMP UTC | For time-of-day heatmaps |
| `customer_key` | BIGINT FK | Nullable for anonymous |
| `channel_key` | TINYINT FK | Web or App only |
| `device_type` | VARCHAR(20) | desktop / mobile_web / ios / android / tablet |
| `traffic_source` | VARCHAR(30) | organic / paid_search / paid_social / email / direct / affiliate / influencer |
| `campaign_key` | INT FK | Nullable |
| `landing_page_type` | VARCHAR(30) | home / pdp / plp / search / promo / lookbook |
| `pages_viewed` | INT | 1–60 (apparel shoppers browse heavily) |
| `session_duration_sec` | INT | |
| `reached_pdp` | BIT | |
| `reached_size_guide` | BIT | Apparel-specific |
| `reached_cart` | BIT | |
| `reached_checkout` | BIT | |
| `converted` | BIT | |
| `order_id` | VARCHAR(20) | Nullable; links to fact_sales if converted |
| `bounce` | BIT | pages_viewed = 1 AND duration < 10s |

### 4.3 `fact_returns`

Same shape as `fact_sales` but adds `return_date_key`, `return_reason_key`, `condition` (sellable / damaged / defective), `refund_amount`, `restocking_fee`. References original `sales_line_key` so reports can drill back to the original sale.

Apparel-specific return reasons (in `dim_return_reason`): "Did not fit (too small)", "Did not fit (too large)", "Color not as shown", "Fabric quality", "Changed mind", "Wrong item sent", "Arrived damaged", "Late delivery", "Better price elsewhere", "Gift return", "Ordered multiple sizes", "Other."

The "Ordered multiple sizes" reason is important — apparel has a "bracketing" behavior where customers order 2–3 sizes of the same item and return the ones that don't fit. This creates a planted pattern: ~15% of returns link back to orders where the customer bought multiple sizes of the same style.

### 4.4 `fact_inventory_daily`

Grain: SKU × store × day for top 600 SKUs. All 3,200 SKUs × 142 stores × 1,100 days = ~500M rows = overkill.

Columns: `units_on_hand`, `units_on_order`, `units_in_transit`, `days_of_supply`, `is_stockout`, `is_low_stock`, `safety_stock_level`.

This table enables stockout anomaly detection and inventory turn analysis.

### 4.5 `dim_product` — SCD Type 2

| Column | Type |
|---|---|
| `product_key` | BIGINT PK (surrogate) |
| `product_id` | VARCHAR(20) (natural key) |
| `style_code` | VARCHAR(20) — e.g., `APP-TOP-00142` |
| `sku_code` | VARCHAR(25) — style + size + color: `APP-TOP-00142-M-OAT` |
| `product_name` | VARCHAR(100) — e.g., "Heritage Oxford Shirt" |
| `brand_line` | VARCHAR(50) — "CSNP Essentials" / "CSNP Signature" / "CSNP Work" / "CSNP Kids" |
| `department` | VARCHAR(30) |
| `category` | VARCHAR(50) |
| `subcategory` | VARCHAR(50) |
| `gender` | VARCHAR(20) — Women / Men / Unisex / Kids |
| `color` | VARCHAR(30) — marketing name |
| `color_family` | VARCHAR(20) — neutral / blue / black / etc. for aggregate analysis |
| `size` | VARCHAR(10) |
| `size_family` | VARCHAR(20) — Apparel / Footwear / Accessory |
| `material` | VARCHAR(50) |
| `season` | VARCHAR(20) — SS24 / FW24 / SS25 / FW25 / SS26 / Core (year-round) |
| `launch_date` | DATE |
| `current_price` | DECIMAL(10,2) |
| `cost` | DECIMAL(10,2) |
| `supplier_id` | VARCHAR(20) |
| `is_private_label` | BIT — mostly yes (CSNP & Co. is primarily private label) |
| `sustainability_flag` | VARCHAR(20) — Organic / Recycled / Standard (for ESG reporting demos) |
| `effective_from` | DATE |
| `effective_to` | DATE (9999-12-31 for current) |
| `is_current` | BIT |

**Hierarchy for drilling:** Department → Category → Subcategory → Brand Line → Style → SKU. Six levels — excellent for treemaps and drill-downs.

**Departments (7):**
- Women's Apparel
- Men's Apparel
- Footwear
- Accessories (bags, belts, jewelry, small leather goods)
- Kids
- Home (candles, throws, small ceramics — newer category)
- Beauty (limited — moisturizer, scents — newest category)

**Categories per dept:** 4–8, so ~38 categories total.
**Subcategories per cat:** 2–5, so ~130 subcategories.

**Realistic product name generation.** Name = `{adjective} {noun}`. Adjective pool curated by category:
- Tops: Heritage, Essential, Classic, Vintage, Weekend, Sunday, Studio, Horizon, Northfield, Meridian
- Bottoms: Straight-leg, Relaxed, Slim-fit, Tailored, Field, Utility, Soft, Everyday
- Outerwear: Field, Mountain, Harbor, Expedition, Commuter, Weekender, Trench, Sherpa
- Nouns match category — "Oxford Shirt," "Chambray Tee," "Denim Trucker," "Cable Crew," etc.

Color names: mix of classic and marketing-friendly — Oat Heather, Stone, Forest, Rust, Ink, Sea Salt, Olive Branch, Canvas, Sun Bleached.

### 4.6 `dim_customer` — SCD Type 2

| Column | Type |
|---|---|
| `customer_key` | BIGINT PK |
| `customer_id` | VARCHAR(20) natural key |
| `first_name`, `last_name` | VARCHAR(50) |
| `email_hash` | VARCHAR(64) |
| `birth_year` | INT |
| `gender` | VARCHAR(20) |
| `postal_code` | VARCHAR(10) FK |
| `acquisition_channel` | VARCHAR(20) |
| `acquisition_date` | DATE |
| `acquisition_campaign_key` | INT |
| `loyalty_tier` | VARCHAR(20) — Member / Silver / Gold / Platinum |
| `lifetime_value_bucket` | VARCHAR(20) — Low / Mid / High / VIP |
| `segment` | VARCHAR(30) — see §7 |
| `preferred_size` | VARCHAR(10) — apparel-specific |
| `is_loyalty_member` | BIT |
| `marketing_opt_in` | BIT |
| `effective_from`, `effective_to`, `is_current` | SCD2 |

Names generated from US Census frequency lists with regional distribution.

### 4.7 `dim_store`

| Column | Type |
|---|---|
| `store_key` | INT PK |
| `store_id` | VARCHAR(10) — `CSNP-{4 digits}` |
| `store_name` | VARCHAR(50) — e.g., "CSNP & Co. Houston — Rice Village" |
| `format_type` | VARCHAR(20) — Flagship / Standard / Outlet / Pop-up |
| `square_footage` | INT |
| `open_date`, `close_date` | DATE |
| `postal_code` | VARCHAR(10) FK |
| `latitude`, `longitude` | DECIMAL(9,6) |
| `region_manager` | VARCHAR(100) |
| `district` | VARCHAR(30) |
| `climate_zone` | VARCHAR(20) — drives seasonality variation |

**Store distribution (142 total):** 108 US, 22 Canada, 8 UK, 4 Mexico. US weighted by population so CA/TX/NY/FL get most. UK expansion in FY25 — all 8 UK stores opened Q3/Q4 FY25.

**Store format mix:** 8 Flagship (15K+ sqft, urban centers), 118 Standard (4–8K sqft, mall/lifestyle center), 12 Outlet (6–10K sqft, off-mall), 4 Pop-up (seasonal, short-lived — good for testing time-bounded dimension rows).

### 4.8 `dim_geography`

Hierarchical: Country → State/Province → Metro → City → Postal. Lat/long for postal centroids. Enables map visuals at any level.

### 4.9 `dim_date`

Standard with bonus flags: `is_holiday`, `holiday_name`, `is_promo_window`, `promo_name`, `fiscal_year`, `fiscal_quarter`, `fiscal_week`, `day_of_week_name`, `is_weekend`, `is_month_end`, `is_quarter_end`, `is_post_payday`, `season`, `week_of_year`, `day_of_fiscal_year`, `is_business_day`.

### 4.10 `dim_campaign`

| Column | Type |
|---|---|
| `campaign_key` | INT PK |
| `campaign_name` | VARCHAR(100) |
| `campaign_type` | VARCHAR(30) — Promo / Paid Search / Paid Social / Email / Affiliate / Influencer |
| `start_date`, `end_date` | DATE |
| `target_segment` | VARCHAR(30) |
| `discount_pct` | DECIMAL(4,2) |
| `planned_spend`, `actual_spend` | DECIMAL(10,2) |
| `target_revenue` | DECIMAL(12,2) |

~180 campaigns over 3 years — always-on paid search + seasonal (Black Friday, Back-to-School, Mother's Day, Memorial Day, end-of-season sales, New Arrivals drops).

---

## 5. Medallion architecture in Fabric

### 5.1 Bronze — raw landing

Parquet in `Files/bronze/`, partitioned by source system and ingestion date.

```
Files/bronze/
  pos/{YYYY}/{MM}/{DD}/store_{storeID}.parquet
  ecom/{YYYY}/{MM}/{DD}/orders.parquet
  app/{YYYY}/{MM}/{DD}/orders.parquet
  clickstream/{YYYY}/{MM}/{DD}/{HH}/sessions.parquet
  inventory/{YYYY}/{MM}/{DD}/snapshot.parquet
  crm/{YYYY}/{MM}/{DD}/customers_delta.parquet
  crm/{YYYY}/{MM}/{DD}/loyalty_events.parquet
  marketing/{YYYY}/{MM}/{DD}/campaigns.parquet
  marketing/{YYYY}/{MM}/{DD}/spend.parquet
  products/{YYYY}/{MM}/{DD}/product_master.parquet   (only on change days)
  stores/{YYYY}/{MM}/{DD}/store_master.parquet       (only on change days)
```

**Deliberate messiness.** Sources drop data with real-world warts:
- POS timestamps are in store-local time (varied TZ), need normalization to UTC in silver
- Ecom uses `customer_email` as natural key; POS uses `loyalty_card_id`; app uses `app_user_id` — identity resolution happens in silver
- ~1.5% of POS records arrive 1–3 days late
- Occasional duplicate records (replay scenarios)
- Inventory snapshots miss some stores on network issues (~0.2% of store-days)
- Schema drift: bronze files for the most recent quarter have two extra columns that older ones don't

This is what Fabric POC would see if it pointed at a real Fabric Lakehouse. Silver absorbs all of it.

### 5.2 Silver — conformed

Delta tables in `Tables/silver/`:

- Deduplicated via MERGE on natural keys
- Types cast and enforced
- Timestamps UTC-normalized
- Identity resolution across sources (unified `customer_id`)
- Schema-on-read handled (missing columns → nulls)
- Full history preserved (no SCD collapsing yet)
- Partitioned by date
- One table per business entity

Tables: `silver_sales_orderlines`, `silver_returns`, `silver_sessions`, `silver_inventory_snapshots`, `silver_loyalty_events`, `silver_customers`, `silver_products`, `silver_stores`, `silver_campaigns`, `silver_marketing_spend`.

### 5.3 Gold — star schema (Direct Lake target)

Delta tables in `Tables/gold/`:

- Surrogate keys assigned
- SCD2 for product and customer (effective-dated rows)
- Facts conformed to dim keys
- Facts partitioned by date_key
- Z-ORDER on high-cardinality filter columns
- No deletes (soft-deletes via `is_current` flags on dims)
- V-Order enabled on Parquet files for Direct Lake performance

This is what the Fabric POC reporting layer points at.

### 5.4 Semantic model

Direct Lake mode on gold tables. Model name: **CSNP_Retail_Model**.

- Date table marked as date table
- Measures in TMDL under `/model/definition/tables/` for version control
- Hierarchies defined: Product (Dept → Category → Subcategory → Brand Line → Style → SKU), Geography (Country → State → Metro → City → Postal), Date (Year → Quarter → Month → Week → Day)
- RLS role `RegionManager` — restricts to assigned region, for testing security propagation through Fabric POC

---

## 6. Planted patterns — the insight matrix

This section is the dataset's real value. Random data generates nothing interesting; engineered patterns do.

Each pattern below is something Fabric POC's multi-agent pipeline should discover and narrate.

### 6.1 Overall trend
**Story.** Revenue grew 14% YoY overall, with acceleration in FY26 as UK expansion matured.
**Plant.** Base monthly revenue grows from $7.2M (FY24 Q1) to $11.6M (FY26 Q1), compound monthly rate ~1.1% with noise.

### 6.2 Channel mix shift
**Story.** Ecommerce was 24% of revenue in FY24 Q1, 41% by FY26 Q1.
**Plant.** Online channel grows 2.9×, stores grow 1.2×. App grows fastest (5× off tiny base).

### 6.3 Geographic anomaly — Texas heat event
**Story.** Texas stores saw a 9-week revenue dip in Jun–Aug FY25 due to Outerwear-category supply issue when a heat dome crushed discretionary spending on anything non-summer.
**Plant.** All TX stores: revenue in Outerwear drops 55% from 2025-06-15 through 2025-08-16. Inventory fact shows elevated `is_stockout=1` for Outerwear SKUs in TX during late summer. Summer apparel (shorts, tees) holds normal.

### 6.4 Segment mix shift (VIP concentration)
**Story.** VIP customers: 4% of base / 16% of revenue in FY24 → 5% / 26% in FY26. Top-heavy shift.
**Plant.** Revenue per VIP customer grows 32%; VIP count grows 40%. Mid/Low LTV buckets flat.

### 6.5 Cohort degradation — BFCM discount dependency
**Story.** Customers acquired during Black Friday FY24 have 33% worse 90-day retention than organic cohorts.
**Plant.** Customers acquired via BFCM24 campaigns: 2nd-order probability cut from 58% baseline to 39%.

### 6.6 Cannibalization — BOPIS rollout
**Story.** After BOPIS launch in a store, in-person revenue drops 14% but total store revenue grows 19%.
**Plant.** Stagger BOPIS rollout across stores (40 stores got BOPIS in FY24, 70 more in FY25, rest in FY26 Q1). Per-store: in-store units drop 14% in the 3 months after launch; BOPIS units over-offset.

### 6.7 Promotion effectiveness
**Story.** Email campaigns have 3.4× higher ROI than paid social, but smaller volume.
**Plant.** Email-attributed orders: higher AOV ($92 vs $64), lower discount (6% vs 24%). Paid social drives more first-time customers with lower LTV.

### 6.8 Price elasticity by category
**Story.** 10% price cut in Apparel drives 19% unit lift; same cut in Accessories only 7% (accessories have gifting moments, less price-sensitive).
**Plant.** During promo windows, apply category-specific elasticity multipliers in unit demand formula.

### 6.9 Weather-driven demand
**Story.** Outerwear sales correlate with temperature drops; Swimwear with heatwaves.
**Plant.** Simulate 6 weather events (2 cold snaps, 4 heatwaves) across 3 years. Boost Outerwear +50% during cold snaps in Northeast/Midwest; Swimwear +45% during heatwaves in South/West.

### 6.10 Conversion funnel by device
**Story.** Mobile web converts at 1.6%, iOS app at 4.5%, desktop at 3.2%. App users 2.1× more valuable. Mobile web is 48% of sessions.
**Plant.** Conversion rates baked into session generation per device_type.

### 6.11 Seasonality break — viral style
**Story.** In Jul FY25, a specific cable-knit cardigan ("Meridian Cable Crew") had a mid-summer spike driven by a TikTok moment — broke the normal Jun/Jul apparel trough.
**Plant.** Inject SKU-level spike for one cardigan in 2025-07, reaching 9× normal volume, then taper through Sept.

### 6.12 Return rate anomaly — sizing issue
**Story.** Brand Line "CSNP Signature" has 23% return rate vs 9% overall for a specific pant style ("Field Straight-Leg") — customers consistently cite "fits small" in returns. Undiscovered sizing spec issue.
**Plant.** For all SKUs with `style_code = 'BTM-FSL-*'` in Signature line: boost return probability from 9% baseline to 23%. Return reasons skewed heavily to "Did not fit (too small)" (60% of returns on this style).

### 6.13 Gender/category cross-shop pattern
**Story.** 34% of Men's customers who purchase Women's items convert to 2-segment loyalists with 2.5× AOV.
**Plant.** Engineer a cross-shop subset in the customer data; boost retention and AOV for these customers.

### 6.14 Size bracketing
**Story.** ~15% of orders contain 2+ sizes of the same style, with ~60% return rate on the second size.
**Plant.** Flag "bracketing orders" in sales generation; corresponding returns follow.

---

## 7. Customer segments

Six segments, each with distinct behavior patterns:

| Segment | % of base | Behavior |
|---|---|---|
| Style Loyalist | 4% | Multi-channel, full-price, loyalty Platinum, high frequency |
| Sale Seeker | 25% | Promo-driven, high return rate, price-sensitive |
| Core Shopper | 46% | Average everything — baseline |
| Gift & Occasion | 16% | 2–3 purchases/yr, mostly gifts, holiday-concentrated |
| Digital Native | 7% | App-first, high session count, mid AOV, highly engaged |
| One-Timer | 2% | Single purchase in FY24, never returned |

---

## 8. Daily incremental load — the detailed plan

This is the part where it gets real. End-to-end flow for a daily job running each morning at 02:00 UTC for the prior calendar day.

### 8.1 The generator — two modes

```bash
# One-time, 3-year backfill
python -m csnp_retail.generate \
  --mode backfill \
  --scale m \
  --start 2023-04-01 \
  --end 2026-03-31 \
  --out abfss://.../Files/bronze/ \
  --seed 42

# Daily incremental (runs every morning)
python -m csnp_retail.generate \
  --mode daily \
  --scale m \
  --date 2026-04-20 \
  --out abfss://.../Files/bronze/ \
  --seed-file manifest.json
```

**Backfill mode** writes 3 years of bronze files in one pass, following the realistic distribution patterns.

**Daily mode** generates *one day* of new activity, behaving as if it were the next day in the fictional timeline. It reads the `manifest.json` from the previous run to know:
- Last customer_id used (for new customer assignments)
- Last order_id number
- Customer segments and their behavior distributions
- Active campaigns for the target date
- Planted pattern windows that should be active

The daily generator also produces ~1.5% "late-arriving" records for the previous 1–3 days — same `order_id` format, but landing in the current day's bronze folder. This simulates real POS reconciliation delays.

### 8.2 Bronze layer — what the daily drop contains

For target date `D` (yesterday in the fictional calendar):

| Source | What's written | Grain |
|---|---|---|
| `pos/D/` | One Parquet per store per day | ~40K rows (all stores combined) |
| `ecom/D/` | One Parquet for day's ecom orders | ~90K rows |
| `app/D/` | One Parquet for day's app orders | ~35K rows |
| `clickstream/D/HH/` | 24 hourly Parquet files | ~12K sessions total |
| `inventory/D/` | Daily snapshot | ~85K rows (top 600 SKUs × 142 stores) |
| `crm/D/` | Customer deltas + loyalty events | ~800 customer records + ~1,900 events |
| `marketing/D/` | Daily campaign spend | ~60 rows |

Also (only when changed):
- `products/D/` — product master if any SKU added, retired, or repriced
- `stores/D/` — store master if any store opened, closed, or changed format

### 8.3 Silver layer — daily MERGE logic

A notebook reads bronze for date `D`, plus any late-arriving records for `D-1` to `D-3`, and MERGEs into silver tables.

**Pattern for sales orderlines:**

```python
# Read today's bronze + late-arriving window
today = spark.read.parquet(f"Files/bronze/pos/{D}/") \
    .union(spark.read.parquet(f"Files/bronze/ecom/{D}/")) \
    .union(spark.read.parquet(f"Files/bronze/app/{D}/"))

# MERGE into silver — handles duplicates, late arrivals, schema drift
silver_sales = DeltaTable.forPath(spark, "Tables/silver/silver_sales_orderlines")
silver_sales.alias("t").merge(
    today.alias("s"),
    "t.order_id = s.order_id AND t.product_id = s.product_id AND t.size = s.size"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

Similar MERGE patterns for each silver table. Runtime target: <3 minutes for full silver layer on M scale.

### 8.4 Gold layer — daily update

Gold layer is where surrogate keys and SCD2 get assigned. Notebook steps:

1. **Refresh dims first.** `dim_customer` and `dim_product` get SCD2 treatment: any row where tracked attributes changed since yesterday → close the existing row (`is_current=0`, `effective_to=D-1`) and insert a new row with today as `effective_from`. Other dims refresh in-place.

2. **Assign surrogate keys.** New natural keys in silver get a surrogate key from the appropriate dim. Use a monotonic sequence persisted in a `_keys_ledger` Delta table.

3. **Load facts.** Read silver for date `D` (plus late-arriving window), join to current dim rows to resolve `*_key` columns, append to gold fact tables. Late-arriving records may require SCD2 lookup — "what was this customer's segment *on the original order date*" — using `effective_from`/`effective_to` ranges.

4. **OPTIMIZE + V-ORDER.** Run `OPTIMIZE` on gold fact tables weekly (Sunday night) with V-Order for Direct Lake performance.

Runtime target: <5 minutes on M scale.

### 8.5 Semantic model refresh

Direct Lake models don't "refresh" in the Import sense — they **reframe**. The pipeline calls the refresh API, which updates the model's frame to point at the latest Delta table versions.

```python
# In the Fabric pipeline, call the reporting layer refresh API
# POST /v1/workspaces/{ws}/semanticModels/{model}/refreshes
# body: { "type": "full" }
```

Reframe takes seconds — no data movement.

### 8.6 Validation step

After gold load and reporting layer reframe, a validation notebook runs:

- Row counts per fact table — compare to expected daily volume range (alert if outside 3σ)
- Null-rate checks on key columns
- Referential integrity — any fact rows with orphaned dim keys?
- DAX smoke test — run 10 canonical queries via `executeQueries` and verify results are non-empty and plausibly valued
- Planted-pattern sanity check — e.g., if today is within the Texas heat event window, is TX Outerwear revenue actually down?

Results logged to a `dq_results` Delta table and (optionally) emailed via Power Automate.

### 8.7 The Data Factory pipeline

End-to-end orchestration as a Fabric Data Factory pipeline:

```
┌───────────────────────────────────────────────────────────────┐
│  Pipeline: daily_csnp_load                                    │
│  Trigger: daily at 02:00 UTC                                  │
└──────────────────────────┬────────────────────────────────────┘
                           │
         ┌─────────────────┴──────────────────┐
         │                                    │
         ▼                                    │
┌────────────────────────┐                    │
│ 1. Set variables       │                    │
│    target_date = D−1   │                    │
└──────────┬─────────────┘                    │
           │                                  │
           ▼                                  │
┌────────────────────────┐                    │
│ 2. Notebook: generate  │                    │
│    csnp_retail.generate│                    │
│    --mode daily --date │                    │
└──────────┬─────────────┘                    │
           │                                  │
           ▼                                  │
┌────────────────────────┐                    │
│ 3. Notebook: bronze    │                    │
│    validation          │                    │
│    (row counts, schema)│                    │
└──────────┬─────────────┘                    │
           │                                  │
           ▼                                  │
┌────────────────────────┐                    │
│ 4. Notebook: silver    │                    │
│    MERGE from bronze   │                    │
└──────────┬─────────────┘                    │
           │                                  │
           ▼                                  │
┌────────────────────────┐                    │
│ 5. Notebook: gold dims │ (parallel-safe)    │
│    SCD2 handling       │                    │
└──────────┬─────────────┘                    │
           │                                  │
           ▼                                  │
┌────────────────────────┐                    │
│ 6. Notebook: gold facts│                    │
│    key resolution +    │                    │
│    append              │                    │
└──────────┬─────────────┘                    │
           │                                  │
           ▼                                  │
┌────────────────────────┐                    │
│ 7. Web activity:       │                    │
│    refresh semantic    │                    │
│    model               │                    │
└──────────┬─────────────┘                    │
           │                                  │
           ▼                                  │
┌────────────────────────┐                    │
│ 8. Notebook: DQ        │                    │
│    validation suite    │                    │
└──────────┬─────────────┘                    │
           │                                  │
           ▼                                  │
┌────────────────────────┐      Failure ──────┘
│ 9. Teams/Email notify  │◄────────────────── (any step)
│    status              │
└────────────────────────┘
```

**Key details:**

- Each notebook activity runs on a small Spark pool (4 cores, 16 GB). Daily volume doesn't justify more.
- Inter-activity dependencies: silver waits for bronze validation; gold dims wait for silver; gold facts wait for dims; model refresh waits for gold facts; DQ waits for model refresh.
- Failure mode: any step fails → immediate Teams alert with error detail and run link. Downstream steps skip. Pipeline status = failed.
- Retry: 2 automatic retries with 5-minute backoff per activity before failing.
- Late-arriving window: silver and gold both look back 3 days for MERGE purposes (not just target date).
- Pipeline outputs a `run_manifest.json` to `Files/pipeline_runs/{run_id}/` with row counts, timing, and validation results — useful for auditing.

### 8.8 Backfill strategy

Backfill is *not* the daily pipeline run 1,100 times. It's a separate one-time flow:

1. Generator writes all 3 years of bronze in one pass (runs in 20–30 min on M scale).
2. Silver notebook reads *all* bronze, MERGEs to silver (tests MERGE performance at scale).
3. Gold notebook builds dims (full SCD2 computed from silver), then builds facts.
4. Post-backfill validation runs.

After backfill completes, the daily pipeline takes over. The handoff day: backfill ends on the fictional calendar "yesterday", and the daily pipeline starts running for "today" onward.

### 8.9 Why the pipeline structure matters for Fabric POC

This structure isn't just a test fixture — it's also a realistic reference implementation that mirrors what Fabric POC's eventual customers will have. When demo'ing:

- "Fabric POC works with your existing medallion pipeline" — shown, not claimed
- "Fabric POC handles SCD2 correctly" — you can demo queries against historical snapshots
- "Fabric POC picks up daily changes automatically" — just wait a day, refresh the insight, watch new anomalies appear
- Daily DQ results become their own Fabric POC demo — point it at the `dq_results` table and generate insight dashboards about your own pipeline

---

## 9. Scale profiles

Single generator parameter `--scale={xs,s,m,l}`:

| Profile | Sales rows | Customers | Products | Stores | Sessions | Use case |
|---|---|---|---|---|---|---|
| XS | 100K | 5K | 600 | 15 | 180K | Dev loop, unit tests |
| S | 1M | 50K | 1,800 | 45 | 1.8M | Demo on laptop |
| M | 8M | 320K | 3,200 | 142 | 14M | Realistic Fabric test (default) |
| L | 50M | 1.5M | 6,000 | 380 | 90M | Direct Lake perf / capacity stress |

All 14 planted patterns exist at every scale. Daily incremental volume scales proportionally.

---

## 10. Visual coverage matrix

| Visual | Natural question | Fields involved |
|---|---|---|
| KPI cards | "Revenue this quarter?" | `fact_sales.net_revenue` |
| Line chart | "Monthly revenue over 3 years" | `dim_date.month`, `net_revenue` |
| Stacked area | "Channel mix over time" | `channel`, month, `net_revenue` |
| Bar (horizontal) | "Top 10 categories by revenue" | `category`, `net_revenue` |
| Column (vertical) | "Revenue by day of week" | `day_of_week_name`, `net_revenue` |
| 100% stacked | "Segment mix %" | `segment`, `net_revenue` |
| Treemap | "Dept → Category → Subcategory hierarchy" | 3-level product hierarchy |
| Sankey | "Traffic source → landing → action → conversion" | `fact_sessions` |
| Funnel | "Session → PDP → cart → checkout → order" | `fact_sessions` |
| Waterfall | "FY25 to FY26 revenue bridge" | Delta decomposition |
| Cohort heatmap | "Retention by acquisition month" | `acquisition_date`, subsequent orders |
| Geo filled map | "Revenue by state" | `dim_store.state`, `net_revenue` |
| Geo bubble map | "Top stores plotted" | `store.lat/long`, `net_revenue` |
| Heatmap (2D) | "Sessions by day × hour" | `session_start_time` |
| Scatter | "Price vs units, colored by category" | `unit_price`, `quantity`, `category` |
| Bubble | "Store sqft vs revenue vs margin" | `square_footage`, `net_revenue`, `gross_margin` |
| Small multiples | "Revenue trend per store grid" | `store`, `month`, `net_revenue` |
| Pareto | "80/20 of SKUs" | `product`, `net_revenue` |
| Gauge | "Target attainment %" | `net_revenue` vs `target_revenue` |
| Distribution / box | "AOV distribution by segment" | `segment`, `net_revenue / order` |
| Ribbon / bump | "Category rank over time" | `category`, `month`, rank |

---

## 11. DAX / measure library

Ships with the reporting layer so every Fabric POC-generated visual references named measures, not ad-hoc expressions.

**Revenue:** Total Revenue, Gross Revenue, Net Revenue, Discount $, Discount %, COGS, Gross Margin $, Gross Margin %, AOV, Units Sold.

**Time intelligence:** Revenue YoY, Revenue YoY %, Revenue MoM %, Revenue QoQ %, Revenue YTD, Revenue PYTD, Revenue LTM, Revenue Rolling 3M/12M, Same-Period-Last-Year.

**Customer:** Active Customers, New Customers, Returning Customers, Customer Retention %, Average LTV, CAC, LTV:CAC Ratio, NRR %, Churn %.

**Product:** SKU Count, Active SKU Count, Sell-through Rate, Return Rate %, Days of Supply, Stockout Days, Full-price Sell-through %.

**Channel/funnel:** Sessions, Conversion Rate, Bounce Rate, Add-to-cart Rate, Checkout Abandonment Rate.

**Store ops:** Sales per Sq Ft, Revenue per Store, Same-Store Sales Growth, BOPIS Attach Rate, Comparable Store Count.

All measures versioned in TMDL under `model/definition/tables/` per best practice.

---

## 12. Generator architecture (build plan)

When we write the generator:

- Python 3.11, single package `csnp_retail/`
- Entry: `python -m csnp_retail.generate`
- Modules: `products.py`, `customers.py`, `stores.py`, `sales.py`, `sessions.py`, `inventory.py`, `returns.py`, `loyalty.py`, `campaigns.py`, `marketing.py`
- Shared `patterns.py` — the 14 planted insights as reusable functions (`apply_tx_heat_event()`, `apply_meridian_cable_spike()`, `apply_signature_sizing_issue()`, etc.)
- Shared `faker_pools.py` — curated name pools, product name templates, color palettes, store naming patterns
- Output writer `io.py` — handles Parquet with proper compression and partitioning, writes to local filesystem OR ABFSS path (ADLS Gen2 / OneLake)
- Fully deterministic with a seed — same seed produces byte-identical output
- Generates `manifest.json` per run (seed, scale, date range, row counts, pattern activations)
- `--format=bronze|silver|gold` flag to output at different medallion layers directly (faster feedback during development)

**Estimated generator build:** One focused weekend of Claude Code work with `/effort xhigh` on Opus 4.7. Patterns module is the hard part; the rest is pandas/polars + Faker.

---

## 13. Open decisions (pick before generator build)

1. **Currencies.** Single USD, or multi-currency (USD/CAD/MXN/GBP)? Multi adds realism + an FX rates dim. *Recommendation: multi.*
2. **Timezone.** UTC-only, or keep `store_local_time` column? *Recommendation: both — UTC in fact tables, local time derivable from store dim.*
3. **SCD depth.** How many price changes per product avg? *Recommendation: 2–3 over 3 years.*
4. **PII realism.** Faker for names, or templated? *Recommendation: Faker for demos, templated mode as a `--lite-names` flag for bench tests.*
5. **Forecast horizon.** Future dates in `dim_date` only, or partial null-actuals? *Recommendation: dates only — avoid confusing forecast-vs-actual semantics.*
6. **Marketing attribution.** Single-touch or multi-touch? *Recommendation: single-touch at the fact level, multi-touch as a separate `fact_attribution_touchpoints` for advanced demos.*

---

## Appendix A — naming conventions

- Tables: `snake_case`, prefix `fact_` or `dim_`
- Columns: `snake_case`, foreign keys end in `_key`
- Surrogate keys: `BIGINT` monotonic
- Natural keys: human-readable, end in `_id`
- Date keys: `INT` in `YYYYMMDD`
- Measures (DAX): Pascal Case with spaces, e.g., `Net Revenue YoY %`

## Appendix B — references baked into data

- US Census name frequency (2020) for realistic names
- Natural Earth postal/metro/state/country geography
- NOAA historical weather for plausible heat/cold event dates
- Public holiday calendars for US/CA/MX/UK in `dim_date`

No real customer data is used anywhere.
