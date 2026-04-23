# CSNP & Co. — Production Build Plan

**Brand:** CSNP & Co. · **Author:** Chiranjeevi Bhogireddy · **Date:** 21 Apr 2026
**Purpose:** Production-ready build plan for the CSNP & Co. synthetic retail dataset and pipeline. Designed to ship fast, stay elegant, and double as a LinkedIn showcase. Every choice here is grounded in the current state of Fabric tooling (as of April 2026).

---

## 1. Positioning — why this becomes a showcase, not just a test fixture

Before architecture, the positioning. What we're actually building has two audiences:

1. **Internal (you + Semantica):** A realistic, daily-updating Fabric dataset to test Semantica end-to-end.
2. **External (LinkedIn, Fabric community):** A reference implementation of modern Fabric CI/CD — AI-assisted authoring, Git-integrated workspaces, code-first deployment, daily orchestration, proper testing, every piece done right.

These two audiences don't conflict — they reinforce. The same repo that lets you test Semantica is also the best possible "Fabric developer does it right" showcase for your LinkedIn. The dataset is the byproduct; the *pipeline and developer workflow* are the story.

**What this becomes in 3–4 weeks:**
- Public GitHub repo: `csnp-retail-platform`
- README with architecture diagrams, screenshots, demo Loom
- 3–5 LinkedIn posts as you build, each showcasing a different piece (the generator, the MCP setup, the daily pipeline, Semantica running on top)
- A repo other Fabric engineers clone as a template
- Your personal signature: "Chiranjeevi built this. Here's how."

---

## 2. The tooling stack — everything verified working in April 2026

Two years ago this project would have needed custom Python, manual clicking, and fragile REST scripts. Today the ecosystem is sharp enough to ship it properly.

### 2.1 Fabric CLI (GA, September 2025)

`pip install ms-fabric-cli`. The Fabric CLI is now generally available — fully supported for production use, backed by Microsoft's SLA, and built to meet the security, compliance, and reliability standards customers expect.

Key v1.5 capabilities (March 2026):
- A new deployment command integrates the fabric-cicd library directly into the CLI, enabling full workspace deployments from a single command.
- Fabric CLI is pre-installed and pre-authenticated in PySpark notebooks — no setup, no login, no pip install. Just open a notebook cell, run !fab commands, and you're managing Fabric resources directly from your notebook.
- File-system metaphor (`fab ls`, `fab cd`, `fab cp`) — Claude Code handles this intuitively
- Service Principal auth with federated credentials for GitHub OIDC

### 2.2 fabric-cicd (officially Microsoft-backed, Feb 2026)

`pip install fabric-cicd`. fabric‑cicd — the open‑source Python deployment library for Microsoft Fabric — is now an officially supported, Microsoft‑backed tool for CI/CD automation across Fabric workspaces.

The core pattern is three lines of Python:

```python
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

target = FabricWorkspace(
    workspace_id="...",
    environment="prod",
    repository_directory="./fabric",
    item_type_in_scope=["Notebook", "DataPipeline", "Lakehouse", "SemanticModel"],
)
publish_all_items(target)
unpublish_all_orphan_items(target)
```

Parameterization via `parameter.yml` handles dev/test/prod differences (lakehouse IDs, connection strings, etc.).

### 2.3 Fabric Core MCP Server (Preview, April 2026)

Cloud-hosted MCP that AI agents like Claude Code connect to for real Fabric operations. Documented on Microsoft Learn (last updated 2026-04-13). Set up once with HTTP transport and Entra ID auth, and Claude Code can directly create workspaces, manage items, update definitions — inside your existing RBAC boundaries with audit logging.

### 2.4 Skills for Fabric (Microsoft, ongoing)

`github.com/microsoft/skills-for-fabric` — AI agent skills for Microsoft Fabric developers, data engineers, admins, and consumers. Optimized for GitHub Copilot CLI, with cross‑tool compatibility for VS Code Copilot, Claude Code, Cursor, Codex/Jules, and Windsurf.

Install via Claude Code's plugin system. Gives Claude Code expert-level knowledge of Fabric CLI commands, item definitions, and best practices without burning context tokens. Install scripts configure MCP setup automatically.

### 2.5 Git integration (native Fabric feature, supports Azure DevOps + GitHub)

Workspaces connect directly to a Git repo; items serialize to JSON/YAML/TMDL on disk. Our flow: develop in a dev workspace connected to `main` branch, fabric-cicd promotes to test and prod workspaces.

### 2.6 Great Expectations for data quality

`pip install great_expectations[fabric]` — Great Expectations ships a dedicated `fabric` extra. Used for data validation in the silver and gold layers and in DQ checkpoints post-deployment.

### 2.7 pytest for unit/integration testing

Classic Python testing for the generator. Delta-rs or local PySpark sessions for the transform notebooks. Great Expectations handles data quality; pytest handles code correctness.

### 2.8 Claude Code as the primary IDE

With Opus 4.7 on `/effort xhigh`. Uses the Fabric CLI, Fabric Core MCP, Skills for Fabric, and Claude Code's own MCP servers (GitHub, Context7 for docs, Brave Search) to author, test, and deploy the entire stack.

---

## 3. Repository structure

Single monorepo — `csnp-retail-platform`. Layout reflects the deployment model: everything Fabric-facing is under `fabric/` (Git-integrated with workspace), the generator is under `generator/`, and everything shared sits at the root.

```
csnp-retail-platform/
├── .github/
│   └── workflows/
│       ├── ci.yml                    # pytest + GX + generator smoke test on PR
│       ├── deploy-dev.yml            # auto-deploy main → dev workspace
│       ├── deploy-test.yml           # manual-trigger → test workspace
│       └── deploy-prod.yml           # tagged-release → prod workspace
├── .claude/
│   ├── CLAUDE.md                     # project instructions for Claude Code
│   └── skills/
│       ├── deploy/SKILL.md           # /deploy command
│       ├── run-daily/SKILL.md        # /run-daily command
│       └── smoke-test/SKILL.md       # /smoke-test command
├── .mcp.json                         # project-level MCP servers (Fabric Core, GitHub)
├── AGENTS.md                         # cross-tool agent instructions
├── README.md                         # architecture, demo, setup
├── ARCHITECTURE.md                   # deeper dive
├── CONTRIBUTING.md
├── LICENSE                           # MIT
├── pyproject.toml                    # uv-managed workspace root
├── uv.lock
├── parameter.yml                     # fabric-cicd parameterization
│
├── generator/                        # the synthetic data generator
│   ├── pyproject.toml
│   ├── src/csnp_retail/
│   │   ├── __init__.py
│   │   ├── cli.py                    # Typer CLI: generate backfill | daily
│   │   ├── config.py                 # Pydantic settings
│   │   ├── manifest.py               # state management across daily runs
│   │   ├── io.py                     # Parquet writers, ABFSS handling
│   │   ├── faker_pools.py            # name/color/product templates
│   │   ├── patterns.py               # 14 planted insight functions
│   │   ├── entities/                 # one module per domain entity
│   │   │   ├── products.py
│   │   │   ├── customers.py
│   │   │   ├── stores.py
│   │   │   ├── sales.py
│   │   │   ├── sessions.py
│   │   │   ├── returns.py
│   │   │   ├── inventory.py
│   │   │   ├── loyalty.py
│   │   │   ├── campaigns.py
│   │   │   └── marketing.py
│   │   └── validators.py             # post-generation sanity checks
│   └── tests/
│       ├── unit/                     # fast, no-IO tests per module
│       ├── integration/              # full generator runs at XS scale
│       └── patterns/                 # verify each planted pattern is detectable
│
├── fabric/                           # Git-integrated with Fabric workspace
│   ├── .platform                     # Fabric workspace metadata
│   ├── CSNP_Bronze.Lakehouse/
│   ├── CSNP_Silver.Lakehouse/
│   ├── CSNP_Gold.Lakehouse/
│   ├── notebooks/
│   │   ├── nb_01_generate.Notebook/  # calls csnp_retail daily
│   │   ├── nb_02_bronze_validate.Notebook/
│   │   ├── nb_03_silver_merge.Notebook/
│   │   ├── nb_04_gold_dims.Notebook/
│   │   ├── nb_05_gold_facts.Notebook/
│   │   ├── nb_06_dq_checkpoint.Notebook/
│   │   └── nb_99_backfill.Notebook/  # one-time historical load
│   ├── pipelines/
│   │   └── pl_daily_load.DataPipeline/
│   ├── semantic_models/
│   │   └── CSNP_Retail_Model.SemanticModel/
│   │       ├── definition/
│   │       │   ├── model.tmdl
│   │       │   ├── database.tmdl
│   │       │   ├── cultures/
│   │       │   ├── expressions/
│   │       │   └── tables/           # one TMDL file per dim and fact
│   │       └── definition.pbism
│   └── environments/
│       └── csnp_env.Environment/     # custom Python env pinning versions
│
├── dq/                               # Great Expectations project
│   ├── great_expectations.yml
│   ├── expectations/                 # expectation suites per table
│   ├── checkpoints/                  # checkpoint definitions
│   └── plugins/
│
├── deploy/
│   ├── deploy.py                     # fabric-cicd wrapper script
│   └── smoke_test.py                 # post-deploy validation via executeQueries
│
├── docs/
│   ├── DESIGN.md                     # the prior design doc, updated
│   ├── SETUP.md                      # one-page setup guide
│   ├── DAILY_RUN.md                  # operations runbook
│   ├── TESTING.md                    # test strategy
│   └── assets/                       # screenshots, ERDs, videos
│
└── scripts/
    ├── bootstrap.sh                  # one-shot local setup
    ├── register-mcp.sh               # configure Fabric MCP for Claude Code
    └── tear-down.sh                  # clean up Fabric workspaces (test env)
```

**Why this structure matters:**
- `fabric/` lives Git-integrated with the workspace, so Fabric CLI/fabric-cicd handle it natively
- `generator/` is a standalone installable Python package — you can `pip install` it in notebooks or tests
- `.claude/` commits project-specific Claude Code behaviors, shared with anyone who clones the repo
- `dq/` keeps data quality separate from code quality
- CI/CD, docs, and deploy scripts are first-class citizens

---

## 4. The three environments

Textbook three-environment setup — dev for active development, test for integration validation, prod for "always-on" daily runs.

| Workspace | Purpose | Capacity | Git-connected | Auto-deploy |
|---|---|---|---|---|
| `CSNP_Dev` | Your daily development | F2 (paused off-hours) | Yes — `main` branch | On push |
| `CSNP_Test` | Integration + DQ | F2 (paused off-hours) | No — deployed via fabric-cicd | On release tag |
| `CSNP_Prod` | Live daily pipeline | F2 (always on) | No — deployed via fabric-cicd | On version bump |

Only Dev is Git-connected. Test and Prod receive deployments from fabric-cicd. This is the standard pattern: only the development workspace is connected to Git; higher environments (test, prod) are not connected to Git. Deployments to these environments are managed through the fabric-cicd library.

**Cost reality.** Running three F2 SKUs 24/7 is ~$780/month. For a personal project, that's a lot. The realistic setup: Dev and Test paused 22 hours/day via Azure automation (~$50/month combined), Prod always on (~$260/month). Total ~$310/month. If that's still too much, run only Dev + Prod (drop Test and test in Dev itself) — ~$290/month. Or use Fabric Trial for the first 60 days and make the decision when it expires.

---

## 5. Authentication architecture

Not glamorous but critical to get right up front. Three identities:

**Identity 1: Your personal Entra account.**
- Used for initial workspace creation, Fabric admin tasks, interactive development
- Claude Code uses this via `fab auth login` (device flow) for exploratory work

**Identity 2: Service principal `csnp-ci-sp`.**
- Used by GitHub Actions for CI/CD deployments
- Minimum permissions: Contributor on all three workspaces, Fabric API access
- Stored as GitHub OIDC federated credential (no long-lived secrets)

**Identity 3: Service principal `csnp-runtime-sp`.**
- Used by the Prod pipeline to run daily
- Minimum permissions: Contributor on `CSNP_Prod` only, OneLake data contributor
- Credentials in Fabric Azure Key Vault, referenced via connections

Setting this up once takes about 30 minutes in the Entra portal. It's the one thing Claude Code cannot do for you — flipping Azure admin switches requires your fingers on the keyboard (or an Azure admin in your org).

---

## 6. The daily pipeline — elegant version

Same flow as the earlier design, now with the right tools plugged in:

```
┌─────────────────────────────────────────────────────────────────────┐
│  Fabric Data Factory Pipeline: pl_daily_load                        │
│  Trigger: daily at 02:00 UTC (post-close)                           │
│  Runtime: ~8-12 min for M scale                                     │
└────────────┬────────────────────────────────────────────────────────┘
             │
             ▼
 ┌───────────────────────────┐
 │ 1. Set variables           │
 │    target_date = UTC NOW−1 │
 └───────────┬────────────────┘
             │
             ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ 2. Notebook: nb_01_generate                                    │
 │    %pip install csnp-retail==$(version from env)               │
 │    from csnp_retail.cli import daily_run                       │
 │    daily_run(date=target_date, out=onelake_bronze_path,        │
 │              scale='m', seed_file='manifest.json')             │
 └───────────┬────────────────────────────────────────────────────┘
             │
             ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ 3. Notebook: nb_02_bronze_validate                             │
 │    GX checkpoint runs against bronze files                     │
 │    Row count bounds, schema, null rates                        │
 └───────────┬────────────────────────────────────────────────────┘
             │
             ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ 4. Notebook: nb_03_silver_merge                                │
 │    Read bronze + 3-day late-arrival window                     │
 │    MERGE into silver Delta tables                              │
 │    Dedup, type-cast, UTC-normalize, identity-resolve           │
 └───────────┬────────────────────────────────────────────────────┘
             │
             ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ 5. Notebook: nb_04_gold_dims                                   │
 │    SCD2 handling for dim_product and dim_customer              │
 │    Assign surrogate keys via key-ledger Delta table            │
 └───────────┬────────────────────────────────────────────────────┘
             │
             ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ 6. Notebook: nb_05_gold_facts                                  │
 │    Resolve FK lookups (date-aware for SCD2)                    │
 │    APPEND facts into gold partitioned Delta tables             │
 └───────────┬────────────────────────────────────────────────────┘
             │
             ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ 7. Web activity: reframe semantic model                        │
 │    POST /workspaces/{id}/semanticModels/{id}/refreshes         │
 │    (Direct Lake: seconds, not minutes)                         │
 └───────────┬────────────────────────────────────────────────────┘
             │
             ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ 8. Notebook: nb_06_dq_checkpoint                               │
 │    GX checkpoint against gold tables                           │
 │    executeQueries smoke test: 10 canonical DAX queries         │
 │    Write results to dq_results Delta table                     │
 └───────────┬────────────────────────────────────────────────────┘
             │
             ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ 9. Teams webhook: notify status + link to pipeline run         │
 └───────────────────────────────────────────────────────────────┘

 On any failure: 2 retries with 5-min backoff → Teams alert → skip downstream
```

Each notebook activity runs on a 4-core / 16 GB Spark pool. The `run_manifest.json` written to `Files/pipeline_runs/{run_id}/` captures row counts, timing, DQ results — auditable, debuggable, and itself a dataset Semantica can point at.

---

## 7. Testing strategy — three layers

Production-ready means proper testing. Three distinct layers:

### 7.1 Unit tests (pytest, run in CI on every PR)

Scope: every function in the generator. No Spark, no Fabric, no IO beyond temp files.

```
tests/unit/
├── test_products.py        # SKU generation correctness
├── test_patterns.py        # each planted pattern function in isolation
├── test_manifest.py        # state handoff between daily runs
├── test_faker_pools.py     # name distribution, uniqueness
└── test_io.py              # Parquet round-trip
```

Target: <30 seconds total. Run on every commit via GitHub Actions.

### 7.2 Integration tests (pytest, run nightly)

Full generator run at XS scale, then assertions:

```
tests/integration/
├── test_backfill_xs.py     # runs full 3-year backfill at XS, checks totals
├── test_daily_append.py    # runs 10 consecutive daily appends, validates continuity
└── test_determinism.py     # same seed → identical output bytes
```

Target: <5 minutes. XS scale = 100K rows.

### 7.3 Pattern verification tests (pytest, run nightly)

The most important and most Semantica-specific layer: verify each planted insight is detectable in the generated data. If generator changes break a pattern, the demo script breaks.

```
tests/patterns/
├── test_texas_heat_event.py      # TX outerwear drops 55% in window
├── test_cohort_bfcm_degradation.py  # BFCM cohort retention verified lower
├── test_sizing_issue.py          # Signature Field pants return >20%
├── test_viral_style_spike.py     # Meridian cardigan 9× spike in July
├── test_channel_mix_shift.py     # online goes 24%→41%
└── ... one test per pattern
```

Each test runs: generate data → run a simple aggregation → assert the pattern is present within tolerances. If Semantica's LLM layer can't find the pattern, either the data broke or the detection layer broke — either way, known.

### 7.4 Data quality (Great Expectations, run in pipeline)

Different from code tests — these run *in* the pipeline against *live* data.

```
dq/expectations/
├── bronze/
│   ├── pos.json            # row count, column presence, date ranges
│   ├── ecom.json
│   └── ...
├── silver/
│   ├── silver_sales.json   # deduplicated, no orphan FKs, type correctness
│   └── ...
└── gold/
    ├── fact_sales.json     # partition presence, measure reasonableness
    └── ...
```

Expectation suites are JSON, versioned in Git, executed as checkpoints in the pipeline. Great Expectations has a dedicated `fabric` extra (`pip install great_expectations[fabric]`).

### 7.5 Contract tests (post-deploy smoke)

The final layer: after any deploy to test or prod, the CI pipeline runs 10 canonical DAX queries via `executeQueries` and validates results are non-empty and plausibly valued. If these fail, deployment is rolled back automatically.

This is what makes CI/CD safe. Every deployment is automatically validated.

---

## 8. The week-by-week build plan

Four weeks, front-loaded. The first week delivers the most value — everything after is refinement and polish.

### Week 1 — the generator (alone, local)

**Goal.** Run `python -m csnp_retail generate --mode backfill --scale s --out ./data/` on your laptop and get 3 years of believable apparel retail data across 10+ Parquet files.

**Day 1–2.** Project skeleton. Claude Code scaffolds the uv-managed project, pyproject.toml, Typer CLI, Pydantic config, pytest harness, CI workflow. Generates `dim_date`, `dim_geography`, `dim_store`, `dim_campaign` — the simple dims. Nothing with planted patterns yet.

**Day 3.** `dim_product` and `dim_customer` with SCD2 mechanics. `faker_pools.py` with curated name/color/product lists. Unit tests for each.

**Day 4.** `fact_sales` generator — the hardest module. Grain, pricing, discounting, order-line structure. No patterns yet, just the mechanics. Unit tests for correctness.

**Day 5.** `fact_sessions`, `fact_returns`, `fact_inventory_daily`, `fact_loyalty_events`, `fact_marketing_spend`. These are structurally similar to sales.

**Day 6–7.** `patterns.py` — implement all 14 planted patterns as composable functions applied *after* the base data generates. Pattern verification tests.

**End of Week 1 deliverable.** The generator works locally, produces deterministic output, has unit + pattern tests passing in CI, runs XS scale in <30 seconds.

**LinkedIn post #1.** "Building a realistic synthetic retail dataset — here's the generator design. 14 planted insight patterns, deterministic, 100K to 50M rows from one flag." Screenshot of CLI run + a sample dashboard from `pandas.describe()`.

### Week 2 — Fabric workspace + silver + gold

**Goal.** Push Week 1's bronze Parquet to OneLake, transform through silver into gold, have a Direct Lake semantic model answering DAX queries.

**Day 8.** Manual one-time setup: create three Fabric workspaces, create service principals, wire GitHub OIDC federated credentials, connect `CSNP_Dev` to GitHub main branch. Document in `SETUP.md`. This is the step Claude Code can't fully automate.

**Day 9.** Configure Fabric Core MCP in Claude Code via VS Code's MCP: Add Server → HTTP. Install Skills for Fabric via `/plugin install fabric-authoring@fabric-collection`. Test: ask Claude Code "list my workspaces" — it answers from MCP. Write `.claude/CLAUDE.md` with project-specific conventions.

**Day 10–11.** Silver notebooks. Claude Code authors `nb_03_silver_merge` directly in the Dev workspace via MCP, including MERGE patterns, timezone normalization, identity resolution, and schema-drift handling. Tests via small synthetic bronze fixtures.

**Day 12–13.** Gold notebooks. SCD2 handling for dims, surrogate-key ledger, fact loading with late-arrival window. This is where the semantic model work starts — TMDL files, measures, hierarchies.

**Day 14.** First end-to-end run: backfill bronze → silver → gold, open the semantic model in Power BI, verify measures calculate correctly, run DAX queries through `executeQueries`.

**End of Week 2 deliverable.** Dev workspace has full 3-year backfill loaded, semantic model works, canonical DAX queries return the expected values.

**LinkedIn post #2.** "From zero to a working Fabric medallion pipeline with Claude Code + Fabric MCP. Here's the flow." Include the architecture diagram and a short Loom of Claude Code authoring notebooks through MCP.

### Week 3 — daily pipeline + CI/CD + tests

**Goal.** Automated daily runs in Prod, every PR goes through CI, fabric-cicd deploys Dev → Test → Prod.

**Day 15.** Data Factory pipeline. Claude Code generates `pl_daily_load.DataPipeline` as JSON in the repo. Deploys via fabric-cicd. Triggers at 02:00 UTC.

**Day 16.** Data Quality layer. Great Expectations project set up. Expectation suites for bronze and gold. Checkpoints in the pipeline. DQ results written to Delta table (the "Semantica monitors itself" story).

**Day 17.** GitHub Actions. `ci.yml` runs pytest + GX + generator smoke on every PR. `deploy-dev.yml` auto-deploys main → Dev workspace. `deploy-test.yml` and `deploy-prod.yml` for manual + tagged releases. Use fabric-cicd throughout.

**Day 18.** Contract tests. Post-deploy smoke script runs 10 canonical DAX queries. Rollback on failure.

**Day 19.** Backfill to Prod. One-time backfill notebook (`nb_99_backfill`) runs 3 years of history. Verify.

**Day 20.** First daily pipeline run in Prod. Watch it. Fix any environment-specific issues. Confirm the fictional calendar advances correctly on Day 2.

**Day 21.** Documentation day. Write `ARCHITECTURE.md`, `DAILY_RUN.md`, `TESTING.md`. Record the demo Loom.

**End of Week 3 deliverable.** Public repo, green CI, Prod running daily, all three environments working.

**LinkedIn post #3.** "Full Fabric CI/CD with fabric-cicd, GitHub Actions, and Great Expectations. Three environments, daily runs, contract tests, automated rollback. Here's the repo." Link to the public GitHub repo.

### Week 4 — Semantica integration + polish

**Goal.** Point Semantica at the Prod semantic model. Generate insight dashboards. Record the demo.

**Day 22–23.** Semantica integration. Connect to `CSNP_Retail_Model`. Generate dashboards for each of the 14 planted patterns. Document which ones Semantica's pipeline catches automatically vs. which need prompting. This is the most valuable internal exercise — it tells you where Semantica needs work.

**Day 24.** Build the "editorial" dashboard from earlier mockup against live data. This becomes the hero screenshot.

**Day 25.** Demo Loom. 5–7 minute walkthrough: prompt → Semantica generates insight dashboard from live Fabric data → contrast with native Power BI equivalent. This goes to Shreyas *and* goes on LinkedIn.

**Day 26–27.** LinkedIn campaign. 3 posts across 2 weeks:
  - Post 4: "I pointed Semantica at a real Fabric semantic model running daily. Here's what the output looks like." (Hero dashboard screenshot)
  - Post 5: "Most AI BI tools hallucinate insights. Semantica runs actual statistical analysis, uses the LLM only for narration. Here's a pattern buried in our data that it found without being told." (The Meridian Cardigan or Signature Sizing story)
  - Post 6: "Everything I built for this is open source. Fork the repo, run it on your own Fabric tenant." (Link to CSNP repo)

**Day 28.** Slack, retrospective, plan Phase 2.

**End of Week 4 deliverable.** Semantica demo for Shreyas. Public repo with stars. LinkedIn posts getting traction.

---

## 9. How Claude Code actually drives this build

The tools are the boring part. The interesting part is how Claude Code's role evolves across the weeks.

**Week 1 (generator).** Claude Code works *autonomously* with `/effort xhigh`. You give it design sections of this doc as context, it writes modules, runs tests, iterates. Almost no Fabric interaction yet — pure Python. Expect 4–6 focused sessions of 30–60 minutes.

**Week 2 (Fabric).** Shift to *MCP-driven* mode. Claude Code uses Fabric Core MCP to directly inspect the Dev workspace, create items, update definitions. It's the difference between writing a notebook locally and syncing vs. authoring it in the workspace itself. The Skills for Fabric library gives Claude Code the knowledge it needs. Expect 6–8 sessions.

**Week 3 (CI/CD).** Back to code, but with more context. Claude Code writes GitHub Actions YAML, fabric-cicd scripts, GX expectations. Each one is straightforward on its own; the integration is where care matters. Expect 5–7 sessions.

**Week 4 (integration).** More hands-on. This is where you sit next to Claude Code and iterate on the Semantica prompts, the dashboard aesthetics, the LinkedIn post wording. Less autonomous execution, more creative back-and-forth. Expect 8–10 shorter sessions.

**Total estimate.** 25–35 focused Claude Code sessions across 4 weeks. At 30–60 min each, that's roughly 15–30 hours of your actual focused time. The rest is waiting (Fabric provisioning, pipeline runs), reviewing, deciding.

---

## 10. The `.claude/CLAUDE.md` that makes this work

Critical file. This is what Claude Code reads at the start of every session. It should be ~200 lines of sharp project context:

```markdown
# CSNP & Co. — Project Instructions for Claude Code

## What this project is
A synthetic retail dataset for Microsoft Fabric, used to test Semantica end-to-end.
Also serves as a public reference implementation of modern Fabric CI/CD.

## Architecture at a glance
[summary of sections 2, 3, 4 of this doc]

## How we work
- Python 3.12, uv for packages, ruff for linting, pytest for tests
- Every change goes through a PR. CI must pass before merge.
- Generator is pure Python. Notebooks use PySpark for transforms.
- Fabric items are Git-integrated (Dev) or deployed via fabric-cicd (Test/Prod).

## The 14 planted patterns
[list with one-line descriptions]
Never modify these without updating tests/patterns/.

## Naming conventions
- snake_case for everything Python
- PascalCase for DAX measures
- fact_* and dim_* prefixes for tables
- CSNP_ prefix for Fabric artifacts

## Do / don't
- DO use fabric-cicd for all Test/Prod deployments
- DO run pytest before committing
- DO write a pattern test for every new planted pattern
- DON'T hardcode workspace IDs — use parameter.yml
- DON'T bypass the MERGE pattern in silver — we rely on idempotency
- DON'T use a notebook where a skill would do

## Tools available
- Fabric CLI (fab) — installed globally
- Fabric Core MCP — for live workspace inspection
- Skills for Fabric — fabric-authoring collection
- GitHub MCP — for PRs, issues, releases
- Context7 — for up-to-date docs
- Brave Search — for research

## Critical references
- docs/DESIGN.md — full schema and pattern spec
- docs/SETUP.md — auth and environment setup
- fabric/semantic_models/CSNP_Retail_Model.SemanticModel/definition/ — measures live here
```

Plus `.claude/skills/` for `/deploy`, `/run-daily`, and `/smoke-test` slash commands.

---

## 11. The LinkedIn strategy — what actually gets engagement

You have a portfolio of bets. The LinkedIn ROI on this one is particularly high because:

1. The Fabric community is *small* and *active*. A well-built open-source Fabric reference implementation gets noticed by MVPs, Microsoft CATs, and hiring managers.
2. "AI-assisted Fabric development" is a new, underexplored topic. You're ahead of the curve.
3. The screenshots are visually distinctive — editorial dashboard + clean repo + CI green badges = scroll-stopping.

Six posts over 4–5 weeks, spaced carefully:

1. **The design tease.** Design doc screenshot, "I'm building a production Fabric reference implementation. Here's the design." Establishes credibility, builds anticipation.
2. **Generator done.** Python + tests + deterministic patterns. "Here's how to make synthetic data actually interesting." Technical crowd responds to the craft.
3. **Fabric + Claude Code.** The MCP setup, Claude Code authoring notebooks live. "Fabric engineering with AI agents — this is the new workflow." MVP-bait.
4. **CI/CD is live.** fabric-cicd, GH Actions, GX checkpoints. "Three environments, daily runs, automated rollback. All open source." Enterprise architects respond.
5. **Semantica on top.** The editorial dashboard, live from Prod. "This is what AI-native analytics looks like." Your actual product gets seen.
6. **The whole thing, packaged.** "Everything is public. Fork the repo." The repo URL in the post. Long-tail inbound.

Post #5 is the one that matters most for Semantica commercially. Posts #1-#4 build the audience and credibility so that post #5 lands on prepared ground rather than cold.

---

## 12. The risks and how to handle them

*Fabric capacity cost.* Addressed in §4 — pause Dev/Test off-hours, trial first 60 days, downgrade to Dev+Prod if needed.

*Service principal permissions.* The most common thing that blocks a build for half a day. Fix: do §5 on Day 1 before any code work. Test that the SP can actually create items in the workspace.

*fabric-cicd item coverage.* It doesn't support *every* Fabric item type yet. Check the compatibility matrix before building — if something critical isn't supported, plan a workaround (e.g., use REST API directly for that item).

*MCP auth instability.* Fabric Core MCP is Preview. Tokens expire, servers hiccup. Claude Code has auto-reconnect with exponential backoff, but worst case you `/mcp` re-auth. Not a blocker, just friction.

*LinkedIn timing.* If you post #1 and only 50 people see it, don't despair. The 2k-impressions post you'll write in 3 months won't happen without the 50-impressions post now. Consistency beats virality.

*Scope creep.* Tempting to add features. Rule: if it's not in this doc, it doesn't ship in the first 4 weeks. Create a `v2.md` and park everything else there.

---

## 13. What gets decided this week before we start

Six small decisions that determine the shape of Week 1:

1. **Repo name.** `csnp-retail-platform`? `csnp-fabric-template`? `csnp-co-retail`? (My pick: `csnp-retail-platform`.)
2. **License.** MIT? Apache-2? (My pick: MIT — maximal reuse.)
3. **Scale default.** Build against M (8M rows) or S (1M)? (My pick: M — more realistic, a bit slower to iterate.)
4. **Multi-currency.** Yes or no? (My pick: yes — more realistic, adds an FX rates dim.)
5. **Faker names vs. templated.** (My pick: Faker by default, `--lite-names` flag for perf tests.)
6. **Private vs. public repo.** Start private until Week 3, then flip? Or public from Day 1? (My pick: public from Day 1 — the public commit history *is* part of the story.)

If all six picks sound right, we can start Monday.

---

## Appendix A — key links verified April 2026

- Fabric CLI docs: `microsoft.github.io/fabric-cli`
- Fabric CLI repo: `github.com/microsoft/fabric-cli`
- fabric-cicd repo: `github.com/microsoft/fabric-cicd`
- fabric-cicd docs: `microsoft.github.io/fabric-cicd`
- Skills for Fabric: `github.com/microsoft/skills-for-fabric`
- Fabric Core MCP Quickstart: `learn.microsoft.com/en-us/rest/api/fabric/articles/mcp-servers/core-remote/get-started-core`
- Claude Code MCP docs: `code.claude.com/docs/en/mcp`
- Claude Code Skills docs: `code.claude.com/docs/en/skills`
- Great Expectations Fabric: `pip install great_expectations[fabric]`

## Appendix B — a realistic hourly budget

For someone who already works 40+ hours/week:

- Weeks 1–2: 5–7 hours per week (weekday evenings + Saturday morning)
- Week 3: 7–9 hours (more deployment friction)
- Week 4: 4–6 hours (mostly polish and posts)

Total: ~25 hours focused time over 4 weeks. If you're busier, stretch to 6 weeks — don't compress to 3.
