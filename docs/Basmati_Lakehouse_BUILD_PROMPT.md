I'm building a portfolio data engineering project to apply for new-grad Data Engineer / Data Platform roles (M.S. CS, RIT, May 2027). Help me build it step by step. Work on my Mac, keep costs at $0 (free tiers / local only), and fit it into about two weekends.

## Project: Basmati Market Lakehouse
A batch data pipeline that ingests Indian agricultural market (mandi) prices, models basmati rice prices and exports in a warehouse, enforces data quality, and answers a real question: **how did domestic basmati paddy and rice prices respond to India's basmati Minimum Export Price (MEP) changes in 2023–2024?**

Business context: I co-founded and ran an agri-input and commodity trading business in India for seven years (Garg Fertilizers). I will share the final results with them, so the output should also include a short plain-English summary that a trading or procurement team could use.

## Step 0: verify before building (do this first and report back)
Do not assume any API, dataset, or date exists. Check each one and tell me what you found:
1. **Agmarknet daily mandi prices on data.gov.in** ("Current Daily Price of Various Commodities from Various Markets (Mandi)"): endpoint, API key signup, rate limits, how far back history goes, fields. If the API serves only current data, find how to get history (agmarknet.gov.in reports, bulk downloads) and tell me the options.
2. **APEDA / DGCIS basmati export data**: monthly export volume and value by destination country. Is there a downloadable or scrapeable source?
3. **Open-Meteo historical weather API**: daily rainfall and temperature for basmati districts in Punjab, Haryana, and western Uttar Pradesh.
4. **MEP policy dates**: confirm from official sources (DGFT notifications or PIB press releases). My understanding, still to be verified: about $1,200/tonne set in August 2023, cut to $950 in October 2023, removed in September 2024. Record the source URL for each date.
If a source isn't usable, propose the closest alternative before writing code.

## Architecture (local-first, reproducible with one command)
- **Versions:** use current stable releases and verify each before pinning:
  - Airflow 3.x
  - Spark 4.x (PySpark)
  - dbt-core 1.8+ with dbt-duckdb
  - DuckDB 1.x
  - Apache Iceberg
  - GX Core 1.x
  Use **uv** for Python dependency management and **Ruff** for lint and format. In the README, note which features are new in these versions and why I used them.
- **Docker Compose:** Airflow 3, MinIO (S3-compatible object storage), Spark 4 in local mode, Postgres for Airflow metadata.
- **Ingestion (Airflow DAG `agmarknet_daily`):**
  - Pull **all commodities** for all states, so the pipeline has real volume.
  - Land raw JSON or CSV in MinIO under `raw/agmarknet/dt=YYYY-MM-DD/`.
  - Must be idempotent: re-running a day overwrites that day's partition with no duplicates.
  - Support a backfill of several years via Airflow catchup or a backfill command.
  - Retries with backoff; respect API rate limits.
  - Use **Airflow 3 assets** (data-aware scheduling): the ingestion task emits a `raw_agmarknet` asset, and the Spark and dbt DAGs are triggered by asset updates, not by fixed times.
- **Processing (Spark):**
  - Clean and deduplicate; cast types.
  - Normalize units (quintal vs. tonne, Rs/quintal).
  - Standardize state, district, and market names.
  - Write **Apache Iceberg** tables in MinIO, with a local REST or SQL catalog, partitioned by date.
  - Use `MERGE INTO` or partition overwrite so runs are idempotent.
  - In the README, show time travel and a schema-evolution example.
  - If Iceberg on local Spark 4 becomes a blocker, tell me and fall back to partitioned Parquet.
- **Late-arriving data:** detect revisions to already-loaded dates and reprocess only the affected partitions.
- **Warehouse and modeling:** dbt with DuckDB as the default target (free). Make a BigQuery free-tier target optional through a dbt profile.
  - Layers: `staging` → `intermediate` → `marts`.
  - DuckDB reads the Iceberg tables directly.
  - Enforce **model contracts** (column names and types) on all marts.
  - Add **dbt unit tests** for the variety mapping logic and the before/after-policy window calculation.
  - `dim_commodity`, plus a **basmati variety mapping**: a seed CSV that maps messy raw names (Pusa 1121, 1509, 1718, 1401, Traditional, Sella, Steam, paddy vs. rice) to a clean `variety_group` and `form` (paddy/rice).
  - **LLM-assisted mapping, with a human in the loop:**
    - A script collects unmapped raw variety names and asks an LLM (Claude API) for structured JSON suggestions: `variety_group`, `form`, `processing`, `confidence`, `reason`.
    - Validate the JSON against a schema, with retries.
    - Write the suggestions to `review/variety_suggestions.csv`.
    - **I approve or edit each row** before it is merged into the seed. Nothing goes in automatically.
    - Log what share of suggestions I accepted unchanged.
    - The pipeline itself never calls an LLM.
  - `dim_market` as **SCD Type 2** using dbt snapshots, to handle renamed or merged markets.
  - `dim_date`, and `dim_policy_event` (MEP dates, as a seed with source URLs).
  - `fct_daily_price` at a grain of market × commodity × variety × date, with min, max, and modal price and arrivals where available.
  - `fct_monthly_exports` (basmati exports by destination).
  - Marts: `mart_basmati_price_vs_policy` (average modal price 30/60/90 days before and after each MEP change, by variety group and state), and `mart_basmati_price_weather`.
- **Data quality:**
  - dbt tests: unique, not_null, relationships, accepted_values.
  - GX Core 1.x (Great Expectations) for:
    - price within plausible bounds
    - min ≤ modal ≤ max
    - freshness
    - row-count drift against a trailing 7-day average
    - unmapped basmati variety names (fail the run and list them)
  - Failures must fail the DAG and write a readable report.
- **Output:**
  - A small Streamlit dashboard with a price trend by variety with MEP event markers, a market map or table, and exports by destination.
  - `RESULTS.md`, a one-page plain-English summary for a trading or procurement team, with honest caveats (correlation, not causation; data gaps).
- **Engineering quality:**
  - Pytest for the ingestion and transform functions.
  - GitHub Actions CI: lint (ruff), tests, and `dbt build` on a small fixture dataset.
  - `.env.example`; no secrets committed.
  - Makefile targets: `up`, `backfill`, `run-day`, `test`, `dashboard`.
- **README:**
  - architecture diagram (Mermaid)
  - how to run
  - data sources with links
  - design decisions and tradeoffs (why DuckDB, the partitioning scheme, the SCD2 approach)
  - measured results

## Metrics to record (I will put these on my resume, so they must be real and measured)
- Rows ingested per day and in total
- Years backfilled
- Number of markets, commodities, and basmati varieties mapped
- End-to-end runtime of a daily run and of the full backfill
- Spark job runtime before and after any optimization
- Number of data-quality checks, and real issues they caught (examples)
- Number of raw variety names mapped, and the share of LLM suggestions I accepted unchanged
- The headline analysis result, with the numbers

## How to work with me
- Start with Step 0 and a short plan. Then build one component at a time: repo skeleton → ingestion → Spark → dbt → quality → dashboard → CI → README.
- Ask before installing anything or running long commands.
- Explain the design choices briefly as you go, because I need to defend them in interviews.
- Don't fabricate data, results, or dates. If something can't be verified, say so.
- Use small, meaningful git commits.
- At the end, give me:
  - a 2-line project description
  - 2 resume bullets built only from the measured metrics above
  - 5 likely interview questions about this project, with short answers
