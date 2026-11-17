# Design decisions (and the options I rejected)

Each entry is written so it can be defended in an interview: **Choice → Why → Rejected options → Honest tradeoff.**
Items marked *(to confirm)* are decided in principle and will be confirmed when that component is built.

---

## 1. Run locally with Docker Compose, not on a managed cloud service
- **Why:** $0 budget, one-command reproducibility (`make up`), and a reviewer can run the whole thing.
- **Rejected:** AWS MWAA / Glue / EMR, Databricks, GCP Composer. They're realistic for production but cost money per hour, and a reviewer can't reproduce them.
- **Tradeoff:** no real cluster scale. To keep it portable, every component speaks a cloud-standard interface: the S3 API (MinIO), the Iceberg REST catalog and a dbt profile. Moving to AWS means changing configuration, not rewriting code.

## 2. India egress: an EC2 SOCKS proxy in Mumbai, with the pipeline staying on the Mac
- **Why:** data.gov.in is geo-blocked outside India. The proxy changes only *where packets exit*, so the code path is identical in India and in the US (`AGMARKNET_PROXY` empty vs. set).
- **Rejected:**
  - *Run the whole stack on EC2:* a free-tier micro instance (1 GB RAM) can't run Airflow + Spark + Postgres + MinIO, and a larger one costs money.
  - *Lambda in ap-south-1 writing to S3:* a valid design and cheap. But it puts a second storage system and a second runtime in the project, and Lambda's 15-minute limit complicates multi-year backfills. **It's the best alternative.** I'd pick it if ingestion had to run unattended without the Mac.
  - *NAT Gateway:* about $30/month, which breaks the budget.
  - *Free VPN:* rarely has India servers, and is unreliable for multi-hour backfills.
  - *Third-party mirrors (Kaggle/CEDA):* avoids the block, but then I'm not ingesting the source, and licensing and freshness are unclear.
- **Tradeoff:** the Mac must be on during ingestion, and an SSH tunnel can drop. Retries with backoff cover short drops.

## 3. Orchestration: Airflow 3 with assets
- **Why:** Airflow is the scheduler most often named in data-engineering job descriptions. Version 3 adds **assets** (data-aware scheduling): the Spark DAG runs *because* `raw_agmarknet` was updated, not at a guessed time. That removes the "ingestion was late so the transform ran on empty data" class of bug.
- **Rejected:**
  - *Dagster:* asset-native and arguably cleaner, but it appears in fewer job descriptions.
  - *Prefect:* lighter, but has weaker backfill semantics.
  - *cron + scripts:* no retries, backfill, lineage or UI.
- **Tradeoff:** Airflow is heavy (scheduler + API server + DAG processor + Postgres) for a single-machine project. That's accepted because operating it is part of the skill being shown.

## 4. Storage: MinIO (S3-compatible)
- **Why:** it's the same API as AWS S3, so I hit the real problems (s3a configuration, path-style access, credentials, eventual listing).
- **Rejected:**
  - *Local filesystem:* hides every object-store issue.
  - *Real S3:* request and storage costs, and egress fees when Spark reads from my Mac.
- **Tradeoff:** MinIO doesn't reproduce S3 latency or throttling.
- **Update 2026-11-13: built from source.** MinIO stopped publishing community images in late 2025 and archived `minio/minio` and `minio/mc` on GitHub. `docker/minio/Dockerfile` compiles the last release (`RELEASE.2025-10-15T17-29-55Z`) in a multi-stage build: a Go builder stage, then a ~30 MB Alpine runtime with a non-root user and a health check. The tag is checked against its commit SHA, because tags can be moved and commits can't.
  - **Rejected:** RustFS (MinIO-compatible, but only reached 1.0 in Oct 2026), Garage (needs a layout/key init step), SeaweedFS (heavier S3 credential config).
  - **Tradeoff:** an archived project gets no security fixes. Acceptable for a localhost-only dev store (ports bound to 127.0.0.1). The pipeline only uses the S3 API, so swapping to RustFS/Garage or real S3 is a config change.

## 5. Raw zone: immutable source JSON under `raw/agmarknet/dt=YYYY-MM-DD/`, overwritten per day
- **Why:** the raw response is the audit trail and makes replay possible. If a transform bug is found, I re-run Spark without re-calling a rate-limited, geo-blocked API. Each day is one gzipped JSON object written with a single PUT, which S3/MinIO apply atomically, so a re-run replaces the day completely: same day in, same object out, no duplicates and no half-written partition. A `_manifest.json` (row count, content hash, source resource, API calls) is written last and acts as the commit marker. No temp-prefix-and-swap is needed because there is only one data object per day (~13k rows, well under 10 MB).
- **Rejected:**
  - *Write straight into tables:* no replay, and source quirks get lost.
  - *Append-only raw with dedup later:* storage grows without limit, and dedup logic moves downstream.
- **Tradeoff:** I keep one snapshot per day, so intra-day revisions before the final pull aren't kept.

## 6. Processing: Spark 4 (PySpark)
- **Why:** most mature engine for **writing** Iceberg (`MERGE INTO`, partition overwrite, schema evolution), plus job-description signal.
- **Rejected:** DuckDB / Polars / pandas for the transform.
- **Version: Spark 4.1.4, not 4.2/4.3.** Iceberg 1.12.0 (latest) publishes `iceberg-spark-runtime` only for Spark 4.0 and 4.1. The table format decides the engine version, not the other way round.
- **Deployment: local mode inside the Airflow image.** Java and PySpark are installed in the Airflow image and the job runs in the task process. No Spark master/worker containers to keep in sync; for real scale the same job would run with `spark-submit` against a cluster.
- **Honest tradeoff (say this in interviews):** I'll measure the volume. If it's tens of millions of rows or fewer, a single-node engine like DuckDB could do this transform faster on a laptop. I used Spark to demonstrate the distributed-write path (Iceberg commits, partitioning, tuning), and I'd keep it only if data or concurrency outgrew one node. The before/after optimisation metric must come from a real measured change: partition pruning, AQE, shuffle partitions, file sizing.

## 7. Table format: Apache Iceberg (fallback: partitioned Parquet)
- **Why:**
  - **Atomic commits** mean readers never see half-written partitions.
  - **MERGE / partition overwrite** makes runs idempotent.
  - **Time travel** lets me audit "what did the table say before the revision".
  - **Schema evolution** without rewriting files.
  - **Hidden partitioning** (`days(arrival_date)`).
  - It's engine-neutral: Spark writes and DuckDB reads.
- **Rejected:**
  - *Delta Lake:* comparable features, but historically centred on Databricks/Spark.
  - *Hudi:* optimised for streaming upserts, more operational knobs than a daily batch needs.
  - *Plain Parquet:* no atomicity or MERGE, so idempotency becomes my own delete-then-write logic.
- **Tradeoff:** more moving parts (a catalog, metadata files, compaction). If Spark 4 plus the Iceberg runtime versions don't line up, I fall back to Parquet and document it.

## 8. Iceberg catalog: REST catalog backed by Postgres
- **Why:** both Spark and DuckDB can attach to a REST catalog, so DuckDB reads the *catalog's* current snapshot rather than a hard-coded metadata file path. It reuses the Postgres container that Airflow already needs.
- **Rejected:**
  - *JDBC/SQL catalog:* Spark supports it, but DuckDB can't attach to it.
  - *Hive Metastore:* heavy and legacy.
  - *Hadoop/file catalog:* not safe for concurrent writers.
  - *Nessie / Polaris:* adds Git-like branching I don't need.

- **Confirmed 2026-11-17:** `apache/iceberg-rest-fixture:1.10.1` (the official Apache image). It only bundles the SQLite driver and starts with `java -jar`, so `docker/iceberg-rest/Dockerfile` adds the Postgres JDBC driver (checksum-verified) and launches the same main class with an explicit classpath. Table pointers live in a separate `iceberg` database on the stack's Postgres; data and metadata files live in MinIO.
  - **Rejected:** Lakekeeper (production-grade, but needs a bootstrap call, a warehouse-creation call and credential-vending config against MinIO: too many moving parts for a local project); Iceberg JDBC catalog straight from Spark (DuckDB cannot read a JDBC catalog, so dbt would need the latest metadata path by hand).
  - **Tradeoff:** the image is labelled a "fixture" (meant for tests). It has no auth. Acceptable on localhost; in production I'd use Polaris, Lakekeeper, Glue or Unity.
- **Spark reads the raw zone through S3A.** Instead of the ~550 MB AWS SDK bundle that `hadoop-aws` normally pulls in, the image adds only `s3-transfer-manager` and `apache-client` at the same SDK version (2.54.17) that `iceberg-aws-bundle` already contains, so Iceberg and S3A share one SDK.

## 9. Partitioning: by day of `arrival_date`; sort by commodity within the partition
- **Why:** the main access pattern and the late-data unit are both "a date", so a revision rewrites exactly one partition. Sorting by commodity gives good min/max pruning for basmati queries.
- **Rejected:**
  - *Partition by commodity or state:* about 300 commodities × days creates thousands of tiny files.
  - *Monthly partitions:* one late day would rewrite a whole month.
- **Tradeoff:** daily partitions are small, so periodic compaction (`rewrite_data_files`) is needed. That's another optimisation I can measure.

## 10. Late-arriving data: content hash per date
- **Why:** the source has no change feed. Each ingest stores a hash of each day's normalised payload, and when a re-pull of a past date changes the hash, only that date is reprocessed (Iceberg partition overwrite).
- **Rejected:**
  - *Full reprocess every run:* simple, but cost grows with history.
  - *Trust "today only":* misses revisions, which Agmarknet does make.
- **Tradeoff:** detection only happens for dates I re-pull, so there's a configurable look-back window (e.g. re-pull the last 7 days).

## 11. Warehouse: dbt + DuckDB (BigQuery as an optional profile)
- **Why:** free, in-process and columnar, so it's fast for analytics on a laptop. dbt brings lineage, tests, contracts, snapshots and documentation. The BigQuery profile shows that the models are warehouse-portable.
- **Rejected:**
  - *Postgres as the warehouse:* a row store, so slow for scans and aggregations.
  - *Snowflake:* the trial expires, which breaks reproducibility.
  - *BigQuery only:* needs a cloud account and a key to reproduce, and the sandbox has limits.
- **Tradeoff:** DuckDB is single-writer, so dbt runs can't overlap. Asset-driven scheduling serialises them anyway.

## 12. Modelling: staging → intermediate → marts, star schema, contracts + unit tests
- **Why:**
  - **Staging:** renaming and typing only.
  - **Intermediate:** business logic, e.g. the variety mapping.
  - **Marts:** stable outputs that consumers rely on.
  - **Contracts:** a renamed column fails at build time, not in the dashboard.
  - **Unit tests:** the two pieces of logic that decide the headline number (variety mapping and the before/after windows) are tested on hand-written inputs.
- **Rejected:** one big flat table (fine for a dashboard, but then the dimensions aren't reusable across the price, export and weather marts).

## 13. `dim_market` as SCD Type 2 (dbt snapshots)
- **Why:** markets get renamed or merged. Type 2 keeps the name that was valid on each date, so historical prices stay attributed correctly. Type 1 would silently rewrite history.
- **Honest tradeoff:** snapshots record changes *from the first run onward*. A backfill can't reconstruct renames that happened before the snapshot existed, so I'll document which history is reconstructed and which is observed.

## 14. Data quality: dbt tests and GX Core, deliberately split
- **dbt tests:** structural rules inside the warehouse (unique, not_null, relationships, accepted_values, contracts).
- **GX Core:** statistical and operational checks on the lake output *before* dbt runs (price bounds, min ≤ modal ≤ max, freshness, row-count drift vs. a 7-day average, unmapped varieties). It also produces readable HTML Data Docs that serve as the failure report.
- **Rejected:** only dbt (with dbt-expectations). Possible, but bad rows would reach the warehouse before the checks run, and there's no standalone report.
- **Tradeoff:** two tools with some overlap. The split is by *layer*, which is defensible.

## 15. LLM variety mapping: offline, human-approved, never in the pipeline
- **Why:**
  - **Determinism:** the same input always gives the same output.
  - **Reproducibility:** CI doesn't need an API key.
  - **Auditability:** every mapping is a reviewed row in a seed CSV.
  - **Cost:** the LLM runs only when new names appear.
  - The acceptance rate becomes an honest metric.
- **Rejected:**
  - *LLM call inside the DAG:* non-deterministic, and it fails when the API is down.
  - *Fuzzy matching only (rapidfuzz):* misses semantic cases like "Pusa Sugandh 4" → 1121, or Sella vs. Steam. It will be the **baseline** I compare the LLM against.

## 16. Dashboard: Streamlit
- **Why:** pure Python, reads DuckDB directly, about 150 lines of code.
- **Rejected:**
  - *Superset / Metabase:* more containers and more disk.
  - *Evidence.dev:* nice, but adds a Node toolchain.

## 17. Tooling: uv + Ruff
- **Why:** uv resolves and installs about 10–100× faster than pip and produces a lockfile (`uv.lock`) for exact pins. Ruff replaces flake8 + isort + black in one fast tool.
- **Rejected:** Poetry (slower, separate lock format) and pip-tools.

## 18. Weather: Open-Meteo at district centroids
- **Why:** free, no key, live call verified, and daily history.
- **Rejected:** IMD gridded rainfall (more authoritative, but binary grids and heavier processing).
- **Tradeoff:** reanalysis data at one point per district is an approximation of field conditions.

- **As built (2026-11-17):** the point is Open-Meteo's geocoder match for each district name (usually the district headquarters town), not a computed centroid; names the geocoder cannot match within the state are logged as unresolved, not guessed. Districts are chosen per run: the 40 GI-state districts with the most basmati price rows.

## 19. DuckDB 1.5.6, not 2.0
- **Why:** the spec says 1.x, and dbt-duckdb currently pins DuckDB 1.5.x in its extras. The Iceberg and REST-catalog extensions are proven on 1.x. 2.0 is brand-new, so I'd upgrade only after the pipeline works.

## 20. Dependency pinning: exact versions + a 14-day cooldown
- **Why:** reproducible builds, and protection against a freshly published malicious or broken release. `uv.lock` pins every Python package; `[tool.uv] exclude-newer` makes the resolver ignore anything published after a fixed date (moved forward deliberately). Docker base images are pinned to exact tags, and the MinIO source to a commit SHA.
- **Consequence:** Airflow is 3.3.2, not 3.4.0 (released 2026-11-06); bump after 2026-11-20.
- **Tradeoff:** security fixes arrive up to two weeks late unless I bump on purpose.

## 21. Silver combines repeat reports instead of picking one
- **Why:** measured on Nov 2026 data, about 10% of market x commodity x variety x grade keys are reported more than once a day with different prices (separate lots or sessions). Keeping the highest modal price biased prices upward. Silver now keeps one row per key with the lowest min, highest max and mean modal, plus `report_count`.
- **Rejected:** keeping every report (no natural key for the grain, and double-counting in averages); picking the first (non-deterministic order across pages).
- **Tradeoff:** the mean of modal prices is not a volume-weighted price; Agmarknet's API gives no quantities to weight by.

## 22. Quality thresholds come from the data, not from defaults
- Measured 2026-11-17: Sundays run at ~0.45x the trailing 7-day row count and Fridays reach ~2x, so drift bounds are 0.3x-3x (a +/-50% rule fails every Sunday). ~0.1-0.25% of rows carry another unit in the Rs/quintal column, so the price-bounds check uses `mostly=0.99`. Min/modal/max checks allow 1 paisa for source-side rounding.
- **Tradeoff:** looser bounds catch fewer small problems; they are re-tuned once the full backfill shows the 2015-2026 distribution.

## 23. API concurrency: 2 callers
- **Why:** each day costs ~1.05 s per 1,000 rows plus Airflow task overhead; one caller gave ~30 s/day (~36 h for 2015-2026). Two concurrent callers (Airflow pool `agmarknet_api` = 2) roughly halve that while keeping load on an undocumented-limit government API small; 429s are retried with backoff.

## 24. dbt-core 1.12, not dbt v2
- dbt now announces v2 as the default for new installations. The spec asks for dbt-core 1.8+, dbt-duckdb supports 1.12, and contracts/unit tests/snapshots are all 1.x features; I'd evaluate v2 after the pipeline is complete.

