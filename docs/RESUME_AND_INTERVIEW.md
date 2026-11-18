# Resume and interview notes

Every number here is measured (see [RESULTS.md](../RESULTS.md)).

## Two-line description

Basmati Market Lakehouse: a batch lakehouse (Airflow 3, Spark 4.1, Apache Iceberg, dbt + DuckDB,
Great Expectations) over 12 years of Indian mandi prices (55M rows), refreshed daily in GitHub
Actions, with a live Streamlit dashboard and a calibrated weekly price forecast.
Live: https://basmati-market.streamlit.app · Code: https://github.com/AGMatrix/basmati-market-lakehouse

## Resume bullets (pick 3-4)

- Built an asset-driven lakehouse (Airflow 3, Spark 4.1, Apache Iceberg on S3-compatible storage) that backfilled 55.4M agricultural price records (4,339 days, 8,325 markets) in 4,330 runs with 0 failures, using a 7-day look-back, content-hash change detection and atomic per-day Iceberg overwrites.
- Automated a daily cloud refresh in GitHub Actions (about 4 min, $0 compute) that starts an AWS proxy for a geo-blocked API, re-runs the same Spark, dbt and quality-gate code on an S3 "lite lake", and publishes to a live dashboard only when all 64 dbt checks and the Great Expectations gate pass. Output is verified identical to the full warehouse.
- Modeled a dbt + DuckDB star schema with enforced contracts, unit tests and an SCD2 market dimension. Diagnosed full-scale failures (out-of-memory kills, ephemeral-port exhaustion, duplicate keys, a full disk) to get a 113 s rebuild of 55M rows.
- Trained a gradient-boosting price forecaster (scikit-learn, quantile loss) across 49 rice series, retrained daily. On 2024–2026 data it had never seen, it beat a no-change baseline at every 1–4 week horizon (5.01% vs 5.25% MAPE), with a conformally calibrated 80% interval covering 79.9% of outcomes.
- Designed a human-in-the-loop LLM step (Claude, typed structured output) that maps messy product names. 97.2% of 72 suggestions were accepted unchanged. Replacing a spelling list with a pattern recovered 18,142 records the quality gate had missed.

**Skills line:** Python, SQL, Apache Airflow, Apache Spark, Apache Iceberg, dbt, DuckDB, Great Expectations, scikit-learn, AWS (S3, EC2, IAM), GitHub Actions, Docker, Streamlit

## Interview questions

**1. How do you handle late-arriving or revised data?**
Each day is stored in raw as one object, together with a manifest that records a content hash
of the rows. Every daily run re-pulls the last 7 days, and only days whose hash changed are
rewritten and passed downstream; the changed dates travel in the Airflow asset event. Silver
replaces exactly those days with Iceberg `overwrite(condition)`, one atomic commit, so readers
never see a half-replaced day and re-runs are idempotent.

**2. Why Spark and DuckDB, and isn't Spark overkill?**
At about 10–21k rows a day, DuckDB alone could run the transform, and I say so in the README.
Spark is there to show the distributed write path: partitioned Iceberg commits and schema
evolution, which matter at larger scale. DuckDB is the warehouse because it's free and
in-process, reads Iceberg through the same REST catalog, and makes CI trivial. Its limit is a
single writer, so dbt runs are serialised by asset scheduling and the dashboard reads a
separately published copy.

**3. Tell me about something that broke.**
After the 12-year backfill, dbt was killed with SIGKILL, which meant out of memory. DuckDB
defaults to 80% of RAM, dbt ran 4 threads, and the staging layer was a view, so every test
re-scanned 55M rows from S3. I capped DuckDB at 3 GB (it spills to disk beyond that) and
materialised staging, so the lake is read once per build. The full build then took 113 s.
Separately, 62 Spark runs had failed with "Cannot assign requested address": local-port
exhaustion from many short S3 connections. I reconciled raw days against silver days, found
65 missing, re-ran just those (safe, because writes are idempotent), and enabled TIME_WAIT
port reuse.

**4. How do you make sure bad data doesn't reach the dashboard?**
There are two gates. After Spark, GX checks the rebuilt days: nulls, price bounds,
min ≤ modal ≤ max with a 1-paisa tolerance, and row-count drift against the trailing 7 days.
After dbt, contracts and 64 tests run, then a warehouse gate that fails if any basmati name is
unmapped. Only if everything passes is the DuckDB file copied and atomically renamed to the
serving copy, so the dashboard always shows the last good build. One real catch: a
uniqueness test found `Kutki` and `kutki` creating duplicate commodity ids, which would have
doubled rows in joins.

**5. Where did you use an LLM, and how did you keep it safe?**
Only offline, for mapping messy variety names like `Basamti Dawat` or `Paddy(Common) | 1121`
to clean groups. The pipeline never calls a model. A script sends the unmapped names to Claude
with a typed (Pydantic) output schema, writes the suggestions to a CSV, and a person approves
or edits each row. Only approved rows are appended to a dbt seed, with an audit log. Of 69
suggestions, 67 (97.1%) were accepted unchanged. The 2 rejections were real errors: the model
had labelled a hybrid paddy and an "Other" bucket as basmati.

**Follow-up worth preparing: "So did the export policy raise prices?"**
1121 paddy rose 21% in the 60 days after the Aug 2023 MEP, versus −8% in the same weeks of
2022, so the season alone doesn't explain it. But it's a before/after comparison, not a
causal estimate. India's non-basmati export ban, the overlapping 2023 windows and changes in
which markets report all confound it. A next step would be a difference-in-differences
design, using non-basmati paddy as a control.

**6. Tell me about your forecasting model. Is it any good?**
It predicts each rice series' price 1–4 weeks ahead as a % change, so series at 2,500 and
7,000 Rs/quintal share one model. Features are price history, last year's move for the same
weeks, mandi activity and the export-policy regime. I compared it with two baselines on
2024–2026 data it never saw. It beats "no change" at every horizon, but honestly by only about
5% of the error, because weekly prices are mostly persistent. The 80% band was too narrow at
first (75% coverage), so I calibrated it on a held-out year (conformal prediction) to 79.9%.
To do much better it needs supply data, i.e. arrival quantities, which the API doesn't expose.

**7. How does the daily refresh work without your laptop?**
A GitHub Actions cron job starts a small EC2 instance in Mumbai (the API is geo-blocked), opens
SSH only for the runner's IP, and tunnels the API calls through it. It runs the same ingest,
Spark, dbt and quality-gate code against a DuckDB "lite lake" in S3 that stands in for the
Iceberg catalog, publishes the dashboard extract only if the gate passes, then closes the IP
rule and stops the instance. I checked that its output is identical to the full warehouse.
