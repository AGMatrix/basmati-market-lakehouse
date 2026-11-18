# Resume and interview notes

Every number here is measured (see [RESULTS.md](../RESULTS.md)).

## Two-line description

Basmati Market Lakehouse: a local, $0 batch lakehouse (Airflow 3, Spark 4.1, Apache Iceberg,
dbt + DuckDB, Great Expectations, Streamlit) that ingests 12 years of Indian mandi prices
(55M rows) and measures how basmati prices moved around India's 2023–2024 export-price changes.

## Resume bullets

- Built an asset-driven lakehouse (Airflow 3, Spark 4.1, Apache Iceberg, MinIO) that backfilled 55.4M mandi price records (4,339 days, 8,325 markets) in 4,330 runs with 0 failures. Daily runs re-pull a 7-day look-back, use content hashes to detect late revisions, and replace only the changed days in one atomic Iceberg commit.
- Modeled a dbt + DuckDB star schema with contracts, unit tests and an SCD2 market dimension, gated by 64 dbt checks and Great Expectations. Diagnosed full-scale failures (OOM kills, ephemeral-port exhaustion, duplicate keys) to get a 113 s full rebuild. Added LLM-suggested variety mapping with human review (97.1% of suggestions accepted unchanged).

## Five interview questions

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
