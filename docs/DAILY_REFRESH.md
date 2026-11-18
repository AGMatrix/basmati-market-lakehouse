# Daily refresh (GitHub Actions)

The hosted dashboard updates itself every day at 09:00 IST, with the Mac off.

```
.github/workflows/daily-refresh.yml   (cron 03:30 UTC, or "Run workflow" by hand)
 ├─ scripts/ci_proxy.sh up      start the Mumbai EC2 proxy, allow this runner's IP, SOCKS :1080
 ├─ python -m basmati.lite
 │    1. ingest_day × (yesterday + 7-day look-back) → s3://BUCKET/raw/agmarknet/dt=…  (manifests,
 │       content hashes: only new or revised days continue)
 │    2. build_silver (the production Spark code, local mode)
 │    3. lite lake  s3://BUCKET/lite/lite_lake.duckdb  (rice + basmati-candidate rows; stands in
 │       for the Iceberg catalog: the dbt source `lake.silver.*` is the same)
 │    4. dbt build --target lite  (same models, 64 tests/contracts, SCD2 snapshot kept in
 │       s3://BUCKET/lite/warehouse_lite.duckdb) → warehouse quality gate (GX)
 │    5. gate passed → s3://BUCKET/serving/basmati_demo.duckdb  → https://basmati-market.streamlit.app
 └─ scripts/ci_proxy.sh down    always: close tunnel, remove runner IP, stop the instance
```

If a new basmati variety name appears, the gate fails, nothing is published, and GitHub
emails the repo owner. Map it (`make map-suggest`, review, `make map-merge`), push, and re-run.

The lite path is checked against the full warehouse: it gives the same basmati rows
(138,044), the same rice rows (977,918), and identical policy-mart results (0 differing rows).

## Cost

- GitHub Actions: free for public repositories.
- EC2 t2.micro: runs about 10 minutes a day, so roughly 5 hours a month, inside the free
  tier while it lasts. Even at on-demand prices that is cents a month.
- S3: about 70 MB stored, which is cents a month or free tier.

## One-time setup

1. **Widen the writer key's policy.** In the AWS console go to IAM → Users →
   `basmati-serving-writer` → Permissions → edit the inline policy → JSON, and replace it
   with:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {"Effect": "Allow", "Action": "s3:ListBucket",
        "Resource": "arn:aws:s3:::BUCKET"},
       {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
        "Resource": ["arn:aws:s3:::BUCKET/raw/*",
                     "arn:aws:s3:::BUCKET/lite/*",
                     "arn:aws:s3:::BUCKET/serving/*"]}
     ]
   }
   ```
   `ListBucket` makes S3 answer "not found" (404) rather than "access denied" (403) for a
   day not fetched yet, which is how the ingest code recognises a new day.
2. **Seed the lite lake** (from the full local lakehouse, once):
   `uv run --env-file .env python scripts/seed_lite.py --upload`
3. **Copy the secrets to GitHub**: `scripts/set_github_secrets.sh`. This reads `.env`, the
   `basmati` AWS profile and the proxy's SSH key, and pipes each value into
   `gh secret set`, so nothing is printed.
4. **Test it**: on GitHub go to Actions → daily-refresh → Run workflow.
