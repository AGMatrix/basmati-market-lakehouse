# Hosted dashboard: S3 extract + Streamlit Community Cloud

```
warehouse_build:  dbt build → warehouse gate → publish (local serving copy) → publish_hosted
                                                                              │ export basmati-only extract (~3 MB)
                                                                              ▼ PUT (writer key: PutObject only)
                                                     s3://<bucket>/serving/basmati_demo.duckdb   (private)
                                                                              │ GET (reader key: GetObject only)
                                                                              ▼
                                                     https://basmati-market.streamlit.app (re-downloads hourly)
```

The upload runs only after the warehouse quality gate passes, so the hosted app never shows
a failed build. If S3 is unreachable, the app falls back to the snapshot committed in
`dashboard/demo/`.

## Cost

S3 Standard storage is about $0.023 per GB per month. A 3 MB object costs a fraction of a
cent, and the free tier covers it. The real cost risk with S3 is downloads by strangers,
which is why the bucket is private and each key can touch exactly one object.

## One-time setup (AWS console, as an admin user)

Replace `BUCKET` with your bucket name, e.g. `basmati-serving-f41df6`.

1. **Create the bucket.** Go to S3 → Create bucket. Region: **US East (N. Virginia)
   us-east-1**, close to Streamlit's servers. Leave **Block all public access** ON and all
   other settings at their defaults.

2. **Create the writer user** (used by Airflow on your Mac). Go to IAM → Users → Create user
   `basmati-serving-writer`, with no console access. Then go to Permissions → Add
   permissions → Create inline policy → JSON:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": "s3:PutObject",
       "Resource": "arn:aws:s3:::BUCKET/serving/basmati_demo.duckdb"
     }]
   }
   ```
   Then go to Security credentials → Create access key → "Application running outside AWS".
   Put the key in `.env` (never in git):
   ```
   SERVING_S3_BUCKET=BUCKET
   SERVING_AWS_ACCESS_KEY_ID=...
   SERVING_AWS_SECRET_ACCESS_KEY=...
   ```

3. **Create the reader user** (used by the hosted app). Create user
   `basmati-serving-reader` with this inline policy:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": "s3:GetObject",
       "Resource": "arn:aws:s3:::BUCKET/serving/basmati_demo.duckdb"
     }]
   }
   ```
   Create an access key, then in Streamlit Community Cloud go to your app → **Settings →
   Secrets** and paste:
   ```toml
   [s3]
   bucket = "BUCKET"
   key = "serving/basmati_demo.duckdb"
   region = "us-east-1"
   aws_access_key_id = "..."
   aws_secret_access_key = "..."
   ```

4. **Budget alarm.** Go to Billing → Budgets → Create budget → "Zero spend budget" or a $1
   monthly cost budget, with an email alert.

5. **Apply.** Run `docker compose up -d` (so Airflow picks up the new `.env` values), then
   `make rebuild-warehouse`. The `publish_hosted` task uploads the extract, and the app shows
   it within the hour (or immediately after a reboot from the Streamlit app menu).

## Why two keys

Least privilege: a leaked reader key can only download one 3 MB public-anyway extract, and a
leaked writer key can only overwrite that one object. Neither can list, delete or read
anything else in the account.
