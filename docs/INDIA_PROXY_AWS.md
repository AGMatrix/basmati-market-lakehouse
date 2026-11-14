# India egress on AWS (for data.gov.in)

data.gov.in is reachable from India but not from the US (confirmed 2026-11-12).
The pipeline stays local. One small EC2 instance in **ap-south-1 (Mumbai)** acts as an SSH SOCKS proxy, and only the Agmarknet client uses it (`AGMARKNET_PROXY`).

```
Mac (Airflow/Spark/MinIO)  --ssh -D 1080-->  EC2 t4g.micro, Mumbai  --HTTPS-->  api.data.gov.in
```

## Why this design
- **Smallest blast radius:** the instance has no inbound ports except SSH from your current IP, no IAM role and no data on it. It only forwards bytes.
- **No code fork:** the same DAG runs from India (proxy empty) or the US (proxy set).
- **Cost control:** stop it when you're not ingesting. `down` deletes everything the script created.

## One-time setup
1. Use a **dedicated IAM user/profile** (e.g. `basmati`) with EC2 permissions plus `ssm:GetParameter`. Do not use a production admin key.
   `aws configure --profile basmati`
2. In the Billing console, create a **$1 budget alert** (AWS Budgets) before launching anything.
3. Check what your account's free tier covers in the Billing → Free Tier page. Accounts created after 15 Jul 2025 are on the credit-based Free plan; older accounts have the 12-month free tier. Choose `INSTANCE_TYPE` accordingly (`t4g.micro` default; on the legacy 12-month tier use `INSTANCE_TYPE=t2.micro`, which is free-tier eligible in Mumbai). The script picks the arm64 or amd64 Ubuntu image to match.
4. `AWS_PROFILE=basmati scripts/aws_india_proxy.sh up`

## Daily use
```
AWS_PROFILE=basmati scripts/aws_india_proxy.sh start    # if stopped
AWS_PROFILE=basmati scripts/aws_india_proxy.sh tunnel   # leave running in a terminal
# .env: AGMARKNET_PROXY=socks5h://127.0.0.1:1080
make run-day  /  make backfill
AWS_PROFILE=basmati scripts/aws_india_proxy.sh stop
```
If your IP changes (campus vs. home), run `... myip`.

Docker note: containers reach the Mac's tunnel at `socks5h://host.docker.internal:1080`.

## Cost notes (verify against your account)
- Public IPv4 addresses are billed hourly while the instance runs; a stopped instance releases its auto-assigned IP.
- The 8 GB gp3 volume is billed even while stopped (small). `down` deletes it.
- Data transfer for JSON API responses is tiny compared with the free data-transfer allowance.
