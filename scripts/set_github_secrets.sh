#!/usr/bin/env bash
# Copy the daily-refresh secrets to this repo's GitHub Actions secrets.
# Values are piped straight into `gh secret set`: never printed, never written to disk.
#   scripts/set_github_secrets.sh
set -euo pipefail
cd "$(dirname "$0")/.."
get() { grep -E "^$1=" .env | tail -1 | cut -d= -f2- | sed -E 's/^[[:space:]]+|[[:space:]]+$//g'; }
put() {
  [[ -n "$2" ]] || { echo "missing value for $1" >&2; exit 1; }
  printf '%s' "$2" | gh secret set "$1" >/dev/null && echo "set $1"
}
for k in DATA_GOV_IN_API_KEY AGMARKNET_HISTORY_RESOURCE_ID AGMARKNET_CURRENT_RESOURCE_ID \
         SERVING_S3_BUCKET SERVING_AWS_ACCESS_KEY_ID SERVING_AWS_SECRET_ACCESS_KEY; do
  put "$k" "$(get "$k")"
done
put PROXY_AWS_ACCESS_KEY_ID "$(aws configure get aws_access_key_id --profile basmati)"
put PROXY_AWS_SECRET_ACCESS_KEY "$(aws configure get aws_secret_access_key --profile basmati)"
gh secret set PROXY_SSH_KEY <"$HOME/.ssh/basmati-india-proxy.pem" >/dev/null && echo "set PROXY_SSH_KEY"
