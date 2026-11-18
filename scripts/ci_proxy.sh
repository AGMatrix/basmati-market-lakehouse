#!/usr/bin/env bash
# India egress for the daily GitHub Actions refresh (data.gov.in is geo-blocked outside India).
# Reuses the Mumbai instance created by scripts/aws_india_proxy.sh.
#
#   scripts/ci_proxy.sh up     start the instance, allow THIS runner's IP for SSH, SOCKS on :1080
#   scripts/ci_proxy.sh down   close the tunnel, remove the runner's IP, stop the instance
#                              (left running if it was already running, e.g. used from the Mac)
#
# Env: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (the basmati proxy user), PROXY_SSH_KEY.
set -euo pipefail
export AWS_REGION="${AWS_REGION:-ap-south-1}"
NAME=basmati-india-proxy
STATE=/tmp/basmati_proxy_state
KEY="$HOME/.ssh/${NAME}.pem"

instance_id() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$NAME" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text
}
sg_id() {
  aws ec2 describe-security-groups --filters "Name=group-name,Values=$NAME" \
    --query 'SecurityGroups[0].GroupId' --output text
}
runner_cidr() { echo "$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')/32"; }

case "${1:-}" in
  up)
    id=$(instance_id)
    state=$(aws ec2 describe-instances --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].State.Name' --output text)
    echo "was=$state" >"$STATE"
    echo "cidr=$(runner_cidr)" >>"$STATE"
    # A stopping instance must finish stopping before it can start again.
    [[ "$state" == "stopping" ]] && aws ec2 wait instance-stopped --instance-ids "$id"
    [[ "$state" != "running" ]] && aws ec2 start-instances --instance-ids "$id" >/dev/null
    aws ec2 wait instance-running --instance-ids "$id"
    ip=$(aws ec2 describe-instances --instance-ids "$id" \
      --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    aws ec2 authorize-security-group-ingress --group-id "$(sg_id)" --protocol tcp --port 22 \
      --cidr "$(grep cidr= "$STATE" | cut -d= -f2)" >/dev/null 2>&1 || true
    mkdir -p "$HOME/.ssh"
    printf '%s\n' "$PROXY_SSH_KEY" >"$KEY"
    chmod 600 "$KEY"
    # sshd may need a few seconds after "running"; retry the tunnel.
    for i in $(seq 1 20); do
      if ssh -i "$KEY" -f -N -D 127.0.0.1:1080 -o StrictHostKeyChecking=accept-new \
           -o ServerAliveInterval=30 -o ExitOnForwardFailure=yes -o ConnectTimeout=10 \
           "ubuntu@$ip" 2>/dev/null; then
        echo "SOCKS proxy on 127.0.0.1:1080 via the Mumbai instance"
        exit 0
      fi
      sleep 6
    done
    echo "could not open the SSH tunnel" >&2
    exit 1
    ;;
  down)
    pkill -f "ssh -i $KEY" || true
    rm -f "$KEY"
    [[ -f "$STATE" ]] || exit 0
    aws ec2 revoke-security-group-ingress --group-id "$(sg_id)" --protocol tcp --port 22 \
      --cidr "$(grep cidr= "$STATE" | cut -d= -f2)" >/dev/null 2>&1 || true
    if grep -q '^was=running$' "$STATE"; then
      echo "instance was already running before this job; leaving it running"
    else
      aws ec2 stop-instances --instance-ids "$(instance_id)" >/dev/null && echo "instance stopping"
    fi
    ;;
  *) sed -n '2,10p' "$0"; exit 1 ;;
esac
