#!/usr/bin/env bash
# India egress for data.gov.in (geo-blocked outside India).
# Creates ONE small EC2 instance in ap-south-1 (Mumbai) and opens an SSH SOCKS tunnel.
# The pipeline still runs on the Mac; only data.gov.in calls go through the tunnel.
#
#   AWS_PROFILE=basmati scripts/aws_india_proxy.sh up       # create key pair, security group, instance
#   AWS_PROFILE=basmati scripts/aws_india_proxy.sh tunnel   # SOCKS proxy on 127.0.0.1:1080 (Ctrl-C to close)
#   AWS_PROFILE=basmati scripts/aws_india_proxy.sh stop     # stop when idle (no compute charge)
#   AWS_PROFILE=basmati scripts/aws_india_proxy.sh start    # start again (public IP changes)
#   AWS_PROFILE=basmati scripts/aws_india_proxy.sh myip     # re-allow SSH from your current IP
#   AWS_PROFILE=basmati scripts/aws_india_proxy.sh down     # terminate and delete everything it created
set -euo pipefail

: "${AWS_PROFILE:?Set AWS_PROFILE to a dedicated profile (not a production admin key)}"
export AWS_REGION="${AWS_REGION:-ap-south-1}"
NAME=basmati-india-proxy
KEY_FILE="$HOME/.ssh/${NAME}.pem"
INSTANCE_TYPE="${INSTANCE_TYPE:-t4g.micro}"
AMI_PARAM=/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id

instance_id() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$NAME" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text
}
sg_id() {
  aws ec2 describe-security-groups --filters "Name=group-name,Values=$NAME" \
    --query 'SecurityGroups[0].GroupId' --output text
}
public_ip() {
  aws ec2 describe-instances --instance-ids "$(instance_id)" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
}
my_cidr() { echo "$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')/32"; }

allow_my_ip() {
  local sg; sg=$(sg_id)
  # Replace any previous SSH rule with the current IP only.
  aws ec2 revoke-security-group-ingress --group-id "$sg" \
    --ip-permissions "$(aws ec2 describe-security-groups --group-ids "$sg" --query 'SecurityGroups[0].IpPermissions' --output json)" \
    >/dev/null 2>&1 || true
  aws ec2 authorize-security-group-ingress --group-id "$sg" --protocol tcp --port 22 --cidr "$(my_cidr)" >/dev/null
  echo "SSH allowed from $(my_cidr)"
}

case "${1:-}" in
  up)
    if [[ "$(instance_id)" != "None" ]]; then echo "Already exists: $(instance_id)"; exit 0; fi
    if [[ ! -f "$KEY_FILE" ]]; then
      aws ec2 create-key-pair --key-name "$NAME" --key-type ed25519 \
        --query KeyMaterial --output text >"$KEY_FILE"
      chmod 600 "$KEY_FILE"
    fi
    if [[ "$(sg_id)" == "None" ]]; then
      aws ec2 create-security-group --group-name "$NAME" \
        --description "SSH-only from owner IP for data.gov.in egress" >/dev/null
    fi
    allow_my_ip
    ami=$(aws ssm get-parameter --name "$AMI_PARAM" --query Parameter.Value --output text)
    aws ec2 run-instances --image-id "$ami" --instance-type "$INSTANCE_TYPE" \
      --key-name "$NAME" --security-groups "$NAME" \
      --metadata-options HttpTokens=required \
      --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=8,VolumeType=gp3,DeleteOnTermination=true}' \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME},{Key=project,Value=basmati-lakehouse}]" \
      --query 'Instances[0].InstanceId' --output text
    aws ec2 wait instance-running --instance-ids "$(instance_id)"
    echo "Running at $(public_ip). Next: $0 tunnel"
    ;;
  tunnel)
    ip=$(public_ip)
    echo "SOCKS5 proxy on 127.0.0.1:1080 via $ip  ->  set AGMARKNET_PROXY=socks5h://127.0.0.1:1080"
    exec ssh -i "$KEY_FILE" -N -D 127.0.0.1:1080 \
      -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30 -o ExitOnForwardFailure=yes \
      "ubuntu@$ip"
    ;;
  stop)  aws ec2 stop-instances --instance-ids "$(instance_id)" >/dev/null && echo stopping ;;
  start) aws ec2 start-instances --instance-ids "$(instance_id)" >/dev/null
         aws ec2 wait instance-running --instance-ids "$(instance_id)" && echo "running at $(public_ip)" ;;
  myip)  allow_my_ip ;;
  down)
    id=$(instance_id)
    if [[ "$id" != "None" ]]; then
      aws ec2 terminate-instances --instance-ids "$id" >/dev/null
      aws ec2 wait instance-terminated --instance-ids "$id"
    fi
    [[ "$(sg_id)" != "None" ]] && aws ec2 delete-security-group --group-id "$(sg_id)"
    aws ec2 delete-key-pair --key-name "$NAME" || true
    rm -f "$KEY_FILE"
    echo "All $NAME resources deleted."
    ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
