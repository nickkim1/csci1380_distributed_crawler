#!/usr/bin/env bash

set -euo pipefail

OS_IMAGE="ami-0b6c6ebed2801a5cb"
INSTANCE_COUNT=1
INSTANCE_TYPE="t3.micro"
KEY_NAME="csci1380"
SECURITY_GROUP="sg-09f4146ed85a6feeb"
SUBNET_ID=""
REGION=""
WORKER_PORT=8080
WAIT_SSH=1
WAIT_SSH_TIMEOUT_SEC=600
WAIT_WORKER_TIMEOUT_SEC=600
SSH_USER="ubuntu"
SSH_KEY_FILE=""
OUTPUT_FILE=""
NODES_FILE=""
USER_DATA_FILE="scripts/ec2_setup.sh"
TAG_NAME=""

usage() {
  cat <<'EOF'
Usage:
  ./scripts/deploy.sh [options]

Options:
  --count <n>                 Number of instances (default: 1)
  --image-id <ami>            AMI id (default: ami-0b6c6ebed2801a5cb)
  --instance-type <type>      Instance type (default: t3.micro)
  --key-name <name>           EC2 key pair name (default: csci1380)
  --security-group <sg-id>    Security group id (default: sg-09f4146ed85a6feeb)
  --subnet-id <subnet-id>     Optional subnet id
  --region <region>           AWS region (default: AWS CLI default)
  --worker-port <port>        Worker service port for nodes file (default: 8080)
  --user-data-file <path>     Cloud-init/user-data script path (default: scripts/ec2_setup.sh)
  --ssh-user <user>           SSH user for readiness checks (default: ubuntu)
  --ssh-key-file <path>       SSH private key path (default: ~/.ssh/<key-name>.pem)
  --wait-ssh-timeout-sec <n>  SSH readiness timeout seconds (default: 600)
  --wait-worker-timeout-sec <n> Wait for worker service/listener readiness (default: 600)
  --no-wait-ssh               Skip SSH readiness checks
  --output <path>             Write deployment metadata JSON
  --nodes-file <path>         Write benchmark nodes JSON [{ip,port}, ...]
  --tag-name <name>           Optional Name tag for instances
  --help                      Show this help

Output:
  Prints deployment metadata JSON to stdout, and optionally to --output.
EOF
}

now_epoch() {
  date +%s
}

stage_start=0
stage_begin() {
  stage_start=$(now_epoch)
}

stage_end_msg() {
  local label="$1"
  local elapsed=$(( $(now_epoch) - stage_start ))
  echo "[deploy] ${label} (${elapsed}s)" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --count)
      INSTANCE_COUNT="$2"; shift 2 ;;
    --image-id)
      OS_IMAGE="$2"; shift 2 ;;
    --instance-type)
      INSTANCE_TYPE="$2"; shift 2 ;;
    --key-name)
      KEY_NAME="$2"; shift 2 ;;
    --security-group)
      SECURITY_GROUP="$2"; shift 2 ;;
    --subnet-id)
      SUBNET_ID="$2"; shift 2 ;;
    --region)
      REGION="$2"; shift 2 ;;
    --worker-port)
      WORKER_PORT="$2"; shift 2 ;;
    --user-data-file)
      USER_DATA_FILE="$2"; shift 2 ;;
    --ssh-user)
      SSH_USER="$2"; shift 2 ;;
    --ssh-key-file)
      SSH_KEY_FILE="$2"; shift 2 ;;
    --wait-ssh-timeout-sec)
      WAIT_SSH_TIMEOUT_SEC="$2"; shift 2 ;;
    --wait-worker-timeout-sec)
      WAIT_WORKER_TIMEOUT_SEC="$2"; shift 2 ;;
    --no-wait-ssh)
      WAIT_SSH=0; shift ;;
    --output)
      OUTPUT_FILE="$2"; shift 2 ;;
    --nodes-file)
      NODES_FILE="$2"; shift 2 ;;
    --tag-name)
      TAG_NAME="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "unknown arg: $1" >&2
      usage
      exit 1 ;;
  esac
done

if ! [[ "$INSTANCE_COUNT" =~ ^[0-9]+$ ]] || [[ "$INSTANCE_COUNT" -lt 1 ]]; then
  echo "--count must be a positive integer" >&2
  exit 1
fi
if ! [[ "$WORKER_PORT" =~ ^[0-9]+$ ]] || [[ "$WORKER_PORT" -lt 1 ]]; then
  echo "--worker-port must be a positive integer" >&2
  exit 1
fi

if [[ -z "$SSH_KEY_FILE" ]]; then
  SSH_KEY_FILE="$HOME/.ssh/${KEY_NAME}.pem"
fi

repo_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

if [[ ! -f "$USER_DATA_FILE" ]]; then
  USER_DATA_FILE="$repo_root/$USER_DATA_FILE"
fi
if [[ ! -f "$USER_DATA_FILE" ]]; then
  echo "user-data file not found: $USER_DATA_FILE" >&2
  exit 1
fi

aws_base=(aws)
if [[ -n "$REGION" ]]; then
  aws_base+=(--region "$REGION")
fi

echo "[deploy] launching $INSTANCE_COUNT instance(s)..." >&2
stage_begin

run_args=(
  ec2 run-instances
  --image-id "$OS_IMAGE"
  --count "$INSTANCE_COUNT"
  --instance-type "$INSTANCE_TYPE"
  --key-name "$KEY_NAME"
  --security-group-ids "$SECURITY_GROUP"
  --user-data "file://$USER_DATA_FILE"
  --query "Instances[].InstanceId"
  --output text
)
if [[ -n "$SUBNET_ID" ]]; then
  run_args+=(--subnet-id "$SUBNET_ID")
fi
if [[ -n "$TAG_NAME" ]]; then
  run_args+=(--tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG_NAME}]")
fi

instance_id_text="$("${aws_base[@]}" "${run_args[@]}")"
stage_end_msg "launch complete"
read -r -a instance_ids <<<"$instance_id_text"
if [[ "${#instance_ids[@]}" -eq 0 ]]; then
  echo "failed to launch any instances" >&2
  exit 1
fi

echo "[deploy] waiting for instance-running..." >&2
stage_begin
"${aws_base[@]}" ec2 wait instance-running --instance-ids "${instance_ids[@]}"
stage_end_msg "instance-running satisfied"
echo "[deploy] waiting for instance-status-ok..." >&2
stage_begin
"${aws_base[@]}" ec2 wait instance-status-ok --instance-ids "${instance_ids[@]}"
stage_end_msg "instance-status-ok satisfied"

describe_query='Reservations[].Instances[].[InstanceId,PublicIpAddress,Placement.AvailabilityZone]'
target_count="${#instance_ids[@]}"
instance_ids_out=()
instance_ips_out=()
instance_azs_out=()
for _attempt in $(seq 1 120); do
  describe_text="$("${aws_base[@]}" ec2 describe-instances --instance-ids "${instance_ids[@]}" --query "$describe_query" --output text)"
  instance_ids_out=()
  instance_ips_out=()
  instance_azs_out=()
  while IFS=$'\t ' read -r iid ip az; do
    [[ -z "${iid:-}" ]] && continue
    if [[ -z "${ip:-}" || "$ip" == "None" ]]; then
      continue
    fi
    instance_ids_out+=("$iid")
    instance_ips_out+=("$ip")
    instance_azs_out+=("$az")
  done <<< "$describe_text"

  if [[ "${#instance_ips_out[@]}" -eq "$target_count" ]]; then
    break
  fi
  sleep 5
done

if [[ "${#instance_ips_out[@]}" -ne "$target_count" ]]; then
  echo "timed out waiting for public IPs for all instances" >&2
  echo "expected=$target_count found=${#instance_ips_out[@]}" >&2
  exit 1
fi

stage_end_msg "public IP discovery complete"

if [[ "$WAIT_SSH" -eq 1 ]]; then
  if [[ ! -f "$SSH_KEY_FILE" ]]; then
    echo "ssh key file not found for readiness checks: $SSH_KEY_FILE" >&2
    exit 1
  fi

  echo "[deploy] waiting for SSH readiness..." >&2
  stage_begin
  ssh_deadline=$(( $(date +%s) + WAIT_SSH_TIMEOUT_SEC ))
  for ip in "${instance_ips_out[@]}"; do
    ok=0
    while [[ $(date +%s) -lt $ssh_deadline ]]; do
      if ssh -o BatchMode=yes -o ConnectTimeout=6 -o StrictHostKeyChecking=accept-new -i "$SSH_KEY_FILE" "$SSH_USER@$ip" 'echo ready' >/dev/null 2>&1; then
        ok=1
        break
      fi
      sleep 5
    done
    if [[ "$ok" -ne 1 ]]; then
      echo "ssh readiness timed out for $ip" >&2
      exit 1
    fi
  done
  stage_end_msg "ssh readiness complete"

  echo "[deploy] waiting for worker service readiness..." >&2
  stage_begin
  worker_deadline=$(( $(date +%s) + WAIT_WORKER_TIMEOUT_SEC ))
  for ip in "${instance_ips_out[@]}"; do
    ok=0
    while [[ $(date +%s) -lt $worker_deadline ]]; do
      if ssh -o BatchMode=yes -o ConnectTimeout=6 -o StrictHostKeyChecking=accept-new -i "$SSH_KEY_FILE" "$SSH_USER@$ip" \
        "sudo systemctl is-active --quiet csci1380-worker && ss -lnt | awk '\$4 ~ /:${WORKER_PORT}\$/ {found=1} END {exit found?0:1}'" >/dev/null 2>&1; then
        ok=1
        break
      fi
      sleep 5
    done
    if [[ "$ok" -ne 1 ]]; then
      echo "worker service/listener readiness timed out for $ip on port $WORKER_PORT" >&2
      exit 1
    fi
  done
  stage_end_msg "worker readiness complete"
fi

tmp_json=$(mktemp)
{
  echo "{"
  echo "  \"generatedAt\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
  echo "  \"imageId\": \"$OS_IMAGE\","
  echo "  \"instanceType\": \"$INSTANCE_TYPE\","
  echo "  \"keyName\": \"$KEY_NAME\","
  echo "  \"securityGroup\": \"$SECURITY_GROUP\","
  if [[ -n "$REGION" ]]; then
    echo "  \"region\": \"$REGION\","
  fi
  echo "  \"workerPort\": $WORKER_PORT,"
  echo "  \"instances\": ["
  for i in "${!instance_ids_out[@]}"; do
    comma=","; if [[ "$i" -eq $((${#instance_ids_out[@]} - 1)) ]]; then comma=""; fi
    echo "    {\"instanceId\": \"${instance_ids_out[$i]}\", \"ip\": \"${instance_ips_out[$i]}\", \"port\": $WORKER_PORT, \"availabilityZone\": \"${instance_azs_out[$i]}\"}$comma"
  done
  echo "  ],"
  echo "  \"instanceIds\": ["
  for i in "${!instance_ids_out[@]}"; do
    comma=","; if [[ "$i" -eq $((${#instance_ids_out[@]} - 1)) ]]; then comma=""; fi
    echo "    \"${instance_ids_out[$i]}\"$comma"
  done
  echo "  ],"
  echo "  \"nodes\": ["
  for i in "${!instance_ips_out[@]}"; do
    comma=","; if [[ "$i" -eq $((${#instance_ips_out[@]} - 1)) ]]; then comma=""; fi
    echo "    {\"ip\": \"${instance_ips_out[$i]}\", \"port\": $WORKER_PORT}$comma"
  done
  echo "  ]"
  echo "}"
} > "$tmp_json"

if [[ -n "$OUTPUT_FILE" ]]; then
  mkdir -p "$(dirname "$OUTPUT_FILE")"
  cp "$tmp_json" "$OUTPUT_FILE"
fi

if [[ -n "$NODES_FILE" ]]; then
  mkdir -p "$(dirname "$NODES_FILE")"
  {
    echo "["
    for i in "${!instance_ips_out[@]}"; do
      comma=","; if [[ "$i" -eq $((${#instance_ips_out[@]} - 1)) ]]; then comma=""; fi
      echo "  {\"ip\": \"${instance_ips_out[$i]}\", \"port\": $WORKER_PORT}$comma"
    done
    echo "]"
  } > "$NODES_FILE"
fi

cat "$tmp_json"
rm -f "$tmp_json"
