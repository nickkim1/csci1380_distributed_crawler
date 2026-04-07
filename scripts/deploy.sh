##!/usr/bin/env bash

OS_IMAGE="ami-0b6c6ebed2801a5cb"
INSTANCE_COUNT=1
INSTANCE_TYPE="t3.micro"
KEY_NAME="csci1380"
SECURITY_GROUP="sg-09f4146ed85a6feeb"
USER_DATA_FILE="file://scripts/ec2_setup.sh"

set -e

echo "launching instance..."
instance_id=$(aws ec2 run-instances \
        --image-id "$OS_IMAGE" \
        --count $INSTANCE_COUNT \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SECURITY_GROUP" \
        --user-data "$USER_DATA_FILE" \
        --query "Instances[0].InstanceId" \
        --output text)

echo "waiting for instance $instance_id to start..."
aws ec2 wait instance-running --instance-ids "$instance_id"

ip=$(aws ec2 describe-instances \
  --instance-ids "$instance_id" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

# echo "connecting to instance w ip $ip"
# ./scripts/ssh.sh "$ip"
