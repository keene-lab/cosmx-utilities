#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <s3-uri> <instance-id> [dest-dir]"
  echo ""
  echo "Syncs files from an S3 path to a directory on a remote EC2 instance via SSM."
  echo ""
  echo "Arguments:"
  echo "  s3-uri        S3 URI to sync from (e.g. s3://my-bucket/napari-stitched/CosMx-GBM/CosMx-GBM-segmentation-test-1.9.26/)"
  echo "  instance-id   EC2 instance ID (e.g. i-0abc123def456789a)"
  echo "  dest-dir      Base directory on EC2; files sync to <dest-dir>/stitched (default: /mnt/cosmx)"
  echo ""
  echo "Prerequisites:"
  echo "  - EC2 instance must have SSM Agent running"
  echo "  - Instance profile needs AmazonSSMManagedInstanceCore + S3 read access"
  echo "  - Caller needs ssm:SendCommand and ssm:GetCommandInvocation permissions"
  exit 1
}

if [[ $# -lt 2 || $# -gt 3 ]]; then
  usage
fi

S3_URI="$1"
INSTANCE_ID="$2"
DEST_DIR="${3:-/mnt/cosmx}/stitched"

# Validate arguments
if [[ ! "$S3_URI" =~ ^s3:// ]]; then
  echo "Error: First argument must be an S3 URI (s3://...)" >&2
  exit 1
fi

if [[ ! "$INSTANCE_ID" =~ ^i- ]]; then
  echo "Error: Second argument must be an EC2 instance ID (i-...)" >&2
  exit 1
fi

echo "Sending s3 sync command to $INSTANCE_ID..."
echo "  Source: $S3_URI"
echo "  Destination: $DEST_DIR"

COMMAND_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[
    \"mkdir -p $DEST_DIR\",
    \"aws s3 sync '$S3_URI' $DEST_DIR\",
    \"echo 'Sync complete. Files:'\",
    \"ls -lh $DEST_DIR\"
  ]" \
  --timeout-seconds 3600 \
  --comment "Sync $S3_URI to EC2" \
  --query "Command.CommandId" \
  --output text)

echo "Command ID: $COMMAND_ID"
echo "Waiting for completion..."

while true; do
  STATUS=$(aws ssm get-command-invocation \
    --command-id "$COMMAND_ID" \
    --instance-id "$INSTANCE_ID" \
    --query "Status" \
    --output text 2>/dev/null || echo "InProgress")

  case "$STATUS" in
    Success)
      echo ""
      echo "Sync completed successfully."
      aws ssm get-command-invocation \
        --command-id "$COMMAND_ID" \
        --instance-id "$INSTANCE_ID" \
        --query "StandardOutputContent" \
        --output text
      exit 0
      ;;
    Failed|Cancelled|TimedOut)
      echo ""
      echo "Command failed with status: $STATUS"
      aws ssm get-command-invocation \
        --command-id "$COMMAND_ID" \
        --instance-id "$INSTANCE_ID" \
        --query "StandardErrorContent" \
        --output text >&2
      exit 1
      ;;
    *)
      printf "."
      sleep 5
      ;;
  esac
done
