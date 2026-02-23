#!/bin/bash
set -euo pipefail

# Register Fargate task definitions with values from .env
#
# Usage: ./fargate/register-task-defs.sh
#
# Reads ACCOUNT_ID from .env and substitutes it into the task definition
# JSON templates before registering with ECS.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load infrastructure config from .env
ENV_FILE="${SCRIPT_DIR}/.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: ${ENV_FILE} not found."
    echo "Copy .env.example to .env and fill in your values:"
    echo "  cp fargate/.env.example fargate/.env"
    exit 1
fi
# shellcheck source=.env
source "$ENV_FILE"

echo "=== Registering Fargate Task Definitions ==="
echo "Account: ${AWS_ACCOUNT_ID}"
echo "Region:  ${AWS_REGION}"
echo ""

register() {
    local template="$1"
    local name
    name=$(basename "$template" .json)
    local tmpfile
    tmpfile=$(mktemp /tmp/task-def-XXXXXX.json)
    trap "rm -f '$tmpfile'" RETURN

    echo "Registering ${name}..."
    sed "s/ACCOUNT_ID/${AWS_ACCOUNT_ID}/g" "$template" > "$tmpfile"
    ARN=$(aws ecs register-task-definition \
        --cli-input-json "file://${tmpfile}" \
        --region "$AWS_REGION" \
        --query 'taskDefinition.taskDefinitionArn' \
        --output text)
    echo "  ${ARN}"
}

register "${SCRIPT_DIR}/fargate-task-process-slide.json"

echo ""
echo "Done. Task definitions registered."
