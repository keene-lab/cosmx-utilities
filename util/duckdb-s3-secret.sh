#!/usr/bin/env bash
# Generates a DuckDB CREATE SECRET statement using your current AWS CLI credentials.
# Usage: ./util/duckdb-s3-secret.sh [region]
#
# Reads credentials from: environment variables, AWS profiles, SSO sessions, etc.
# (whatever `aws sts get-caller-identity` would use)

set -euo pipefail

REGION="${1:-$(aws configure get region 2>/dev/null || echo "us-east-1")}"

# Resolve current credentials via the AWS CLI
eval "$(aws configure export-credentials --format env 2>/dev/null)" || true

KEY_ID="${AWS_ACCESS_KEY_ID:?Could not resolve AWS_ACCESS_KEY_ID from current credentials}"
SECRET="${AWS_SECRET_ACCESS_KEY:?Could not resolve AWS_SECRET_ACCESS_KEY from current credentials}"
SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"

echo "-- Generated from AWS CLI credentials ($(aws sts get-caller-identity --query Arn --output text 2>/dev/null || echo 'unknown identity'))"
echo "-- Region: ${REGION}"
echo

if [ -n "$SESSION_TOKEN" ]; then
    cat <<EOF
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID '${KEY_ID}',
    SECRET '${SECRET}',
    SESSION_TOKEN '${SESSION_TOKEN}',
    REGION '${REGION}'
);
EOF
else
    cat <<EOF
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID '${KEY_ID}',
    SECRET '${SECRET}',
    REGION '${REGION}'
);
EOF
fi
