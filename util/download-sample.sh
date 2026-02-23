#!/bin/bash
# Download only specific FOVs (FOV00001-10) from CellStatsDir and AnalysisResults

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../fargate/.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: ${ENV_FILE} not found."
    echo "Copy fargate/.env.example to fargate/.env and fill in your values."
    exit 1
fi
# shellcheck source=../fargate/.env
source "$ENV_FILE"

BUCKET="${S3_BUCKET:?S3_BUCKET not set in fargate/.env}"
GIT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BASE_PATH="s3://${BUCKET}/CosMx-GBM/CosMx-GBM-segmentation-test-1.9.26/Bsbneurosegmentationtest1826_12_01_2026_12_26_12_282/DecodedFiles/7522A77582A6/20241220_213822_S2"
LOCAL_DIR="${GIT_ROOT}/downloads/20241220_213822_S2"
mkdir -p "$LOCAL_DIR"

aws s3 sync "s3://${BUCKET}/CosMx-GBM/CosMx-GBM-segmentation-test-1.9.26/Bsbneurosegmentationtest1826_12_01_2026_12_26_12_282/flatFiles/7522A77582A6/" "${LOCAL_DIR}/flatFiles"

# Download CellStatsDir for FOV00001-10
echo "Downloading CellStatsDir for FOV00001-10..."
aws s3 sync "${BASE_PATH}/CellStatsDir/" "${LOCAL_DIR}/CellStatsDir/" \
    --exclude "*" \
    --include "FOV00001/*" \
    --include "FOV00002/*" \
    --include "FOV00003/*" \
    --include "FOV00004/*" \
    --include "FOV00005/*" \
    --include "FOV00006/*" \
    --include "FOV00007/*" \
    --include "FOV00008/*" \
    --include "FOV00009/*" \
    --include "FOV00010/*" \
    --include "Morphology2D/*"

# Download AnalysisResults/x2jsi65avk for FOV00001-10
echo "Downloading AnalysisResults/x2jsi65avk for FOV00001-10..."
aws s3 sync "${BASE_PATH}/AnalysisResults/x2jsi65avk/" "${LOCAL_DIR}/AnalysisResults/x2jsi65avk/" \
    --exclude "*" \
    --include "FOV00001/*" \
    --include "FOV00002/*" \
    --include "FOV00003/*" \
    --include "FOV00004/*" \
    --include "FOV00005/*" \
    --include "FOV00006/*" \
    --include "FOV00007/*" \
    --include "FOV00008/*" \
    --include "FOV00009/*" \
    --include "FOV00010/*"

# Download RunSummary directory
echo "Downloading RunSummary..."
aws s3 sync "${BASE_PATH}/RunSummary/" "${LOCAL_DIR}/RunSummary/"

echo "Download complete!"
