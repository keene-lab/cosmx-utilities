#!/bin/bash
set -euo pipefail

# Smoke test for cosmx-utilities Docker images
# Usage: ./util/smoke-test.sh [--build]
#
# Options:
#   --build    Build images before testing (default: assume images exist)

BUILD=false
if [ "${1:-}" = "--build" ]; then
    BUILD=true
fi

HEADLESS_TAG="cosmx-utilities:headless-test"
GUI_TAG="cosmx-utilities:gui-test"

PASS=0
FAIL=0

run_test() {
    local description="$1"
    shift
    echo -n "  ${description}... "
    if "$@" > /dev/null 2>&1; then
        echo "OK"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        FAIL=$((FAIL + 1))
    fi
}

# --- Build ---
if [ "$BUILD" = true ]; then
    echo "Building images..."
    docker build --target headless -t "$HEADLESS_TAG" .
    docker build --target gui -t "$GUI_TAG" .
    echo ""
fi

echo "Headless image (${HEADLESS_TAG}):"
run_test "stitch-images --help" docker run --rm "$HEADLESS_TAG" stitch-images --help
run_test "read-targets --help"  docker run --rm "$HEADLESS_TAG" read-targets --help

echo "GUI image (${GUI_TAG}):"
echo "  (build-only, no runtime test — requires display server)"

echo ""
TOTAL=$((PASS + FAIL))
echo "Results: ${PASS}/${TOTAL} passed"
if [ "$FAIL" -gt 0 ]; then
    echo "SMOKE TESTS FAILED"
    exit 1
fi
echo "All smoke tests passed."
