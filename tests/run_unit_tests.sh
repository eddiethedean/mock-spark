#!/bin/bash
# Run only unit tests (fast, no integration or system tests)

set -e

echo "🧪 Running Unit Tests"
echo "===================="

python -m pytest tests/unit/ \
    --cov=mock_spark \
    --cov-report=term-missing \
    --cov-report=html \
    --cov-report=xml \
    -v \
    "$@"

echo ""
echo "✅ Unit tests completed!"
