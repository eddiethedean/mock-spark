#!/bin/bash
# Run integration tests

set -e

echo "🔗 Running Integration Tests"
echo "============================="

python -m pytest tests/integration/ \
    --cov=mock_spark \
    --cov-report=term-missing \
    --cov-report=html \
    --cov-report=xml \
    -v \
    "$@"

echo ""
echo "✅ Integration tests completed!"

