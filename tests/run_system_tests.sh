#!/bin/bash
# Run system tests (end-to-end workflows)

set -e

echo "🏗️  Running System Tests"
echo "======================="

python -m pytest tests/system/ \
    --cov=mock_spark \
    --cov-report=term-missing \
    --cov-report=html \
    --cov-report=xml \
    -v \
    "$@"

echo ""
echo "✅ System tests completed!"

