#!/bin/bash
# Script to run fast tests (unit, integration, documentation) without compatibility tests

set -e

echo "🧪 Running Fast Test Suite (No Compatibility Tests)"
echo "===================================================="
echo ""

# Check if pytest-xdist is available for parallel execution
if python3 -c "import xdist" 2>/dev/null; then
    PARALLEL_AVAILABLE=true
    echo "✓ pytest-xdist available - will run tests in parallel"
else
    PARALLEL_AVAILABLE=false
    echo "ℹ pytest-xdist not available - running tests serially"
fi
echo ""

# Run fast tests (unit, integration, documentation - no compatibility)
if [ "$PARALLEL_AVAILABLE" = true ]; then
    echo "📋 Running unit, integration, and documentation tests in parallel..."
    python3 -m pytest tests/unit/ tests/integration/ tests/documentation/ -v -n 8 --tb=short --disable-warnings
else
    echo "📋 Running unit, integration, and documentation tests..."
    python3 -m pytest tests/unit/ tests/integration/ tests/documentation/ -v --tb=short --disable-warnings
fi

echo ""
echo "✅ Fast tests completed successfully!"
echo ""
echo "💡 These tests run quickly and don't require PySpark"
echo "💡 To run ALL tests including compatibility: bash tests/run_all_tests.sh"
echo "💡 To see coverage: pytest tests/unit/ tests/integration/ tests/documentation/ --cov=mock_spark --cov-report=html"

