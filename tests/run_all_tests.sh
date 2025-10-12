#!/bin/bash
# Script to run all tests with proper isolation between Delta and non-Delta tests

set -e

echo "🧪 Running Mock Spark Test Suite"
echo "================================"
echo ""

# Check if pytest-xdist is available for parallel execution
if python3 -c "import xdist" 2>/dev/null; then
    PARALLEL_AVAILABLE=true
    echo "✓ pytest-xdist available - will run tests in parallel"
else
    PARALLEL_AVAILABLE=false
    echo "ℹ pytest-xdist not available - running tests serially"
    echo "  (Install with: pip install pytest-xdist for faster execution)"
fi
echo ""

# Run non-Delta tests (with or without parallelization)
if [ "$PARALLEL_AVAILABLE" = true ]; then
    echo "📋 Step 1: Running non-Delta tests in parallel (8 cores)..."
    python3 -m pytest tests/ -v -n 8 -m "not delta" --tb=short --disable-warnings
else
    echo "📋 Step 1: Running non-Delta tests..."
    python3 -m pytest tests/ -v -m "not delta" --tb=short --disable-warnings
fi

echo ""
echo "📋 Step 2: Running Delta tests serially for JAR isolation..."
python3 -m pytest tests/ -v -m "delta" --tb=short --disable-warnings

echo ""
echo "✅ All tests completed successfully!"
echo ""
echo "Summary:"
if [ "$PARALLEL_AVAILABLE" = true ]; then
    echo "- Non-Delta tests: Run in parallel with 8 cores"
else
    echo "- Non-Delta tests: Run serially"
fi
echo "- Delta tests: Run serially with isolated Spark sessions"
echo ""
echo "💡 Tip: Add --cov=mock_spark --cov-report=html to see coverage report"
