#!/bin/bash
# Script to run fast tests (unit, integration, documentation) without compatibility tests

set -e

# Force Python 3.8 for compatibility
if command -v python3.8 >/dev/null 2>&1; then
    PYTHON_BIN="python3.8"
    echo "✓ Using Python 3.8"
elif command -v python3 >/dev/null 2>&1; then
    # Check if python3 is actually 3.8
    PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    if [[ "$PYTHON_VERSION" == "3.8" ]]; then
        PYTHON_BIN="python3"
        echo "✓ Using Python 3.8 (via python3)"
    else
        echo "⚠️  Warning: Python 3.8 not found, using python3 ($PYTHON_VERSION)"
        echo "   Tests are designed for Python 3.8 compatibility"
        PYTHON_BIN="python3"
    fi
else
    echo "❌ Error: Python not found"
    exit 1
fi

echo "🧪 Running Fast Test Suite (No Compatibility Tests)"
echo "===================================================="
echo ""

# Check if pytest-xdist is available for parallel execution
if $PYTHON_BIN -c "import xdist" 2>/dev/null; then
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
    $PYTHON_BIN -m pytest tests/unit/ tests/integration/ tests/documentation/ -v -n 8 --tb=short --disable-warnings
else
    echo "📋 Running unit, integration, and documentation tests..."
    $PYTHON_BIN -m pytest tests/unit/ tests/integration/ tests/documentation/ -v --tb=short --disable-warnings
fi

echo ""
echo "✅ Fast tests completed successfully!"
echo ""
echo "💡 These tests run quickly and don't require PySpark"
echo "💡 To run ALL tests including compatibility: bash tests/run_all_tests.sh"
echo "💡 To see coverage: pytest tests/unit/ tests/integration/ tests/documentation/ --cov=mock_spark --cov-report=html"

