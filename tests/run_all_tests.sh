#!/bin/bash
# Test runner for overhauled test suite

echo "Running Mock Spark Test Suite (Overhauled)"
echo "=========================================="

# Check if pytest-xdist is available for parallel execution
if python3 -c "import pytest_xdist" 2>/dev/null; then
    echo "✅ pytest-xdist available - using parallel execution"
    PARALLEL_FLAGS="-n 8 --dist loadfile"
else
    echo "⚠️  pytest-xdist not available - running serially"
    echo "   Install with: pip install pytest-xdist"
    PARALLEL_FLAGS=""
fi

# Step 1: Unit tests - run in parallel if available
echo "Running unit tests..."
python3 -m pytest tests/unit/ -v $PARALLEL_FLAGS --tb=short -m "not performance"
unit_exit=$?

# Step 2: Compatibility tests - validate against expected outputs (no PySpark required)
echo "Running compatibility tests..."
echo "  - DataFrame operations compatibility"
echo "  - Function operations compatibility" 
echo "  - Join operations compatibility"
echo "  - Aggregation operations compatibility"
echo "  - Window functions compatibility"
echo "  - Array functions compatibility"
echo "  - Datetime functions compatibility"
echo "  - Null handling compatibility"
echo "  - Set operations compatibility"
echo "  - Complex scenarios compatibility"
python3 -m pytest tests/compatibility/ -v $PARALLEL_FLAGS --tb=short
compatibility_exit=$?

# Step 3: Performance tests - run serially for stable timing
echo "Running Performance tests (serial)..."
python3 -m pytest tests/unit/ -v -m performance --tb=short
performance_exit=$?

# Check if performance tests exist (exit code 5 means no tests found)
if [ $performance_exit -eq 5 ]; then
    echo "No performance tests found, skipping..."
    performance_exit=0
fi

# Step 4: Documentation tests
echo "Running documentation tests..."
python3 -m pytest tests/documentation/ -v --tb=short
doc_exit=$?

# Generate test summary
echo ""
echo "Test Summary"
echo "============"
echo "Unit tests: $([ $unit_exit -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")"
echo "Compatibility tests: $([ $compatibility_exit -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")"
echo "Performance tests: $([ $performance_exit -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")"
echo "Documentation tests: $([ $doc_exit -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")"

# Count total tests
total_tests=$(python3 -c "
import subprocess
import sys
try:
    result = subprocess.run([sys.executable, '-m', 'pytest', '--collect-only', '-q', 'tests/'], 
                          capture_output=True, text=True)
    lines = result.stdout.split('\n')
    for line in lines:
        if 'collected' in line and 'item' in line:
            print(line.split()[0])
            break
except:
    print('Unknown')
" 2>/dev/null)

echo "Total tests: $total_tests"

# Final result
if [ $unit_exit -ne 0 ] || [ $compatibility_exit -ne 0 ] || [ $performance_exit -ne 0 ] || [ $doc_exit -ne 0 ]; then
    echo ""
    echo "❌ Test suite FAILED"
    exit 1
else
    echo ""
    echo "✅ Test suite PASSED"
    echo "✅ All tests completed successfully without PySpark runtime dependency"
    echo "✅ Comprehensive compatibility testing across all major MockSpark features"
    exit 0
fi