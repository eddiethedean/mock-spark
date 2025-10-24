#!/bin/bash
# Simple test runner

echo "Running Mock Spark Test Suite"
echo "============================="

# Step 1: All unit tests (excluding delta and performance) - run in parallel
echo "Running unit tests (parallel, 8 cores)..."
python3 -m pytest tests/unit/ -v -n 8 --dist loadfile --tb=short
unit_exit=$?

# Step 2: Delta tests - run serially to avoid JAR conflicts
echo "Running Delta tests (serial)..."
python3 -m pytest tests/compatibility/ -v -m delta --tb=short
delta_exit=$?

# Step 3: Performance tests - run serially for stable timing
echo "Running Performance tests (serial)..."
python3 -m pytest tests/unit/ -v -m performance --tb=short
performance_exit=$?

# Check if performance tests exist (exit code 5 means no tests found)
if [ $performance_exit -eq 5 ]; then
    echo "No performance tests found, skipping..."
    performance_exit=0
fi

# Final result
if [ $unit_exit -ne 0 ] || [ $delta_exit -ne 0 ] || [ $performance_exit -ne 0 ]; then
    echo "Test suite FAILED"
    exit 1
else
    echo "Test suite PASSED"
    exit 0
fi