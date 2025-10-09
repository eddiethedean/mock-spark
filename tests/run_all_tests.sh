#!/bin/bash
# Script to run all tests with proper isolation between Delta and non-Delta tests

set -e

echo "🧪 Running Mock Spark Test Suite"
echo "================================"
echo ""

# Run non-Delta tests in parallel
echo "📋 Step 1: Running non-Delta tests in parallel (8 cores)..."
python3 -m pytest tests/ -v -n 8 -m "not delta" --tb=short

echo ""
echo "📋 Step 2: Running Delta tests serially for JAR isolation..."
python3 -m pytest tests/ -v -m "delta" --tb=short

echo ""
echo "✅ All tests completed successfully!"
echo ""
echo "Summary:"
echo "- Non-Delta tests: Run in parallel with 8 cores"
echo "- Delta tests: Run serially with isolated Spark sessions"
