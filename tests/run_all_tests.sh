#!/bin/bash
# Simple test runner with progress monitoring

# Disable exit-on-error for test execution (we handle errors manually)
set +e

# Function to run tests with progress monitoring
run_tests_with_progress() {
    local test_pattern=$1
    local test_name=$2
    local parallel=$3
    local markers=$4
    
    echo ""
    echo "📋 $test_name"
    echo "================================="
    
    # Create temporary file for output
    local output_file=$(mktemp)
    
    # Start pytest in background
    if [ "$parallel" = "true" ]; then
        python3 -m pytest $test_pattern -v -n 8 --dist loadfile $markers --tb=no -q > $output_file 2>&1 &
    else
        python3 -m pytest $test_pattern -v $markers --tb=no -q > $output_file 2>&1 &
    fi
    
    local test_pid=$!
    
    echo "🔍 Running tests..."
    
    # Wait for completion
    wait $test_pid
    local exit_code=$?
    
    # Get final results from pytest output
    local passed=$(grep -c "PASSED" "$output_file" 2>/dev/null || echo "0")
    local failed=$(grep -c "FAILED" "$output_file" 2>/dev/null || echo "0")
    local skipped=$(grep -c "SKIPPED" "$output_file" 2>/dev/null || echo "0")
    local errors=$(grep -c "ERROR" "$output_file" 2>/dev/null || echo "0")
    
    # Also try to get results from pytest summary line
    local summary_line=$(grep -E "(failed|passed|skipped|error)" "$output_file" | tail -1 || echo "")
    if [ -n "$summary_line" ]; then
        # Extract numbers from summary line like "5 failed, 672 passed, 15 skipped, 29 error in 45.67s"
        local summary_passed=$(echo "$summary_line" | grep -o '[0-9]* passed' | grep -o '[0-9]*' || echo "0")
        local summary_failed=$(echo "$summary_line" | grep -o '[0-9]* failed' | grep -o '[0-9]*' || echo "0")
        local summary_skipped=$(echo "$summary_line" | grep -o '[0-9]* skipped' | grep -o '[0-9]*' || echo "0")
        local summary_errors=$(echo "$summary_line" | grep -o '[0-9]* error' | grep -o '[0-9]*' || echo "0")
        
        # Use summary line if it has better data
        if [ "$summary_passed" -gt 0 ] || [ "$summary_failed" -gt 0 ]; then
            passed="$summary_passed"
            failed="$summary_failed"
            skipped="$summary_skipped"
            errors="$summary_errors"
        fi
    fi
    
    # Ensure we have clean integer values
    passed=$(echo "$passed" | tr -d '\n' | grep -o '^[0-9]*$' || echo "0")
    failed=$(echo "$failed" | tr -d '\n' | grep -o '^[0-9]*$' || echo "0")
    skipped=$(echo "$skipped" | tr -d '\n' | grep -o '^[0-9]*$' || echo "0")
    errors=$(echo "$errors" | tr -d '\n' | grep -o '^[0-9]*$' || echo "0")
    
    # If we still have no results, try to get them from the exit code
    if [ "$passed" -eq 0 ] && [ "$failed" -eq 0 ] && [ "$skipped" -eq 0 ] && [ "$errors" -eq 0 ]; then
        # Check if there were any tests at all
        if grep -q "no tests ran" "$output_file" 2>/dev/null; then
            echo "⚠️ No tests found matching the criteria"
        elif grep -q "collected 0 items" "$output_file" 2>/dev/null; then
            echo "⚠️ No tests collected"
        else
            echo "⚠️ Could not parse test results"
            # Show the last few lines for debugging
            echo "Last 10 lines of output:"
            tail -10 "$output_file" | sed 's/^/  /'
        fi
    fi
    
    # Show final results
    echo "✅ Passed: $passed"
    echo "❌ Failed: $failed"
    echo "⏭️ Skipped: $skipped"
    echo "⚠️ Errors: $errors"
    
    # Show summary
    if [ "$failed" -eq 0 ] && [ "$errors" -eq 0 ]; then
        echo "🎉 All tests passed!"
    else
        echo "⚠️ Some tests failed or had errors"
        
        # Show recent failures if any
        if [ "$failed" -gt 0 ] || [ "$errors" -gt 0 ]; then
            echo "📋 Recent failures:"
            tail -20 "$output_file" | grep -E "(FAILED|ERROR)" | head -3 | sed 's/^/  /'
        fi
    fi
    
    # Clean up
    rm -f "$output_file"
    
    return $exit_code
}

# Main execution
echo "🧪 Running Mock Spark Test Suite"
echo "================================="
echo ""

# Initialize exit codes
step1_exit=0
step2_exit=0
step3_exit=0

# Step 1: All unit tests (excluding delta and performance)
echo "Starting Step 1: Unit tests..."
run_tests_with_progress "tests/unit/" "Step 1: Unit tests (parallel, 8 cores)" "true" ""
step1_exit=$?

if [ $step1_exit -ne 0 ]; then
    echo "❌ Step 1 failed with exit code $step1_exit"
else
    echo "✅ Step 1 passed"
fi

# Step 2: Delta tests  
echo "Starting Step 2: Delta tests..."
run_tests_with_progress "tests/unit/" "Step 2: Delta tests (serial)" "false" "-m delta"
step2_exit=$?

if [ $step2_exit -ne 0 ]; then
    echo "❌ Step 2 failed with exit code $step2_exit"
else
    echo "✅ Step 2 passed"
fi

# Step 3: Performance tests
echo "Starting Step 3: Performance tests..."
run_tests_with_progress "tests/unit/" "Step 3: Performance tests (serial)" "false" "-m performance"
step3_exit=$?

if [ $step3_exit -ne 0 ]; then
    echo "❌ Step 3 failed with exit code $step3_exit"
else
    echo "✅ Step 3 passed"
fi

echo ""
echo "================================="
echo "📊 Test Suite Summary"
echo "================================="
echo ""

# Show individual phase results
if [ $step1_exit -eq 0 ]; then
    echo "✅ Non-Delta tests: PASSED"
else
    echo "❌ Non-Delta tests: FAILED (exit code: $step1_exit)"
fi

if [ $step2_exit -eq 0 ]; then
    echo "✅ Delta tests: PASSED"
else
    echo "❌ Delta tests: FAILED (exit code: $step2_exit)"
fi

if [ $step3_exit -eq 0 ]; then
    echo "✅ Performance tests: PASSED"
else
    echo "❌ Performance tests: FAILED (exit code: $step3_exit)"
fi

echo ""
echo "Configuration:"
echo "• Non-Delta tests: Run in parallel with 8 cores (loadfile distribution)"
echo "• Delta tests: Run serially with isolated Spark sessions"
echo "• Performance tests: Run serially for stable timing measurements"
echo ""
echo "Note: --dist loadfile ensures each worker processes complete test files,"
echo "      preventing PySpark session conflicts in compatibility tests."

# Final result
if [ $step1_exit -ne 0 ] || [ $step2_exit -ne 0 ] || [ $step3_exit -ne 0 ]; then
    echo ""
    echo "❌ Test suite FAILED"
    exit 1
else
    echo ""
    echo "🎉 Test suite PASSED"
    exit 0
fi