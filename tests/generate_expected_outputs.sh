#!/bin/bash
# Script to generate expected outputs from PySpark

echo "Generating Expected Outputs from PySpark"
echo "========================================"

# Check if PySpark is available
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo "❌ PySpark not available. Install with:"
    echo "   pip install mock-spark[generate-outputs]"
    echo "   or"
    echo "   pip install pyspark delta-spark"
    exit 1
fi

# Check if expected outputs directory exists
if [ ! -d "tests/expected_outputs" ]; then
    echo "❌ Expected outputs directory not found. Run setup first."
    exit 1
fi

echo "✅ PySpark available"
echo "📁 Expected outputs directory: tests/expected_outputs"
echo ""

# Generate expected outputs
echo "Generating expected outputs..."
python3 tests/tools/generate_expected_outputs.py --all

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Expected outputs generated successfully"
    echo "📊 Check tests/expected_outputs/ for generated files"
    echo ""
    echo "Next steps:"
    echo "1. Run tests: ./tests/run_all_tests.sh"
    echo "2. Or run specific tests: pytest tests/compatibility/"
else
    echo "❌ Failed to generate expected outputs"
    exit 1
fi
