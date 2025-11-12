#!/bin/bash
# Installation script for Mock Spark package

set -e

echo "🚀 Installing Mock Spark package..."

# Check if Python is available
if ! command -v python &> /dev/null; then
    echo "❌ Python is not installed or not in PATH"
    exit 1
fi

# Check Python version
python_version=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
required_version="3.9"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Python $required_version or higher is required. Found: $python_version"
    exit 1
fi

echo "✅ Python $python_version detected"

# Install the package
echo "📦 Installing Mock Spark..."
pip install -e .

# Run full test suite with proper isolation
echo "🧪 Running full test suite..."
bash tests/run_all_tests.sh

echo "✅ Mock Spark installed successfully!"
echo ""
echo "Usage:"
echo "  from mock_spark import MockSparkSession"
echo "  spark = MockSparkSession('MyApp')"
echo ""
echo "For development:"
echo "  pip install -e .[dev]"
