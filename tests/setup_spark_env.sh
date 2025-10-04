#!/bin/bash

# Setup Spark environment with Java 11, Python 3.8, and PySpark 3.2

# Set Java 11
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.28/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

# Verify versions
echo "=== Environment Setup ==="
echo "Java version:"
java -version

echo -e "\nPython version:"
python --version

echo -e "\nPySpark version:"
python -c "import pyspark; print(pyspark.__version__)"

echo -e "\n=== Testing PySpark ==="
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
print('Spark version:', spark.version)
print('Spark context created successfully!')
spark.stop()
print('Spark session stopped.')
"

echo -e "\n=== Testing Mock Spark ==="
python -c "
import sys
sys.path.insert(0, '.')
from mock_spark import MockSparkSession
spark = MockSparkSession()
print('Mock Spark session created successfully!')
print('Mock Spark version:', spark.version)
"

echo -e "\n=== Environment Ready! ==="
echo "You can now run tests with:"
echo "export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.28/libexec/openjdk.jdk/Contents/Home"
echo "export PATH=\$JAVA_HOME/bin:\$PATH"
echo "python -m pytest tests/ -v"
