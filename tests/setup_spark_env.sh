#!/bin/bash

# Setup Spark environment with Java 11, Python 3.x, and PySpark

set -euo pipefail

# Resolve Python binary
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python is not installed or not on PATH." >&2
  exit 1
fi
PIP_BIN="$PYTHON_BIN -m pip"

# Locate Java 11 (prefer Homebrew openjdk@11)
JAVA_CANDIDATE=""
if [ -d "/opt/homebrew/Cellar/openjdk@11" ]; then
  # Pick the latest installed openjdk@11 under Homebrew
  BREW_JDK_PATH=$(ls -d /opt/homebrew/Cellar/openjdk@11/* 2>/dev/null | sort -V | tail -n 1)/libexec/openjdk.jdk/Contents/Home
  if [ -d "$BREW_JDK_PATH" ]; then
    JAVA_CANDIDATE="$BREW_JDK_PATH"
  fi
fi
if [ -z "${JAVA_CANDIDATE}" ]; then
  if /usr/libexec/java_home -v 11 >/dev/null 2>&1; then
    JAVA_CANDIDATE="$(/usr/libexec/java_home -v 11)"
  fi
fi

if [ -z "${JAVA_CANDIDATE}" ]; then
  echo "Java 11 not found. Please install Java 11 (e.g., 'brew install openjdk@11') and re-run." >&2
  exit 1
fi

export JAVA_HOME="${JAVA_CANDIDATE}"
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PYTHON_BIN"
export SPARK_LOCAL_IP=127.0.0.1
# Helpful JVM flags for newer macOS/JDKs; safe no-ops on JDK 11
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"

# Verify versions
echo "=== Environment Setup ==="
echo "Java version:"
java -version

echo -e "\nPython version:"
"$PYTHON_BIN" --version

echo -e "\nPySpark version:"
"$PYTHON_BIN" - <<'PY'
try:
    import pyspark  # type: ignore
    print(pyspark.__version__)
except Exception as e:
    print(f"PySpark not available: {e}")
PY

# Ensure PySpark 3.2.x
NEED_INSTALL=0
CURRENT_VER=$("$PYTHON_BIN" - <<'PY'
try:
    import pyspark
    print(pyspark.__version__)
except Exception:
    print("")
PY
)
case "$CURRENT_VER" in
  3.2.*) echo "PySpark already 3.2.x ($CURRENT_VER)" ;;
  "") echo "Installing PySpark 3.2.x..." ; NEED_INSTALL=1 ;;
  *) echo "Found PySpark $CURRENT_VER; installing 3.2.x for compatibility..." ; NEED_INSTALL=1 ;;
esac

if [ "$NEED_INSTALL" = "1" ]; then
  # Try best-effort install of PySpark 3.2 latest patch
  $PIP_BIN install --quiet 'pyspark>=3.2,<3.3' || {
    echo "Failed to install PySpark 3.2.x. Please install manually: pip install 'pyspark>=3.2,<3.3'" >&2
    exit 1
  }
fi

echo -e "\n=== Testing PySpark ==="
"$PYTHON_BIN" - <<'PY'
try:
    from pyspark.sql import SparkSession  # type: ignore
    spark = SparkSession.builder.appName('test').getOrCreate()
    print('Spark version:', spark.version)
    print('Spark context created successfully!')
    spark.stop()
    print('Spark session stopped.')
except Exception as e:
    print(f"Skipping PySpark test: {e}")
PY

echo -e "\n=== Testing Mock Spark ==="
"$PYTHON_BIN" - <<'PY'
import sys
sys.path.insert(0, '.')
from mock_spark import MockSparkSession
spark = MockSparkSession()
print('Mock Spark session created successfully!')
print('Mock Spark version:', spark.version)
PY

echo -e "\n=== Environment Ready! ==="
echo "You can now run tests with:"
echo "export JAVA_HOME=\"$JAVA_HOME\""
echo "export PATH=\$JAVA_HOME/bin:\$PATH"
echo "$PYTHON_BIN -m pytest tests/ -v"
