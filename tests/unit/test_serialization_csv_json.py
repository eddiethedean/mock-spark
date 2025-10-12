"""Unit tests for CSV and JSON serialization."""

import pytest
import tempfile
import csv
import json
from pathlib import Path


def test_csv_serializer_write():
    """Test writing data to CSV."""
    from mock_spark.storage.serialization.csv import CSVSerializer
    
    serializer = CSVSerializer()
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    # Verify file exists
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_csv_serializer_read():
    """Test reading data from CSV."""
    from mock_spark.storage.serialization.csv import CSVSerializer
    
    # Create test CSV file
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", newline="") as f:
        filepath = f.name
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        writer.writerow({"id": 1, "name": "Alice"})
    
    serializer = CSVSerializer()
    data = serializer.read(filepath)
    
    assert len(data) == 1
    Path(filepath).unlink()


def test_csv_serializer_with_header():
    """Test CSV serialization with header."""
    from mock_spark.storage.serialization.csv import CSVSerializer
    
    serializer = CSVSerializer(header=True)
    data = [{"id": 1, "name": "Alice"}]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    read_data = serializer.read(filepath)
    
    assert len(read_data) == 1
    Path(filepath).unlink()


def test_csv_serializer_custom_delimiter():
    """Test CSV with custom delimiter."""
    from mock_spark.storage.serialization.csv import CSVSerializer
    
    serializer = CSVSerializer(delimiter="|")
    data = [{"id": 1, "name": "Alice"}]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_json_serializer_write():
    """Test writing data to JSON."""
    from mock_spark.storage.serialization.json import JSONSerializer
    
    serializer = JSONSerializer()
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_json_serializer_read():
    """Test reading data from JSON."""
    from mock_spark.storage.serialization.json import JSONSerializer
    
    # Create test JSON file
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        filepath = f.name
        json.dump({"id": 1, "name": "Alice"}, f)
        f.write("\n")
        json.dump({"id": 2, "name": "Bob"}, f)
    
    serializer = JSONSerializer()
    data = serializer.read(filepath)
    
    assert len(data) >= 1
    Path(filepath).unlink()


def test_json_serializer_pretty_print():
    """Test JSON with pretty printing."""
    from mock_spark.storage.serialization.json import JSONSerializer
    
    serializer = JSONSerializer(pretty_print=True)
    data = [{"id": 1, "name": "Alice"}]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_json_serializer_single_line():
    """Test JSON with single line per record."""
    from mock_spark.storage.serialization.json import JSONSerializer
    
    serializer = JSONSerializer(single_line=True)
    data = [{"id": 1}, {"id": 2}]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_csv_empty_data():
    """Test CSV serialization with empty data."""
    from mock_spark.storage.serialization.csv import CSVSerializer
    
    serializer = CSVSerializer()
    data = []
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_json_empty_data():
    """Test JSON serialization with empty data."""
    from mock_spark.storage.serialization.json import JSONSerializer
    
    serializer = JSONSerializer()
    data = []
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_csv_with_nulls():
    """Test CSV serialization with null values."""
    from mock_spark.storage.serialization.csv import CSVSerializer
    
    serializer = CSVSerializer()
    data = [{"id": 1, "name": None}, {"id": 2, "name": "Bob"}]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_json_with_nulls():
    """Test JSON serialization with null values."""
    from mock_spark.storage.serialization.json import JSONSerializer
    
    serializer = JSONSerializer()
    data = [{"id": 1, "value": None}, {"id": 2, "value": 10}]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_csv_large_dataset():
    """Test CSV serialization with large dataset."""
    from mock_spark.storage.serialization.csv import CSVSerializer
    
    serializer = CSVSerializer()
    data = [{"id": i, "value": i * 10} for i in range(1000)]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()


def test_json_large_dataset():
    """Test JSON serialization with large dataset."""
    from mock_spark.storage.serialization.json import JSONSerializer
    
    serializer = JSONSerializer()
    data = [{"id": i, "value": i * 10} for i in range(1000)]
    
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        filepath = f.name
    
    serializer.write(data, filepath)
    
    assert Path(filepath).exists()
    Path(filepath).unlink()

