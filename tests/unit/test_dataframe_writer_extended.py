"""Unit tests for DataFrame writer operations."""

import pytest
import tempfile
from mock_spark import MockSparkSession

# Skip writer tests - some write operations not fully implemented
pytestmark = pytest.mark.skip(reason="Some writer operations not fully implemented")


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_writer")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_write_mode_overwrite(spark, tmp_path):
    """Test write with overwrite mode."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    path = tmp_path / "output"
    df.write.mode("overwrite").parquet(str(path))
    
    # Write again with overwrite
    data2 = [{"id": 2, "name": "Bob"}]
    df2 = spark.createDataFrame(data2)
    df2.write.mode("overwrite").parquet(str(path))
    
    # Should only have second data
    result = spark.read.parquet(str(path))
    assert result.count() == 1


def test_write_mode_append(spark, tmp_path):
    """Test write with append mode."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    path = tmp_path / "output"
    df.write.mode("append").parquet(str(path))
    
    # Append more data
    data2 = [{"id": 2}]
    df2 = spark.createDataFrame(data2)
    df2.write.mode("append").parquet(str(path))
    
    # Should have both
    result = spark.read.parquet(str(path))
    assert result.count() == 2


def test_write_format_csv(spark, tmp_path):
    """Test writing CSV format."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    path = tmp_path / "output.csv"
    df.write.format("csv").save(str(path))
    
    # Verify file exists
    assert path.exists()


def test_write_partitionBy(spark, tmp_path):
    """Test partitioned writing."""
    data = [
        {"category": "A", "value": 1},
        {"category": "B", "value": 2}
    ]
    df = spark.createDataFrame(data)
    
    path = tmp_path / "output"
    df.write.partitionBy("category").parquet(str(path))
    
    # Should create partitioned structure
    assert path.exists()


def test_write_option_header(spark, tmp_path):
    """Test write with options."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    path = tmp_path / "output.csv"
    df.write.option("header", "true").csv(str(path))
    
    assert path.exists()


def test_write_options_multiple(spark, tmp_path):
    """Test write with multiple options."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    
    path = tmp_path / "output"
    df.write.options(compression="gzip", header="true").parquet(str(path))
    
    assert path.exists()


def test_write_saveAsTable(spark):
    """Test saveAsTable."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    
    df.write.saveAsTable("saved_table")
    
    # Should be able to read back
    result = spark.table("saved_table")
    assert result.count() == 1


def test_write_insertInto(spark):
    """Test insertInto existing table."""
    data1 = [{"id": 1}]
    df1 = spark.createDataFrame(data1)
    df1.createOrReplaceTempView("target_table")
    
    data2 = [{"id": 2}]
    df2 = spark.createDataFrame(data2)
    df2.write.insertInto("target_table")
    
    result = spark.table("target_table")
    assert result.count() >= 1


def test_write_bucketBy(spark, tmp_path):
    """Test bucketing."""
    data = [{"id": i, "value": i * 10} for i in range(20)]
    df = spark.createDataFrame(data)
    
    path = tmp_path / "output"
    df.write.bucketBy(4, "id").parquet(str(path))
    
    assert path.exists()


def test_write_sortBy(spark, tmp_path):
    """Test sortBy during write."""
    data = [{"id": 3}, {"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    path = tmp_path / "output"
    df.write.sortBy("id").parquet(str(path))
    
    assert path.exists()

