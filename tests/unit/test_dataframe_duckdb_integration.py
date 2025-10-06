"""
Test DataFrame integration with DuckDB for Phase 2 migration.

Tests the new toPandas() optional dependency and toDuckDB() analytical features.
"""

import pytest
import duckdb
from unittest.mock import patch, MagicMock
from mock_spark.dataframe.dataframe import MockDataFrame
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType, DoubleType


class TestDataFrameDuckDBIntegration:
    """Test DataFrame integration with DuckDB."""

    def test_topandas_optional_dependency_success(self):
        """Test toPandas() works when pandas is available."""
        # Create test DataFrame
        schema = MockStructType([
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType())
        ])
        
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ]
        
        df = MockDataFrame(data, schema)
        
        # Mock pandas import
        with patch('pandas.DataFrame') as mock_pd:
            mock_df = MagicMock()
            mock_pd.return_value = mock_df
            
            # Should not raise ImportError
            result = df.toPandas()
            
            # Verify pandas DataFrame was called with correct data
            mock_pd.assert_called_once_with(data)
            assert result == mock_df

    def test_topandas_optional_dependency_missing(self):
        """Test toPandas() raises ImportError when pandas is missing."""
        # Create test DataFrame
        schema = MockStructType([
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType())
        ])
        
        data = [{"name": "Alice", "age": 30}]
        df = MockDataFrame(data, schema)
        
        # Mock pandas import to raise ImportError
        with patch('builtins.__import__', side_effect=ImportError("No module named 'pandas'")):
            with pytest.raises(ImportError) as exc_info:
                df.toPandas()
            
            assert "pandas is required for toPandas() method" in str(exc_info.value)
            assert "pip install mock-spark[pandas]" in str(exc_info.value)

    def test_topandas_empty_dataframe(self):
        """Test toPandas() with empty DataFrame."""
        schema = MockStructType([
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType())
        ])
        
        df = MockDataFrame([], schema)
        
        with patch('pandas.DataFrame') as mock_pd:
            mock_df = MagicMock()
            mock_pd.return_value = mock_df
            
            result = df.toPandas()
            
            # Should create empty DataFrame with correct columns
            mock_pd.assert_called_once_with(columns=["name", "age"])
            assert result == mock_df

    def test_toduckdb_basic_functionality(self):
        """Test toDuckDB() basic functionality."""
        # Create test DataFrame
        schema = MockStructType([
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType()),
            MockStructField("salary", DoubleType())
        ])
        
        data = [
            {"name": "Alice", "age": 30, "salary": 75000.0},
            {"name": "Bob", "age": 25, "salary": 60000.0}
        ]
        
        df = MockDataFrame(data, schema)
        
        # Test with default connection
        table_name = df.toDuckDB()
        assert table_name.startswith("temp_df_")
        
        # Test with custom connection and table name
        conn = duckdb.connect(":memory:")
        custom_table = df.toDuckDB(connection=conn, table_name="employees")
        assert custom_table == "employees"

    def test_toduckdb_data_integrity(self):
        """Test that data is correctly inserted into DuckDB."""
        schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ])
        
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        
        df = MockDataFrame(data, schema)
        conn = duckdb.connect(":memory:")
        table_name = df.toDuckDB(connection=conn, table_name="test_table")
        
        # Verify data was inserted correctly
        result = conn.execute(f"SELECT * FROM {table_name} ORDER BY id").fetchall()
        
        assert len(result) == 2
        assert result[0] == (1, "Alice")
        assert result[1] == (2, "Bob")

    def test_toduckdb_schema_mapping(self):
        """Test that MockSpark types are correctly mapped to DuckDB types."""
        schema = MockStructType([
            MockStructField("str_field", StringType()),
            MockStructField("int_field", IntegerType()),
            MockStructField("double_field", DoubleType())
        ])
        
        df = MockDataFrame([], schema)
        conn = duckdb.connect(":memory:")
        table_name = df.toDuckDB(connection=conn, table_name="type_test")
        
        # Check table schema
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        
        # Verify type mappings
        types = {row[0]: row[1] for row in result}
        assert types["str_field"] == "VARCHAR"
        assert types["int_field"] == "INTEGER"
        assert types["double_field"] == "DOUBLE"

    def test_toduckdb_empty_dataframe(self):
        """Test toDuckDB() with empty DataFrame."""
        schema = MockStructType([
            MockStructField("name", StringType()),
            MockStructField("age", IntegerType())
        ])
        
        df = MockDataFrame([], schema)
        conn = duckdb.connect(":memory:")
        table_name = df.toDuckDB(connection=conn, table_name="empty_table")
        
        # Table should be created but empty
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        assert result[0] == 0

    def test_toduckdb_analytical_operations(self):
        """Test analytical operations on DuckDB table."""
        schema = MockStructType([
            MockStructField("department", StringType()),
            MockStructField("salary", DoubleType())
        ])
        
        data = [
            {"department": "Engineering", "salary": 80000.0},
            {"department": "Engineering", "salary": 90000.0},
            {"department": "Marketing", "salary": 60000.0},
            {"department": "Marketing", "salary": 70000.0}
        ]
        
        df = MockDataFrame(data, schema)
        conn = duckdb.connect(":memory:")
        table_name = df.toDuckDB(connection=conn, table_name="analytics_test")
        
        # Test analytical query
        result = conn.execute(f"""
            SELECT 
                department,
                COUNT(*) as count,
                AVG(salary) as avg_salary,
                MAX(salary) as max_salary
            FROM {table_name}
            GROUP BY department
            ORDER BY avg_salary DESC
        """).fetchall()
        
        assert len(result) == 2
        # Engineering should have higher average salary
        assert result[0][0] == "Engineering"
        assert result[0][1] == 2  # count
        assert result[0][2] == 85000.0  # avg_salary
        assert result[0][3] == 90000.0  # max_salary

    def test_toduckdb_duckdb_missing(self):
        """Test toDuckDB() raises ImportError when DuckDB is missing."""
        schema = MockStructType([
            MockStructField("name", StringType())
        ])
        
        df = MockDataFrame([{"name": "Alice"}], schema)
        
        # Mock duckdb import to raise ImportError
        with patch('builtins.__import__', side_effect=ImportError("No module named 'duckdb'")):
            with pytest.raises(ImportError) as exc_info:
                df.toDuckDB()
            
            assert "duckdb is required for toDuckDB() method" in str(exc_info.value)

    def test_duckdb_type_mapping_comprehensive(self):
        """Test comprehensive type mapping from MockSpark to DuckDB."""
        from mock_spark.spark_types import LongType, FloatType, BooleanType
        
        schema = MockStructType([
            MockStructField("string_col", StringType()),
            MockStructField("integer_col", IntegerType()),
            MockStructField("long_col", LongType()),
            MockStructField("double_col", DoubleType()),
            MockStructField("float_col", FloatType()),
            MockStructField("boolean_col", BooleanType()),
        ])
        
        df = MockDataFrame([], schema)
        
        # Test type mapping method directly
        assert df._get_duckdb_type(StringType()) == "VARCHAR"
        assert df._get_duckdb_type(IntegerType()) == "INTEGER"
        assert df._get_duckdb_type(LongType()) == "BIGINT"
        assert df._get_duckdb_type(DoubleType()) == "DOUBLE"
        assert df._get_duckdb_type(FloatType()) == "DOUBLE"
        assert df._get_duckdb_type(BooleanType()) == "BOOLEAN"
        assert df._get_duckdb_type("unknown_type") == "VARCHAR"  # fallback

    def test_dataframe_duckdb_integration_performance(self):
        """Test performance of DataFrame to DuckDB conversion."""
        # Create larger dataset
        schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("value", DoubleType()),
            MockStructField("category", StringType())
        ])
        
        # Generate 1000 rows of test data
        data = []
        for i in range(1000):
            data.append({
                "id": i,
                "value": float(i * 1.5),
                "category": f"cat_{i % 10}"
            })
        
        df = MockDataFrame(data, schema)
        
        # Test conversion performance
        import time
        start_time = time.time()
        
        conn = duckdb.connect(":memory:")
        table_name = df.toDuckDB(connection=conn, table_name="perf_test")
        
        conversion_time = time.time() - start_time
        
        # Verify all data was inserted
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        assert result[0] == 1000
        
        # Performance should be reasonable (less than 1 second for 1000 rows)
        assert conversion_time < 1.0
        
        # Test analytical query performance
        start_time = time.time()
        result = conn.execute(f"""
            SELECT 
                category,
                COUNT(*) as count,
                AVG(value) as avg_value,
                SUM(value) as total_value
            FROM {table_name}
            GROUP BY category
            ORDER BY avg_value DESC
        """).fetchall()
        
        query_time = time.time() - start_time
        
        assert len(result) == 10  # 10 categories
        assert query_time < 0.1  # Analytical queries should be very fast


if __name__ == "__main__":
    pytest.main([__file__])
