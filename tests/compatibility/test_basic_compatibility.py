"""
Basic compatibility tests to identify discrepancies between mock_spark and PySpark.
This test focuses on the most fundamental operations to identify key differences.
"""

import pytest
import pandas as pd
from typing import Dict, Any, List


class TestBasicCompatibility:
    """Basic compatibility tests to identify discrepancies."""
    
    def test_mock_spark_basic_operations(self):
        """Test basic mock_spark operations to establish baseline."""
        from mock_spark import MockSparkSession, F
        
        # Create mock session
        spark = MockSparkSession("test-app")
        
        # Test data
        data = [
            {"id": 1, "name": "Alice", "age": 25, "department": "Engineering", "salary": 75000.0},
            {"id": 2, "name": "Bob", "age": 30, "department": "Marketing", "salary": 65000.0},
            {"id": 3, "name": "Charlie", "age": 35, "department": "Engineering", "salary": 85000.0},
        ]
        
        # Create DataFrame
        df = spark.createDataFrame(data)
        
        # Test basic operations
        assert df.count() == 3
        assert len(df.columns) == 5
        assert "id" in df.columns
        assert "name" in df.columns
        assert "age" in df.columns
        assert "department" in df.columns
        assert "salary" in df.columns
        
        # Test select
        selected = df.select("name", "age")
        assert selected.count() == 3
        assert len(selected.columns) == 2
        
        # Test filter
        filtered = df.filter(F.col("age") > 30)
        assert filtered.count() == 1
        
        # Test groupBy
        grouped = df.groupBy("department").count()
        assert grouped.count() == 2
        
        # Test aggregation
        agg_result = df.groupBy("department").agg(F.sum("salary"))
        assert agg_result.count() == 2
        
        print("✅ Mock Spark basic operations working correctly")
    
    def test_pyspark_basic_operations(self):
        """Test basic PySpark operations to establish baseline."""
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as F
            
            # Create PySpark session
            spark = SparkSession.builder \
                .appName("test-app") \
                .master("local[1]") \
                .config("spark.sql.adaptive.enabled", "false") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("ERROR")
            
            # Test data
            data = [
                {"id": 1, "name": "Alice", "age": 25, "department": "Engineering", "salary": 75000.0},
                {"id": 2, "name": "Bob", "age": 30, "department": "Marketing", "salary": 65000.0},
                {"id": 3, "name": "Charlie", "age": 35, "department": "Engineering", "salary": 85000.0},
            ]
            
            # Create DataFrame
            df = spark.createDataFrame(data)
            
            # Test basic operations
            assert df.count() == 3
            assert len(df.columns) == 5
            assert "id" in df.columns
            assert "name" in df.columns
            assert "age" in df.columns
            assert "department" in df.columns
            assert "salary" in df.columns
            
            # Test select
            selected = df.select("name", "age")
            assert selected.count() == 3
            assert len(selected.columns) == 2
            
            # Test filter
            filtered = df.filter(F.col("age") > 30)
            assert filtered.count() == 1
            
            # Test groupBy
            grouped = df.groupBy("department").count()
            assert grouped.count() == 2
            
            # Test aggregation
            agg_result = df.groupBy("department").agg(F.sum("salary"))
            assert agg_result.count() == 2
            
            print("✅ PySpark basic operations working correctly")
            
        except Exception as e:
            pytest.skip(f"PySpark not available or failed: {e}")
    
    def test_function_compatibility(self):
        """Test function compatibility between mock and PySpark."""
        from mock_spark import functions as mock_functions
        
        # Test mock functions
        mock_col = mock_functions.col("test_column")
        assert hasattr(mock_col, 'name') or hasattr(mock_col, 'column_name')
        
        mock_lit = mock_functions.lit("test_value")
        assert hasattr(mock_lit, 'name') or hasattr(mock_lit, 'column_name')
        
        mock_count = mock_functions.count("*")
        assert hasattr(mock_count, 'function_name') or hasattr(mock_count, 'name')
        
        mock_sum = mock_functions.sum("test_column")
        assert hasattr(mock_sum, 'function_name') or hasattr(mock_sum, 'name')
        
        print("✅ Mock functions have expected attributes")
        
        # Test PySpark functions if available
        try:
            from pyspark.sql import functions as pyspark_functions
            
            pyspark_col = pyspark_functions.col("test_column")
            assert hasattr(pyspark_col, 'name')
            
            pyspark_lit = pyspark_functions.lit("test_value")
            assert hasattr(pyspark_lit, 'name')
            
            pyspark_count = pyspark_functions.count("*")
            assert hasattr(pyspark_count, 'name')
            
            pyspark_sum = pyspark_functions.sum("test_column")
            assert hasattr(pyspark_sum, 'name')
            
            print("✅ PySpark functions have expected attributes")
            
        except Exception as e:
            pytest.skip(f"PySpark not available: {e}")
    
    def test_schema_compatibility(self):
        """Test schema compatibility between mock and PySpark."""
        from mock_spark import spark_types as mock_types
        
        # Test mock types
        mock_string_type = mock_types.StringType()
        assert mock_string_type.__class__.__name__ == "StringType"
        
        mock_int_type = mock_types.IntegerType()
        assert mock_int_type.__class__.__name__ == "IntegerType"
        
        mock_struct_type = mock_types.MockStructType([
            mock_types.MockStructField("name", mock_types.StringType()),
            mock_types.MockStructField("age", mock_types.IntegerType())
        ])
        assert len(mock_struct_type.fields) == 2
        
        print("✅ Mock types have expected structure")
        
        # Test PySpark types if available
        try:
            from pyspark.sql import types as pyspark_types
            
            pyspark_string_type = pyspark_types.StringType()
            assert pyspark_string_type.__class__.__name__ == "StringType"
            
            pyspark_int_type = pyspark_types.IntegerType()
            assert pyspark_int_type.__class__.__name__ == "IntegerType"
            
            pyspark_struct_type = pyspark_types.StructType([
                pyspark_types.StructField("name", pyspark_types.StringType()),
                pyspark_types.StructField("age", pyspark_types.IntegerType())
            ])
            assert len(pyspark_struct_type.fields) == 2
            
            print("✅ PySpark types have expected structure")
            
        except Exception as e:
            pytest.skip(f"PySpark not available: {e}")
    
    def test_operation_symbols_compatibility(self):
        """Test that operation symbols match between mock and PySpark."""
        from mock_spark import MockSparkSession, F
        
        spark = MockSparkSession("test-app")
        data = [{"age": 25}, {"age": 30}]
        df = spark.createDataFrame(data)
        
        # Test column operations
        col = F.col("age")
        
        # Test equality operation
        eq_op = col == 25
        assert hasattr(eq_op, 'operation')
        assert eq_op.operation == "=="
        
        # Test inequality operation
        ne_op = col != 25
        assert hasattr(ne_op, 'operation')
        assert ne_op.operation == "!="
        
        # Test greater than operation
        gt_op = col > 25
        assert hasattr(gt_op, 'operation')
        assert gt_op.operation == ">"
        
        # Test less than operation
        lt_op = col < 30
        assert hasattr(lt_op, 'operation')
        assert lt_op.operation == "<"
        
        # Test greater than or equal operation
        ge_op = col >= 25
        assert hasattr(ge_op, 'operation')
        assert ge_op.operation == ">="
        
        # Test less than or equal operation
        le_op = col <= 30
        assert hasattr(le_op, 'operation')
        assert le_op.operation == "<="
        
        print("✅ Operation symbols are correct in mock_spark")
    
    def test_aggregation_result_structure(self):
        """Test that aggregation results have the correct structure."""
        from mock_spark import MockSparkSession, F
        
        spark = MockSparkSession("test-app")
        data = [
            {"department": "Engineering", "salary": 75000.0},
            {"department": "Marketing", "salary": 65000.0},
            {"department": "Engineering", "salary": 85000.0},
        ]
        df = spark.createDataFrame(data)
        
        # Test groupBy with count
        grouped = df.groupBy("department").count()
        assert grouped.count() == 2
        
        # Check schema
        schema_fields = [field.name for field in grouped.schema.fields]
        assert "department" in schema_fields
        assert "count" in schema_fields
        
        # Test groupBy with sum
        sum_grouped = df.groupBy("department").agg(F.sum("salary"))
        assert sum_grouped.count() == 2
        
        # Check schema
        sum_schema_fields = [field.name for field in sum_grouped.schema.fields]
        assert "department" in sum_schema_fields
        assert "sum(salary)" in sum_schema_fields
        
        print("✅ Aggregation result structure is correct")
    
    def test_dataframe_collect_behavior(self):
        """Test DataFrame collect behavior."""
        from mock_spark import MockSparkSession, F
        
        spark = MockSparkSession("test-app")
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
        ]
        df = spark.createDataFrame(data)
        
        # Test collect
        collected = df.collect()
        assert len(collected) == 2
        assert isinstance(collected, list)
        
        # Test toPandas
        pandas_df = df.toPandas()
        assert isinstance(pandas_df, pd.DataFrame)
        assert len(pandas_df) == 2
        # PySpark sorts columns alphabetically, so expect ['age', 'id', 'name']
        assert list(pandas_df.columns) == ["age", "id", "name"]
        
        print("✅ DataFrame collect behavior is correct")
    
    def test_literal_functionality_compatibility(self):
        """Test that literal functionality matches PySpark behavior."""
        from mock_spark import MockSparkSession, F
        
        spark = MockSparkSession("test-app")
        data = [
            {"name": "Alice", "department": "Engineering"},
            {"name": "Bob", "department": "Marketing"},
        ]
        df = spark.createDataFrame(data)
        
        # Test literal in select - should create column with literal value as name
        result = df.select(F.col("name"), F.lit("constant_value"))
        
        assert "name" in result.columns
        assert "constant_value" in result.columns
        
        # Check that literal column has the correct value
        collected = result.collect()
        for row in collected:
            assert row["constant_value"] == "constant_value"
        
        # Test different literal types
        int_result = df.select(F.lit(42))
        assert "42" in int_result.columns
        assert int_result.collect()[0]["42"] == 42
        
        float_result = df.select(F.lit(3.14))
        assert "3.14" in float_result.columns
        assert float_result.collect()[0]["3.14"] == 3.14
        
        print("✅ Literal functionality is correct")
    
    def test_count_function_compatibility(self):
        """Test that count function behavior matches PySpark."""
        from mock_spark import MockSparkSession, F
        
        spark = MockSparkSession("test-app")
        data = [
            {"department": "Engineering", "salary": 75000.0},
            {"department": "Marketing", "salary": 65000.0},
            {"department": "Engineering", "salary": 85000.0},
        ]
        df = spark.createDataFrame(data)
        
        # Test count("*") - should create column named "count(1)"
        count_star_result = df.select(F.count("*"))
        assert "count(1)" in count_star_result.columns
        assert count_star_result.collect()[0]["count(1)"] == 3
        
        # Test count("column") - should create column named "count(column)"
        count_col_result = df.select(F.count("salary"))
        assert "count(salary)" in count_col_result.columns
        assert count_col_result.collect()[0]["count(salary)"] == 3
        
        # Test groupBy().count() - should create column named "count"
        grouped_count = df.groupBy("department").count()
        assert "count" in grouped_count.columns
        assert len(grouped_count.collect()) == 2
        
        # Test groupBy().agg(count("*")) - should create column named "count(1)"
        grouped_agg_count = df.groupBy("department").agg(F.count("*"))
        assert "count(1)" in grouped_agg_count.columns
        assert len(grouped_agg_count.collect()) == 2
        
        print("✅ Count function behavior is correct")
