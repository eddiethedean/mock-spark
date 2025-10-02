"""
Tests for Mock Functions implementation.
"""

import pytest
from mock_spark.functions import F, MockColumn, MockLiteral, MockAggregateFunction, MockWindowFunction
from mock_spark.spark_types import StringType, IntegerType


class TestMockColumn:
    """Test MockColumn."""
    
    def test_column_creation(self):
        """Test creating MockColumn."""
        col = F.col("test_column")
        assert isinstance(col, MockColumn)
        assert col.column_name == "test_column"
        assert col.column_type == StringType()
    
    def test_column_creation_with_type(self):
        """Test creating MockColumn with specific type."""
        col = F.col("test_column", IntegerType())
        assert col.column_name == "test_column"
        assert col.column_type == IntegerType()
    
    def test_equality_operation(self):
        """Test equality operation."""
        col = F.col("age")
        result = col == 25
        assert isinstance(result, MockColumn)
        assert result.column_name == "age"
        assert result.operation == "=="
        assert result.operand == 25
    
    def test_inequality_operation(self):
        """Test inequality operation."""
        col = F.col("age")
        result = col != 25
        assert result.operation == "!="
        assert result.operand == 25
    
    def test_less_than_operation(self):
        """Test less than operation."""
        col = F.col("age")
        result = col < 30
        assert result.operation == "<"
        assert result.operand == 30
    
    def test_less_than_equal_operation(self):
        """Test less than or equal operation."""
        col = F.col("age")
        result = col <= 30
        assert result.operation == "<="
        assert result.operand == 30
    
    def test_greater_than_operation(self):
        """Test greater than operation."""
        col = F.col("age")
        result = col > 30
        assert result.operation == ">"
        assert result.operand == 30
    
    def test_greater_than_equal_operation(self):
        """Test greater than or equal operation."""
        col = F.col("age")
        result = col >= 30
        assert result.operation == ">="
        assert result.operand == 30
    
    def test_is_null(self):
        """Test isNull operation."""
        col = F.col("name")
        result = col.isNull()
        assert result.operation == "isNull"
        assert result.operand is None
    
    def test_is_not_null(self):
        """Test isNotNull operation."""
        col = F.col("name")
        result = col.isNotNull()
        assert result.operation == "isNotNull"
        assert result.operand is None
    
    def test_like_operation(self):
        """Test like operation."""
        col = F.col("name")
        result = col.like("A%")
        assert result.operation == "like"
        assert result.operand == "A%"
    
    def test_isin_operation(self):
        """Test isin operation."""
        col = F.col("age")
        result = col.isin([25, 30, 35])
        assert result.operation == "isin"
        assert result.operand == [25, 30, 35]
    
    def test_between_operation(self):
        """Test between operation."""
        col = F.col("age")
        result = col.between(25, 35)
        assert result.operation == "between"
        assert result.operand == (25, 35)
    
    def test_and_operation(self):
        """Test AND operation."""
        col1 = F.col("age") > 25
        col2 = F.col("age") < 35
        result = col1 & col2
        assert result.operation == "and"
        assert result.operand == col2
    
    def test_or_operation(self):
        """Test OR operation."""
        col1 = F.col("age") == 25
        col2 = F.col("age") == 35
        result = col1 | col2
        assert result.operation == "or"
        assert result.operand == col2
    
    def test_repr(self):
        """Test string representation."""
        col = F.col("test_column")
        repr_str = repr(col)
        assert "MockColumn" in repr_str
        assert "test_column" in repr_str


class TestMockLiteral:
    """Test MockLiteral."""
    
    def test_literal_creation(self):
        """Test creating MockLiteral."""
        lit = F.lit("test_value")
        assert isinstance(lit, MockLiteral)
        assert lit.value == "test_value"
        assert lit.column_type == StringType()
    
    def test_literal_creation_with_type(self):
        """Test creating MockLiteral with specific type."""
        lit = F.lit(42, IntegerType())
        assert lit.value == 42
        assert lit.column_type == IntegerType()
    
    def test_literal_repr(self):
        """Test string representation."""
        lit = F.lit("test")
        repr_str = repr(lit)
        assert "MockLiteral" in repr_str
        assert "test" in repr_str


class TestMockAggregateFunction:
    """Test MockAggregateFunction."""
    
    def test_count_function(self):
        """Test count function."""
        count_func = F.count()
        assert isinstance(count_func, MockAggregateFunction)
        assert count_func.function_name == "count"
        assert count_func.column_name is None
    
    def test_count_function_with_column(self):
        """Test count function with column."""
        count_func = F.count("age")
        assert count_func.function_name == "count"
        assert count_func.column_name == "age"
    
    def test_sum_function(self):
        """Test sum function."""
        sum_func = F.sum("salary")
        assert isinstance(sum_func, MockAggregateFunction)
        assert sum_func.function_name == "sum"
        assert sum_func.column_name == "salary"
    
    def test_avg_function(self):
        """Test avg function."""
        avg_func = F.avg("salary")
        assert avg_func.function_name == "avg"
        assert avg_func.column_name == "salary"
    
    def test_max_function(self):
        """Test max function."""
        max_func = F.max("salary")
        assert max_func.function_name == "max"
        assert max_func.column_name == "salary"
    
    def test_min_function(self):
        """Test min function."""
        min_func = F.min("salary")
        assert min_func.function_name == "min"
        assert min_func.column_name == "salary"
    
    def test_aggregate_function_repr(self):
        """Test string representation."""
        sum_func = F.sum("salary")
        repr_str = repr(sum_func)
        assert "MockAggregateFunction" in repr_str
        assert "sum" in repr_str
        assert "salary" in repr_str


class TestMockWindowFunction:
    """Test MockWindowFunction."""
    
    def test_row_number_function(self):
        """Test row_number function."""
        row_num = F.row_number()
        assert isinstance(row_num, MockWindowFunction)
        assert row_num.function_name == "row_number"
        assert row_num.column_name is None
    
    def test_rank_function(self):
        """Test rank function."""
        rank = F.rank()
        assert rank.function_name == "rank"
        assert rank.column_name is None
    
    def test_dense_rank_function(self):
        """Test dense_rank function."""
        dense_rank = F.dense_rank()
        assert dense_rank.function_name == "dense_rank"
        assert dense_rank.column_name is None
    
    def test_window_function_repr(self):
        """Test string representation."""
        row_num = F.row_number()
        repr_str = repr(row_num)
        assert "MockWindowFunction" in repr_str
        assert "row_number" in repr_str


class TestFunctionsModule:
    """Test the F module."""
    
    def test_col_function(self):
        """Test F.col function."""
        col = F.col("test")
        assert isinstance(col, MockColumn)
        assert col.column_name == "test"
    
    def test_lit_function(self):
        """Test F.lit function."""
        lit = F.lit("test")
        assert isinstance(lit, MockLiteral)
        assert lit.value == "test"
    
    def test_count_function(self):
        """Test F.count function."""
        count = F.count()
        assert isinstance(count, MockAggregateFunction)
        assert count.function_name == "count"
    
    def test_sum_function(self):
        """Test F.sum function."""
        sum_func = F.sum("test")
        assert isinstance(sum_func, MockAggregateFunction)
        assert sum_func.function_name == "sum"
    
    def test_avg_function(self):
        """Test F.avg function."""
        avg = F.avg("test")
        assert isinstance(avg, MockAggregateFunction)
        assert avg.function_name == "avg"
    
    def test_max_function(self):
        """Test F.max function."""
        max_func = F.max("test")
        assert isinstance(max_func, MockAggregateFunction)
        assert max_func.function_name == "max"
    
    def test_min_function(self):
        """Test F.min function."""
        min_func = F.min("test")
        assert isinstance(min_func, MockAggregateFunction)
        assert min_func.function_name == "min"
    
    def test_row_number_function(self):
        """Test F.row_number function."""
        row_num = F.row_number()
        assert isinstance(row_num, MockWindowFunction)
        assert row_num.function_name == "row_number"
    
    def test_rank_function(self):
        """Test F.rank function."""
        rank = F.rank()
        assert isinstance(rank, MockWindowFunction)
        assert rank.function_name == "rank"
    
    def test_dense_rank_function(self):
        """Test F.dense_rank function."""
        dense_rank = F.dense_rank()
        assert isinstance(dense_rank, MockWindowFunction)
        assert dense_rank.function_name == "dense_rank"