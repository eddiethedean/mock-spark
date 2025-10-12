"""Unit tests for execution exceptions."""

import pytest
from mock_spark.core.exceptions.execution import (
    ExecutionException,
    DataSourceException,
    QueryExecutionException,
    TaskFailedException
)


def test_execution_exception_creation():
    """Test ExecutionException can be created and raised."""
    with pytest.raises(ExecutionException) as exc_info:
        raise ExecutionException("Execution failed")
    
    assert "Execution failed" in str(exc_info.value)


def test_data_source_exception():
    """Test DataSourceException."""
    with pytest.raises(DataSourceException) as exc_info:
        raise DataSourceException("Failed to read data source")
    
    assert "data source" in str(exc_info.value).lower()
    assert isinstance(exc_info.value, ExecutionException)


def test_data_source_with_path():
    """Test DataSourceException with file path."""
    exc = DataSourceException("Cannot read file", path="/data/file.csv")
    assert "/data/file.csv" in str(exc)


def test_query_execution_exception():
    """Test QueryExecutionException."""
    with pytest.raises(QueryExecutionException) as exc_info:
        raise QueryExecutionException("Query execution failed")
    
    assert "Query execution" in str(exc_info.value)
    assert isinstance(exc_info.value, ExecutionException)


def test_query_execution_with_query():
    """Test QueryExecutionException with SQL query."""
    exc = QueryExecutionException("Execution error", query="SELECT * FROM table")
    assert "SELECT" in str(exc)


def test_task_failed_exception():
    """Test TaskFailedException."""
    with pytest.raises(TaskFailedException) as exc_info:
        raise TaskFailedException("Task execution failed")
    
    assert "Task" in str(exc_info.value)
    assert isinstance(exc_info.value, ExecutionException)


def test_task_failed_with_details():
    """Test TaskFailedException with task details."""
    exc = TaskFailedException("Task failed", task_id=123, stage_id=5)
    assert "123" in str(exc) or "5" in str(exc)


def test_execution_exception_inheritance():
    """Test execution exception inheritance."""
    exc = DataSourceException("test")
    assert isinstance(exc, ExecutionException)
    assert isinstance(exc, Exception)


def test_execution_exception_with_cause():
    """Test execution exception with underlying cause."""
    try:
        try:
            raise IOError("File not found")
        except IOError as e:
            raise DataSourceException("Failed to read") from e
    except DataSourceException as exc:
        assert exc.__cause__ is not None
        assert isinstance(exc.__cause__, IOError)

