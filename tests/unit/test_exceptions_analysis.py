"""Unit tests for analysis exceptions."""

import pytest
from mock_spark.core.exceptions.analysis import (
    AnalysisException,
    ColumnNotFoundException,
    TableNotFoundException,
    AmbiguousColumnException,
    TemporaryViewAlreadyExistsException
)


def test_analysis_exception_creation():
    """Test AnalysisException can be created and raised."""
    with pytest.raises(AnalysisException) as exc_info:
        raise AnalysisException("Test analysis error")
    
    assert "Test analysis error" in str(exc_info.value)


def test_analysis_exception_with_context():
    """Test AnalysisException with error context."""
    exc = AnalysisException("Query failed", error_class="QUERY_ERROR")
    assert "Query failed" in str(exc)


def test_column_not_found_exception():
    """Test ColumnNotFoundException."""
    with pytest.raises(ColumnNotFoundException) as exc_info:
        raise ColumnNotFoundException("Column 'age' not found")
    
    assert "age" in str(exc_info.value)
    assert isinstance(exc_info.value, AnalysisException)


def test_column_not_found_with_column_name():
    """Test ColumnNotFoundException with specific column."""
    exc = ColumnNotFoundException("Column not found", column_name="user_id")
    assert "user_id" in str(exc)


def test_table_not_found_exception():
    """Test TableNotFoundException."""
    with pytest.raises(TableNotFoundException) as exc_info:
        raise TableNotFoundException("Table 'users' not found")
    
    assert "users" in str(exc_info.value)
    assert isinstance(exc_info.value, AnalysisException)


def test_table_not_found_with_table_name():
    """Test TableNotFoundException with specific table."""
    exc = TableNotFoundException("Table not found", table_name="orders")
    assert "orders" in str(exc)


def test_ambiguous_column_exception():
    """Test AmbiguousColumnException."""
    with pytest.raises(AmbiguousColumnException) as exc_info:
        raise AmbiguousColumnException("Column 'id' is ambiguous")
    
    assert "ambiguous" in str(exc_info.value).lower()
    assert isinstance(exc_info.value, AnalysisException)


def test_ambiguous_column_with_details():
    """Test AmbiguousColumnException with column details."""
    exc = AmbiguousColumnException(
        "Ambiguous column reference",
        column_name="id",
        tables=["t1", "t2"]
    )
    assert "id" in str(exc)


def test_temporary_view_already_exists():
    """Test TemporaryViewAlreadyExistsException."""
    with pytest.raises(TemporaryViewAlreadyExistsException) as exc_info:
        raise TemporaryViewAlreadyExistsException("View 'temp_view' already exists")
    
    assert "temp_view" in str(exc_info.value)
    assert isinstance(exc_info.value, AnalysisException)


def test_temporary_view_exists_with_view_name():
    """Test TemporaryViewAlreadyExistsException with view name."""
    exc = TemporaryViewAlreadyExistsException(
        "View already exists",
        view_name="my_temp_view"
    )
    assert "my_temp_view" in str(exc)


def test_exception_inheritance_chain():
    """Test exception inheritance is correct."""
    exc = ColumnNotFoundException("test")
    assert isinstance(exc, AnalysisException)
    assert isinstance(exc, Exception)


def test_exception_str_representation():
    """Test exception string representation."""
    exc = AnalysisException("Error occurred")
    str_repr = str(exc)
    assert "Error occurred" in str_repr


def test_exception_repr():
    """Test exception repr."""
    exc = ColumnNotFoundException("Column missing")
    repr_str = repr(exc)
    assert "ColumnNotFoundException" in repr_str


def test_exception_with_none_message():
    """Test exception handles None message."""
    exc = AnalysisException(None)
    assert exc is not None


def test_exception_equality():
    """Test exception comparison."""
    exc1 = AnalysisException("Same error")
    exc2 = AnalysisException("Same error")
    # Exceptions are not equal by default
    assert exc1 is not exc2


def test_nested_exception_catching():
    """Test catching nested exceptions."""
    with pytest.raises(AnalysisException):
        try:
            raise ColumnNotFoundException("Column not found")
        except ColumnNotFoundException:
            # Re-raise as base exception
            raise AnalysisException("Wrapped error")


def test_exception_with_cause():
    """Test exception with __cause__."""
    try:
        try:
            raise ValueError("Original error")
        except ValueError as e:
            raise AnalysisException("Analysis failed") from e
    except AnalysisException as exc:
        assert exc.__cause__ is not None
        assert isinstance(exc.__cause__, ValueError)

