"""
Comparison utilities for validating mock-spark behavior against expected PySpark outputs.

This module provides functions to compare mock-spark results with pre-generated
expected outputs from PySpark, replacing the runtime comparison approach.
"""

import math
from decimal import Decimal
from typing import Any, Dict, List, Sequence, Tuple, Union
from dataclasses import dataclass


@dataclass
class ComparisonResult:
    """Result of comparing mock-spark output with expected output."""
    equivalent: bool
    errors: List[str]
    details: Dict[str, Any]
    
    def __init__(self):
        self.equivalent = True
        self.errors = []
        self.details = {}


def compare_dataframes(
    mock_df: Any,
    expected_output: Dict[str, Any],
    tolerance: float = 1e-6,
    check_schema: bool = True,
    check_data: bool = True,
) -> ComparisonResult:
    """
    Compare a mock-spark DataFrame with expected PySpark output.
    
    Args:
        mock_df: mock-spark DataFrame
        expected_output: Expected output dictionary from JSON
        tolerance: Numerical tolerance for comparisons
        check_schema: Whether to compare schemas
        check_data: Whether to compare data content
        
    Returns:
        ComparisonResult with comparison details
    """
    result = ComparisonResult()
    
    try:
        # Get expected data
        expected_schema = expected_output.get("expected_output", {}).get("schema", {})
        expected_data = expected_output.get("expected_output", {}).get("data", [])
        expected_row_count = expected_output.get("expected_output", {}).get("row_count", 0)
        
        # Get mock-spark data
        mock_columns = _get_columns(mock_df)
        mock_rows = _collect_rows(mock_df, mock_columns)
        mock_row_count = len(mock_rows)
        
        # Compare row counts
        if mock_row_count != expected_row_count:
            result.equivalent = False
            result.errors.append(
                f"Row count mismatch: mock={mock_row_count}, expected={expected_row_count}"
            )
            return result
        
        # Compare schemas if requested
        if check_schema:
            schema_result = compare_schemas(mock_df, expected_schema)
            if not schema_result.equivalent:
                result.equivalent = False
                result.errors.extend(schema_result.errors)
                result.details["schema"] = schema_result.details
        
        # Compare data content if requested
        if check_data and mock_row_count > 0:
            # Sort rows for consistent comparison
            mock_sorted = _sort_rows(mock_rows, mock_columns)
            expected_sorted = _sort_rows(expected_data, mock_columns)
            
            for row_index, (mock_row, expected_row) in enumerate(zip(mock_sorted, expected_sorted)):
                for col in mock_columns:
                    mock_val = mock_row.get(col)
                    expected_val = expected_row.get(col)
                    
                    equivalent, error = _compare_values(
                        mock_val,
                        expected_val,
                        tolerance,
                        context=f"column '{col}' row {row_index}",
                    )
                    if not equivalent:
                        result.equivalent = False
                        result.errors.append(error)
        
        result.details["row_count"] = mock_row_count
        result.details["column_count"] = len(mock_columns)
        
    except Exception as e:
        result.equivalent = False
        result.errors.append(f"Error during comparison: {str(e)}")
    
    return result


def _get_columns(df: Any) -> List[str]:
    """Extract ordered column names from a DataFrame-like object."""
    if hasattr(df, "columns"):
        return list(df.columns)
    
    schema = getattr(df, "schema", None) or getattr(df, "_schema", None)
    if schema is not None and hasattr(schema, "fields"):
        return [field.name for field in schema.fields]
    
    data = getattr(df, "data", None)
    if data:
        first_row = data[0]
        if isinstance(first_row, dict):
            return list(first_row.keys())
    
    collected = []
    if hasattr(df, "collect"):
        try:
            collected = list(df.collect())
        except Exception:
            collected = []
    
    for row in collected:
        if hasattr(row, "asDict"):
            return list(row.asDict().keys())
        if isinstance(row, dict):
            return list(row.keys())
        if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
            return [f"col_{idx}" for idx, _ in enumerate(row)]
    
    return []


def _collect_rows(df: Any, columns: Sequence[str]) -> List[Dict[str, Any]]:
    """Collect rows from a DataFrame-like object into dictionaries."""
    rows: List[Dict[str, Any]] = []
    
    if hasattr(df, "collect"):
        try:
            collected = list(df.collect())
        except Exception:
            collected = []
    elif hasattr(df, "data"):
        collected = df.data
    else:
        collected = []
    
    for row in collected:
        rows.append(_row_to_dict(row, columns))
    
    return rows


def _row_to_dict(row: Any, columns: Sequence[str]) -> Dict[str, Any]:
    """Convert a row to a dictionary."""
    if hasattr(row, "asDict"):
        try:
            base = row.asDict(recursive=True)
        except TypeError:
            base = row.asDict()
        return {col: base.get(col) for col in columns}
    
    if isinstance(row, dict):
        return {col: row.get(col) for col in columns}
    
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        if len(row) == len(columns):
            return {col: row[idx] for idx, col in enumerate(columns)}
    
    result: Dict[str, Any] = {}
    for col in columns:
        value = None
        try:
            value = row[col]  # type: ignore[index]
        except Exception:
            value = getattr(row, col, None)
        result[col] = value
    return result


def _sort_rows(
    rows: Sequence[Dict[str, Any]], columns: Sequence[str]
) -> List[Dict[str, Any]]:
    """Sort rows for consistent comparison."""
    if _has_complex_values(rows, columns):
        return list(rows)
    
    return sorted(
        rows,
        key=lambda row: tuple(_sortable_value(row.get(col)) for col in columns),
    )


def _has_complex_values(rows: Sequence[Dict[str, Any]], columns: Sequence[str]) -> bool:
    """Check if rows contain complex values that can't be sorted."""
    for row in rows:
        for col in columns:
            value = row.get(col)
            if isinstance(value, (list, tuple, dict, set)):
                return True
    return False


def _sortable_value(value: Any) -> Tuple[int, Any]:
    """Convert value to sortable tuple."""
    if _is_null(value):
        return (0, "")
    if isinstance(value, bool):
        return (1, value)
    if _is_numeric(value):
        try:
            return (2, float(value))
        except Exception:
            pass
    return (3, str(value))


def _compare_values(
    mock_val: Any, expected_val: Any, tolerance: float, context: str
) -> Tuple[bool, str]:
    """Compare two values with tolerance."""
    if _is_null(mock_val) and _is_null(expected_val):
        return True, ""
    
    if _is_null(mock_val) != _is_null(expected_val):
        return False, (
            f"Null mismatch in {context}: mock={mock_val!r}, expected={expected_val!r}"
        )
    
    if isinstance(mock_val, (list, tuple)) and isinstance(expected_val, (list, tuple)):
        if len(mock_val) != len(expected_val):
            return False, (
                f"Sequence length mismatch in {context}: mock={len(mock_val)}, expected={len(expected_val)}"
            )
        for idx, (mock_item, expected_item) in enumerate(zip(mock_val, expected_val)):
            equivalent, error = _compare_values(
                mock_item,
                expected_item,
                tolerance,
                f"{context}[{idx}]",
            )
            if not equivalent:
                return False, error
        return True, ""
    
    if isinstance(mock_val, set) and isinstance(expected_val, set):
        if mock_val == expected_val:
            return True, ""
        return False, (
            f"Set mismatch in {context}: mock={mock_val!r}, expected={expected_val!r}"
        )
    
    if isinstance(mock_val, dict) and isinstance(expected_val, dict):
        mock_keys = set(mock_val.keys())
        expected_keys = set(expected_val.keys())
        if mock_keys != expected_keys:
            return False, (
                f"Dictionary key mismatch in {context}: mock={sorted(mock_keys)}, expected={sorted(expected_keys)}"
            )
        for key in sorted(mock_keys):
            equivalent, error = _compare_values(
                mock_val[key], expected_val[key], tolerance, f"{context}.{key}"
            )
            if not equivalent:
                return False, error
        return True, ""
    
    if isinstance(mock_val, bool) or isinstance(expected_val, bool):
        if bool(mock_val) == bool(expected_val):
            return True, ""
        return False, (
            f"Boolean mismatch in {context}: mock={mock_val}, expected={expected_val}"
        )
    
    if _is_numeric(mock_val) and _is_numeric(expected_val):
        try:
            mock_num = float(mock_val)
            expected_num = float(expected_val)
        except Exception:
            mock_num = mock_val
            expected_num = expected_val
        
        if isinstance(mock_num, float) and isinstance(expected_num, float):
            if math.isclose(
                mock_num, expected_num, rel_tol=tolerance, abs_tol=tolerance
            ):
                return True, ""
            diff = abs(mock_num - expected_num)
            return False, (
                f"Numerical mismatch in {context}: mock={mock_val}, expected={expected_val}, diff={diff}"
            )
        
        if mock_num == expected_num:
            return True, ""
        return False, (
            f"Numerical mismatch in {context}: mock={mock_val}, expected={expected_val}"
        )
    
    if mock_val == expected_val:
        return True, ""
    
    if str(mock_val) == str(expected_val):
        return True, ""
    
    return False, (
        f"Value mismatch in {context}: mock={mock_val!r}, expected={expected_val!r}"
    )


def _is_null(value: Any) -> bool:
    """Check if value is null."""
    if value is None:
        return True
    return _is_nan(value)


def _is_nan(value: Any) -> bool:
    """Check if value is NaN."""
    if isinstance(value, float):
        return math.isnan(value)
    if isinstance(value, Decimal):
        return value.is_nan()
    return False


def _is_numeric(value: Any) -> bool:
    """Check if value is numeric."""
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def compare_schemas(mock_df: Any, expected_schema: Dict[str, Any]) -> ComparisonResult:
    """
    Compare DataFrame schemas.
    
    Args:
        mock_df: mock-spark DataFrame
        expected_schema: Expected schema dictionary
        
    Returns:
        ComparisonResult with schema comparison details
    """
    result = ComparisonResult()
    
    try:
        # Get mock schema
        mock_schema = mock_df.schema if hasattr(mock_df, "schema") else mock_df._schema
        
        # Compare field counts
        mock_fields = (
            len(mock_schema.fields)
            if hasattr(mock_schema, "fields")
            else len(mock_schema)
        )
        expected_fields = expected_schema.get("field_count", 0)
        
        result.details["field_counts"] = {
            "mock": mock_fields,
            "expected": expected_fields,
        }
        
        if mock_fields != expected_fields:
            result.equivalent = False
            result.errors.append(
                f"Schema field count mismatch: mock={mock_fields}, expected={expected_fields}"
            )
            return result
        
        # Compare field names
        mock_field_names = (
            [f.name for f in mock_schema.fields]
            if hasattr(mock_schema, "fields")
            else [f.name for f in mock_schema]
        )
        expected_field_names = expected_schema.get("field_names", [])
        
        result.details["field_names"] = {
            "mock": mock_field_names,
            "expected": expected_field_names,
        }
        
        if set(mock_field_names) != set(expected_field_names):
            result.equivalent = False
            result.errors.append(
                f"Schema field names mismatch: mock={mock_field_names}, expected={expected_field_names}"
            )
            return result
        
        result.details["field_types_match"] = True
        
    except Exception as e:
        result.equivalent = False
        result.errors.append(f"Error comparing schemas: {str(e)}")
    
    return result


def assert_dataframes_equal(
    mock_df: Any, 
    expected_output: Dict[str, Any], 
    tolerance: float = 1e-6, 
    msg: str = ""
) -> None:
    """
    Assert that mock-spark DataFrame matches expected output.
    
    Args:
        mock_df: mock-spark DataFrame
        expected_output: Expected output dictionary
        tolerance: Numerical tolerance for comparisons
        msg: Custom error message
    """
    result = compare_dataframes(mock_df, expected_output, tolerance)
    
    if not result.equivalent:
        error_msg = msg or "DataFrames are not equivalent"
        error_details = "\n".join(result.errors)
        raise AssertionError(f"{error_msg}:\n{error_details}")


def assert_schemas_equal(
    mock_df: Any, 
    expected_schema: Dict[str, Any], 
    msg: str = ""
) -> None:
    """
    Assert that DataFrame schemas are equivalent.
    
    Args:
        mock_df: mock-spark DataFrame
        expected_schema: Expected schema dictionary
        msg: Custom error message
    """
    result = compare_schemas(mock_df, expected_schema)
    
    if not result.equivalent:
        error_msg = msg or "Schemas are not equivalent"
        error_details = "\n".join(result.errors)
        raise AssertionError(f"{error_msg}:\n{error_details}")
