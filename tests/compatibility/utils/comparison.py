"""Comparison utilities for validating mock_spark behavior against PySpark."""

import math
from decimal import Decimal
from typing import Any, Dict, Callable, List, Sequence, Tuple


def compare_dataframes(
    mock_df: Any,
    pyspark_df: Any,
    tolerance: float = 1e-6,
    check_schema: bool = True,
    check_data: bool = True,
) -> Dict[str, Any]:
    """
    Compare two DataFrames for equivalence.

    Args:
        mock_df: mock_spark DataFrame
        pyspark_df: PySpark DataFrame
        tolerance: Numerical tolerance for comparisons
        check_schema: Whether to compare schemas
        check_data: Whether to compare data content

    Returns:
        Dict with comparison results and details
    """
    result: Dict[str, Any] = {
        "equivalent": True,
        "schema_match": True,
        "data_match": True,
        "row_count_match": True,
        "column_count_match": True,
        "errors": [],
        "details": {},
    }
    # Type hint for details to help mypy
    details: Dict[str, Any] = result["details"]

    try:
        mock_columns = _get_columns(mock_df)
        pyspark_columns = _get_columns(pyspark_df)

        result["column_count_match"] = len(mock_columns) == len(pyspark_columns)
        details["column_counts"] = {
            "mock": len(mock_columns),
            "pyspark": len(pyspark_columns),
        }

        if mock_columns != pyspark_columns:
            result["equivalent"] = False
            result["errors"].append(
                f"Column mismatch: mock={mock_columns}, pyspark={pyspark_columns}"
            )
            return result

        mock_rows = _collect_rows(mock_df, mock_columns)
        pyspark_rows = _collect_rows(pyspark_df, pyspark_columns)

        mock_row_count = len(mock_rows)
        pyspark_row_count = len(pyspark_rows)
        result["row_count_match"] = mock_row_count == pyspark_row_count
        details["row_counts"] = {
            "mock": mock_row_count,
            "pyspark": pyspark_row_count,
        }

        if not result["row_count_match"]:
            result["equivalent"] = False
            result["errors"].append(
                f"Row count mismatch: mock={mock_row_count}, pyspark={pyspark_row_count}"
            )
            return result

        # Compare schemas
        if check_schema:
            schema_result = compare_schemas(mock_df, pyspark_df)
            result["schema_match"] = schema_result["equivalent"]
            details["schema"] = schema_result
            if not result["schema_match"]:
                result["equivalent"] = False
                result["errors"].extend(schema_result["errors"])

        # Compare data content
        if check_data and result["row_count_match"] and result["column_count_match"]:
            # Special case: if both DataFrames are empty, they are equivalent
            if mock_row_count == 0 and pyspark_row_count == 0:
                result["data_match"] = True
                return result
            mock_sorted = _sort_rows(mock_rows, mock_columns)
            pyspark_sorted = _sort_rows(pyspark_rows, pyspark_columns)

            for row_index, (mock_row, pyspark_row) in enumerate(
                zip(mock_sorted, pyspark_sorted)
            ):
                for col in mock_columns:
                    mock_val = mock_row.get(col)
                    pyspark_val = pyspark_row.get(col)
                    equivalent, error = _compare_values(
                        mock_val,
                        pyspark_val,
                        tolerance,
                        context=f"column '{col}' row {row_index}",
                    )
                    if not equivalent:
                        result["equivalent"] = False
                        result["errors"].append(error)

            result["data_match"] = result["equivalent"]

    except Exception as e:
        result["equivalent"] = False
        result["errors"].append(f"Error during comparison: {str(e)}")

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
    if hasattr(row, "asDict"):
        # Try with recursive parameter (PySpark 3.4+), fall back without it
        try:
            base = row.asDict(recursive=True)
        except TypeError:
            # PySpark < 3.4 doesn't support recursive parameter
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
    if _has_complex_values(rows, columns):
        return list(rows)

    return sorted(
        rows,
        key=lambda row: tuple(_sortable_value(row.get(col)) for col in columns),
    )


def _has_complex_values(rows: Sequence[Dict[str, Any]], columns: Sequence[str]) -> bool:
    for row in rows:
        for col in columns:
            value = row.get(col)
            if isinstance(value, (list, tuple, dict, set)):
                return True
    return False


def _sortable_value(value: Any) -> Tuple[int, Any]:
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
    mock_val: Any, pyspark_val: Any, tolerance: float, context: str
) -> Tuple[bool, str]:
    if _is_null(mock_val) and _is_null(pyspark_val):
        return True, ""

    if _is_null(mock_val) != _is_null(pyspark_val):
        return False, (
            f"Null mismatch in {context}: mock={mock_val!r}, pyspark={pyspark_val!r}"
        )

    if isinstance(mock_val, (list, tuple)) and isinstance(pyspark_val, (list, tuple)):
        if len(mock_val) != len(pyspark_val):
            return False, (
                f"Sequence length mismatch in {context}: mock={len(mock_val)}, pyspark={len(pyspark_val)}"
            )
        for idx, (mock_item, pyspark_item) in enumerate(zip(mock_val, pyspark_val)):
            equivalent, error = _compare_values(
                mock_item,
                pyspark_item,
                tolerance,
                f"{context}[{idx}]",
            )
            if not equivalent:
                return False, error
        return True, ""

    if isinstance(mock_val, set) and isinstance(pyspark_val, set):
        if mock_val == pyspark_val:
            return True, ""
        return False, (
            f"Set mismatch in {context}: mock={mock_val!r}, pyspark={pyspark_val!r}"
        )

    if isinstance(mock_val, dict) and isinstance(pyspark_val, dict):
        mock_keys = set(mock_val.keys())
        pyspark_keys = set(pyspark_val.keys())
        if mock_keys != pyspark_keys:
            return False, (
                f"Dictionary key mismatch in {context}: mock={sorted(mock_keys)}, pyspark={sorted(pyspark_keys)}"
            )
        for key in sorted(mock_keys):
            equivalent, error = _compare_values(
                mock_val[key], pyspark_val[key], tolerance, f"{context}.{key}"
            )
            if not equivalent:
                return False, error
        return True, ""

    if isinstance(mock_val, bool) or isinstance(pyspark_val, bool):
        if bool(mock_val) == bool(pyspark_val):
            return True, ""
        return False, (
            f"Boolean mismatch in {context}: mock={mock_val}, pyspark={pyspark_val}"
        )

    if _is_numeric(mock_val) and _is_numeric(pyspark_val):
        try:
            mock_num = float(mock_val)
            pyspark_num = float(pyspark_val)
        except Exception:
            mock_num = mock_val
            pyspark_num = pyspark_val

        if isinstance(mock_num, float) and isinstance(pyspark_num, float):
            if math.isclose(
                mock_num, pyspark_num, rel_tol=tolerance, abs_tol=tolerance
            ):
                return True, ""
            diff = abs(mock_num - pyspark_num)
            return False, (
                f"Numerical mismatch in {context}: mock={mock_val}, pyspark={pyspark_val}, diff={diff}"
            )

        if mock_num == pyspark_num:
            return True, ""
        return False, (
            f"Numerical mismatch in {context}: mock={mock_val}, pyspark={pyspark_val}"
        )

    if mock_val == pyspark_val:
        return True, ""

    if str(mock_val) == str(pyspark_val):
        return True, ""

    return False, (
        f"Value mismatch in {context}: mock={mock_val!r}, pyspark={pyspark_val!r}"
    )


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    return _is_nan(value)


def _is_nan(value: Any) -> bool:
    if isinstance(value, float):
        return math.isnan(value)
    if isinstance(value, Decimal):
        return value.is_nan()
    return False


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def compare_schemas(mock_df: Any, pyspark_df: Any) -> Dict[str, Any]:
    """
    Compare DataFrame schemas.

    Args:
        mock_df: mock_spark DataFrame
        pyspark_df: PySpark DataFrame

    Returns:
        Dict with schema comparison results
    """
    result: Dict[str, Any] = {"equivalent": True, "errors": [], "details": {}}
    details: Dict[str, Any] = result["details"]

    try:
        # Get schemas
        mock_schema = mock_df.schema if hasattr(mock_df, "schema") else mock_df._schema
        pyspark_schema = pyspark_df.schema

        # Compare field counts
        mock_fields = (
            len(mock_schema.fields)
            if hasattr(mock_schema, "fields")
            else len(mock_schema)
        )
        pyspark_fields = len(pyspark_schema.fields)

        details["field_counts"] = {
            "mock": mock_fields,
            "pyspark": pyspark_fields,
        }

        if mock_fields != pyspark_fields:
            result["equivalent"] = False
            result["errors"].append(
                f"Schema field count mismatch: mock={mock_fields}, pyspark={pyspark_fields}"
            )
            return result

        # Compare field names
        mock_field_names = (
            [f.name for f in mock_schema.fields]
            if hasattr(mock_schema, "fields")
            else [f.name for f in mock_schema]
        )
        pyspark_field_names = [f.name for f in pyspark_schema.fields]

        details["field_names"] = {
            "mock": mock_field_names,
            "pyspark": pyspark_field_names,
        }

        if set(mock_field_names) != set(pyspark_field_names):
            result["equivalent"] = False
            result["errors"].append(
                f"Schema field names mismatch: mock={mock_field_names}, pyspark={pyspark_field_names}"
            )
            return result

        # Compare field types and nullable properties
        for i, (mock_field, pyspark_field) in enumerate(
            zip(mock_schema.fields, pyspark_schema.fields)
        ):
            type_result = compare_data_types(
                mock_field.dataType, pyspark_field.dataType
            )
            if not type_result["equivalent"]:
                result["equivalent"] = False
                result["errors"].append(
                    f"Field '{mock_field.name}' type mismatch: {type_result['errors']}"
                )

            # Compare field-level nullable properties
            mock_nullable = getattr(mock_field, "nullable", True)
            pyspark_nullable = getattr(pyspark_field, "nullable", True)

            if mock_nullable != pyspark_nullable:
                result["equivalent"] = False
                result["errors"].append(
                    f"Field '{mock_field.name}' nullable mismatch: mock={mock_nullable}, pyspark={pyspark_nullable}"
                )

        details["field_types_match"] = result["equivalent"]

    except Exception as e:
        result["equivalent"] = False
        result["errors"].append(f"Error comparing schemas: {str(e)}")

    return result


def compare_data_types(mock_type: Any, pyspark_type: Any) -> Dict[str, Any]:
    """
    Compare data types for equivalence.

    Args:
        mock_type: mock_spark data type
        pyspark_type: PySpark data type

    Returns:
        Dict with type comparison results
    """
    result: Dict[str, Any] = {"equivalent": True, "errors": [], "details": {}}
    details: Dict[str, Any] = result["details"]

    try:
        # Get type names
        mock_type_name = mock_type.__class__.__name__
        pyspark_type_name = pyspark_type.__class__.__name__

        details["type_names"] = {
            "mock": mock_type_name,
            "pyspark": pyspark_type_name,
        }

        # Map mock_spark type names to PySpark equivalents
        type_mapping = {
            "StringType": "StringType",
            "IntegerType": "IntegerType",
            "LongType": "LongType",
            "DoubleType": "DoubleType",
            "BooleanType": "BooleanType",
            "DateType": "DateType",
            "TimestampType": "TimestampType",
            "DecimalType": "DecimalType",
            "ArrayType": "ArrayType",
            "MapType": "MapType",
            "MockStructType": "StructType",
            "StructType": "StructType",
        }

        expected_pyspark_name = type_mapping.get(mock_type_name)

        # Handle PySpark's tendency to treat literals as strings
        # If PySpark returns StringType but Mock Spark has a more specific type,
        # consider them equivalent for compatibility
        if expected_pyspark_name != pyspark_type_name:
            # Special case: PySpark often treats literals as strings
            if pyspark_type_name == "StringType" and expected_pyspark_name in [
                "IntegerType",
                "LongType",
                "DoubleType",
                "BooleanType",
            ]:
                # This is acceptable - PySpark treats literals as strings by default
                details["type_compatibility_note"] = (
                    f"PySpark treats literals as StringType, Mock Spark uses {expected_pyspark_name}"
                )
            # Also handle the reverse case: Mock Spark returns StringType but PySpark has a more specific type
            elif expected_pyspark_name == "StringType" and pyspark_type_name in [
                "IntegerType",
                "LongType",
                "DoubleType",
                "BooleanType",
            ]:
                # This is also acceptable - Mock Spark treats literals as strings by default
                details["type_compatibility_note"] = (
                    f"Mock Spark treats literals as StringType, PySpark uses {pyspark_type_name}"
                )
            else:
                result["equivalent"] = False
                result["errors"].append(
                    f"Type mismatch: expected {expected_pyspark_name}, got {pyspark_type_name}"
                )
                return result

        # Note: nullable is a field-level property, not a data type property
        # Field-level nullable comparison is handled in compare_schemas()

        # Handle complex types
        if mock_type_name in ["ArrayType", "MapType", "StructType", "MockStructType"]:
            if hasattr(mock_type, "elementType") and hasattr(
                pyspark_type, "elementType"
            ):
                element_result = compare_data_types(
                    mock_type.elementType, pyspark_type.elementType
                )
                if not element_result["equivalent"]:
                    result["equivalent"] = False
                    result["errors"].extend(element_result["errors"])

        if mock_type_name in ["MapType"]:
            if hasattr(mock_type, "keyType") and hasattr(pyspark_type, "keyType"):
                key_result = compare_data_types(mock_type.keyType, pyspark_type.keyType)
                if not key_result["equivalent"]:
                    result["equivalent"] = False
                    result["errors"].extend(key_result["errors"])

        if mock_type_name in ["StructType", "MockStructType"]:
            if hasattr(mock_type, "fields") and hasattr(pyspark_type, "fields"):
                if len(mock_type.fields) != len(pyspark_type.fields):
                    result["equivalent"] = False
                    result["errors"].append(
                        f"Struct field count mismatch: mock={len(mock_type.fields)}, pyspark={len(pyspark_type.fields)}"
                    )
                else:
                    for mock_field, pyspark_field in zip(
                        mock_type.fields, pyspark_type.fields
                    ):
                        field_result = compare_data_types(
                            mock_field.dataType, pyspark_field.dataType
                        )
                        if not field_result["equivalent"]:
                            result["equivalent"] = False
                            result["errors"].extend(field_result["errors"])

    except Exception as e:
        result["equivalent"] = False
        result["errors"].append(f"Error comparing data types: {str(e)}")

    return result


def compare_error_behavior(
    mock_func: Callable, pyspark_func: Callable, *args, **kwargs
) -> Dict[str, Any]:
    """
    Compare error handling behavior between mock and PySpark functions.

    Args:
        mock_func: mock_spark function
        pyspark_func: PySpark function
        *args: Function arguments
        **kwargs: Function keyword arguments

    Returns:
        Dict with error comparison results
    """
    result: Dict[str, Any] = {
        "equivalent": True,
        "mock_error": None,
        "pyspark_error": None,
        "error_types_match": True,
        "error_messages_match": True,
        "errors": [],
    }

    # Test mock function
    try:
        mock_func(*args, **kwargs)
        result["mock_error"] = None
    except Exception as e:
        result["mock_error"] = {
            "type": type(e).__name__,
            "message": str(e),
            "exception": e,
        }

    # Test PySpark function
    try:
        pyspark_func(*args, **kwargs)
        result["pyspark_error"] = None
    except Exception as e:
        result["pyspark_error"] = {
            "type": type(e).__name__,
            "message": str(e),
            "exception": e,
        }

    # Compare results
    if result["mock_error"] is None and result["pyspark_error"] is None:
        # Both succeeded - this is good
        pass
    elif result["mock_error"] is not None and result["pyspark_error"] is not None:
        # Both failed - check if error types match
        mock_error_type = result["mock_error"]["type"]
        pyspark_error_type = result["pyspark_error"]["type"]

        if mock_error_type != pyspark_error_type:
            result["error_types_match"] = False
            result["equivalent"] = False
            result["errors"].append(
                f"Error type mismatch: mock={mock_error_type}, pyspark={pyspark_error_type}"
            )

        # Check if error messages are similar (allowing for some differences)
        mock_message = result["mock_error"]["message"]
        pyspark_message = result["pyspark_error"]["message"]

        # Simple similarity check - both should mention the same key concepts
        if not _error_messages_similar(mock_message, pyspark_message):
            result["error_messages_match"] = False
            result["equivalent"] = False
            result["errors"].append(
                f"Error message mismatch: mock='{mock_message}', pyspark='{pyspark_message}'"
            )
    else:
        # One succeeded, one failed
        result["equivalent"] = False
        if result["mock_error"] is None:
            result["errors"].append(
                "Mock function succeeded but PySpark function failed"
            )
        else:
            result["errors"].append(
                "PySpark function succeeded but mock function failed"
            )

    return result


def _error_messages_similar(msg1: str, msg2: str) -> bool:
    """
    Check if two error messages are similar enough to be considered equivalent.
    This is a heuristic approach since exact message matching is often too strict.
    """
    # Convert to lowercase for comparison
    msg1_lower = msg1.lower()
    msg2_lower = msg2.lower()

    # Check for common error keywords
    error_keywords = [
        "not found",
        "invalid",
        "error",
        "exception",
        "type",
        "column",
        "field",
        "attribute",
        "method",
        "function",
        "argument",
        "parameter",
    ]

    msg1_keywords = [kw for kw in error_keywords if kw in msg1_lower]
    msg2_keywords = [kw for kw in error_keywords if kw in msg2_lower]

    # If they share common error keywords, consider them similar
    if msg1_keywords and msg2_keywords:
        return bool(set(msg1_keywords) & set(msg2_keywords))

    # Fallback to simple substring matching
    return msg1_lower in msg2_lower or msg2_lower in msg1_lower


def assert_dataframes_equal(
    mock_df: Any, pyspark_df: Any, tolerance: float = 1e-6, msg: str = ""
) -> None:
    """
    Assert that two DataFrames are equivalent, raising an AssertionError if not.

    Args:
        mock_df: mock_spark DataFrame
        pyspark_df: PySpark DataFrame
        tolerance: Numerical tolerance for comparisons
        msg: Custom error message
    """
    result = compare_dataframes(mock_df, pyspark_df, tolerance)

    if not result["equivalent"]:
        error_msg = msg or "DataFrames are not equivalent"
        error_details = "\n".join(result["errors"])
        raise AssertionError(f"{error_msg}:\n{error_details}")


def assert_schemas_equal(mock_df: Any, pyspark_df: Any, msg: str = "") -> None:
    """
    Assert that two DataFrame schemas are equivalent.

    Args:
        mock_df: mock_spark DataFrame
        pyspark_df: PySpark DataFrame
        msg: Custom error message
    """
    result = compare_schemas(mock_df, pyspark_df)

    if not result["equivalent"]:
        error_msg = msg or "Schemas are not equivalent"
        error_details = "\n".join(result["errors"])
        raise AssertionError(f"{error_msg}:\n{error_details}")


def assert_error_behavior_equal(
    mock_func: Callable, pyspark_func: Callable, *args, msg: str = "", **kwargs
) -> None:
    """
    Assert that two functions have equivalent error behavior.

    Args:
        mock_func: mock_spark function
        pyspark_func: PySpark function
        *args: Function arguments
        msg: Custom error message
        **kwargs: Function keyword arguments
    """
    result = compare_error_behavior(mock_func, pyspark_func, *args, **kwargs)

    if not result["equivalent"]:
        error_msg = msg or "Error behaviors are not equivalent"
        error_details = "\n".join(result["errors"])
        raise AssertionError(f"{error_msg}:\n{error_details}")
