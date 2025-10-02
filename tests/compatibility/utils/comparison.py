"""
Comparison utilities for validating mock_spark behavior against PySpark.
"""

import pandas as pd
from typing import Any, Dict, List, Optional, Union, Tuple
import numpy as np


def compare_dataframes(
    mock_df: Any, 
    pyspark_df: Any, 
    tolerance: float = 1e-6,
    check_schema: bool = True,
    check_data: bool = True
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
    result = {
        "equivalent": True,
        "schema_match": True,
        "data_match": True,
        "row_count_match": True,
        "column_count_match": True,
        "errors": [],
        "details": {}
    }
    
    try:
        # Convert to pandas for easier comparison
        mock_pandas = mock_df.toPandas() if hasattr(mock_df, 'toPandas') else pd.DataFrame(mock_df.data)
        pyspark_pandas = pyspark_df.toPandas()
        
        # Compare row counts
        mock_rows = len(mock_pandas)
        pyspark_rows = len(pyspark_pandas)
        result["row_count_match"] = mock_rows == pyspark_rows
        result["details"]["row_counts"] = {"mock": mock_rows, "pyspark": pyspark_rows}
        
        if not result["row_count_match"]:
            result["equivalent"] = False
            result["errors"].append(f"Row count mismatch: mock={mock_rows}, pyspark={pyspark_rows}")
            return result
        
        # Compare column counts
        mock_cols = len(mock_pandas.columns)
        pyspark_cols = len(pyspark_pandas.columns)
        result["column_count_match"] = mock_cols == pyspark_cols
        result["details"]["column_counts"] = {"mock": mock_cols, "pyspark": pyspark_cols}
        
        if not result["column_count_match"]:
            result["equivalent"] = False
            result["errors"].append(f"Column count mismatch: mock={mock_cols}, pyspark={pyspark_cols}")
            return result
        
        # Compare schemas
        if check_schema:
            schema_result = compare_schemas(mock_df, pyspark_df)
            result["schema_match"] = schema_result["equivalent"]
            result["details"]["schema"] = schema_result
            if not result["schema_match"]:
                result["equivalent"] = False
                result["errors"].extend(schema_result["errors"])
        
        # Compare data content
        if check_data and result["row_count_match"] and result["column_count_match"]:
            # Sort dataframes by all columns to ensure consistent ordering
            mock_sorted = mock_pandas.sort_values(list(mock_pandas.columns)).reset_index(drop=True)
            pyspark_sorted = pyspark_pandas.sort_values(list(pyspark_pandas.columns)).reset_index(drop=True)
            
            # Compare column by column
            for col in mock_pandas.columns:
                if col not in pyspark_pandas.columns:
                    result["equivalent"] = False
                    result["errors"].append(f"Column '{col}' missing in PySpark result")
                    continue
                
                mock_series = mock_sorted[col]
                pyspark_series = pyspark_sorted[col]
                
                # Handle NaN values specially
                mock_na = mock_series.isna()
                pyspark_na = pyspark_series.isna()
                
                if not mock_na.equals(pyspark_na):
                    result["equivalent"] = False
                    result["errors"].append(f"Null value pattern mismatch in column '{col}'")
                    continue
                
                # Compare non-null values
                mock_non_null = mock_series[~mock_na]
                pyspark_non_null = pyspark_series[~pyspark_na]
                
                if len(mock_non_null) != len(pyspark_non_null):
                    result["equivalent"] = False
                    result["errors"].append(f"Non-null value count mismatch in column '{col}'")
                    continue
                
                # Try different comparison methods based on data type
                try:
                    if pd.api.types.is_numeric_dtype(mock_non_null):
                        # Numerical comparison with tolerance
                        diff = np.abs(mock_non_null - pyspark_non_null)
                        if not np.allclose(mock_non_null, pyspark_non_null, atol=tolerance, rtol=tolerance):
                            result["equivalent"] = False
                            result["errors"].append(f"Numerical values differ in column '{col}' (max diff: {diff.max()})")
                    else:
                        # String/categorical comparison
                        if not mock_non_null.equals(pyspark_non_null):
                            result["equivalent"] = False
                            result["errors"].append(f"String values differ in column '{col}'")
                except Exception as e:
                    result["equivalent"] = False
                    result["errors"].append(f"Error comparing column '{col}': {str(e)}")
            
            result["data_match"] = result["equivalent"]
    
    except Exception as e:
        result["equivalent"] = False
        result["errors"].append(f"Error during comparison: {str(e)}")
    
    return result


def compare_schemas(mock_df: Any, pyspark_df: Any) -> Dict[str, Any]:
    """
    Compare DataFrame schemas.
    
    Args:
        mock_df: mock_spark DataFrame
        pyspark_df: PySpark DataFrame
        
    Returns:
        Dict with schema comparison results
    """
    result = {
        "equivalent": True,
        "errors": [],
        "details": {}
    }
    
    try:
        # Get schemas
        mock_schema = mock_df.schema if hasattr(mock_df, 'schema') else mock_df._schema
        pyspark_schema = pyspark_df.schema
        
        # Compare field counts
        mock_fields = len(mock_schema.fields) if hasattr(mock_schema, 'fields') else len(mock_schema)
        pyspark_fields = len(pyspark_schema.fields)
        
        result["details"]["field_counts"] = {"mock": mock_fields, "pyspark": pyspark_fields}
        
        if mock_fields != pyspark_fields:
            result["equivalent"] = False
            result["errors"].append(f"Schema field count mismatch: mock={mock_fields}, pyspark={pyspark_fields}")
            return result
        
        # Compare field names
        mock_field_names = [f.name for f in mock_schema.fields] if hasattr(mock_schema, 'fields') else [f.name for f in mock_schema]
        pyspark_field_names = [f.name for f in pyspark_schema.fields]
        
        result["details"]["field_names"] = {"mock": mock_field_names, "pyspark": pyspark_field_names}
        
        if set(mock_field_names) != set(pyspark_field_names):
            result["equivalent"] = False
            result["errors"].append(f"Schema field names mismatch: mock={mock_field_names}, pyspark={pyspark_field_names}")
            return result
        
        # Compare field types
        for i, (mock_field, pyspark_field) in enumerate(zip(mock_schema.fields, pyspark_schema.fields)):
            type_result = compare_data_types(mock_field.dataType, pyspark_field.dataType)
            if not type_result["equivalent"]:
                result["equivalent"] = False
                result["errors"].append(f"Field '{mock_field.name}' type mismatch: {type_result['errors']}")
        
        result["details"]["field_types_match"] = result["equivalent"]
    
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
    result = {
        "equivalent": True,
        "errors": [],
        "details": {}
    }
    
    try:
        # Get type names
        mock_type_name = mock_type.__class__.__name__
        pyspark_type_name = pyspark_type.__class__.__name__
        
        result["details"]["type_names"] = {"mock": mock_type_name, "pyspark": pyspark_type_name}
        
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
            "StructType": "StructType"
        }
        
        expected_pyspark_name = type_mapping.get(mock_type_name)
        
        if expected_pyspark_name != pyspark_type_name:
            result["equivalent"] = False
            result["errors"].append(f"Type mismatch: expected {expected_pyspark_name}, got {pyspark_type_name}")
            return result
        
        # Compare nullable property
        mock_nullable = getattr(mock_type, 'nullable', True)
        pyspark_nullable = getattr(pyspark_type, 'nullable', True)
        
        if mock_nullable != pyspark_nullable:
            result["equivalent"] = False
            result["errors"].append(f"Nullable mismatch: mock={mock_nullable}, pyspark={pyspark_nullable}")
        
        result["details"]["nullable"] = {"mock": mock_nullable, "pyspark": pyspark_nullable}
        
        # Handle complex types
        if mock_type_name in ["ArrayType", "MapType", "StructType", "MockStructType"]:
            if hasattr(mock_type, 'elementType') and hasattr(pyspark_type, 'elementType'):
                element_result = compare_data_types(mock_type.elementType, pyspark_type.elementType)
                if not element_result["equivalent"]:
                    result["equivalent"] = False
                    result["errors"].extend(element_result["errors"])
        
        if mock_type_name in ["MapType"]:
            if hasattr(mock_type, 'keyType') and hasattr(pyspark_type, 'keyType'):
                key_result = compare_data_types(mock_type.keyType, pyspark_type.keyType)
                if not key_result["equivalent"]:
                    result["equivalent"] = False
                    result["errors"].extend(key_result["errors"])
        
        if mock_type_name in ["StructType", "MockStructType"]:
            if hasattr(mock_type, 'fields') and hasattr(pyspark_type, 'fields'):
                if len(mock_type.fields) != len(pyspark_type.fields):
                    result["equivalent"] = False
                    result["errors"].append(f"Struct field count mismatch: mock={len(mock_type.fields)}, pyspark={len(pyspark_type.fields)}")
                else:
                    for mock_field, pyspark_field in zip(mock_type.fields, pyspark_type.fields):
                        field_result = compare_data_types(mock_field.dataType, pyspark_field.dataType)
                        if not field_result["equivalent"]:
                            result["equivalent"] = False
                            result["errors"].extend(field_result["errors"])
    
    except Exception as e:
        result["equivalent"] = False
        result["errors"].append(f"Error comparing data types: {str(e)}")
    
    return result


def compare_error_behavior(
    mock_func: callable, 
    pyspark_func: callable, 
    *args, 
    **kwargs
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
    result = {
        "equivalent": True,
        "mock_error": None,
        "pyspark_error": None,
        "error_types_match": True,
        "error_messages_match": True,
        "errors": []
    }
    
    # Test mock function
    try:
        mock_result = mock_func(*args, **kwargs)
        result["mock_error"] = None
    except Exception as e:
        result["mock_error"] = {
            "type": type(e).__name__,
            "message": str(e),
            "exception": e
        }
    
    # Test PySpark function
    try:
        pyspark_result = pyspark_func(*args, **kwargs)
        result["pyspark_error"] = None
    except Exception as e:
        result["pyspark_error"] = {
            "type": type(e).__name__,
            "message": str(e),
            "exception": e
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
            result["errors"].append(f"Error type mismatch: mock={mock_error_type}, pyspark={pyspark_error_type}")
        
        # Check if error messages are similar (allowing for some differences)
        mock_message = result["mock_error"]["message"]
        pyspark_message = result["pyspark_error"]["message"]
        
        # Simple similarity check - both should mention the same key concepts
        if not _error_messages_similar(mock_message, pyspark_message):
            result["error_messages_match"] = False
            result["equivalent"] = False
            result["errors"].append(f"Error message mismatch: mock='{mock_message}', pyspark='{pyspark_message}'")
    else:
        # One succeeded, one failed
        result["equivalent"] = False
        if result["mock_error"] is None:
            result["errors"].append("Mock function succeeded but PySpark function failed")
        else:
            result["errors"].append("PySpark function succeeded but mock function failed")
    
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
        "not found", "invalid", "error", "exception", "type", "column", 
        "field", "attribute", "method", "function", "argument", "parameter"
    ]
    
    msg1_keywords = [kw for kw in error_keywords if kw in msg1_lower]
    msg2_keywords = [kw for kw in error_keywords if kw in msg2_lower]
    
    # If they share common error keywords, consider them similar
    if msg1_keywords and msg2_keywords:
        return bool(set(msg1_keywords) & set(msg2_keywords))
    
    # Fallback to simple substring matching
    return msg1_lower in msg2_lower or msg2_lower in msg1_lower


def assert_dataframes_equal(
    mock_df: Any, 
    pyspark_df: Any, 
    tolerance: float = 1e-6,
    msg: str = ""
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
    mock_func: callable, 
    pyspark_func: callable, 
    *args, 
    msg: str = "",
    **kwargs
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
