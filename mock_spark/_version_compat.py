"""
PySpark version compatibility module.

This module controls which functions and methods are available based on
the PySpark version compatibility mode set during installation.

By default, all features are available. When installed with a version specifier
like `pip install mock-spark[pyspark-3-1]`, only APIs available in that
PySpark version will be exposed.
"""

import json
import os
from pathlib import Path
from typing import Dict, Optional


# Global version setting - None means all features available
_PYSPARK_COMPAT_VERSION: Optional[str] = None

# Cache for API matrix
_API_MATRIX: Optional[Dict[str, Dict[str, Dict[str, bool]]]] = None


def set_pyspark_version(version: Optional[str]) -> None:
    """
    Set PySpark compatibility version.
    
    Args:
        version: PySpark version string (e.g., '3.1', '3.2', '3.5') or None for all features
    
    Examples:
        >>> set_pyspark_version('3.1')
        >>> # Only PySpark 3.1-compatible APIs will be available
    """
    global _PYSPARK_COMPAT_VERSION
    
    # Handle None - reset to all features
    if version is None:
        _PYSPARK_COMPAT_VERSION = None
        return
    
    # Normalize version format (3.1 or 3.1.3 -> 3.1.3)
    if version.count('.') == 1:
        # Map major.minor to specific patch version
        version_map = {
            '3.0': '3.0.3',
            '3.1': '3.1.3',
            '3.2': '3.2.4',
            '3.3': '3.3.4',
            '3.4': '3.4.3',
            '3.5': '3.5.2',
        }
        version = version_map.get(version, version)
    
    _PYSPARK_COMPAT_VERSION = version


def get_pyspark_version() -> Optional[str]:
    """
    Get current PySpark compatibility version.
    
    Returns:
        Version string or None if all features available
    """
    return _PYSPARK_COMPAT_VERSION


def _load_api_matrix() -> Dict[str, Dict[str, Dict[str, bool]]]:
    """
    Load the API availability matrix from JSON file.
    
    Returns:
        Dictionary with 'functions' and 'dataframe_methods' matrices
    """
    global _API_MATRIX
    
    if _API_MATRIX is not None:
        return _API_MATRIX
    
    # Load from package data
    matrix_path = Path(__file__).parent / "pyspark_api_matrix.json"
    
    if not matrix_path.exists():
        # Fallback: all items available if matrix file missing
        return {"functions": {}, "dataframe_methods": {}}
    
    with open(matrix_path, 'r') as f:
        _API_MATRIX = json.load(f)
    
    return _API_MATRIX


def is_available(item_name: str, item_type: str = "function") -> bool:
    """
    Check if a function or method is available in current compatibility mode.
    
    Args:
        item_name: Name of the function or method
        item_type: Either 'function' or 'dataframe_method'
    
    Returns:
        True if item is available in current version mode
    
    Examples:
        >>> set_pyspark_version('3.0')
        >>> is_available('make_date', 'function')
        False
        >>> is_available('overlay', 'function')
        True
    """
    # If no version set, all features available
    if _PYSPARK_COMPAT_VERSION is None:
        return True
    
    # Load matrix
    matrix = _load_api_matrix()
    
    # Determine which matrix to check
    if item_type == "function":
        item_matrix = matrix.get("functions", {})
    elif item_type == "dataframe_method":
        item_matrix = matrix.get("dataframe_methods", {})
    else:
        # Unknown type, allow by default
        return True
    
    # Check if item exists in matrix
    if item_name not in item_matrix:
        # Item not in matrix, allow by default (might be mock-spark specific)
        return True
    
    # Check if available in target version
    availability = item_matrix[item_name]
    return availability.get(_PYSPARK_COMPAT_VERSION, False)


def check_version_from_env() -> None:
    """
    Check environment variable for version setting.
    
    Looks for MOCK_SPARK_PYSPARK_VERSION environment variable.
    """
    env_version = os.environ.get("MOCK_SPARK_PYSPARK_VERSION")
    if env_version:
        set_pyspark_version(env_version)


def check_version_from_marker() -> None:
    """
    Check for version marker file in package directory.
    
    Looks for .pyspark_X_Y_compat marker files created during installation.
    """
    marker_dir = Path(__file__).parent
    
    # Check for marker files in order (latest first)
    version_markers = [
        ('.pyspark_3_5_compat', '3.5'),
        ('.pyspark_3_4_compat', '3.4'),
        ('.pyspark_3_3_compat', '3.3'),
        ('.pyspark_3_2_compat', '3.2'),
        ('.pyspark_3_1_compat', '3.1'),
        ('.pyspark_3_0_compat', '3.0'),
    ]
    
    for marker_file, version in version_markers:
        if (marker_dir / marker_file).exists():
            set_pyspark_version(version)
            return


# Auto-check version on module import
check_version_from_env()
check_version_from_marker()

