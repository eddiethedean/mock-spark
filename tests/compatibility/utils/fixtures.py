"""
Test data fixtures for compatibility testing.
"""

from typing import List, Dict, Any
import pandas as pd


def get_simple_test_data() -> List[Dict[str, Any]]:
    """Get simple test data for basic operations."""
    return [
        {
            "id": 1,
            "name": "Alice",
            "age": 25,
            "department": "Engineering",
            "salary": 75000.0,
        },
        {
            "id": 2,
            "name": "Bob",
            "age": 30,
            "department": "Marketing",
            "salary": 65000.0,
        },
        {
            "id": 3,
            "name": "Charlie",
            "age": 35,
            "department": "Engineering",
            "salary": 85000.0,
        },
        {
            "id": 4,
            "name": "Diana",
            "age": 28,
            "department": "Marketing",
            "salary": 70000.0,
        },
        {"id": 5, "name": "Eve", "age": 32, "department": "Sales", "salary": 60000.0},
        {
            "id": 6,
            "name": None,
            "age": 40,
            "department": "HR",
            "salary": 80000.0,
        },  # Null name for testing
    ]


def get_complex_test_data() -> List[Dict[str, Any]]:
    """Get complex test data with nested structures and edge cases."""
    return [
        {
            "id": 1,
            "name": "Alice",
            "age": 25,
            "salary": 75000.0,
            "is_active": True,
            "hobbies": ["reading", "swimming"],
            "metadata": {"department": "Engineering", "level": "Senior"},
            "last_login": "2023-01-15T10:30:00Z",
        },
        {
            "id": 2,
            "name": "Bob",
            "age": 30,
            "salary": 65000.0,
            "is_active": False,
            "hobbies": ["gaming", "coding"],
            "metadata": {"department": "Marketing", "level": "Junior"},
            "last_login": "2023-01-10T15:45:00Z",
        },
        {
            "id": 3,
            "name": "Charlie",
            "age": None,  # Null value
            "salary": 85000.0,
            "is_active": True,
            "hobbies": [],
            "metadata": None,  # Null value
            "last_login": "2023-01-20T09:15:00Z",
        },
        {
            "id": 4,
            "name": "Diana",
            "age": 28,
            "salary": None,  # Null value
            "is_active": True,
            "hobbies": ["painting", "music", "travel"],
            "metadata": {"department": "Sales", "level": "Manager"},
            "last_login": None,  # Null value
        },
    ]


def get_edge_case_data() -> List[Dict[str, Any]]:
    """Get edge case test data with boundary values and special cases."""
    return [
        {"id": 0, "name": "", "age": 0, "value": 0.0},  # Zero values
        {"id": -1, "name": " ", "age": -1, "value": -1.0},  # Negative values
        {"id": None, "name": None, "age": None, "value": None},  # All nulls
        {"id": 999999, "name": "A" * 1000, "age": 150, "value": 1e10},  # Large values
        {
            "id": 1,
            "name": "Special\nChars\tHere",
            "age": 25,
            "value": 3.14159,
        },  # Special characters
        {
            "id": 2,
            "name": "Unicode: 你好世界",
            "age": 30,
            "value": float("inf"),
        },  # Unicode and inf
    ]


def get_numerical_test_data() -> List[Dict[str, Any]]:
    """Get numerical test data for mathematical operations."""
    return [
        {"x": 1.0, "y": 2.0, "z": 3.0},
        {"x": 4.0, "y": 5.0, "z": 6.0},
        {"x": 7.0, "y": 8.0, "z": 9.0},
        {"x": 10.0, "y": 11.0, "z": 12.0},
        {"x": 13.0, "y": 14.0, "z": 15.0},
    ]


def get_string_test_data() -> List[Dict[str, Any]]:
    """Get string test data for string operations."""
    return [
        {"text": "Hello World", "category": "A", "length": 11},
        {"text": "Python Programming", "category": "B", "length": 18},
        {"text": "Data Engineering", "category": "A", "length": 16},
        {"text": "Machine Learning", "category": "C", "length": 16},
        {"text": "Big Data Analytics", "category": "B", "length": 18},
    ]


def get_date_test_data() -> List[Dict[str, Any]]:
    """Get date/time test data for temporal operations."""
    return [
        {"date": "2023-01-01", "timestamp": "2023-01-01T00:00:00Z", "year": 2023},
        {"date": "2023-06-15", "timestamp": "2023-06-15T12:30:00Z", "year": 2023},
        {"date": "2023-12-31", "timestamp": "2023-12-31T23:59:59Z", "year": 2023},
        {
            "date": "2024-02-29",
            "timestamp": "2024-02-29T12:00:00Z",
            "year": 2024,
        },  # Leap year
        {"date": "2022-12-25", "timestamp": "2022-12-25T08:15:30Z", "year": 2022},
    ]


def get_join_test_data_left() -> List[Dict[str, Any]]:
    """Get left table data for join tests."""
    return [
        {"id": 1, "name": "Alice", "dept_id": 10},
        {"id": 2, "name": "Bob", "dept_id": 20},
        {"id": 3, "name": "Charlie", "dept_id": 30},
        {"id": 4, "name": "Diana", "dept_id": 40},
    ]


def get_join_test_data_right() -> List[Dict[str, Any]]:
    """Get right table data for join tests."""
    return [
        {"dept_id": 10, "department": "Engineering", "budget": 100000},
        {"dept_id": 20, "department": "Marketing", "budget": 80000},
        {"dept_id": 30, "department": "Sales", "budget": 120000},
        {"dept_id": 50, "department": "HR", "budget": 60000},  # No matching employee
    ]


def get_empty_data() -> List[Dict[str, Any]]:
    """Get empty dataset for edge case testing."""
    return []


def get_single_row_data() -> List[Dict[str, Any]]:
    """Get single row dataset for edge case testing."""
    return [{"id": 1, "name": "Single", "value": 42.0}]


def get_large_dataset_data() -> List[Dict[str, Any]]:
    """Get larger dataset for performance testing."""
    data = []
    for i in range(1000):
        data.append(
            {
                "id": i,
                "name": f"User_{i}",
                "age": 20 + (i % 50),
                "salary": 50000 + (i * 100),
                "department": ["Engineering", "Marketing", "Sales"][i % 3],
            }
        )
    return data


def get_test_dataframes_dict() -> Dict[str, List[Dict[str, Any]]]:
    """Get a dictionary of all test datasets."""
    return {
        "simple": get_simple_test_data(),
        "complex": get_complex_test_data(),
        "edge_cases": get_edge_case_data(),
        "numerical": get_numerical_test_data(),
        "string": get_string_test_data(),
        "date": get_date_test_data(),
        "join_left": get_join_test_data_left(),
        "join_right": get_join_test_data_right(),
        "empty": get_empty_data(),
        "single_row": get_single_row_data(),
        "large": get_large_dataset_data(),
    }


def get_test_schemas() -> Dict[str, Dict[str, str]]:
    """Get schema definitions for test data."""
    return {
        "simple": {
            "id": "int",
            "name": "string",
            "age": "int",
            "department": "string",
            "salary": "double",
        },
        "complex": {
            "id": "int",
            "name": "string",
            "age": "int",
            "salary": "double",
            "is_active": "boolean",
            "hobbies": "array<string>",
            "metadata": "struct<department:string,level:string>",
            "last_login": "timestamp",
        },
        "numerical": {"x": "double", "y": "double", "z": "double"},
        "string": {"text": "string", "category": "string", "length": "int"},
        "date": {"date": "date", "timestamp": "timestamp", "year": "int"},
    }
