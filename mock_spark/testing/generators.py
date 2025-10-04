"""
Data generators for Mock Spark testing.

This module provides comprehensive data generation utilities for creating
test data with various patterns, distributions, and edge cases.
"""

import random
import string
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
    MockDataType,
)


class DataGenerator:
    """Base data generator with common functionality."""
    
    @staticmethod
    def generate_string(length: int = 10, charset: str = None) -> str:
        """Generate a random string."""
        if charset is None:
            charset = string.ascii_letters + string.digits
        return ''.join(random.choices(charset, k=length))
    
    @staticmethod
    def generate_integer(min_val: int = 0, max_val: int = 1000) -> int:
        """Generate a random integer."""
        return random.randint(min_val, max_val)
    
    @staticmethod
    def generate_long(min_val: int = 0, max_val: int = 1000000) -> int:
        """Generate a random long integer."""
        return random.randint(min_val, max_val)
    
    @staticmethod
    def generate_double(min_val: float = 0.0, max_val: float = 1000.0) -> float:
        """Generate a random double."""
        return round(random.uniform(min_val, max_val), 2)
    
    @staticmethod
    def generate_boolean() -> bool:
        """Generate a random boolean."""
        return random.choice([True, False])
    
    @staticmethod
    def generate_date(start_date: datetime = None, end_date: datetime = None) -> str:
        """Generate a random date string."""
        if start_date is None:
            start_date = datetime(2020, 1, 1)
        if end_date is None:
            end_date = datetime(2023, 12, 31)
        
        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        random_date = start_date + timedelta(days=random_days)
        return random_date.strftime('%Y-%m-%d')
    
    @staticmethod
    def generate_timestamp(start_date: datetime = None, end_date: datetime = None) -> str:
        """Generate a random timestamp string."""
        if start_date is None:
            start_date = datetime(2020, 1, 1)
        if end_date is None:
            end_date = datetime(2023, 12, 31)
        
        delta = end_date - start_date
        random_seconds = random.randint(0, int(delta.total_seconds()))
        random_timestamp = start_date + timedelta(seconds=random_seconds)
        return random_timestamp.strftime('%Y-%m-%d %H:%M:%S')


class TestDataBuilder:
    """Builder pattern for creating test data with various configurations."""
    
    def __init__(self):
        self.data = []
        self.schema = None
        self.num_rows = 100
        self.corruption_rate = 0.0
        self.null_rate = 0.1
    
    def with_schema(self, schema: MockStructType) -> 'TestDataBuilder':
        """Set the schema for data generation."""
        self.schema = schema
        return self
    
    def with_rows(self, num_rows: int) -> 'TestDataBuilder':
        """Set the number of rows to generate."""
        self.num_rows = num_rows
        return self
    
    def with_corruption(self, corruption_rate: float) -> 'TestDataBuilder':
        """Set the corruption rate (0.0 to 1.0)."""
        self.corruption_rate = max(0.0, min(1.0, corruption_rate))
        return self
    
    def with_nulls(self, null_rate: float) -> 'TestDataBuilder':
        """Set the null rate (0.0 to 1.0)."""
        self.null_rate = max(0.0, min(1.0, null_rate))
        return self
    
    def build(self) -> List[Dict[str, Any]]:
        """Build the test data."""
        if self.schema is None:
            raise ValueError("Schema must be set before building data")
        
        data = []
        for i in range(self.num_rows):
            row = {}
            for field in self.schema.fields:
                value = self._generate_field_value(field)
                
                # Apply null rate
                if random.random() < self.null_rate:
                    value = None
                
                # Apply corruption
                if random.random() < self.corruption_rate:
                    value = self._corrupt_value(value, field.dataType)
                
                row[field.name] = value
            data.append(row)
        
        return data
    
    def _generate_field_value(self, field: MockStructField) -> Any:
        """Generate a value for a specific field."""
        data_type = field.dataType
        
        if isinstance(data_type, StringType):
            return DataGenerator.generate_string()
        elif isinstance(data_type, IntegerType):
            return DataGenerator.generate_integer()
        elif isinstance(data_type, LongType):
            return DataGenerator.generate_long()
        elif isinstance(data_type, DoubleType):
            return DataGenerator.generate_double()
        elif isinstance(data_type, BooleanType):
            return DataGenerator.generate_boolean()
        elif isinstance(data_type, DateType):
            return DataGenerator.generate_date()
        elif isinstance(data_type, TimestampType):
            return DataGenerator.generate_timestamp()
        else:
            return None
    
    def _corrupt_value(self, value: Any, data_type: MockDataType) -> Any:
        """Corrupt a value for testing error handling."""
        if value is None:
            return value
        
        if isinstance(data_type, StringType):
            return DataGenerator.generate_string(length=100)  # Very long string
        elif isinstance(data_type, (IntegerType, LongType)):
            return "invalid_number"  # String instead of number
        elif isinstance(data_type, DoubleType):
            return "invalid_float"  # String instead of float
        elif isinstance(data_type, BooleanType):
            return "invalid_bool"  # String instead of boolean
        else:
            return value


class RealisticDataGenerator:
    """Generator for realistic test data with common patterns."""
    
    # Common names for realistic data
    FIRST_NAMES = [
        "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry",
        "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul",
        "Quinn", "Ruby", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier",
        "Yara", "Zoe"
    ]
    
    LAST_NAMES = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
        "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
        "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"
    ]
    
    DEPARTMENTS = [
        "Engineering", "Marketing", "Sales", "HR", "Finance", "Operations",
        "Customer Service", "Product", "Design", "Legal"
    ]
    
    @staticmethod
    def generate_person_data(num_rows: int = 100) -> List[Dict[str, Any]]:
        """Generate realistic person data."""
        data = []
        for i in range(num_rows):
            data.append({
                "id": i + 1,
                "first_name": random.choice(RealisticDataGenerator.FIRST_NAMES),
                "last_name": random.choice(RealisticDataGenerator.LAST_NAMES),
                "age": random.randint(22, 65),
                "department": random.choice(RealisticDataGenerator.DEPARTMENTS),
                "salary": round(random.uniform(30000, 150000), 2),
                "active": random.choice([True, False]),
                "hire_date": DataGenerator.generate_date(
                    datetime(2015, 1, 1), 
                    datetime(2023, 12, 31)
                ),
            })
        return data
    
    @staticmethod
    def generate_ecommerce_data(num_rows: int = 100) -> List[Dict[str, Any]]:
        """Generate realistic e-commerce data."""
        products = [
            "Laptop", "Smartphone", "Tablet", "Headphones", "Camera",
            "Watch", "Speaker", "Monitor", "Keyboard", "Mouse"
        ]
        
        data = []
        for i in range(num_rows):
            data.append({
                "order_id": f"ORD-{i+1:06d}",
                "product": random.choice(products),
                "quantity": random.randint(1, 5),
                "price": round(random.uniform(10.0, 2000.0), 2),
                "customer_id": random.randint(1, 1000),
                "order_date": DataGenerator.generate_timestamp(),
                "shipped": random.choice([True, False]),
            })
        return data


class EdgeCaseDataGenerator:
    """Generator for edge case data to test boundary conditions."""
    
    @staticmethod
    def generate_boundary_values() -> List[Dict[str, Any]]:
        """Generate data with boundary values."""
        return [
            {"id": 0, "value": 0.0, "name": ""},  # Zero values
            {"id": 1, "value": 0.001, "name": "a"},  # Very small values
            {"id": 2, "value": 999999.999, "name": "x" * 1000},  # Large values
            {"id": -1, "value": -999999.999, "name": "negative"},  # Negative values
        ]
    
    @staticmethod
    def generate_unicode_data() -> List[Dict[str, Any]]:
        """Generate data with unicode characters."""
        return [
            {"id": 1, "name": "测试", "description": "Unicode test"},
            {"id": 2, "name": "café", "description": "Accented characters"},
            {"id": 3, "name": "🚀", "description": "Emoji characters"},
            {"id": 4, "name": "αβγ", "description": "Greek letters"},
        ]
    
    @staticmethod
    def generate_special_characters() -> List[Dict[str, Any]]:
        """Generate data with special characters."""
        return [
            {"id": 1, "name": "test@email.com", "description": "Email format"},
            {"id": 2, "name": "file/path/with/slashes", "description": "Path separators"},
            {"id": 3, "name": "quotes\"and'apostrophes", "description": "Quote characters"},
            {"id": 4, "name": "tabs\tand\nnewlines", "description": "Whitespace characters"},
        ]


# Convenience functions for easy use
def create_test_data(schema: MockStructType, num_rows: int = 100) -> List[Dict[str, Any]]:
    """Create test data for a given schema."""
    return TestDataBuilder().with_schema(schema).with_rows(num_rows).build()


def create_realistic_data(data_type: str = "person", num_rows: int = 100) -> List[Dict[str, Any]]:
    """Create realistic test data."""
    if data_type == "person":
        return RealisticDataGenerator.generate_person_data(num_rows)
    elif data_type == "ecommerce":
        return RealisticDataGenerator.generate_ecommerce_data(num_rows)
    else:
        raise ValueError(f"Unknown data type: {data_type}")


def create_edge_case_data() -> List[Dict[str, Any]]:
    """Create edge case test data."""
    return EdgeCaseDataGenerator.generate_boundary_values()
