"""
# mypy: ignore-errors
Mock utilities and factories for Mock Spark testing.

This module provides factory patterns and mock utilities for creating
test objects with consistent configurations and behaviors.
"""

from typing import Dict, List, Any, Optional
from mock_spark import MockSparkSession
from mock_spark.core.interfaces.dataframe import IDataFrame
from mock_spark.functions import MockColumn, MockFunctions
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
)
from mock_spark.storage import MemoryStorageManager


class MockDataFrameFactory:
    """Factory for creating MockDataFrame instances with various configurations."""

    @staticmethod
    def create_simple_dataframe(session: MockSparkSession) -> IDataFrame:
        """Create a simple DataFrame with basic data."""
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        schema = MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("name", StringType(), True),
                MockStructField("age", IntegerType(), True),
            ]
        )
        return session.createDataFrame(data, schema)

    @staticmethod
    def create_empty_dataframe(session: MockSparkSession) -> IDataFrame:
        """Create an empty DataFrame."""
        schema = MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("value", StringType(), True),
            ]
        )
        return session.createDataFrame([], schema)

    @staticmethod
    def create_dataframe_with_nulls(session: MockSparkSession) -> IDataFrame:
        """Create a DataFrame with null values."""
        data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": None, "age": 30},
            {"id": 3, "name": "Charlie", "age": None},
        ]
        schema = MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("name", StringType(), True),
                MockStructField("age", IntegerType(), True),
            ]
        )
        return session.createDataFrame(data, schema)

    @staticmethod
    def create_large_dataframe(session: MockSparkSession, num_rows: int = 1000) -> IDataFrame:
        """Create a large DataFrame for performance testing."""
        data = []
        for i in range(num_rows):
            data.append(
                {
                    "id": i,
                    "name": f"User_{i}",
                    "age": 20 + (i % 50),
                    "salary": 30000.0 + (i * 100),
                }
            )

        schema = MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("name", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("salary", DoubleType(), True),
            ]
        )
        return session.createDataFrame(data, schema)

    @staticmethod
    def create_dataframe_with_schema(
        session: MockSparkSession,
        schema: MockStructType,
        data: Optional[List[Dict[str, Any]]] = None,
    ) -> IDataFrame:
        """Create a DataFrame with a custom schema and data."""
        if data is None:
            data = []
        return session.createDataFrame(data, schema)


class MockSessionFactory:
    """Factory for creating MockSparkSession instances with various configurations."""

    @staticmethod
    def create_default_session() -> MockSparkSession:
        """Create a default MockSparkSession."""
        if MockSparkSession.builder is None:
            return MockSparkSession("test_app")
        return MockSparkSession("test_app")

    @staticmethod
    def create_session_with_config(config: Dict[str, str]) -> MockSparkSession:
        """Create a session with custom configuration."""
        if MockSparkSession.builder is None:
            return MockSparkSession("test_app")
        builder = MockSparkSession.builder.appName("test_app")
        for key, value in config.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    @staticmethod
    def create_session_with_storage(storage_manager: Any = None) -> MockSparkSession:
        """Create a session with a custom storage manager."""
        if MockSparkSession.builder is None:
            session = MockSparkSession("test_app")
        else:
            session = MockSparkSession("test_app")
        if storage_manager:
            session.storage = storage_manager
        return session

    @staticmethod
    def create_session_with_data(dataframes: Dict[str, IDataFrame]) -> MockSparkSession:
        """Create a session with pre-populated DataFrames as tables."""
        session = MockSparkSession("test_app")

        for table_name, df in dataframes.items():
            # IDataFrame interface doesn't have createOrReplaceTempView
            # This is a mock limitation - in real implementation, we'd need to handle this differently
            pass

        return session


class MockFunctionFactory:
    """Factory for creating MockColumn and function instances."""

    @staticmethod
    def create_column(name: str) -> MockColumn:
        """Create a MockColumn with the given name."""
        return MockColumn(name)

    @staticmethod
    def create_literal(value: Any) -> MockColumn:
        """Create a literal MockColumn."""
        from typing import cast

        return cast(MockColumn, MockFunctions.lit(value))

    @staticmethod
    def create_arithmetic_expression(
        left: MockColumn, operator: str, right: MockColumn
    ) -> MockColumn:
        """Create an arithmetic expression."""
        from typing import cast

        if operator == "+":
            return cast(MockColumn, left + right)
        elif operator == "-":
            return cast(MockColumn, left - right)
        elif operator == "*":
            return cast(MockColumn, left * right)
        elif operator == "/":
            return cast(MockColumn, left / right)
        elif operator == "%":
            return cast(MockColumn, left % right)
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    @staticmethod
    def create_comparison_expression(
        left: MockColumn, operator: str, right: MockColumn
    ) -> MockColumn:
        """Create a comparison expression."""
        from typing import cast

        if operator == "==":
            return cast(MockColumn, left == right)
        elif operator == "!=":
            return cast(MockColumn, left != right)
        elif operator == ">":
            return cast(MockColumn, left > right)
        elif operator == ">=":
            return cast(MockColumn, left >= right)
        elif operator == "<":
            return cast(MockColumn, left < right)
        elif operator == "<=":
            return cast(MockColumn, left <= right)
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    @staticmethod
    def create_logical_expression(left: MockColumn, operator: str, right: MockColumn) -> MockColumn:
        """Create a logical expression."""
        from typing import cast

        if operator == "&":
            return cast(MockColumn, left & right)
        elif operator == "|":
            return cast(MockColumn, left | right)
        else:
            raise ValueError(f"Unsupported operator: {operator}")


class MockStorageFactory:
    """Factory for creating storage-related mock objects."""

    @staticmethod
    def create_memory_storage() -> MemoryStorageManager:
        """Create a memory storage manager."""
        return MemoryStorageManager()

    @staticmethod
    def create_storage_with_data(data: Dict[str, List[Dict[str, Any]]]) -> MemoryStorageManager:
        """Create a storage manager with pre-populated data."""
        storage = MemoryStorageManager()

        for table_name, table_data in data.items():
            # Create a simple schema based on the first row
            if table_data:
                first_row = table_data[0]
                fields = []
                for key, value in first_row.items():
                    from ...spark_types import MockDataType

                    if isinstance(value, int):
                        field_type: MockDataType = IntegerType()
                    elif isinstance(value, float):
                        field_type = DoubleType()
                    elif isinstance(value, bool):
                        field_type = BooleanType()
                    else:
                        field_type = StringType()

                    fields.append(MockStructField(key, field_type, True))

                schema = MockStructType(fields)
                storage.create_table("default", table_name, schema)
                storage.insert_data("default", table_name, table_data)

        return storage


class MockSchemaFactory:
    """Factory for creating schema objects."""

    @staticmethod
    def create_simple_schema() -> MockStructType:
        """Create a simple schema with basic types."""
        return MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("name", StringType(), True),
                MockStructField("age", IntegerType(), True),
            ]
        )

    @staticmethod
    def create_complex_schema() -> MockStructType:
        """Create a complex schema with various types."""
        return MockStructType(
            [
                MockStructField("id", LongType(), True),
                MockStructField("name", StringType(), False),
                MockStructField("age", IntegerType(), True),
                MockStructField("salary", DoubleType(), True),
                MockStructField("active", BooleanType(), True),
            ]
        )

    @staticmethod
    def create_schema_from_data(data: List[Dict[str, Any]]) -> MockStructType:
        """Create a schema based on sample data."""
        if not data:
            return MockStructType([])

        first_row = data[0]
        fields = []

        for key, value in first_row.items():
            from ...spark_types import MockDataType

            if isinstance(value, int):
                field_type: MockDataType = IntegerType()
            elif isinstance(value, float):
                field_type = DoubleType()
            elif isinstance(value, bool):
                field_type = BooleanType()
            else:
                field_type = StringType()

            fields.append(MockStructField(key, field_type, True))

        return MockStructType(fields)


# Convenience functions for easy use
def create_test_session() -> MockSparkSession:
    """Create a test session quickly."""
    return MockSessionFactory.create_default_session()


def create_test_dataframe(
    session: MockSparkSession,
    data: Optional[List[Dict[str, Any]]] = None,
    schema: Optional[MockStructType] = None,
) -> IDataFrame:
    """Create a test DataFrame quickly."""
    if data is None:
        data = [{"id": 1, "name": "test", "value": 100}]

    if schema is None:
        schema = MockSchemaFactory.create_simple_schema()

    return MockDataFrameFactory.create_dataframe_with_schema(session, schema, data)


def create_test_storage() -> MemoryStorageManager:
    """Create a test storage manager quickly."""
    return MockStorageFactory.create_memory_storage()
