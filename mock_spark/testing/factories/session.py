"""
Session test factory for Mock Spark.

This module provides the SessionTestFactory class for creating
comprehensive session test scenarios and configurations.
"""

from typing import Dict, Any, List
from ...session import MockSparkSession
from ...spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)


class SessionTestFactory:
    """Factory for creating comprehensive session test scenarios."""

    @staticmethod
    def create_session_with_tables(
        session: MockSparkSession, table_configs: Dict[str, Dict[str, Any]]
    ) -> MockSparkSession:
        """Create a session with pre-populated tables."""
        for table_name, config in table_configs.items():
            data = config.get("data", [])
            schema = config.get("schema")

            if schema is None and data:
                # Auto-generate schema from data
                first_row = data[0]
                fields = []
                for key, value in first_row.items():
                    field_type: Any  # Can be IntegerType, DoubleType, BooleanType, StringType
                    if isinstance(value, int):
                        field_type = IntegerType()
                    elif isinstance(value, float):
                        field_type = DoubleType()
                    elif isinstance(value, bool):
                        field_type = BooleanType()
                    else:
                        field_type = StringType()
                    fields.append(MockStructField(key, field_type, True))
                schema = MockStructType(fields)

            if schema:
                df = session.createDataFrame(data, schema)
                df.createGlobalTempView(table_name)

        return session

    @staticmethod
    def create_session_with_configs(
        session: MockSparkSession, configs: Dict[str, str]
    ) -> MockSparkSession:
        """Create a session with specific configurations."""
        for key, value in configs.items():
            session.conf.set(key, value)
        return session

    @staticmethod
    def create_session_with_storage_data(
        session: MockSparkSession, storage_data: Dict[str, List[Dict[str, Any]]]
    ) -> MockSparkSession:
        """Create a session with data in storage."""
        for table_name, data in storage_data.items():
            if data:
                # Create schema from first row
                first_row = data[0]
                fields = []
                for key, value in first_row.items():
                    field_type: Any  # Can be IntegerType, DoubleType, BooleanType, StringType
                    if isinstance(value, int):
                        field_type = IntegerType()
                    elif isinstance(value, float):
                        field_type = DoubleType()
                    elif isinstance(value, bool):
                        field_type = BooleanType()
                    else:
                        field_type = StringType()
                    fields.append(MockStructField(key, field_type, True))

                schema = MockStructType(fields)
                session.storage.create_table("default", table_name, schema)
                session.storage.insert_data("default", table_name, data)

        return session
