"""
In-memory storage backend for Mock Spark.

This module provides an in-memory storage system using SQLite for fast,
SQL-compatible operations that can handle all DataFrame operations.
"""

import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from contextlib import contextmanager
from dataclasses import dataclass

from .spark_types import (
    MockStructType,
    MockStructField,
    IntegerType,
    StringType,
    MockDataType,
)


@dataclass
class TableMetadata:
    """Metadata for a table."""

    schema: str
    table: str
    columns: List[MockStructField]
    created_at: float
    row_count: int = 0

    @property
    def fqn(self) -> str:
        """Fully qualified name."""
        return f"{self.schema}.{self.table}"


class MockTable:
    """Mock table for storing data."""

    def __init__(self, schema_name: str, table_name: str, schema: MockStructType):
        """Initialize MockTable."""
        self.schema_name = schema_name
        self.table_name = table_name
        self.schema = schema
        self.data: List[Dict[str, Any]] = []
        self.fqn = f"{schema_name}.{table_name}"

    def insert_data(self, data: List[Dict[str, Any]], mode: str = "append") -> None:
        """Insert data into table."""
        if mode == "append":
            self.data.extend(data)
        elif mode == "overwrite":
            self.data = data
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query data from table."""
        if filter_expr is None:
            return self.data.copy()

        # Simple filter implementation for basic comparisons
        filtered_data = []
        for row in self.data:
            if self._evaluate_filter(row, filter_expr):
                filtered_data.append(row)
        return filtered_data

    def _evaluate_filter(self, row: Dict[str, Any], filter_expr: str) -> bool:
        """Evaluate a simple filter expression on a row."""
        # Simple implementation for basic comparisons like "age > 30"
        try:
            # Extract column name, operator, and value
            if " > " in filter_expr:
                col_name, value_str = filter_expr.split(" > ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_value: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_value > value
            elif " < " in filter_expr:
                col_name, value_str = filter_expr.split(" < ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_lt: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_lt < value
            elif " >= " in filter_expr:
                col_name, value_str = filter_expr.split(" >= ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_gte: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_gte >= value
            elif " <= " in filter_expr:
                col_name, value_str = filter_expr.split(" <= ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_lte: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_lte <= value
            elif " == " in filter_expr:
                col_name, value_str = filter_expr.split(" == ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_eq: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_eq == value
            elif " != " in filter_expr:
                col_name, value_str = filter_expr.split(" != ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_ne: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_ne != value
            else:
                # Default to True if we can't parse the filter
                return True
        except Exception:
            # Default to True if evaluation fails
            return True

    def get_schema(self) -> MockStructType:
        """Get table schema."""
        return self.schema

    def get_metadata(self) -> Dict[str, Any]:
        """Get table metadata."""
        return {
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "fqn": self.fqn,
            "row_count": len(self.data),
            "column_count": len(self.schema),
        }


class MockStorageManager:
    """In-memory storage manager using SQLite."""

    def __init__(self) -> None:
        # Each schema gets its own in-memory SQLite database
        self._databases: Dict[str, sqlite3.Connection] = {}
        self._table_metadata: Dict[str, TableMetadata] = {}
        self._temp_tables: Set[str] = set()

        # Compatibility attributes for tests
        self.schemas: Dict[str, List[str]] = {}
        self.tables: Dict[str, Dict[str, MockTable]] = {}

    def get_database(self, schema: str) -> sqlite3.Connection:
        """Get or create database for schema."""
        if schema not in self._databases:
            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            self._databases[schema] = conn
        return self._databases[schema]

    def create_schema(self, schema: str) -> None:
        """Create a schema (database)."""
        if not schema:
            from .errors import IllegalArgumentException

            raise IllegalArgumentException("Schema name cannot be empty")

        if schema not in self._databases:
            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            self._databases[schema] = conn

        # Update compatibility attributes
        if schema not in self.schemas:
            self.schemas[schema] = []
        if schema not in self.tables:
            self.tables[schema] = {}

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists."""
        return schema in self._databases

    def drop_schema(self, schema: str) -> None:
        """Drop a schema."""
        if schema in self._databases:
            # Close the connection
            self._databases[schema].close()
            del self._databases[schema]

    def list_schemas(self) -> List[str]:
        """List all schemas."""
        return list(self._databases.keys())

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists."""
        try:
            conn = self.get_database(schema)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            )
            return cursor.fetchone() is not None
        except Exception:
            return False

    def get_table(self, schema: str, table: str) -> Optional[MockTable]:
        """Get table object."""
        if not self.table_exists(schema, table):
            return None

        # Create a MockTable from the SQLite data
        columns = self.get_table_schema(schema, table)
        if not columns:
            return None

        # columns is already a MockStructType, use it directly
        struct_type = columns
        mock_table = MockTable(schema, table, struct_type)

        # Load data from SQLite
        data = self.query_table(schema, table)
        mock_table.data = data

        return mock_table

    def insert_data(
        self, schema: str, table: str, data: List[Dict[str, Any]], mode: str = "append"
    ) -> int:
        """Insert data into table."""
        if not self.table_exists(schema, table):
            raise ValueError(f"Table {schema}.{table} does not exist")

        conn = self.get_database(schema)

        if mode == "overwrite":
            # Clear existing data
            conn.execute(f"DELETE FROM {table}")

        # Insert new data
        if data:
            columns = list(data[0].keys())
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = (
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
            )

            for row in data:
                values = []
                for col in columns:
                    value = row.get(col)
                    # Convert complex types to strings for SQLite storage
                    if isinstance(value, (list, dict)):
                        import json

                        value = json.dumps(value)
                    elif isinstance(value, datetime):
                        value = value.isoformat()
                    values.append(value)
                conn.execute(insert_sql, values)

            conn.commit()

        return len(data)

    def query_table(
        self, schema: str, table: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query table data."""
        if not self.table_exists(schema, table):
            return []

        conn = self.get_database(schema)

        if filter_expr:
            # Simple filter implementation - in real implementation would parse SQL
            # For now, just return all data and let MockTable handle filtering
            sql = f"SELECT * FROM {table}"
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            all_data = [dict(row) for row in rows]

            # Apply filter using MockTable logic
            filtered_data = []
            for row in all_data:
                if self._evaluate_filter(row, filter_expr):
                    filtered_data.append(row)
            return filtered_data
        else:
            sql = f"SELECT * FROM {table}"
            cursor = conn.execute(sql)
            rows = cursor.fetchall()

            # Convert to list of dicts
            return [dict(row) for row in rows]

    def _evaluate_filter(self, row: Dict[str, Any], filter_expr: str) -> bool:
        """Evaluate a simple filter expression on a row."""
        # Simple implementation for basic comparisons like "age > 30"
        try:
            # Extract column name, operator, and value
            if " > " in filter_expr:
                col_name, value_str = filter_expr.split(" > ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_gt: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_gt > value
            elif " < " in filter_expr:
                col_name, value_str = filter_expr.split(" < ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_lt: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_lt < value
            elif " >= " in filter_expr:
                col_name, value_str = filter_expr.split(" >= ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_gte: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_gte >= value
            elif " <= " in filter_expr:
                col_name, value_str = filter_expr.split(" <= ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_lte: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_lte <= value
            elif " == " in filter_expr:
                col_name, value_str = filter_expr.split(" == ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_eq: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_eq == value
            elif " != " in filter_expr:
                col_name, value_str = filter_expr.split(" != ")
                col_name = col_name.strip()
                value = int(value_str.strip())
                row_val_ne: int = (
                    row.get(col_name, 0) if isinstance(row.get(col_name, 0), int) else 0
                )
                return row_val_ne != value
            else:
                # Default to True if we can't parse the filter
                return True
        except Exception:
            # Default to True if evaluation fails
            return True

    def get_table_schema(self, schema: str, table: str) -> Optional[MockStructType]:
        """Get table schema."""
        if not self.table_exists(schema, table):
            return None

        # Try to get from metadata first - this preserves the original schema
        fqn = f"{schema}.{table}"
        if fqn in self._table_metadata:
            metadata = self._table_metadata[fqn]
            # Return the original MockStructType to preserve equality
            from .spark_types import MockStructType

            return MockStructType(metadata.columns)

        # Fallback to SQLite introspection
        conn = self.get_database(schema)
        cursor = conn.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()

        fields = []
        for col in columns:
            col_name = col[1]  # Column name
            col_type = col[2]  # Column type

            # Convert SQLite type to MockStructField
            if "INT" in col_type.upper():
                field_type: MockDataType = IntegerType()
            elif "TEXT" in col_type.upper() or "VARCHAR" in col_type.upper():
                field_type = StringType()
            else:
                field_type = StringType()  # Default

            fields.append(MockStructField(col_name, field_type))

        from .spark_types import MockStructType

        return MockStructType(fields)

    def get_table_metadata(self, schema: str, table: str) -> Dict[str, Any]:
        """Get table metadata."""
        if not self.table_exists(schema, table):
            return {}

        conn = self.get_database(schema)
        cursor = conn.execute(f"SELECT COUNT(*) as count FROM {table}")
        row_count = cursor.fetchone()[0]

        columns = self.get_table_schema(schema, table)
        column_count = len(columns) if columns else 0

        return {
            "schema_name": schema,
            "table_name": table,
            "fqn": f"{schema}.{table}",
            "row_count": row_count,
            "column_count": column_count,
        }

    def list_tables(self, schema: str) -> List[str]:
        """List tables in schema."""
        if schema not in self._databases:
            return []

        conn = self.get_database(schema)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        return tables

    def clear_all(self) -> None:
        """Clear all data."""
        self._databases.clear()
        self._table_metadata.clear()
        self._temp_tables.clear()
        self.schemas.clear()
        self.tables.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        total_schemas = len(self._databases)
        total_tables = sum(len(self.list_tables(schema)) for schema in self._databases)
        total_rows = 0

        for schema in self._databases:
            for table in self.list_tables(schema):
                metadata = self.get_table_metadata(schema, table)
                total_rows += metadata.get("row_count", 0)

        return {
            "total_schemas": total_schemas,
            "total_tables": total_tables,
            "total_rows": total_rows,
        }

    def create_table(
        self,
        schema: str,
        table: str,
        columns: Union[List[MockStructField], MockStructType],
    ) -> None:
        """Create table with schema."""
        if not schema:
            from .errors import IllegalArgumentException

            raise IllegalArgumentException("Schema name cannot be empty")

        if not table:
            from .errors import IllegalArgumentException

            raise IllegalArgumentException("Table name cannot be empty")

        if not self.schema_exists(schema):
            from .errors import AnalysisException

            raise AnalysisException(f"Schema '{schema}' does not exist")

        conn = self.get_database(schema)

        # Handle both MockStructType and List[MockStructField]
        if isinstance(columns, MockStructType):
            struct_type = columns
            fields = columns.fields
        else:
            fields = columns
            struct_type = MockStructType(fields)

        # Validate fields
        if not fields:
            from .errors import IllegalArgumentException

            raise IllegalArgumentException("Table must have at least one column")

        # Convert MockStructField to SQLite schema
        sqlite_columns = []
        for field in fields:
            if not field.name:
                from .errors import IllegalArgumentException

                raise IllegalArgumentException("Column name cannot be empty")
            sqlite_type = self._convert_mock_type_to_sqlite(field.dataType)
            nullable = "NULL" if field.nullable else "NOT NULL"
            sqlite_columns.append(f"{field.name} {sqlite_type} {nullable}")

        sqlite_schema = ", ".join(sqlite_columns)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({sqlite_schema})")

        # Store metadata
        fqn = f"{schema}.{table}"
        self._table_metadata[fqn] = TableMetadata(
            schema=schema, table=table, columns=fields, created_at=time.time()
        )

        # Update compatibility attributes - use the original struct_type to preserve equality
        mock_table = MockTable(schema, table, struct_type)
        if schema not in self.tables:
            self.tables[schema] = {}
        self.tables[schema][table] = mock_table

    def drop_table(self, schema: str, table: str) -> None:
        """Drop table."""
        if not schema:
            from .errors import IllegalArgumentException

            raise IllegalArgumentException("Schema name cannot be empty")

        if not table:
            from .errors import IllegalArgumentException

            raise IllegalArgumentException("Table name cannot be empty")

        if not self.schema_exists(schema):
            from .errors import AnalysisException

            raise AnalysisException(f"Schema '{schema}' does not exist")

        if not self.table_exists(schema, table):
            from .errors import AnalysisException

            raise AnalysisException(f"Table '{schema}.{table}' does not exist")

        conn = self.get_database(schema)
        conn.execute(
            f"DROP TABLE IF EXISTS `{table}`"
        )  # Use backticks to avoid SQL keyword conflicts

        # Remove metadata
        fqn = f"{schema}.{table}"
        if fqn in self._table_metadata:
            del self._table_metadata[fqn]

        # Update compatibility attributes
        if schema in self.tables and table in self.tables[schema]:
            del self.tables[schema][table]

    @contextmanager
    def transaction(self, schema: str) -> Any:
        """Provide transaction support for ACID properties."""
        conn = self.get_database(schema)
        try:
            conn.execute("BEGIN TRANSACTION")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def _convert_mock_type_to_sqlite(self, mock_type: Any) -> str:
        """Convert MockDataType to SQLite type."""
        type_mapping = {
            "StringType": "TEXT",
            "IntegerType": "INTEGER",
            "LongType": "INTEGER",
            "DoubleType": "REAL",
            "BooleanType": "INTEGER",  # SQLite uses INTEGER for boolean
            "DateType": "TEXT",
            "TimestampType": "TEXT",
        }

        type_name = mock_type.__class__.__name__
        return type_mapping.get(type_name, "TEXT")

    def create_index(self, schema: str, table: str, column: str) -> None:
        """Create index on column for performance."""
        conn = self.get_database(schema)
        index_name = f"idx_{table}_{column}"
        conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column})")
        conn.commit()

    def get_table_stats(self, schema: str, table: str) -> Dict[str, Any]:
        """Get table statistics."""
        fqn = f"{schema}.{table}"
        metadata = self._table_metadata.get(fqn)

        if not metadata:
            return {"row_count": 0, "column_count": 0}

        return {
            "row_count": metadata.row_count,
            "column_count": len(metadata.columns),
            "created_at": metadata.created_at,
            "schema": metadata.schema,
            "table": metadata.table,
        }

    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get data from table."""
        return self.query_table(schema, table)

    def create_temp_view(self, name: str, dataframe) -> None:
        """Create a temporary view from a DataFrame."""
        # Store the DataFrame as a temporary view
        self._temp_tables.add(name)

        # Create a schema and table for the temporary view
        schema = "default"
        self.create_schema(schema)

        # Convert DataFrame data to table format
        if hasattr(dataframe, "collect"):
            data = dataframe.collect()
        else:
            data = dataframe.df.data if hasattr(dataframe, "df") else []

        # Create table metadata
        fqn = f"{schema}.{name}"
        columns = []
        if hasattr(dataframe, "schema") and dataframe.schema:
            columns = dataframe.schema.fields
        elif data:
            # Infer columns from first row
            first_row = data[0] if data else {}
            for col_name, value in first_row.items():
                from .spark_types import MockStructField, StringType

                columns.append(MockStructField(col_name, StringType()))

        metadata = TableMetadata(
            schema=schema,
            table=name,
            columns=columns,
            created_at=time.time(),
            row_count=len(data),
        )

        self._table_metadata[fqn] = metadata

        # Store in compatibility tables dict
        if schema not in self.tables:
            self.tables[schema] = {}

        from .spark_types import MockStructType

        struct_type = MockStructType(columns)
        mock_table = MockTable(schema, name, struct_type)
        mock_table.data = data
        self.tables[schema][name] = mock_table

        # Also create the table in SQLite database for list_tables compatibility
        self._create_table_in_sqlite(schema, name, columns, data)

    def _create_table_in_sqlite(
        self,
        schema: str,
        table: str,
        columns: List[MockStructField],
        data: List[Dict[str, Any]],
    ) -> None:
        """Create a table in SQLite database."""
        conn = self.get_database(schema)

        # Create table schema
        column_defs = []
        for field in columns:
            sql_type = self._convert_mock_type_to_sqlite(field.dataType)
            column_defs.append(f"{field.name} {sql_type}")

        create_sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(column_defs)})"
        conn.execute(create_sql)

        # Insert data
        if data:
            column_names = [field.name for field in columns]
            placeholders = ", ".join(["?" for _ in column_names])
            insert_sql = f"INSERT INTO {table} ({', '.join(column_names)}) VALUES ({placeholders})"

            for row in data:
                values = [row.get(col_name) for col_name in column_names]
                conn.execute(insert_sql, values)

        conn.commit()
