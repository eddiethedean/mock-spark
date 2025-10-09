"""
DuckDB storage backend with SQLModel integration.

This module provides a type-safe, high-performance storage backend using DuckDB
with SQLModel for enhanced type safety and maintainability.
"""

import duckdb
from sqlmodel import Session, select
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import time
from sqlalchemy import create_engine, MetaData, Table, Column, insert, inspect
from sqlalchemy.engine import Engine

from ..interfaces import IStorageManager, ITable, ISchema
from ..models import (
    MockTableMetadata,
    MockColumnDefinition,
    StorageMode,
    DuckDBTableModel,
    StorageOperationResult,
    QueryResult,
    create_duckdb_engine,
    create_session,
    initialize_metadata_tables,
)
from mock_spark.spark_types import MockStructType, MockStructField
from mock_spark.storage.sqlalchemy_helpers import (
    create_table_from_mock_schema,
    mock_type_to_sqlalchemy,
)


class DuckDBTable(ITable):
    """Type-safe DuckDB table implementation with simplified metadata."""

    def __init__(
        self,
        name: str,
        schema: MockStructType,
        connection: duckdb.DuckDBPyConnection,
        sqlmodel_session: Optional[Session],
        engine: Optional[Engine] = None,
    ):
        """Initialize DuckDB table with type safety."""
        self.name = name
        self.schema = schema
        self.connection = connection
        self.sqlmodel_session = sqlmodel_session
        
        # Create or use provided SQLAlchemy engine for type-safe operations
        if engine is None:
            self.engine = create_engine("duckdb:///:memory:")
        else:
            self.engine = engine
        
        self.table_metadata = MetaData()
        self.sqlalchemy_table: Optional[Table] = None

        # Create simplified metadata (without SQLModel for now)
        self.metadata = {
            "table_name": name,
            "schema_name": "default",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": None,
            "row_count": 0,
            "schema_version": "1.0",
            "storage_format": "columnar",
            "is_temporary": False,
        }

        # Create table using SQLAlchemy
        self._create_table_from_schema()

    def _create_table_from_schema(self) -> None:
        """Create table from MockSpark schema using SQLAlchemy."""
        # Check if table already exists in metadata
        if self.name in self.table_metadata.tables:
            self.sqlalchemy_table = self.table_metadata.tables[self.name]
        else:
            # Create SQLAlchemy table from MockSpark schema
            self.sqlalchemy_table = create_table_from_mock_schema(
                self.name,
                self.schema,
                self.table_metadata
            )
        
        # Create the table in the database
        self.sqlalchemy_table.create(self.engine, checkfirst=True)

    def _get_duckdb_type(self, data_type) -> str:
        """Convert MockSpark data type to DuckDB type."""
        type_name = type(data_type).__name__
        if "String" in type_name:
            return "VARCHAR"
        elif "Integer" in type_name or "Long" in type_name:
            return "INTEGER"
        elif "Double" in type_name or "Float" in type_name:
            return "DOUBLE"
        elif "Boolean" in type_name:
            return "BOOLEAN"
        elif "Date" in type_name:
            return "DATE"
        elif "Timestamp" in type_name:
            return "TIMESTAMP"
        else:
            return "VARCHAR"

    def insert_data(self, data: List[Dict[str, Any]], mode: str = "append") -> None:
        """Type-safe data insertion with validation using SQLAlchemy."""
        if not data:
            return

        start_time = time.time()

        try:
            # Validate data against schema
            validated_data = self._validate_data(data)

            # Handle mode-specific operations
            if mode == StorageMode.OVERWRITE:
                # Drop and recreate table using SQLAlchemy
                if self.sqlalchemy_table is not None:
                    self.sqlalchemy_table.drop(self.engine, checkfirst=True)
                self._create_table_from_schema()
            elif mode == StorageMode.IGNORE:
                # Use INSERT OR IGNORE for DuckDB
                pass

            # Type-safe insertion using SQLAlchemy bulk insert
            if self.sqlalchemy_table is not None:
                # Use SQLAlchemy bulk insert for better performance
                with self.engine.begin() as conn:
                    conn.execute(insert(self.sqlalchemy_table), validated_data)

            # Update metadata with type safety
            self._update_row_count(len(validated_data))

            execution_time = (time.time() - start_time) * 1000

            # Log operation result
            result = StorageOperationResult(
                success=True,
                rows_affected=len(validated_data),
                operation_type=f"insert_{mode}",
                table_name=self.name,
                execution_time_ms=execution_time,
            )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            result = StorageOperationResult(
                success=False,
                rows_affected=0,
                operation_type=f"insert_{mode}",
                table_name=self.name,
                error_message=str(e),
                execution_time_ms=execution_time,
            )
            raise ValueError(f"Failed to insert data: {e}") from e

    def _validate_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate data against schema using type checking."""
        validated = []
        for row in data:
            # Check required fields exist
            for field in self.schema.fields:
                if field.name not in row:
                    raise ValueError(f"Missing required field: {field.name}")

            validated.append(row)
        return validated

    def _update_row_count(self, new_rows: int) -> None:
        """Update row count with type safety."""
        self.metadata["row_count"] += new_rows
        self.metadata["updated_at"] = datetime.utcnow().isoformat()

    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Optimized querying using SQLAlchemy select()."""
        start_time = time.time()

        try:
            if self.sqlalchemy_table is None:
                return []
            
            # Build SQLAlchemy select statement
            from sqlalchemy import select as sa_select
            stmt = sa_select(self.sqlalchemy_table)
            
            # Add filter if provided (Note: filter_expr is a string, will need translation)
            if filter_expr:
                # For now, we'll need to use the SQL translator for complex filters
                # This is a limitation we'll address in Phase 3
                # As a temporary measure, use DuckDB directly for filtered queries
                query = f"SELECT * FROM {self.name} WHERE {filter_expr}"
                result = self.connection.execute(query).fetchall()
                columns = [desc[0] for desc in self.connection.description]
                data = [dict(zip(columns, row)) for row in result]
            else:
                # Use SQLAlchemy for unfiltered queries
                with self.engine.connect() as conn:
                    result = conn.execute(stmt).fetchall()
                    columns = [col.name for col in self.sqlalchemy_table.columns]
                    data = [dict(zip(columns, row)) for row in result]

            execution_time = (time.time() - start_time) * 1000

            # Create query result
            query_result = QueryResult(
                data=data,
                row_count=len(data),
                column_count=len(columns),
                execution_time_ms=execution_time,
                query=str(stmt) if not filter_expr else f"SELECT * FROM {self.name} WHERE {filter_expr}",
            )

            return data

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            raise ValueError(f"Query failed: {e}") from e

    def get_schema(self) -> MockStructType:
        """Get table schema."""
        return self.schema

    def get_metadata(self) -> Dict[str, Any]:
        """Get table metadata with type safety."""
        return self.metadata.copy()


class DuckDBSchema(ISchema):
    """DuckDB schema implementation with type safety."""

    def __init__(
        self, name: str, connection: duckdb.DuckDBPyConnection, sqlmodel_session: Optional[Session], engine: Optional[Engine] = None
    ):
        """Initialize DuckDB schema."""
        self.name = name
        self.connection = connection
        self.sqlmodel_session = sqlmodel_session
        self.tables: Dict[str, DuckDBTable] = {}
        
        # Create or use provided SQLAlchemy engine
        if engine is None:
            self.engine = create_engine("duckdb:///:memory:")
        else:
            self.engine = engine

    def create_table(
        self, table: str, columns: Union[List[MockStructField], MockStructType]
    ) -> Optional[DuckDBTable]:
        """Create a new table with type safety."""
        if isinstance(columns, list):
            schema = MockStructType(columns)
        else:
            schema = columns

        # Create table using SQLAlchemy
        duckdb_table = DuckDBTable(table, schema, self.connection, self.sqlmodel_session, self.engine)
        self.tables[table] = duckdb_table
        return duckdb_table

    def table_exists(self, table: str) -> bool:
        """Check if table exists using SQLAlchemy Inspector."""
        inspector = inspect(self.engine)
        return inspector.has_table(table)

    def drop_table(self, table: str) -> None:
        """Drop a table using SQLAlchemy."""
        # Drop using SQLAlchemy if we have the table object
        if table in self.tables and self.tables[table].sqlalchemy_table is not None:
            self.tables[table].sqlalchemy_table.drop(self.engine, checkfirst=True)
        else:
            # Try to reflect and drop
            try:
                metadata = MetaData()
                table_obj = Table(table, metadata, autoload_with=self.engine)
                table_obj.drop(self.engine, checkfirst=True)
            except:
                pass  # Table doesn't exist

        # Remove from metadata
        if table in self.tables:
            del self.tables[table]

    def list_tables(self) -> List[str]:
        """List all tables in schema using SQLAlchemy Inspector."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()


class DuckDBStorageManager(IStorageManager):
    """Type-safe DuckDB storage manager with in-memory storage by default."""

    def __init__(
        self,
        db_path: Optional[str] = None,
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
    ):
        """Initialize DuckDB storage manager with in-memory storage by default.

        Args:
            db_path: Optional path to database file. If None, uses in-memory storage.
            max_memory: Maximum memory for DuckDB to use (e.g., '1GB', '4GB', '8GB').
                       Default is '1GB' for test isolation.
            allow_disk_spillover: If True, allows DuckDB to spill to disk when memory is full.
                                 If False (default), disables spillover for test isolation.
                                 When enabled, uses a unique temp directory per connection.
        """
        self.db_path = db_path
        self._temp_dir = None

        if db_path is None:
            # Use in-memory storage with temp directory configuration
            self.connection = duckdb.connect(":memory:")
            self.is_in_memory = True
        else:
            # Use persistent storage
            self.connection = duckdb.connect(db_path)
            self.is_in_memory = False

        # Create SQLAlchemy engine for type-safe operations
        db_url = f"duckdb:///{db_path}" if db_path else "duckdb:///:memory:"
        self.engine = create_engine(db_url)
        
        self.schemas: Dict[str, DuckDBSchema] = {}

        # Configure DuckDB memory and spillover settings
        # Note: These SET commands are DuckDB configuration, not data queries
        try:
            # Set memory limit
            self.connection.execute(f"SET max_memory='{max_memory}'")

            if allow_disk_spillover:
                # Create unique temp directory for this connection to prevent test conflicts
                import tempfile
                import uuid

                self._temp_dir = tempfile.mkdtemp(prefix=f"duckdb_test_{uuid.uuid4().hex[:8]}_")
                self.connection.execute(f"SET temp_directory='{self._temp_dir}'")
            else:
                # Disable disk spillover for test isolation
                self.connection.execute("SET temp_directory=''")
        except:
            pass  # Ignore if settings not supported

        # Create default schema with SQLAlchemy engine
        self.schemas["default"] = DuckDBSchema("default", self.connection, None, self.engine)

        # Enable extensions using DuckDB Python API (zero raw SQL)
        try:
            self.connection.install_extension("sqlite")
            self.connection.load_extension("sqlite")
        except:
            pass  # Extensions might not be available

    def create_schema(self, schema: str) -> None:
        """Create a new schema."""
        if schema not in self.schemas:
            self.schemas[schema] = DuckDBSchema(schema, self.connection, None, self.engine)

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists."""
        return schema in self.schemas

    def drop_schema(self, schema: str) -> None:
        """Drop a schema."""
        if schema in self.schemas and schema != "default":
            del self.schemas[schema]

    def list_schemas(self) -> List[str]:
        """List all schemas."""
        return list(self.schemas.keys())

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists."""
        if schema not in self.schemas:
            return False
        return self.schemas[schema].table_exists(table)

    def create_table(
        self,
        schema: str,
        table: str,
        fields: Union[List[MockStructField], MockStructType],
    ) -> Optional[DuckDBTable]:
        """Create a new table with type safety."""
        if schema not in self.schemas:
            self.create_schema(schema)

        return self.schemas[schema].create_table(table, fields)

    def drop_table(self, schema: str, table: str) -> None:
        """Drop a table."""
        if schema in self.schemas:
            self.schemas[schema].drop_table(table)

    def insert_data(
        self, schema: str, table: str, data: List[Dict[str, Any]], mode: str = "append"
    ) -> None:
        """Insert data with type safety."""
        if schema in self.schemas and table in self.schemas[schema].tables:
            self.schemas[schema].tables[table].insert_data(data, mode)

    def query_table(
        self, schema: str, table: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query data with type safety."""
        if schema in self.schemas and table in self.schemas[schema].tables:
            return self.schemas[schema].tables[table].query_data(filter_expr)
        return []

    def get_table_schema(self, schema: str, table: str) -> Optional[MockStructType]:
        """Get table schema."""
        if schema in self.schemas and table in self.schemas[schema].tables:
            return self.schemas[schema].tables[table].get_schema()
        return None

    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all data from table."""
        return self.query_table(schema, table)

    def create_temp_view(self, name: str, dataframe) -> None:
        """Create temporary view with type safety."""
        schema = "default"
        self.create_schema(schema)

        # Convert DataFrame data to table format
        data = dataframe.data
        schema_obj = dataframe.schema

        # Create the table
        self.create_table(schema, name, schema_obj)

        # Insert the data
        self.insert_data(schema, name, data, mode="overwrite")

    def get_table(self, schema: str, table: str) -> Optional[DuckDBTable]:
        """Get an existing table."""
        if schema not in self.schemas:
            return None
        return self.schemas[schema].tables.get(table)

    def list_tables(self, schema: str = "default") -> List[str]:
        """List tables in schema."""
        if schema not in self.schemas:
            return []
        return self.schemas[schema].list_tables()

    def get_database_info(self) -> Dict[str, Any]:
        """Get database information with type safety."""
        tables = {}
        total_tables = 0

        for schema_name, schema in self.schemas.items():
            schema_tables = schema.list_tables()
            tables.update({f"{schema_name}.{table}": table for table in schema_tables})
            total_tables += len(schema_tables)

        return {
            "database_path": self.db_path,
            "tables": tables,
            "total_tables": total_tables,
            "schemas": list(self.schemas.keys()),
            "storage_engine": "DuckDB",
            "type_safety": "SQLModel + Pydantic",
        }

    def execute_analytical_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute complex analytical queries with DuckDB's optimizer."""
        start_time = time.time()

        try:
            result = self.connection.execute(query).fetchall()
            columns = [desc[0] for desc in self.connection.description]
            data = [dict(zip(columns, row)) for row in result]

            execution_time = (time.time() - start_time) * 1000

            return data

        except Exception as e:
            raise ValueError(f"Analytical query failed: {e}") from e

    def close(self):
        """Close connections with proper cleanup."""
        if self.connection:
            try:
                # Clean up all schemas and tables
                for schema_name in list(self.schemas.keys()):
                    if schema_name != "default":
                        try:
                            self.drop_schema(schema_name)
                        except:
                            pass

                # Close the connection
                self.connection.close()
                self.connection = None
            except Exception:
                pass  # Ignore errors during cleanup

        # Clean up unique temp directory if it exists
        if self._temp_dir:
            try:
                import os
                import shutil

                if os.path.exists(self._temp_dir):
                    shutil.rmtree(self._temp_dir, ignore_errors=True)
                self._temp_dir = None
            except:
                pass  # Ignore cleanup errors

    def __del__(self):
        """Cleanup on deletion to prevent resource leaks."""
        try:
            self.close()
        except:
            pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
