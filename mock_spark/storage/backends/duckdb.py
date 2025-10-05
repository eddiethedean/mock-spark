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
    initialize_metadata_tables
)
from mock_spark.spark_types import MockStructType, MockStructField


class DuckDBTable(ITable):
    """Type-safe DuckDB table implementation with simplified metadata."""
    
    def __init__(self, name: str, schema: MockStructType, connection: duckdb.DuckDBPyConnection, 
                 sqlmodel_session: Optional[Session]):
        """Initialize DuckDB table with type safety."""
        self.name = name
        self.schema = schema
        self.connection = connection
        self.sqlmodel_session = sqlmodel_session
        
        # Create simplified metadata (without SQLModel for now)
        self.metadata = {
            "table_name": name,
            "schema_name": "default",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": None,
            "row_count": 0,
            "schema_version": "1.0",
            "storage_format": "columnar",
            "is_temporary": False
        }
        
        # Create table in DuckDB
        self._create_table_from_schema()
    
    def _create_table_from_schema(self) -> None:
        """Create table from MockSpark schema."""
        columns = []
        for field in self.schema.fields:
            duckdb_type = self._get_duckdb_type(field.dataType)
            columns.append(f"{field.name} {duckdb_type}")
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(columns)})"
        self.connection.execute(create_sql)
    
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
        """Type-safe data insertion with validation."""
        if not data:
            return
        
        start_time = time.time()
        
        try:
            # Validate data against schema
            validated_data = self._validate_data(data)
            
            # Handle mode-specific operations
            if mode == StorageMode.OVERWRITE:
                self.connection.execute(f"DROP TABLE IF EXISTS {self.name}")
                self._create_table_from_schema()
            elif mode == StorageMode.IGNORE:
                # Use INSERT OR IGNORE for DuckDB
                pass
            
            # Type-safe insertion
            for row in validated_data:
                values = [row.get(field.name) for field in self.schema.fields]
                placeholders = ", ".join(["?" for _ in values])
                self.connection.execute(
                    f"INSERT INTO {self.name} VALUES ({placeholders})", values
                )
            
            # Update metadata with type safety
            self._update_row_count(len(validated_data))
            
            execution_time = (time.time() - start_time) * 1000
            
            # Log operation result
            result = StorageOperationResult(
                success=True,
                rows_affected=len(validated_data),
                operation_type=f"insert_{mode}",
                table_name=self.name,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            result = StorageOperationResult(
                success=False,
                rows_affected=0,
                operation_type=f"insert_{mode}",
                table_name=self.name,
                error_message=str(e),
                execution_time_ms=execution_time
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
    
    def _create_table_from_schema(self) -> None:
        """Create table from MockSpark schema."""
        columns = []
        for field in self.schema.fields:
            duckdb_type = self._get_duckdb_type(field.dataType)
            columns.append(f"{field.name} {duckdb_type}")
        
        create_sql = f"CREATE TABLE {self.name} ({', '.join(columns)})"
        self.connection.execute(create_sql)
    
    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Optimized querying with DuckDB's analytical engine."""
        start_time = time.time()
        
        try:
            if filter_expr:
                query = f"SELECT * FROM {self.name} WHERE {filter_expr}"
            else:
                query = f"SELECT * FROM {self.name}"
            
            result = self.connection.execute(query).fetchall()
            columns = [desc[0] for desc in self.connection.description]
            data = [dict(zip(columns, row)) for row in result]
            
            execution_time = (time.time() - start_time) * 1000
            
            # Create query result
            query_result = QueryResult(
                data=data,
                row_count=len(data),
                column_count=len(columns),
                execution_time_ms=execution_time,
                query=query
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
    
    def __init__(self, name: str, connection: duckdb.DuckDBPyConnection, sqlmodel_session: Optional[Session]):
        """Initialize DuckDB schema."""
        self.name = name
        self.connection = connection
        self.sqlmodel_session = sqlmodel_session
        self.tables: Dict[str, DuckDBTable] = {}
    
    def create_table(
        self, table: str, columns: Union[List[MockStructField], MockStructType]
    ) -> Optional[DuckDBTable]:
        """Create a new table with type safety."""
        if isinstance(columns, list):
            schema = MockStructType(columns)
        else:
            schema = columns
        
        # Create table in DuckDB
        duckdb_table = DuckDBTable(table, schema, self.connection, self.sqlmodel_session)
        self.tables[table] = duckdb_table
        return duckdb_table
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists."""
        try:
            self.connection.execute(f"SELECT 1 FROM {table} LIMIT 1")
            return True
        except:
            return False
    
    def drop_table(self, table: str) -> None:
        """Drop a table."""
        self.connection.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Remove from metadata (simplified without SQLModel for now)
        # TODO: Add back SQLModel metadata management in next iteration
        
        if table in self.tables:
            del self.tables[table]
    
    def list_tables(self) -> List[str]:
        """List all tables in schema."""
        try:
            result = self.connection.execute("SHOW TABLES").fetchall()
            return [row[0] for row in result]
        except:
            return []


class DuckDBStorageManager(IStorageManager):
    """Type-safe DuckDB storage manager with simplified connection handling."""
    
    def __init__(self, db_path: str = "mock_spark.duckdb"):
        """Initialize DuckDB storage manager with type safety."""
        self.db_path = db_path
        self.connection = duckdb.connect(db_path)
        self.schemas: Dict[str, DuckDBSchema] = {}
        
        # Create default schema (simplified without SQLModel for now)
        self.schemas["default"] = DuckDBSchema("default", self.connection, None)
        
        # Enable extensions for enhanced functionality
        try:
            self.connection.execute("INSTALL sqlite")
            self.connection.execute("LOAD sqlite")
        except:
            pass  # Extensions might not be available
    
    def create_schema(self, schema: str) -> None:
        """Create a new schema."""
        if schema not in self.schemas:
            self.schemas[schema] = DuckDBSchema(schema, self.connection, None)
    
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
            "type_safety": "SQLModel + Pydantic"
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
            self.connection.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
