"""
Mock SparkSession implementation.
"""

from typing import Any, Dict, List, Optional, Union
from .storage import MockStorageManager
from .dataframe import MockDataFrame
from .spark_types import MockStructType
from .functions import MockFunctions
from .errors import (
    raise_table_not_found,
    raise_schema_not_found,
    raise_invalid_argument,
    raise_value_error,
    AnalysisException,
    IllegalArgumentException,
)


class MockDatabase:
    """Mock database object for catalog operations."""
    
    def __init__(self, name: str):
        self.name = name


class MockJVMContext:
    """Mock JVM context for testing."""
    
    def __init__(self) -> None:
        self.functions = MockFunctions()
    
    def __getattr__(self, name: str) -> Any:
        """Return mock functions for any attribute access."""
        return getattr(self.functions, name, None)


class MockSparkContext:
    """Mock SparkContext for testing without PySpark."""
    
    def __init__(self, app_name: str = "MockSparkApp"):
        """Initialize MockSparkContext."""
        self.app_name = app_name
        self._jvm = MockJVMContext()
    
    def setLogLevel(self, level: str) -> None:
        """Set log level."""
        pass  # Mock implementation


class MockSparkSession:
    """Mock SparkSession for testing without PySpark."""
    
    def __init__(self, app_name: str = "MockSparkApp"):
        """Initialize MockSparkSession."""
        self.app_name = app_name
        self.storage = MockStorageManager()
        self._catalog = MockCatalog(self.storage)
        self.sparkContext = MockSparkContext(app_name)
    
    @property
    def catalog(self) -> "MockCatalog":
        """Get the catalog."""
        return self._catalog
    
    def createDataFrame(self, data: List[Union[Dict[str, Any], tuple]], schema: Optional[Union[MockStructType, List[str]]] = None) -> MockDataFrame:
        """Create a DataFrame from data."""
        if not isinstance(data, list):
            raise_value_error("Data must be a list of dictionaries or tuples")  # type: ignore[unreachable]
        
        # Handle list of column names as schema
        if isinstance(schema, list):
            from .spark_types import MockStructType, MockStructField, StringType
            fields = [MockStructField(name, StringType()) for name in schema]
            schema = MockStructType(fields)
        
        if schema is None:
            # Infer schema from data
            if not data:
                # Create empty DataFrame with empty schema
                from .spark_types import MockStructType
                schema = MockStructType([])
            else:
                # Simple schema inference
                sample_row = data[0]
                if not isinstance(sample_row, (dict, tuple)):
                    raise_value_error("Data must be a list of dictionaries or tuples")  # type: ignore[unreachable]
                
                fields = []
                if isinstance(sample_row, dict):
                    # Dictionary format - sort keys alphabetically to match PySpark behavior
                    for key in sorted(sample_row.keys()):
                        value = sample_row[key]
                        from .spark_types import StringType, LongType, DoubleType, BooleanType, MockDataType
                        
                        field_type: MockDataType
                        if isinstance(value, int):
                            field_type = LongType()  # Use LongType to match PySpark
                        elif isinstance(value, float):
                            field_type = DoubleType()
                        elif isinstance(value, bool):
                            field_type = BooleanType()
                        else:
                            field_type = StringType()
                        
                        from .spark_types import MockStructField
                        fields.append(MockStructField(key, field_type))
                else:
                    # Tuple format - need schema to convert
                    raise_value_error("Cannot infer schema from tuples without explicit schema")
                
                from .spark_types import MockStructType
                schema = MockStructType(fields)
        
        # Convert tuples to dictionaries if schema is provided
        if data and isinstance(data[0], tuple) and schema:
            converted_data: List[Dict[str, Any]] = []
            field_names = [field.name for field in schema.fields]
            for row in data:
                if len(row) != len(field_names):
                    raise_value_error(f"Row length {len(row)} doesn't match schema field count {len(field_names)}")
                converted_data.append(dict(zip(field_names, row)))
            data = converted_data  # type: ignore[assignment]
        
        # Sort data rows to match schema column order
        if data and isinstance(data[0], dict):
            field_names = [field.name for field in schema.fields]
            sorted_data = []
            for row in data:
                sorted_row = {key: row[key] for key in field_names if key in row}
                sorted_data.append(sorted_row)
            data = sorted_data
        
        return MockDataFrame(data, schema, self.storage)  # type: ignore[arg-type]
    
    def table(self, table_name: str) -> MockDataFrame:
        """Get a table as DataFrame."""
        if not isinstance(table_name, str):
            raise_value_error("Table name must be a string")  # type: ignore[unreachable]  # type: ignore[unreachable]
        
        if "." in table_name:
            schema_name, table_name = table_name.split(".", 1)
        else:
            schema_name = "default"
        
        if not self.storage.table_exists(schema_name, table_name):
            raise_table_not_found(f"{schema_name}.{table_name}")
        
        table = self.storage.get_table(schema_name, table_name)
        if table is None:
            raise_table_not_found(f"{schema_name}.{table_name}")
        
        # At this point, table is guaranteed to be not None
        assert table is not None
        return MockDataFrame(table.data, table.schema, self.storage)
    
    def sql(self, query: str) -> MockDataFrame:  # type: ignore[return]
        """Execute SQL query."""
        if not isinstance(query, str):
            raise_value_error("Query must be a string")  # type: ignore[unreachable]
        
        # Simple SQL parsing for basic operations
        query = query.strip().upper()
        
        if query.startswith("CREATE DATABASE"):
            # Extract database name
            parts = query.split()
            if len(parts) >= 3:
                db_name = parts[2].strip("`\"'")
                self.catalog.createDatabase(db_name)
                return MockDataFrame([], MockStructType([]), self.storage)
        
        elif query.startswith("DROP DATABASE"):
            # Extract database name
            parts = query.split()
            if len(parts) >= 3:
                db_name = parts[2].strip("`\"'")
                if self.storage.schema_exists(db_name):
                    self.storage.drop_schema(db_name)
                return MockDataFrame([], MockStructType([]), self.storage)
        
        elif query.startswith("SHOW DATABASES"):
            databases = self.catalog.listDatabases()
            data = [{"databaseName": db.name} for db in databases]
            from .spark_types import MockStructType, MockStructField, StringType
            schema = MockStructType([MockStructField("databaseName", StringType())])
            return MockDataFrame(data, schema, self.storage)
        
        else:
            # For other queries, return empty DataFrame
            from .spark_types import MockStructType
            return MockDataFrame([], MockStructType([]), self.storage)
    
    def stop(self) -> None:
        """Stop the Spark session."""
        self.storage.clear_all()


class MockCatalog:
    """Mock Catalog for Spark session."""
    
    def __init__(self, storage: MockStorageManager):
        """Initialize MockCatalog."""
        self.storage = storage
    
    def listDatabases(self) -> List[MockDatabase]:
        """List all databases."""
        return [MockDatabase(name) for name in self.storage.list_schemas()]
    
    def createDatabase(self, name: str, ignoreIfExists: bool = True) -> None:
        """Create a database."""
        if not isinstance(name, str):
            raise_value_error("Database name must be a string")  # type: ignore[unreachable]
        
        if not name:
            raise_value_error("Database name cannot be empty")
        
        if not ignoreIfExists and self.storage.schema_exists(name):
            raise AnalysisException(f"Database '{name}' already exists", None)
        
        self.storage.create_schema(name)
    
    def tableExists(self, dbName: str, tableName: str) -> bool:
        """Check if table exists."""
        if not isinstance(dbName, str):
            raise_value_error("Database name must be a string")  # type: ignore[unreachable]
        
        if not isinstance(tableName, str):
            raise_value_error("Table name must be a string")  # type: ignore[unreachable]
        
        return self.storage.table_exists(dbName, tableName)
    
    def listTables(self, dbName: str) -> List[str]:
        """List tables in database."""
        if not isinstance(dbName, str):
            raise_value_error("Database name must be a string")  # type: ignore[unreachable]
        
        if not self.storage.schema_exists(dbName):
            raise_schema_not_found(dbName)
        
        return self.storage.list_tables(dbName)
