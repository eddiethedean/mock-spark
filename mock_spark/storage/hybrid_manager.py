"""
Hybrid Storage Manager for Mock Spark 1.0.0

Provides seamless migration between storage backends and hybrid operations.
"""

from typing import List, Dict, Any, Optional, Union
from .interfaces import IStorageManager
from .manager import StorageManagerFactory, UnifiedStorageManager
from .backends.sqlite import SQLiteStorageManager
from .backends.duckdb import DuckDBStorageManager
from mock_spark.spark_types import MockStructType, MockStructField
import logging

logger = logging.getLogger(__name__)


class HybridStorageManager(IStorageManager):
    """Hybrid storage manager that can operate with multiple backends simultaneously."""
    
    def __init__(
        self, 
        primary_backend: str = "duckdb",
        sqlite_path: str = "mock_spark.db",
        duckdb_path: str = "mock_spark.duckdb"
    ):
        """Initialize hybrid storage manager.
        
        Args:
            primary_backend: Primary backend to use ("duckdb" or "sqlite")
            sqlite_path: Path to SQLite database
            duckdb_path: Path to DuckDB database
        """
        self.primary_backend = primary_backend
        self.sqlite_path = sqlite_path
        self.duckdb_path = duckdb_path
        
        # Initialize both backends
        self.sqlite_manager = StorageManagerFactory.create_sqlite_manager(sqlite_path)
        self.duckdb_manager = StorageManagerFactory.create_duckdb_manager(duckdb_path)
        
        # Set primary backend
        if primary_backend == "duckdb":
            self.current_backend = self.duckdb_manager
            self.secondary_backend = self.sqlite_manager
        else:
            self.current_backend = self.sqlite_manager
            self.secondary_backend = self.duckdb_manager
        
        logger.info(f"HybridStorageManager initialized with primary backend: {primary_backend}")
    
    def switch_primary_backend(self, backend: str) -> None:
        """Switch the primary backend.
        
        Args:
            backend: New primary backend ("duckdb" or "sqlite")
        """
        if backend not in ["duckdb", "sqlite"]:
            raise ValueError(f"Unsupported backend: {backend}")
        
        self.primary_backend = backend
        
        if backend == "duckdb":
            self.current_backend = self.duckdb_manager
            self.secondary_backend = self.sqlite_manager
        else:
            self.current_backend = self.sqlite_manager
            self.secondary_backend = self.duckdb_manager
        
        logger.info(f"Switched primary backend to: {backend}")
    
    def migrate_data(
        self, 
        schema: str, 
        table: str,
        source_backend: Optional[str] = None,
        target_backend: Optional[str] = None
    ) -> bool:
        """Migrate data between backends.
        
        Args:
            schema: Schema name
            table: Table name
            source_backend: Source backend ("sqlite" or "duckdb"), None for secondary
            target_backend: Target backend ("sqlite" or "duckdb"), None for primary
            
        Returns:
            True if migration successful, False otherwise
        """
        try:
            # Determine source and target backends
            if source_backend is None:
                source = self.secondary_backend
            elif source_backend == "sqlite":
                source = self.sqlite_manager
            elif source_backend == "duckdb":
                source = self.duckdb_manager
            else:
                raise ValueError(f"Unsupported source backend: {source_backend}")
            
            if target_backend is None:
                target = self.current_backend
            elif target_backend == "sqlite":
                target = self.sqlite_manager
            elif target_backend == "duckdb":
                target = self.duckdb_manager
            else:
                raise ValueError(f"Unsupported target backend: {target_backend}")
            
            # Check if source table exists
            if not source.table_exists(schema, table):
                logger.warning(f"Source table {schema}.{table} does not exist")
                return False
            
            # Get source data and schema
            source_schema = source.get_table_schema(schema, table)
            source_data = source.get_data(schema, table)
            
            if source_schema is None:
                logger.error(f"Could not retrieve schema for {schema}.{table}")
                return False
            
            # Create target table if it doesn't exist
            if not target.table_exists(schema, table):
                target.create_table(schema, table, source_schema)
            
            # Insert data into target
            if source_data:
                target.insert_data(schema, table, source_data, mode="overwrite")
            
            logger.info(f"Successfully migrated {len(source_data)} rows from {source.__class__.__name__} to {target.__class__.__name__}")
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False
    
    def migrate_all_tables(self, schema: str = "default") -> Dict[str, bool]:
        """Migrate all tables from secondary to primary backend.
        
        Args:
            schema: Schema to migrate
            
        Returns:
            Dictionary mapping table names to migration success status
        """
        results = {}
        
        try:
            # Get list of tables from secondary backend
            tables = self.secondary_backend.list_tables(schema)
            
            for table in tables:
                results[table] = self.migrate_data(schema, table)
            
            logger.info(f"Migration completed for {len(tables)} tables in schema {schema}")
            
        except Exception as e:
            logger.error(f"Bulk migration failed: {e}")
        
        return results
    
    def sync_tables(self, schema: str = "default") -> Dict[str, bool]:
        """Synchronize tables between both backends.
        
        Args:
            schema: Schema to synchronize
            
        Returns:
            Dictionary mapping table names to sync success status
        """
        results = {}
        
        try:
            # Get tables from both backends
            primary_tables = set(self.current_backend.list_tables(schema))
            secondary_tables = set(self.secondary_backend.list_tables(schema))
            
            all_tables = primary_tables.union(secondary_tables)
            
            for table in all_tables:
                # Determine which backend has the most recent data
                primary_exists = table in primary_tables
                secondary_exists = table in secondary_tables
                
                if primary_exists and secondary_exists:
                    # Both exist - check row counts to determine which is more recent
                    try:
                        primary_count = len(self.current_backend.get_data(schema, table))
                        secondary_count = len(self.secondary_backend.get_data(schema, table))
                        
                        if primary_count >= secondary_count:
                            # Primary has more or equal data - sync to secondary
                            results[table] = self.migrate_data(schema, table, target_backend=self.secondary_backend.__class__.__name__.replace("StorageManager", "").lower())
                        else:
                            # Secondary has more data - sync to primary
                            results[table] = self.migrate_data(schema, table, source_backend=self.secondary_backend.__class__.__name__.replace("StorageManager", "").lower())
                    except:
                        # If we can't determine, skip this table
                        results[table] = False
                        
                elif primary_exists:
                    # Only primary exists - sync to secondary
                    results[table] = self.migrate_data(schema, table, target_backend=self.secondary_backend.__class__.__name__.replace("StorageManager", "").lower())
                    
                elif secondary_exists:
                    # Only secondary exists - sync to primary
                    results[table] = self.migrate_data(schema, table, source_backend=self.secondary_backend.__class__.__name__.replace("StorageManager", "").lower())
            
            logger.info(f"Synchronization completed for {len(all_tables)} tables in schema {schema}")
            
        except Exception as e:
            logger.error(f"Table synchronization failed: {e}")
        
        return results
    
    def get_backend_info(self) -> Dict[str, Any]:
        """Get information about both backends.
        
        Returns:
            Dictionary with backend information
        """
        return {
            "primary_backend": self.primary_backend,
            "current_backend": self.current_backend.__class__.__name__,
            "secondary_backend": self.secondary_backend.__class__.__name__,
            "sqlite_path": self.sqlite_path,
            "duckdb_path": self.duckdb_path,
            "sqlite_tables": {
                schema: self.sqlite_manager.list_tables(schema) 
                for schema in self.sqlite_manager.list_schemas()
            },
            "duckdb_tables": {
                schema: self.duckdb_manager.list_tables(schema) 
                for schema in self.duckdb_manager.list_schemas()
            }
        }
    
    # Delegate all IStorageManager methods to current backend
    def create_schema(self, schema: str) -> None:
        """Create a new schema."""
        self.current_backend.create_schema(schema)
        # Also create in secondary backend for consistency
        if not self.secondary_backend.schema_exists(schema):
            self.secondary_backend.create_schema(schema)
    
    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists."""
        return self.current_backend.schema_exists(schema)
    
    def drop_schema(self, schema: str) -> None:
        """Drop a schema."""
        self.current_backend.drop_schema(schema)
        # Also drop from secondary backend for consistency
        if self.secondary_backend.schema_exists(schema):
            self.secondary_backend.drop_schema(schema)
    
    def list_schemas(self) -> List[str]:
        """List all schemas."""
        return self.current_backend.list_schemas()
    
    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists."""
        return self.current_backend.table_exists(schema, table)
    
    def create_table(
        self,
        schema: str,
        table: str,
        columns: Union[List[MockStructField], MockStructType],
    ) -> None:
        """Create a new table."""
        self.current_backend.create_table(schema, table, columns)
    
    def drop_table(self, schema: str, table: str) -> None:
        """Drop a table."""
        self.current_backend.drop_table(schema, table)
        # Also drop from secondary backend for consistency
        if self.secondary_backend.table_exists(schema, table):
            self.secondary_backend.drop_table(schema, table)
    
    def insert_data(
        self, schema: str, table: str, data: List[Dict[str, Any]], mode: str = "append"
    ) -> None:
        """Insert data into table."""
        self.current_backend.insert_data(schema, table, data, mode)
    
    def query_table(
        self, schema: str, table: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query data from table."""
        return self.current_backend.query_table(schema, table, filter_expr)
    
    def get_table_schema(self, schema: str, table: str) -> Optional[MockStructType]:
        """Get table schema."""
        return self.current_backend.get_table_schema(schema, table)
    
    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all data from table."""
        return self.current_backend.get_data(schema, table)
    
    def create_temp_view(self, name: str, dataframe) -> None:
        """Create a temporary view from a DataFrame."""
        self.current_backend.create_temp_view(name, dataframe)
    
    def list_tables(self, schema: str) -> List[str]:
        """List tables in schema."""
        return self.current_backend.list_tables(schema)
    
    def close(self):
        """Close all backend connections."""
        try:
            if hasattr(self.sqlite_manager, 'close'):
                self.sqlite_manager.close()
            if hasattr(self.duckdb_manager, 'close'):
                self.duckdb_manager.close()
        except Exception as e:
            logger.warning(f"Error closing backend connections: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def create_hybrid_manager(
    primary_backend: str = "duckdb",
    sqlite_path: str = "mock_spark.db",
    duckdb_path: str = "mock_spark.duckdb"
) -> HybridStorageManager:
    """Factory function to create a hybrid storage manager.
    
    Args:
        primary_backend: Primary backend to use
        sqlite_path: Path to SQLite database
        duckdb_path: Path to DuckDB database
        
    Returns:
        Configured HybridStorageManager instance
    """
    return HybridStorageManager(primary_backend, sqlite_path, duckdb_path)
