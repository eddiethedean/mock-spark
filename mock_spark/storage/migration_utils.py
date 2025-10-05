"""
Migration Utilities for Mock Spark 1.0.0

Provides utilities for migrating data from SQLite to DuckDB.
"""

import os
import shutil
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import logging
from datetime import datetime

from .backends.sqlite import SQLiteStorageManager, SQLiteTable
from .backends.duckdb import DuckDBStorageManager, DuckDBTable
from .hybrid_manager import HybridStorageManager
from mock_spark.spark_types import MockStructType, MockStructField
from mock_spark.storage.models import StorageMode

logger = logging.getLogger(__name__)


class StorageMigrationTool:
    """Tool for migrating data between storage backends."""
    
    def __init__(
        self,
        source_path: str,
        target_path: str,
        source_type: str = "sqlite",
        target_type: str = "duckdb"
    ):
        """Initialize migration tool.
        
        Args:
            source_path: Path to source database
            target_path: Path to target database
            source_type: Type of source backend ("sqlite")
            target_type: Type of target backend ("duckdb")
        """
        self.source_path = source_path
        self.target_path = target_path
        self.source_type = source_type.lower()
        self.target_type = target_type.lower()
        
        # Validate source and target types
        if self.source_type not in ["sqlite"]:
            raise ValueError(f"Unsupported source type: {source_type}")
        if self.target_type not in ["duckdb"]:
            raise ValueError(f"Unsupported target type: {target_type}")
        
        # Initialize source and target managers
        if self.source_type == "sqlite":
            self.source_manager = SQLiteStorageManager(source_path)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        if self.target_type == "duckdb":
            self.target_manager = DuckDBStorageManager(target_path)
        else:
            raise ValueError(f"Unsupported target type: {target_type}")
        
        logger.info(f"Migration tool initialized: {source_type} -> {target_type}")
    
    def analyze_migration(self) -> Dict[str, Any]:
        """Analyze the source database for migration planning.
        
        Returns:
            Dictionary with migration analysis
        """
        analysis = {
            "source_type": self.source_type,
            "target_type": self.target_type,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "schemas": {},
            "total_tables": 0,
            "total_rows": 0,
            "estimated_size": 0
        }
        
        try:
            # Analyze each schema
            schemas = self.source_manager.list_schemas()
            for schema in schemas:
                schema_info = {
                    "tables": {},
                    "table_count": 0,
                    "total_rows": 0
                }
                
                tables = self.source_manager.list_tables(schema)
                schema_info["table_count"] = len(tables)
                
                for table in tables:
                    try:
                        # Get table schema
                        table_schema = self.source_manager.get_table_schema(schema, table)
                        
                        # Get row count
                        data = self.source_manager.get_data(schema, table)
                        row_count = len(data)
                        
                        # Estimate size (rough calculation)
                        estimated_size = sum(
                            len(str(row)) for row in data[:100]  # Sample first 100 rows
                        ) * (row_count / min(100, row_count))
                        
                        table_info = {
                            "schema": table_schema,
                            "row_count": row_count,
                            "estimated_size": estimated_size,
                            "columns": [field.name for field in table_schema.fields] if table_schema else []
                        }
                        
                        schema_info["tables"][table] = table_info
                        schema_info["total_rows"] += row_count
                        analysis["total_rows"] += row_count
                        analysis["estimated_size"] += estimated_size
                        
                    except Exception as e:
                        logger.warning(f"Error analyzing table {schema}.{table}: {e}")
                        schema_info["tables"][table] = {"error": str(e)}
                
                analysis["schemas"][schema] = schema_info
                analysis["total_tables"] += schema_info["table_count"]
            
            logger.info(f"Migration analysis completed: {analysis['total_tables']} tables, {analysis['total_rows']} rows")
            
        except Exception as e:
            logger.error(f"Migration analysis failed: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    def migrate_schema(
        self, 
        schema: str, 
        batch_size: int = 1000,
        validate: bool = True
    ) -> Dict[str, Any]:
        """Migrate a specific schema.
        
        Args:
            schema: Schema name to migrate
            batch_size: Batch size for data migration
            validate: Whether to validate migration
            
        Returns:
            Migration results
        """
        results = {
            "schema": schema,
            "tables_migrated": 0,
            "tables_failed": 0,
            "total_rows_migrated": 0,
            "errors": [],
            "start_time": datetime.utcnow().isoformat(),
            "end_time": None
        }
        
        try:
            # Create target schema if it doesn't exist
            if not self.target_manager.schema_exists(schema):
                self.target_manager.create_schema(schema)
            
            # Get list of tables
            tables = self.source_manager.list_tables(schema)
            
            for table in tables:
                try:
                    table_result = self.migrate_table(schema, table, batch_size, validate)
                    
                    if table_result["success"]:
                        results["tables_migrated"] += 1
                        results["total_rows_migrated"] += table_result["rows_migrated"]
                    else:
                        results["tables_failed"] += 1
                        results["errors"].append(f"{schema}.{table}: {table_result['error']}")
                        
                except Exception as e:
                    results["tables_failed"] += 1
                    results["errors"].append(f"{schema}.{table}: {e}")
                    logger.error(f"Failed to migrate table {schema}.{table}: {e}")
            
            results["end_time"] = datetime.utcnow().isoformat()
            logger.info(f"Schema {schema} migration completed: {results['tables_migrated']} tables migrated, {results['tables_failed']} failed")
            
        except Exception as e:
            results["errors"].append(f"Schema migration failed: {e}")
            results["end_time"] = datetime.utcnow().isoformat()
            logger.error(f"Schema {schema} migration failed: {e}")
        
        return results
    
    def migrate_table(
        self, 
        schema: str, 
        table: str,
        batch_size: int = 1000,
        validate: bool = True
    ) -> Dict[str, Any]:
        """Migrate a specific table.
        
        Args:
            schema: Schema name
            table: Table name
            batch_size: Batch size for data migration
            validate: Whether to validate migration
            
        Returns:
            Migration results
        """
        result = {
            "schema": schema,
            "table": table,
            "success": False,
            "rows_migrated": 0,
            "error": None,
            "start_time": datetime.utcnow().isoformat(),
            "end_time": None
        }
        
        try:
            # Check if source table exists
            if not self.source_manager.table_exists(schema, table):
                result["error"] = f"Source table {schema}.{table} does not exist"
                return result
            
            # Get source table schema
            source_schema = self.source_manager.get_table_schema(schema, table)
            if source_schema is None:
                result["error"] = f"Could not retrieve schema for {schema}.{table}"
                return result
            
            # Create target table
            if not self.target_manager.table_exists(schema, table):
                self.target_manager.create_table(schema, table, source_schema)
            
            # Migrate data in batches
            total_rows = 0
            offset = 0
            
            while True:
                # Get batch of data
                batch_data = self.source_manager.query_table(
                    schema, table, 
                    f"LIMIT {batch_size} OFFSET {offset}"
                )
                
                if not batch_data:
                    break
                
                # Insert batch into target
                self.target_manager.insert_data(schema, table, batch_data, mode="append")
                total_rows += len(batch_data)
                offset += batch_size
                
                logger.debug(f"Migrated batch of {len(batch_data)} rows for {schema}.{table}")
            
            # Validate migration if requested
            if validate:
                source_count = len(self.source_manager.get_data(schema, table))
                target_count = len(self.target_manager.get_data(schema, table))
                
                if source_count != target_count:
                    result["error"] = f"Row count mismatch: source={source_count}, target={target_count}"
                    return result
            
            result["success"] = True
            result["rows_migrated"] = total_rows
            result["end_time"] = datetime.utcnow().isoformat()
            
            logger.info(f"Table {schema}.{table} migrated successfully: {total_rows} rows")
            
        except Exception as e:
            result["error"] = str(e)
            result["end_time"] = datetime.utcnow().isoformat()
            logger.error(f"Table {schema}.{table} migration failed: {e}")
        
        return result
    
    def migrate_all(self, batch_size: int = 1000, validate: bool = True) -> Dict[str, Any]:
        """Migrate all schemas and tables.
        
        Args:
            batch_size: Batch size for data migration
            validate: Whether to validate migration
            
        Returns:
            Complete migration results
        """
        results = {
            "total_schemas": 0,
            "schemas_migrated": 0,
            "schemas_failed": 0,
            "total_tables": 0,
            "tables_migrated": 0,
            "tables_failed": 0,
            "total_rows_migrated": 0,
            "start_time": datetime.utcnow().isoformat(),
            "end_time": None,
            "schema_results": {},
            "errors": []
        }
        
        try:
            # Get all schemas
            schemas = self.source_manager.list_schemas()
            results["total_schemas"] = len(schemas)
            
            for schema in schemas:
                try:
                    schema_result = self.migrate_schema(schema, batch_size, validate)
                    results["schema_results"][schema] = schema_result
                    
                    if schema_result["tables_failed"] == 0:
                        results["schemas_migrated"] += 1
                    else:
                        results["schemas_failed"] += 1
                    
                    results["total_tables"] += schema_result["tables_migrated"] + schema_result["tables_failed"]
                    results["tables_migrated"] += schema_result["tables_migrated"]
                    results["tables_failed"] += schema_result["tables_failed"]
                    results["total_rows_migrated"] += schema_result["total_rows_migrated"]
                    
                    if schema_result["errors"]:
                        results["errors"].extend(schema_result["errors"])
                        
                except Exception as e:
                    results["schemas_failed"] += 1
                    results["errors"].append(f"Schema {schema}: {e}")
                    logger.error(f"Schema {schema} migration failed: {e}")
            
            results["end_time"] = datetime.utcnow().isoformat()
            
            logger.info(f"Migration completed: {results['tables_migrated']} tables migrated, {results['tables_failed']} failed")
            
        except Exception as e:
            results["errors"].append(f"Migration failed: {e}")
            results["end_time"] = datetime.utcnow().isoformat()
            logger.error(f"Migration failed: {e}")
        
        return results
    
    def backup_source(self, backup_path: str) -> bool:
        """Create a backup of the source database.
        
        Args:
            backup_path: Path for the backup
            
        Returns:
            True if backup successful, False otherwise
        """
        try:
            if self.source_type == "sqlite":
                # For SQLite, copy the database file
                shutil.copy2(self.source_path, backup_path)
                logger.info(f"SQLite backup created: {backup_path}")
                return True
            else:
                logger.error(f"Backup not supported for source type: {self.source_type}")
                return False
                
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False
    
    def validate_migration(self, schema: str = "default") -> Dict[str, Any]:
        """Validate that migration was successful.
        
        Args:
            schema: Schema to validate
            
        Returns:
            Validation results
        """
        validation = {
            "schema": schema,
            "tables_validated": 0,
            "tables_failed": 0,
            "row_count_matches": 0,
            "row_count_mismatches": 0,
            "errors": []
        }
        
        try:
            tables = self.source_manager.list_tables(schema)
            
            for table in tables:
                try:
                    # Get row counts
                    source_count = len(self.source_manager.get_data(schema, table))
                    target_count = len(self.target_manager.get_data(schema, table))
                    
                    if source_count == target_count:
                        validation["row_count_matches"] += 1
                    else:
                        validation["row_count_mismatches"] += 1
                        validation["errors"].append(
                            f"{schema}.{table}: source={source_count}, target={target_count}"
                        )
                    
                    validation["tables_validated"] += 1
                    
                except Exception as e:
                    validation["tables_failed"] += 1
                    validation["errors"].append(f"{schema}.{table}: {e}")
            
            logger.info(f"Validation completed for {validation['tables_validated']} tables")
            
        except Exception as e:
            validation["errors"].append(f"Validation failed: {e}")
            logger.error(f"Validation failed: {e}")
        
        return validation
    
    def close(self):
        """Close all connections."""
        try:
            if hasattr(self.source_manager, 'close'):
                self.source_manager.close()
            if hasattr(self.target_manager, 'close'):
                self.target_manager.close()
        except Exception as e:
            logger.warning(f"Error closing connections: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def migrate_sqlite_to_duckdb(
    sqlite_path: str,
    duckdb_path: str,
    backup: bool = True,
    batch_size: int = 1000,
    validate: bool = True
) -> Dict[str, Any]:
    """Convenience function to migrate from SQLite to DuckDB.
    
    Args:
        sqlite_path: Path to SQLite database
        duckdb_path: Path to DuckDB database
        backup: Whether to create a backup of the source
        batch_size: Batch size for migration
        validate: Whether to validate the migration
        
    Returns:
        Migration results
    """
    results = {
        "success": False,
        "backup_created": False,
        "migration_results": None,
        "validation_results": None,
        "error": None
    }
    
    try:
        # Create backup if requested
        if backup:
            backup_path = f"{sqlite_path}.backup.{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            migration_tool = StorageMigrationTool(sqlite_path, duckdb_path)
            results["backup_created"] = migration_tool.backup_source(backup_path)
            migration_tool.close()
        
        # Perform migration
        with StorageMigrationTool(sqlite_path, duckdb_path) as migration_tool:
            # Analyze first
            analysis = migration_tool.analyze_migration()
            logger.info(f"Migration analysis: {analysis['total_tables']} tables, {analysis['total_rows']} rows")
            
            # Migrate all
            migration_results = migration_tool.migrate_all(batch_size, validate)
            results["migration_results"] = migration_results
            
            # Validate if requested
            if validate:
                validation_results = migration_tool.validate_migration()
                results["validation_results"] = validation_results
            
            # Determine overall success
            results["success"] = (
                migration_results["tables_failed"] == 0 and
                (not validate or validation_results["row_count_mismatches"] == 0)
            )
        
        logger.info(f"Migration completed successfully: {results['success']}")
        
    except Exception as e:
        results["error"] = str(e)
        logger.error(f"Migration failed: {e}")
    
    return results
