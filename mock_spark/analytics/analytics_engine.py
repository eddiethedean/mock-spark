"""
Analytics Engine for Mock Spark 1.0.0

Provides high-performance analytical operations using DuckDB.
"""

import duckdb
from typing import List, Dict, Any, Optional, Union
from mock_spark.dataframe.core.dataframe import MockDataFrame
from mock_spark.spark_types import MockStructType


class AnalyticsEngine:
    """High-performance analytics engine using DuckDB."""
    
    def __init__(self, db_path: str = ":memory:"):
        """Initialize analytics engine with DuckDB connection.
        
        Args:
            db_path: Path to DuckDB database (default: in-memory)
        """
        self.connection = duckdb.connect(db_path)
        self._temp_tables = {}
        
        # Enable extensions for enhanced functionality
        try:
            self.connection.execute("INSTALL sqlite")
            self.connection.execute("LOAD sqlite")
        except:
            pass  # Extensions might not be available
    
    def register_dataframe(self, df: MockDataFrame, table_name: str = None) -> str:
        """Register a DataFrame for analytical operations.
        
        Args:
            df: MockDataFrame to register
            table_name: Name for the table (auto-generated if None)
            
        Returns:
            Table name in DuckDB
        """
        if table_name is None:
            table_name = f"analytics_df_{id(df)}"
        
        # Use DataFrame's toDuckDB method
        actual_table_name = df.toDuckDB(self.connection, table_name)
        self._temp_tables[actual_table_name] = df
        
        return actual_table_name
    
    def execute_analytical_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a raw analytical SQL query.
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of result dictionaries
        """
        try:
            result = self.connection.execute(query).fetchall()
            columns = [desc[0] for desc in self.connection.description]
            return [dict(zip(columns, row)) for row in result]
        except Exception as e:
            raise ValueError(f"Analytical query failed: {e}") from e
    
    def aggregate_by_group(
        self, 
        table_name: str, 
        group_columns: List[str], 
        aggregations: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """Perform grouped aggregations.
        
        Args:
            table_name: Name of the registered table
            group_columns: Columns to group by
            aggregations: Dict of column -> aggregation function
            
        Returns:
            Aggregated results
        """
        agg_parts = []
        for column, func in aggregations.items():
            agg_parts.append(f"{func.upper()}({column}) as {column}_{func}")
        
        query = f"""
            SELECT 
                {', '.join(group_columns)},
                {', '.join(agg_parts)}
            FROM {table_name}
            GROUP BY {', '.join(group_columns)}
            ORDER BY {group_columns[0]}
        """
        
        return self.execute_analytical_query(query)
    
    def window_functions(
        self,
        table_name: str,
        window_columns: List[str],
        partition_by: List[str],
        order_by: List[str],
        window_functions: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """Apply window functions.
        
        Args:
            table_name: Name of the registered table
            window_columns: Columns to include in result
            partition_by: Columns to partition by
            order_by: Columns to order by
            window_functions: Dict of column -> window function
            
        Returns:
            Results with window functions applied
        """
        # Build window function columns
        window_cols = []
        for column, func in window_functions.items():
            window_cols.append(f"{func}() OVER (PARTITION BY {', '.join(partition_by)} ORDER BY {', '.join(order_by)}) as {column}_{func.lower()}")
        
        # Combine all columns
        all_columns = window_columns + window_cols
        
        query = f"""
            SELECT {', '.join(all_columns)}
            FROM {table_name}
            ORDER BY {', '.join(order_by)}
        """
        
        return self.execute_analytical_query(query)
    
    def time_series_analysis(
        self,
        table_name: str,
        time_column: str,
        value_column: str,
        group_by: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Perform time series analysis.
        
        Args:
            table_name: Name of the registered table
            time_column: Column containing time data
            value_column: Column containing values to analyze
            group_by: Optional columns to group by
            
        Returns:
            Time series analysis results
        """
        group_clause = ""
        group_select = ""
        if group_by:
            group_select = f"{', '.join(group_by)}, "
            group_clause = f"GROUP BY {', '.join(group_by)}"
        
        query = f"""
            SELECT 
                {group_select}
                {time_column},
                {value_column},
                LAG({value_column}) OVER (PARTITION BY {', '.join(group_by) if group_by else '1'} ORDER BY {time_column}) as prev_value,
                LEAD({value_column}) OVER (PARTITION BY {', '.join(group_by) if group_by else '1'} ORDER BY {time_column}) as next_value,
                {value_column} - LAG({value_column}) OVER (PARTITION BY {', '.join(group_by) if group_by else '1'} ORDER BY {time_column}) as diff,
                AVG({value_column}) OVER (PARTITION BY {', '.join(group_by) if group_by else '1'} ORDER BY {time_column} ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as moving_avg_5
            FROM {table_name}
            {group_clause}
            ORDER BY {', '.join(group_by + [time_column]) if group_by else time_column}
        """
        
        return self.execute_analytical_query(query)
    
    def correlation_analysis(
        self,
        table_name: str,
        columns: List[str]
    ) -> List[Dict[str, Any]]:
        """Perform correlation analysis between columns.
        
        Args:
            table_name: Name of the registered table
            columns: Columns to analyze correlations for
            
        Returns:
            Correlation matrix
        """
        # Create correlation matrix using DuckDB's statistical functions
        correlations = []
        
        for i, col1 in enumerate(columns):
            for j, col2 in enumerate(columns):
                if i <= j:  # Only compute upper triangle
                    query = f"""
                        SELECT 
                            '{col1}' as column1,
                            '{col2}' as column2,
                            CORR({col1}, {col2}) as correlation
                        FROM {table_name}
                        WHERE {col1} IS NOT NULL AND {col2} IS NOT NULL
                    """
                    
                    result = self.execute_analytical_query(query)
                    if result and result[0]['correlation'] is not None:
                        correlations.append(result[0])
        
        return correlations
    
    def statistical_summary(
        self,
        table_name: str,
        numeric_columns: List[str]
    ) -> List[Dict[str, Any]]:
        """Generate statistical summary for numeric columns.
        
        Args:
            table_name: Name of the registered table
            numeric_columns: Numeric columns to summarize
            
        Returns:
            Statistical summaries
        """
        summaries = []
        
        for column in numeric_columns:
            query = f"""
                SELECT 
                    '{column}' as column_name,
                    COUNT({column}) as count,
                    AVG({column}) as mean,
                    MEDIAN({column}) as median,
                    MIN({column}) as min,
                    MAX({column}) as max,
                    STDDEV({column}) as stddev,
                    VARIANCE({column}) as variance,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) as q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) as q3
                FROM {table_name}
                WHERE {column} IS NOT NULL
            """
            
            result = self.execute_analytical_query(query)
            if result:
                summaries.append(result[0])
        
        return summaries
    
    def create_view(self, view_name: str, query: str) -> None:
        """Create a view for complex queries.
        
        Args:
            view_name: Name of the view
            query: SQL query to create the view
        """
        self.connection.execute(f"CREATE VIEW {view_name} AS {query}")
    
    def drop_view(self, view_name: str) -> None:
        """Drop a view.
        
        Args:
            view_name: Name of the view to drop
        """
        self.connection.execute(f"DROP VIEW IF EXISTS {view_name}")
    
    def export_to_parquet(self, table_name: str, file_path: str) -> None:
        """Export table to Parquet format.
        
        Args:
            table_name: Name of the table to export
            file_path: Path for the Parquet file
        """
        query = f"COPY {table_name} TO '{file_path}' (FORMAT PARQUET)"
        self.connection.execute(query)
    
    def export_to_csv(self, table_name: str, file_path: str) -> None:
        """Export table to CSV format.
        
        Args:
            table_name: Name of the table to export
            file_path: Path for the CSV file
        """
        query = f"COPY {table_name} TO '{file_path}' (FORMAT CSV, HEADER)"
        self.connection.execute(query)
    
    def cleanup_temp_tables(self) -> None:
        """Clean up temporary tables."""
        for table_name in list(self._temp_tables.keys()):
            try:
                self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                del self._temp_tables[table_name]
            except:
                pass  # Table might not exist
    
    def close(self):
        """Close the analytics engine."""
        self.cleanup_temp_tables()
        if self.connection:
            self.connection.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def create_analytics_engine(db_path: str = ":memory:") -> AnalyticsEngine:
    """Factory function to create an analytics engine.
    
    Args:
        db_path: Path to DuckDB database
        
    Returns:
        Configured AnalyticsEngine instance
    """
    return AnalyticsEngine(db_path)
