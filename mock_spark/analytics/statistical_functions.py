"""
Statistical Functions for Mock Spark 1.0.0

Provides comprehensive statistical analysis capabilities using DuckDB.
"""

import duckdb
from typing import List, Dict, Any, Optional, Union
from .analytics_engine import AnalyticsEngine


class StatisticalFunctions:
    """Statistical analysis functions using DuckDB's analytical capabilities."""
    
    def __init__(self, analytics_engine: AnalyticsEngine):
        """Initialize with analytics engine.
        
        Args:
            analytics_engine: AnalyticsEngine instance
        """
        self.engine = analytics_engine
    
    def descriptive_statistics(
        self, 
        table_name: str, 
        column: str,
        group_by: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Calculate comprehensive descriptive statistics.
        
        Args:
            table_name: Name of the table
            column: Column to analyze
            group_by: Optional grouping columns
            
        Returns:
            Descriptive statistics
        """
        group_select = ""
        group_clause = ""
        partition_clause = "1"
        
        if group_by:
            group_select = f"{', '.join(group_by)}, "
            group_clause = f"GROUP BY {', '.join(group_by)}"
            partition_clause = f"{', '.join(group_by)}"
        
        query = f"""
            SELECT 
                {group_select}
                COUNT({column}) as count,
                COUNT(DISTINCT {column}) as distinct_count,
                AVG({column}) as mean,
                MEDIAN({column}) as median,
                MODE({column}) as mode,
                MIN({column}) as min,
                MAX({column}) as max,
                STDDEV({column}) as stddev,
                VARIANCE({column}) as variance,
                PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY {column}) as p10,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) as q1,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column}) as q2,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) as q3,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {column}) as p90,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {column}) as p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY {column}) as p99,
                SKEWNESS({column}) as skewness,
                KURTOSIS({column}) as kurtosis
            FROM {table_name}
            WHERE {column} IS NOT NULL
            {group_clause}
            ORDER BY {group_by[0] if group_by else 'count DESC'}
        """
        
        return self.engine.execute_analytical_query(query)
    
    def correlation_matrix(
        self,
        table_name: str,
        numeric_columns: List[str]
    ) -> Dict[str, Dict[str, float]]:
        """Calculate correlation matrix between numeric columns.
        
        Args:
            table_name: Name of the table
            numeric_columns: List of numeric column names
            
        Returns:
            Correlation matrix as nested dictionary
        """
        correlations = {}
        
        for col1 in numeric_columns:
            correlations[col1] = {}
            for col2 in numeric_columns:
                if col1 == col2:
                    correlations[col1][col2] = 1.0
                elif col2 in correlations and col1 in correlations[col2]:
                    # Use symmetric property
                    correlations[col1][col2] = correlations[col2][col1]
                else:
                    # Calculate correlation
                    query = f"""
                        SELECT CORR({col1}, {col2}) as correlation
                        FROM {table_name}
                        WHERE {col1} IS NOT NULL AND {col2} IS NOT NULL
                    """
                    
                    result = self.engine.execute_analytical_query(query)
                    corr_value = result[0]['correlation'] if result and result[0]['correlation'] else 0.0
                    correlations[col1][col2] = corr_value
        
        return correlations
    
    def hypothesis_testing(
        self,
        table_name: str,
        column1: str,
        column2: str,
        test_type: str = "ttest"
    ) -> Dict[str, Any]:
        """Perform hypothesis testing between two columns.
        
        Args:
            table_name: Name of the table
            column1: First column for comparison
            column2: Second column for comparison
            test_type: Type of test ('ttest', 'wilcoxon', 'chi2')
            
        Returns:
            Test results
        """
        if test_type == "ttest":
            return self._t_test(table_name, column1, column2)
        elif test_type == "wilcoxon":
            return self._wilcoxon_test(table_name, column1, column2)
        elif test_type == "chi2":
            return self._chi_square_test(table_name, column1, column2)
        else:
            raise ValueError(f"Unsupported test type: {test_type}")
    
    def _t_test(self, table_name: str, column1: str, column2: str) -> Dict[str, Any]:
        """Perform t-test between two columns."""
        query = f"""
            SELECT 
                COUNT(*) as n,
                AVG({column1}) as mean1,
                AVG({column2}) as mean2,
                STDDEV({column1}) as std1,
                STDDEV({column2}) as std2,
                VARIANCE({column1}) as var1,
                VARIANCE({column2}) as var2,
                CORR({column1}, {column2}) as correlation
            FROM {table_name}
            WHERE {column1} IS NOT NULL AND {column2} IS NOT NULL
        """
        
        result = self.engine.execute_analytical_query(query)[0]
        
        # Calculate t-statistic and p-value (simplified)
        n = result['n']
        mean_diff = result['mean1'] - result['mean2']
        pooled_std = ((result['var1'] + result['var2']) / 2) ** 0.5
        t_stat = mean_diff / (pooled_std * (2/n) ** 0.5)
        
        return {
            "test_type": "t-test",
            "n": n,
            "mean1": result['mean1'],
            "mean2": result['mean2'],
            "mean_difference": mean_diff,
            "t_statistic": t_stat,
            "p_value_approx": min(1.0, abs(t_stat) * 0.1),  # Simplified approximation
            "correlation": result['correlation']
        }
    
    def _wilcoxon_test(self, table_name: str, column1: str, column2: str) -> Dict[str, Any]:
        """Perform Wilcoxon signed-rank test."""
        query = f"""
            SELECT 
                COUNT(*) as n,
                SUM(CASE WHEN {column1} > {column2} THEN 1 ELSE 0 END) as positive_ranks,
                SUM(CASE WHEN {column1} < {column2} THEN 1 ELSE 0 END) as negative_ranks,
                SUM(CASE WHEN {column1} = {column2} THEN 1 ELSE 0 END) as ties
            FROM {table_name}
            WHERE {column1} IS NOT NULL AND {column2} IS NOT NULL
        """
        
        result = self.engine.execute_analytical_query(query)[0]
        
        return {
            "test_type": "Wilcoxon signed-rank test",
            "n": result['n'],
            "positive_ranks": result['positive_ranks'],
            "negative_ranks": result['negative_ranks'],
            "ties": result['ties'],
            "test_statistic": abs(result['positive_ranks'] - result['negative_ranks'])
        }
    
    def _chi_square_test(self, table_name: str, column1: str, column2: str) -> Dict[str, Any]:
        """Perform chi-square test of independence."""
        query = f"""
            SELECT 
                {column1},
                {column2},
                COUNT(*) as observed
            FROM {table_name}
            WHERE {column1} IS NOT NULL AND {column2} IS NOT NULL
            GROUP BY {column1}, {column2}
            ORDER BY {column1}, {column2}
        """
        
        contingency_table = self.engine.execute_analytical_query(query)
        
        # Calculate chi-square statistic (simplified)
        total_obs = sum(row['observed'] for row in contingency_table)
        chi_square = sum((row['observed'] - total_obs/len(contingency_table))**2 / (total_obs/len(contingency_table)) 
                        for row in contingency_table)
        
        return {
            "test_type": "Chi-square test of independence",
            "contingency_table": contingency_table,
            "chi_square_statistic": chi_square,
            "degrees_of_freedom": len(set(row[column1] for row in contingency_table)) * 
                                len(set(row[column2] for row in contingency_table)) - 1
        }
    
    def regression_analysis(
        self,
        table_name: str,
        dependent_var: str,
        independent_vars: List[str]
    ) -> Dict[str, Any]:
        """Perform linear regression analysis.
        
        Args:
            table_name: Name of the table
            dependent_var: Dependent variable column
            independent_vars: List of independent variable columns
            
        Returns:
            Regression analysis results
        """
        # Build regression query
        vars_str = ", ".join(independent_vars)
        
        query = f"""
            SELECT 
                COUNT(*) as n,
                AVG({dependent_var}) as mean_y,
                STDDEV({dependent_var}) as std_y
            FROM {table_name}
            WHERE {dependent_var} IS NOT NULL
        """
        
        basic_stats = self.engine.execute_analytical_query(query)[0]
        
        # Calculate correlations with dependent variable
        correlations = {}
        for var in independent_vars:
            corr_query = f"""
                SELECT CORR({dependent_var}, {var}) as correlation
                FROM {table_name}
                WHERE {dependent_var} IS NOT NULL AND {var} IS NOT NULL
            """
            result = self.engine.execute_analytical_query(corr_query)
            correlations[var] = result[0]['correlation'] if result and result[0]['correlation'] else 0.0
        
        return {
            "dependent_variable": dependent_var,
            "independent_variables": independent_vars,
            "n_observations": basic_stats['n'],
            "mean_y": basic_stats['mean_y'],
            "std_y": basic_stats['std_y'],
            "correlations": correlations,
            "r_squared": sum(corr**2 for corr in correlations.values()) / len(correlations) if correlations else 0.0
        }
    
    def outlier_detection(
        self,
        table_name: str,
        column: str,
        method: str = "iqr",
        threshold: float = 1.5
    ) -> List[Dict[str, Any]]:
        """Detect outliers in a column.
        
        Args:
            table_name: Name of the table
            column: Column to analyze
            method: Detection method ('iqr', 'zscore', 'modified_zscore')
            threshold: Threshold for outlier detection
            
        Returns:
            List of outliers with their values and indices
        """
        if method == "iqr":
            return self._iqr_outliers(table_name, column, threshold)
        elif method == "zscore":
            return self._zscore_outliers(table_name, column, threshold)
        elif method == "modified_zscore":
            return self._modified_zscore_outliers(table_name, column, threshold)
        else:
            raise ValueError(f"Unsupported outlier detection method: {method}")
    
    def _iqr_outliers(self, table_name: str, column: str, threshold: float) -> List[Dict[str, Any]]:
        """Detect outliers using IQR method."""
        query = f"""
            SELECT 
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) as q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) as q3
            FROM {table_name}
            WHERE {column} IS NOT NULL
        """
        
        result = self.engine.execute_analytical_query(query)
        if not result:
            return []
        
        q1 = result[0]['q1']
        q3 = result[0]['q3']
        iqr = q3 - q1
        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr
        
        outliers_query = f"""
            SELECT 
                ROW_NUMBER() OVER (ORDER BY {column}) as row_index,
                {column}
            FROM {table_name}
            WHERE {column} < {lower_bound} OR {column} > {upper_bound}
            ORDER BY {column}
        """
        
        outliers = self.engine.execute_analytical_query(outliers_query)
        
        return [
            {
                "row_index": outlier['row_index'],
                "value": outlier[column],
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "method": "iqr"
            }
            for outlier in outliers
        ]
    
    def _zscore_outliers(self, table_name: str, column: str, threshold: float) -> List[Dict[str, Any]]:
        """Detect outliers using Z-score method."""
        query = f"""
            SELECT 
                ROW_NUMBER() OVER (ORDER BY {column}) as row_index,
                {column},
                ABS(({column} - AVG({column}) OVER ()) / STDDEV({column}) OVER ()) as z_score
            FROM {table_name}
            WHERE {column} IS NOT NULL
            HAVING ABS(({column} - AVG({column}) OVER ()) / STDDEV({column}) OVER ()) > {threshold}
            ORDER BY ABS(({column} - AVG({column}) OVER ()) / STDDEV({column}) OVER ()) DESC
        """
        
        outliers = self.engine.execute_analytical_query(query)
        
        return [
            {
                "row_index": outlier['row_index'],
                "value": outlier[column],
                "z_score": outlier['z_score'],
                "method": "zscore"
            }
            for outlier in outliers
        ]
    
    def _modified_zscore_outliers(self, table_name: str, column: str, threshold: float) -> List[Dict[str, Any]]:
        """Detect outliers using modified Z-score method."""
        query = f"""
            SELECT 
                ROW_NUMBER() OVER (ORDER BY {column}) as row_index,
                {column},
                ABS(0.6745 * ({column} - MEDIAN({column}) OVER ()) / (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) OVER () - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) OVER ())) as modified_z_score
            FROM {table_name}
            WHERE {column} IS NOT NULL
            HAVING ABS(0.6745 * ({column} - MEDIAN({column}) OVER ()) / (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) OVER () - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) OVER ())) > {threshold}
            ORDER BY ABS(0.6745 * ({column} - MEDIAN({column}) OVER ()) / (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) OVER () - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) OVER ())) DESC
        """
        
        outliers = self.engine.execute_analytical_query(query)
        
        return [
            {
                "row_index": outlier['row_index'],
                "value": outlier[column],
                "modified_z_score": outlier['modified_z_score'],
                "method": "modified_zscore"
            }
            for outlier in outliers
        ]
