"""
Machine Learning Preprocessing for Mock Spark 1.0.0

Provides data preprocessing capabilities for ML workflows using DuckDB.
"""

import duckdb
from typing import List, Dict, Any, Optional, Union
from .analytics_engine import AnalyticsEngine


class MLPreprocessing:
    """Machine learning preprocessing using DuckDB's analytical capabilities."""
    
    def __init__(self, analytics_engine: AnalyticsEngine):
        """Initialize with analytics engine.
        
        Args:
            analytics_engine: AnalyticsEngine instance
        """
        self.engine = analytics_engine
    
    def feature_engineering(
        self,
        table_name: str,
        target_column: str,
        feature_columns: List[str],
        transformations: Optional[Dict[str, List[str]]] = None
    ) -> List[Dict[str, Any]]:
        """Apply feature engineering transformations.
        
        Args:
            table_name: Name of the table
            target_column: Target variable column
            feature_columns: List of feature columns
            transformations: Dict of column -> list of transformations to apply
            
        Returns:
            Engineered features
        """
        if transformations is None:
            transformations = {}
        
        select_parts = [target_column]
        
        for column in feature_columns:
            select_parts.append(column)
            
            # Apply transformations if specified
            if column in transformations:
                for transform in transformations[column]:
                    if transform == "log":
                        select_parts.append(f"LN({column}) as {column}_log")
                    elif transform == "sqrt":
                        select_parts.append(f"SQRT({column}) as {column}_sqrt")
                    elif transform == "square":
                        select_parts.append(f"POWER({column}, 2) as {column}_squared")
                    elif transform == "normalize":
                        select_parts.append(f"({column} - AVG({column}) OVER ()) / STDDEV({column}) OVER () as {column}_normalized")
                    elif transform == "standardize":
                        select_parts.append(f"({column} - AVG({column}) OVER ()) / STDDEV({column}) OVER () as {column}_standardized")
        
        query = f"""
            SELECT {', '.join(select_parts)}
            FROM {table_name}
            WHERE {target_column} IS NOT NULL
        """
        
        return self.engine.execute_analytical_query(query)
    
    def categorical_encoding(
        self,
        table_name: str,
        categorical_columns: List[str],
        encoding_method: str = "one_hot"
    ) -> List[Dict[str, Any]]:
        """Encode categorical variables.
        
        Args:
            table_name: Name of the table
            categorical_columns: List of categorical column names
            encoding_method: Encoding method ('one_hot', 'label', 'target')
            
        Returns:
            Encoded categorical features
        """
        if encoding_method == "one_hot":
            return self._one_hot_encoding(table_name, categorical_columns)
        elif encoding_method == "label":
            return self._label_encoding(table_name, categorical_columns)
        elif encoding_method == "target":
            return self._target_encoding(table_name, categorical_columns)
        else:
            raise ValueError(f"Unsupported encoding method: {encoding_method}")
    
    def _one_hot_encoding(self, table_name: str, categorical_columns: List[str]) -> List[Dict[str, Any]]:
        """Apply one-hot encoding to categorical variables."""
        # Get all unique values for each categorical column
        select_parts = []
        
        for column in categorical_columns:
            # Get unique values
            unique_query = f"SELECT DISTINCT {column} FROM {table_name} WHERE {column} IS NOT NULL"
            unique_values = self.engine.execute_analytical_query(unique_query)
            
            # Create one-hot columns
            for value_row in unique_values:
                value = value_row[column]
                # Escape single quotes in values
                escaped_value = str(value).replace("'", "''")
                select_parts.append(f"CASE WHEN {column} = '{escaped_value}' THEN 1 ELSE 0 END as {column}_{value}")
        
        # Add non-categorical columns
        all_columns_query = f"SELECT * FROM {table_name} LIMIT 1"
        all_columns_result = self.engine.execute_analytical_query(all_columns_query)
        if all_columns_result:
            all_columns = list(all_columns_result[0].keys())
            non_categorical = [col for col in all_columns if col not in categorical_columns]
            select_parts.extend(non_categorical)
        
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        return self.engine.execute_analytical_query(query)
    
    def _label_encoding(self, table_name: str, categorical_columns: List[str]) -> List[Dict[str, Any]]:
        """Apply label encoding to categorical variables."""
        select_parts = []
        
        for column in categorical_columns:
            select_parts.append(f"DENSE_RANK() OVER (ORDER BY {column}) - 1 as {column}_encoded")
        
        # Add non-categorical columns
        all_columns_query = f"SELECT * FROM {table_name} LIMIT 1"
        all_columns_result = self.engine.execute_analytical_query(all_columns_query)
        if all_columns_result:
            all_columns = list(all_columns_result[0].keys())
            non_categorical = [col for col in all_columns if col not in categorical_columns]
            select_parts.extend(non_categorical)
        
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        return self.engine.execute_analytical_query(query)
    
    def _target_encoding(self, table_name: str, categorical_columns: List[str]) -> List[Dict[str, Any]]:
        """Apply target encoding to categorical variables."""
        select_parts = []
        
        for column in categorical_columns:
            select_parts.append(f"AVG(target) OVER (PARTITION BY {column}) as {column}_target_encoded")
        
        # Add non-categorical columns
        all_columns_query = f"SELECT * FROM {table_name} LIMIT 1"
        all_columns_result = self.engine.execute_analytical_query(all_columns_query)
        if all_columns_result:
            all_columns = list(all_columns_result[0].keys())
            non_categorical = [col for col in all_columns if col not in categorical_columns]
            select_parts.extend(non_categorical)
        
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        return self.engine.execute_analytical_query(query)
    
    def feature_selection(
        self,
        table_name: str,
        target_column: str,
        feature_columns: List[str],
        method: str = "correlation",
        threshold: float = 0.1
    ) -> List[str]:
        """Select relevant features for ML model.
        
        Args:
            table_name: Name of the table
            target_column: Target variable column
            feature_columns: List of feature columns
            method: Selection method ('correlation', 'variance', 'mutual_info')
            threshold: Threshold for feature selection
            
        Returns:
            List of selected feature names
        """
        if method == "correlation":
            return self._correlation_feature_selection(table_name, target_column, feature_columns, threshold)
        elif method == "variance":
            return self._variance_feature_selection(table_name, feature_columns, threshold)
        elif method == "mutual_info":
            return self._mutual_info_feature_selection(table_name, target_column, feature_columns, threshold)
        else:
            raise ValueError(f"Unsupported feature selection method: {method}")
    
    def _correlation_feature_selection(
        self, 
        table_name: str, 
        target_column: str, 
        feature_columns: List[str], 
        threshold: float
    ) -> List[str]:
        """Select features based on correlation with target."""
        selected_features = []
        
        for feature in feature_columns:
            query = f"""
                SELECT ABS(CORR({feature}, {target_column})) as correlation
                FROM {table_name}
                WHERE {feature} IS NOT NULL AND {target_column} IS NOT NULL
            """
            
            result = self.engine.execute_analytical_query(query)
            if result and result[0]['correlation'] and abs(result[0]['correlation']) >= threshold:
                selected_features.append(feature)
        
        return selected_features
    
    def _variance_feature_selection(
        self, 
        table_name: str, 
        feature_columns: List[str], 
        threshold: float
    ) -> List[str]:
        """Select features based on variance."""
        selected_features = []
        
        for feature in feature_columns:
            query = f"""
                SELECT VARIANCE({feature}) as variance
                FROM {table_name}
                WHERE {feature} IS NOT NULL
            """
            
            result = self.engine.execute_analytical_query(query)
            if result and result[0]['variance'] and result[0]['variance'] >= threshold:
                selected_features.append(feature)
        
        return selected_features
    
    def _mutual_info_feature_selection(
        self, 
        table_name: str, 
        target_column: str, 
        feature_columns: List[str], 
        threshold: float
    ) -> List[str]:
        """Select features based on mutual information (simplified)."""
        # Simplified mutual information using correlation as proxy
        return self._correlation_feature_selection(table_name, target_column, feature_columns, threshold)
    
    def data_scaling(
        self,
        table_name: str,
        numeric_columns: List[str],
        method: str = "standard"
    ) -> List[Dict[str, Any]]:
        """Scale numeric features.
        
        Args:
            table_name: Name of the table
            numeric_columns: List of numeric column names
            method: Scaling method ('standard', 'minmax', 'robust')
            
        Returns:
            Scaled features
        """
        if method == "standard":
            return self._standard_scaling(table_name, numeric_columns)
        elif method == "minmax":
            return self._minmax_scaling(table_name, numeric_columns)
        elif method == "robust":
            return self._robust_scaling(table_name, numeric_columns)
        else:
            raise ValueError(f"Unsupported scaling method: {method}")
    
    def _standard_scaling(self, table_name: str, numeric_columns: List[str]) -> List[Dict[str, Any]]:
        """Apply standard scaling (z-score normalization)."""
        select_parts = []
        
        for column in numeric_columns:
            select_parts.append(f"({column} - AVG({column}) OVER ()) / STDDEV({column}) OVER () as {column}_scaled")
        
        # Add non-numeric columns
        all_columns_query = f"SELECT * FROM {table_name} LIMIT 1"
        all_columns_result = self.engine.execute_analytical_query(all_columns_query)
        if all_columns_result:
            all_columns = list(all_columns_result[0].keys())
            non_numeric = [col for col in all_columns if col not in numeric_columns]
            select_parts.extend(non_numeric)
        
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        return self.engine.execute_analytical_query(query)
    
    def _minmax_scaling(self, table_name: str, numeric_columns: List[str]) -> List[Dict[str, Any]]:
        """Apply min-max scaling."""
        select_parts = []
        
        for column in numeric_columns:
            select_parts.append(f"({column} - MIN({column}) OVER ()) / (MAX({column}) OVER () - MIN({column}) OVER ()) as {column}_scaled")
        
        # Add non-numeric columns
        all_columns_query = f"SELECT * FROM {table_name} LIMIT 1"
        all_columns_result = self.engine.execute_analytical_query(all_columns_query)
        if all_columns_result:
            all_columns = list(all_columns_result[0].keys())
            non_numeric = [col for col in all_columns if col not in numeric_columns]
            select_parts.extend(non_numeric)
        
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        return self.engine.execute_analytical_query(query)
    
    def _robust_scaling(self, table_name: str, numeric_columns: List[str]) -> List[Dict[str, Any]]:
        """Apply robust scaling using median and IQR."""
        select_parts = []
        
        for column in numeric_columns:
            select_parts.append(f"({column} - MEDIAN({column}) OVER ()) / (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) OVER () - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) OVER ()) as {column}_scaled")
        
        # Add non-numeric columns
        all_columns_query = f"SELECT * FROM {table_name} LIMIT 1"
        all_columns_result = self.engine.execute_analytical_query(all_columns_query)
        if all_columns_result:
            all_columns = list(all_columns_result[0].keys())
            non_numeric = [col for col in all_columns if col not in numeric_columns]
            select_parts.extend(non_numeric)
        
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        return self.engine.execute_analytical_query(query)
    
    def train_test_split(
        self,
        table_name: str,
        test_size: float = 0.2,
        random_seed: int = 42
    ) -> Dict[str, str]:
        """Split data into training and testing sets.
        
        Args:
            table_name: Name of the table
            test_size: Proportion of data for testing
            random_seed: Random seed for reproducibility
            
        Returns:
            Dict with train and test table names
        """
        # Create a temporary table with random values
        temp_query = f"""
            CREATE TABLE {table_name}_temp AS 
            SELECT *, RANDOM() as rand_val FROM {table_name}
        """
        self.engine.connection.execute(temp_query)
        
        # Create training set (rows where random >= test_size)
        train_query = f"""
            CREATE TABLE {table_name}_train AS 
            SELECT * FROM {table_name}_temp
            WHERE rand_val >= {test_size}
        """
        self.engine.connection.execute(train_query)
        
        # Create testing set (rows where random < test_size)
        test_query = f"""
            CREATE TABLE {table_name}_test AS 
            SELECT * FROM {table_name}_temp
            WHERE rand_val < {test_size}
        """
        self.engine.connection.execute(test_query)
        
        # Clean up temporary table
        self.engine.connection.execute(f"DROP TABLE {table_name}_temp")
        
        return {
            "train_table": f"{table_name}_train",
            "test_table": f"{table_name}_test"
        }
    
    def cross_validation_folds(
        self,
        table_name: str,
        n_folds: int = 5,
        random_seed: int = 42
    ) -> Dict[str, List[str]]:
        """Create cross-validation folds.
        
        Args:
            table_name: Name of the table
            n_folds: Number of folds
            random_seed: Random seed for reproducibility
            
        Returns:
            Dict with fold table names
        """
        folds = {}
        
        for fold in range(n_folds):
            fold_query = f"""
                CREATE TABLE {table_name}_fold_{fold} AS 
                SELECT * FROM {table_name}
                WHERE (ROW_NUMBER() OVER (ORDER BY RANDOM()) - 1) % {n_folds} = {fold}
            """
            self.engine.connection.execute(fold_query)
            
            if f"fold_{fold}" not in folds:
                folds[f"fold_{fold}"] = []
            folds[f"fold_{fold}"].append(f"{table_name}_fold_{fold}")
        
        return folds
    
    def feature_importance_analysis(
        self,
        table_name: str,
        target_column: str,
        feature_columns: List[str]
    ) -> List[Dict[str, Any]]:
        """Analyze feature importance using statistical methods.
        
        Args:
            table_name: Name of the table
            target_column: Target variable column
            feature_columns: List of feature columns
            
        Returns:
            Feature importance scores
        """
        importance_scores = []
        
        for feature in feature_columns:
            # Calculate correlation with target
            corr_query = f"""
                SELECT ABS(CORR({feature}, {target_column})) as correlation
                FROM {table_name}
                WHERE {feature} IS NOT NULL AND {target_column} IS NOT NULL
            """
            
            corr_result = self.engine.execute_analytical_query(corr_query)
            correlation = corr_result[0]['correlation'] if corr_result and corr_result[0]['correlation'] else 0.0
            
            # Calculate variance
            var_query = f"""
                SELECT VARIANCE({feature}) as variance
                FROM {table_name}
                WHERE {feature} IS NOT NULL
            """
            
            var_result = self.engine.execute_analytical_query(var_query)
            variance = var_result[0]['variance'] if var_result and var_result[0]['variance'] else 0.0
            
            # Calculate importance score (combination of correlation and variance)
            importance_score = abs(correlation) * variance
            
            importance_scores.append({
                "feature": feature,
                "correlation": correlation,
                "variance": variance,
                "importance_score": importance_score
            })
        
        # Sort by importance score
        importance_scores.sort(key=lambda x: x['importance_score'], reverse=True)
        
        return importance_scores
