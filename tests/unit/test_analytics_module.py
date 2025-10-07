"""
Test Analytics Module for Mock Spark 1.0.0

Tests the new analytics capabilities using DuckDB.
"""

import pytest
import duckdb
from datetime import datetime, timedelta
from mock_spark.analytics import (
    AnalyticsEngine,
    StatisticalFunctions,
    TimeSeriesAnalysis,
    MLPreprocessing,
)
from mock_spark.dataframe.dataframe import MockDataFrame
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


class TestAnalyticsEngine:
    """Test AnalyticsEngine functionality."""

    def test_analytics_engine_initialization(self):
        """Test analytics engine initialization."""
        engine = AnalyticsEngine()
        assert engine.connection is not None
        assert isinstance(engine._temp_tables, dict)
        engine.close()

    def test_register_dataframe(self):
        """Test DataFrame registration with analytics engine."""
        # Create test DataFrame
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("value", DoubleType()),
                MockStructField("category", StringType()),
            ]
        )

        data = [
            {"id": 1, "value": 10.5, "category": "A"},
            {"id": 2, "value": 20.3, "category": "B"},
            {"id": 3, "value": 15.7, "category": "A"},
        ]

        df = MockDataFrame(data, schema)
        engine = AnalyticsEngine()

        # Register DataFrame
        table_name = engine.register_dataframe(df, "test_table")
        assert table_name == "test_table"
        assert table_name in engine._temp_tables

        engine.close()

    def test_execute_analytical_query(self):
        """Test executing analytical queries."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute("CREATE TABLE test (id INTEGER, value DOUBLE)")
        engine.connection.execute("INSERT INTO test VALUES (1, 10.0), (2, 20.0), (3, 30.0)")

        # Execute analytical query
        result = engine.execute_analytical_query("SELECT AVG(value) as avg_value FROM test")

        assert len(result) == 1
        assert result[0]["avg_value"] == 20.0

        engine.close()

    def test_aggregate_by_group(self):
        """Test grouped aggregations."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute(
            "CREATE TABLE sales (category VARCHAR, amount DOUBLE, region VARCHAR)"
        )
        engine.connection.execute(
            """
            INSERT INTO sales VALUES 
            ('Electronics', 100.0, 'North'),
            ('Electronics', 150.0, 'South'),
            ('Clothing', 75.0, 'North'),
            ('Clothing', 80.0, 'South')
        """
        )

        # Test aggregation
        result = engine.aggregate_by_group("sales", ["category"], {"amount": "sum"})

        assert len(result) == 2
        electronics_sum = next(r for r in result if r["category"] == "Electronics")
        assert electronics_sum["amount_sum"] == 250.0

        engine.close()

    def test_window_functions(self):
        """Test window function operations."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute(
            "CREATE TABLE employees (name VARCHAR, salary DOUBLE, department VARCHAR)"
        )
        engine.connection.execute(
            """
            INSERT INTO employees VALUES 
            ('Alice', 50000, 'Engineering'),
            ('Bob', 60000, 'Engineering'),
            ('Charlie', 45000, 'Marketing'),
            ('David', 55000, 'Marketing')
        """
        )

        # Test window functions
        result = engine.window_functions(
            "employees",
            ["name", "salary", "department"],
            ["department"],
            ["salary"],
            {"salary": "ROW_NUMBER"},
        )

        assert len(result) == 4
        # Check that row numbers are assigned within departments
        engineering_rows = [r for r in result if r["department"] == "Engineering"]
        assert engineering_rows[0]["salary_row_number"] == 1
        assert engineering_rows[1]["salary_row_number"] == 2

        engine.close()

    def test_time_series_analysis(self):
        """Test time series analysis."""
        engine = AnalyticsEngine()

        # Create test time series data
        engine.connection.execute("CREATE TABLE timeseries (date DATE, value DOUBLE)")
        engine.connection.execute(
            """
            INSERT INTO timeseries VALUES 
            ('2023-01-01', 100.0),
            ('2023-01-02', 110.0),
            ('2023-01-03', 105.0),
            ('2023-01-04', 120.0)
        """
        )

        # Test time series analysis
        result = engine.time_series_analysis("timeseries", "date", "value")

        assert len(result) == 4
        assert "prev_value" in result[1]
        assert "next_value" in result[1]
        assert "diff" in result[1]

        engine.close()

    def test_export_functionality(self):
        """Test export to Parquet and CSV."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute("CREATE TABLE export_test (id INTEGER, value DOUBLE)")
        engine.connection.execute("INSERT INTO export_test VALUES (1, 10.0), (2, 20.0)")

        # Test CSV export
        engine.export_to_csv("export_test", "/tmp/test_export.csv")

        # Test Parquet export
        engine.export_to_parquet("export_test", "/tmp/test_export.parquet")

        engine.close()


class TestStatisticalFunctions:
    """Test StatisticalFunctions functionality."""

    def test_descriptive_statistics(self):
        """Test descriptive statistics calculation."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute("CREATE TABLE stats_test (value DOUBLE)")
        engine.connection.execute("INSERT INTO stats_test VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")

        stats_func = StatisticalFunctions(engine)
        result = stats_func.descriptive_statistics("stats_test", "value")

        assert len(result) == 1
        stats = result[0]
        assert stats["count"] == 5
        assert stats["mean"] == 3.0
        assert stats["min"] == 1.0
        assert stats["max"] == 5.0

        engine.close()

    def test_correlation_matrix(self):
        """Test correlation matrix calculation."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute("CREATE TABLE corr_test (x DOUBLE, y DOUBLE)")
        engine.connection.execute("INSERT INTO corr_test VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0)")

        stats_func = StatisticalFunctions(engine)
        result = stats_func.correlation_matrix("corr_test", ["x", "y"])

        assert "x" in result
        assert "y" in result
        assert result["x"]["x"] == 1.0  # Self-correlation
        assert abs(result["x"]["y"] - 1.0) < 0.01  # Perfect correlation

        engine.close()

    def test_hypothesis_testing(self):
        """Test hypothesis testing functionality."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute("CREATE TABLE htest_test (group1 DOUBLE, group2 DOUBLE)")
        engine.connection.execute(
            """
            INSERT INTO htest_test VALUES 
            (10.0, 12.0), (11.0, 13.0), (9.0, 11.0), (10.5, 12.5)
        """
        )

        stats_func = StatisticalFunctions(engine)
        result = stats_func.hypothesis_testing("htest_test", "group1", "group2", "ttest")

        assert result["test_type"] == "t-test"
        assert "n" in result
        assert "mean1" in result
        assert "mean2" in result
        assert "t_statistic" in result

        engine.close()

    def test_outlier_detection(self):
        """Test outlier detection functionality."""
        engine = AnalyticsEngine()

        # Create test data with outliers
        engine.connection.execute("CREATE TABLE outlier_test (value DOUBLE)")
        engine.connection.execute(
            """
            INSERT INTO outlier_test VALUES 
            (1.0), (2.0), (3.0), (4.0), (5.0), (100.0)  -- 100 is an outlier
        """
        )

        stats_func = StatisticalFunctions(engine)
        result = stats_func.outlier_detection("outlier_test", "value", "iqr")

        assert len(result) >= 1
        outlier = next((o for o in result if o["value"] == 100.0), None)
        assert outlier is not None
        assert outlier["method"] == "iqr"

        engine.close()


class TestTimeSeriesAnalysis:
    """Test TimeSeriesAnalysis functionality."""

    def test_moving_averages(self):
        """Test moving average calculations."""
        engine = AnalyticsEngine()

        # Create test time series data
        engine.connection.execute("CREATE TABLE ts_test (date DATE, value DOUBLE)")
        engine.connection.execute(
            """
            INSERT INTO ts_test VALUES 
            ('2023-01-01', 10.0),
            ('2023-01-02', 20.0),
            ('2023-01-03', 30.0),
            ('2023-01-04', 40.0),
            ('2023-01-05', 50.0)
        """
        )

        ts_analysis = TimeSeriesAnalysis(engine)
        result = ts_analysis.moving_averages("ts_test", "date", "value", [3])

        assert len(result) == 5
        # Check that moving average is calculated
        assert "ma3" in result[2]  # Third row should have 3-period MA

        engine.close()

    def test_exponential_smoothing(self):
        """Test exponential smoothing."""
        engine = AnalyticsEngine()

        # Create test time series data
        engine.connection.execute("CREATE TABLE exp_test (date DATE, value DOUBLE)")
        engine.connection.execute(
            """
            INSERT INTO exp_test VALUES 
            ('2023-01-01', 10.0),
            ('2023-01-02', 15.0),
            ('2023-01-03', 12.0),
            ('2023-01-04', 18.0)
        """
        )

        ts_analysis = TimeSeriesAnalysis(engine)
        result = ts_analysis.exponential_smoothing("exp_test", "date", "value", 0.3)

        assert len(result) == 4
        assert "exponential_smooth" in result[1]

        engine.close()

    def test_anomaly_detection(self):
        """Test time series anomaly detection."""
        engine = AnalyticsEngine()

        # Create test data with anomalies
        engine.connection.execute("CREATE TABLE anomaly_test (date DATE, value DOUBLE)")
        engine.connection.execute(
            """
            INSERT INTO anomaly_test VALUES 
            ('2023-01-01', 10.0),
            ('2023-01-02', 12.0),
            ('2023-01-03', 100.0),  -- Anomaly
            ('2023-01-04', 11.0)
        """
        )

        ts_analysis = TimeSeriesAnalysis(engine)
        result = ts_analysis.time_series_anomaly_detection(
            "anomaly_test", "date", "value", threshold=1.0
        )

        assert len(result) == 4
        anomaly_row = next((r for r in result if r["original"] == 100.0), None)
        assert anomaly_row is not None
        assert anomaly_row["is_anomaly"] == 1

        engine.close()


class TestMLPreprocessing:
    """Test MLPreprocessing functionality."""

    def test_feature_engineering(self):
        """Test feature engineering transformations."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute("CREATE TABLE ml_test (target DOUBLE, feature DOUBLE)")
        engine.connection.execute("INSERT INTO ml_test VALUES (1.0, 2.0), (2.0, 4.0), (3.0, 6.0)")

        ml_prep = MLPreprocessing(engine)
        result = ml_prep.feature_engineering(
            "ml_test", "target", ["feature"], {"feature": ["log", "sqrt"]}
        )

        assert len(result) == 3
        assert "feature_log" in result[0]
        assert "feature_sqrt" in result[0]

        engine.close()

    def test_categorical_encoding(self):
        """Test categorical encoding."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute("CREATE TABLE cat_test (category VARCHAR, value DOUBLE)")
        engine.connection.execute(
            """
            INSERT INTO cat_test VALUES 
            ('A', 10.0),
            ('B', 20.0),
            ('A', 15.0),
            ('C', 25.0)
        """
        )

        ml_prep = MLPreprocessing(engine)
        result = ml_prep.categorical_encoding("cat_test", ["category"], "one_hot")

        assert len(result) == 4
        # Check that one-hot columns are created
        one_hot_cols = [col for col in result[0].keys() if col.startswith("category_")]
        assert len(one_hot_cols) >= 3  # At least 3 categories

        engine.close()

    def test_feature_selection(self):
        """Test feature selection."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute(
            "CREATE TABLE fs_test (target DOUBLE, good_feature DOUBLE, bad_feature DOUBLE)"
        )
        engine.connection.execute(
            """
            INSERT INTO fs_test VALUES 
            (1.0, 1.0, 0.1),
            (2.0, 2.0, 0.2),
            (3.0, 3.0, 0.3),
            (4.0, 4.0, 0.4)
        """
        )

        ml_prep = MLPreprocessing(engine)
        selected = ml_prep.feature_selection(
            "fs_test", "target", ["good_feature", "bad_feature"], "correlation", 0.5
        )

        assert "good_feature" in selected
        # bad_feature should not be selected due to low correlation

        engine.close()

    def test_data_scaling(self):
        """Test data scaling."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute("CREATE TABLE scale_test (feature1 DOUBLE, feature2 DOUBLE)")
        engine.connection.execute(
            "INSERT INTO scale_test VALUES (1.0, 10.0), (2.0, 20.0), (3.0, 30.0)"
        )

        ml_prep = MLPreprocessing(engine)
        result = ml_prep.data_scaling("scale_test", ["feature1", "feature2"], "standard")

        assert len(result) == 3
        assert "feature1_scaled" in result[0]
        assert "feature2_scaled" in result[0]

        # Check that scaling centers data around 0
        feature1_scaled = [r["feature1_scaled"] for r in result]
        assert abs(sum(feature1_scaled)) < 0.01  # Should sum to approximately 0

        engine.close()

    def test_train_test_split(self):
        """Test train-test split."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute("CREATE TABLE split_test (id INTEGER, value DOUBLE)")
        engine.connection.execute(
            "INSERT INTO split_test VALUES (1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), (5, 5.0)"
        )

        ml_prep = MLPreprocessing(engine)
        splits = ml_prep.train_test_split("split_test", test_size=0.4)

        assert "train_table" in splits
        assert "test_table" in splits

        # Check that tables were created
        train_count = engine.execute_analytical_query(
            f"SELECT COUNT(*) as count FROM {splits['train_table']}"
        )
        test_count = engine.execute_analytical_query(
            f"SELECT COUNT(*) as count FROM {splits['test_table']}"
        )

        assert train_count[0]["count"] + test_count[0]["count"] == 5

        engine.close()

    def test_feature_importance_analysis(self):
        """Test feature importance analysis."""
        engine = AnalyticsEngine()

        # Create test data
        engine.connection.execute(
            "CREATE TABLE importance_test (target DOUBLE, important DOUBLE, unimportant DOUBLE)"
        )
        engine.connection.execute(
            """
            INSERT INTO importance_test VALUES 
            (1.0, 1.0, 0.1),
            (2.0, 2.0, 0.2),
            (3.0, 3.0, 0.3),
            (4.0, 4.0, 0.4)
        """
        )

        ml_prep = MLPreprocessing(engine)
        importance = ml_prep.feature_importance_analysis(
            "importance_test", "target", ["important", "unimportant"]
        )

        assert len(importance) == 2
        # Important feature should have higher importance score
        important_score = next(
            f["importance_score"] for f in importance if f["feature"] == "important"
        )
        unimportant_score = next(
            f["importance_score"] for f in importance if f["feature"] == "unimportant"
        )
        assert important_score > unimportant_score

        engine.close()


if __name__ == "__main__":
    pytest.main([__file__])
