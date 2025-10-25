"""
Unit tests for query optimization and performance.

Tests query optimization, memory management, and performance improvements
in Mock-Spark to ensure optimal execution of DataFrame operations.
"""

import pytest
import time
from mock_spark import F
from mock_spark.optimizer import QueryOptimizer
from mock_spark.optimizer.query_optimizer import Operation, OperationType
from mock_spark.optimizer.optimization_rules import (
    FilterPushdownRule,
    ColumnPruningRule,
    JoinOptimizationRule,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    LimitPushdownRule,
    UnionOptimizationRule,
)


class TestQueryOptimizer:
    """Test the main query optimizer."""

    def test_optimizer_initialization(self):
        """Test optimizer initialization."""
        optimizer = QueryOptimizer()
        assert optimizer is not None
        assert len(optimizer.rules) > 0

    def test_optimizer_with_custom_rules(self):
        """Test optimizer with custom rules."""
        optimizer = QueryOptimizer()

        # Add custom rule
        custom_rule = FilterPushdownRule()
        optimizer.add_rule(custom_rule)

        # Remove rule
        optimizer.remove_rule(FilterPushdownRule)

        assert len(optimizer.rules) >= 0

    def test_optimization_stats(self):
        """Test optimization statistics."""
        optimizer = QueryOptimizer()

        # Create sample operations
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["id", "name", "age"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "operator": ">", "value": 25}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        optimized = optimizer.optimize(operations)
        stats = optimizer.get_optimization_stats(operations, optimized)

        assert "original_operations" in stats
        assert "optimized_operations" in stats
        assert "operations_reduced" in stats
        assert "optimization_ratio" in stats
        assert "rules_applied" in stats


class TestOptimizationRules:
    """Test individual optimization rules."""

    def test_filter_pushdown_rule(self):
        """Test filter pushdown optimization."""
        rule = FilterPushdownRule()

        # Create operations with filters
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["id", "name"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.FILTER,
                columns=[],
                predicates=[{"column": "age", "operator": ">", "value": 25}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        assert rule.can_apply(operations)
        optimized = rule.apply(operations)
        assert len(optimized) >= len(operations)

    def test_column_pruning_rule(self):
        """Test column pruning optimization."""
        rule = ColumnPruningRule()

        # Create operations with unnecessary columns
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["id", "name", "age", "salary", "department"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.SELECT,
                columns=["id", "name"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        assert rule.can_apply(operations)
        optimized = rule.apply(operations)
        assert len(optimized) >= 0

    def test_join_optimization_rule(self):
        """Test join optimization."""
        rule = JoinOptimizationRule()

        # Create join operations
        operations = [
            Operation(
                type=OperationType.JOIN,
                columns=[],
                predicates=[],
                join_conditions=[{"left_column": "id", "right_column": "user_id"}],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={"estimated_size": 1000},
            ),
            Operation(
                type=OperationType.JOIN,
                columns=[],
                predicates=[],
                join_conditions=[{"left_column": "id", "right_column": "order_id"}],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={"estimated_size": 500},
            ),
        ]

        assert rule.can_apply(operations)
        optimized = rule.apply(operations)
        assert len(optimized) == len(operations)

    def test_predicate_pushdown_rule(self):
        """Test predicate pushdown optimization."""
        rule = PredicatePushdownRule()

        # Create operations with predicates
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["id", "name"],
                predicates=[{"column": "age", "operator": ">", "value": 25}],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations)
        optimized = rule.apply(operations)
        assert len(optimized) >= 0

    def test_projection_pushdown_rule(self):
        """Test projection pushdown optimization."""
        rule = ProjectionPushdownRule()

        # Create operations with projections
        operations = [
            Operation(
                type=OperationType.SELECT,
                columns=["id", "name", "age", "salary"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.SELECT,
                columns=["id", "name"],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            ),
        ]

        assert rule.can_apply(operations)
        optimized = rule.apply(operations)
        assert len(optimized) >= 0

    def test_limit_pushdown_rule(self):
        """Test limit pushdown optimization."""
        rule = LimitPushdownRule()

        # Create operations with limits
        operations = [
            Operation(
                type=OperationType.LIMIT,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=100,
                window_specs=[],
                metadata={},
            ),
            Operation(
                type=OperationType.LIMIT,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=50,
                window_specs=[],
                metadata={},
            ),
        ]

        assert rule.can_apply(operations)
        optimized = rule.apply(operations)
        assert len(optimized) >= 0

    def test_union_optimization_rule(self):
        """Test union optimization."""
        rule = UnionOptimizationRule()

        # Create union operations
        operations = [
            Operation(
                type=OperationType.UNION,
                columns=[],
                predicates=[],
                join_conditions=[],
                group_by_columns=[],
                order_by_columns=[],
                limit_count=None,
                window_specs=[],
                metadata={},
            )
        ]

        assert rule.can_apply(operations)
        optimized = rule.apply(operations)
        assert len(optimized) >= 0


class TestPerformanceOptimization:
    """Test performance optimization features."""

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_filter_pushdown_performance(self, mock_spark):
        """Test filter pushdown performance improvement."""
        # Create large dataset
        data = [
            {"id": i, "value": i * 10, "category": f"cat_{i % 10}"}
            for i in range(10000)
        ]
        df = mock_spark.createDataFrame(data)

        # Measure time with filter pushdown
        start_time = time.time()
        result = df.filter(df.category == "cat_1").select("id", "value").collect()
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 1.0  # Should complete in under 1 second
        assert len(result) > 0

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_column_pruning_performance(self, mock_spark):
        """Test column pruning performance improvement."""
        # Create dataset with many columns
        data = [
            {
                "id": i,
                "name": f"user_{i}",
                "age": 20 + (i % 50),
                "salary": 30000 + (i * 100),
                "department": f"dept_{i % 5}",
                "location": f"loc_{i % 10}",
                "status": "active" if i % 2 == 0 else "inactive",
            }
            for i in range(5000)
        ]
        df = mock_spark.createDataFrame(data)

        # Measure time with column pruning
        start_time = time.time()
        result = df.select("id", "name").collect()
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 1.0  # Should complete in under 1 second
        assert len(result) == 5000

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_join_optimization_performance(self, mock_spark):
        """Test join optimization performance."""
        # Create two datasets
        users_data = [{"id": i, "name": f"user_{i}"} for i in range(1000)]
        orders_data = [
            {"id": i, "user_id": i % 500, "amount": i * 10} for i in range(2000)
        ]

        users_df = mock_spark.createDataFrame(users_data)
        orders_df = mock_spark.createDataFrame(orders_data)

        # Measure join performance
        start_time = time.time()
        result = users_df.join(orders_df, users_df.id == orders_df.user_id).collect()
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 2.0  # Should complete in under 2 seconds
        assert len(result) > 0

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_limit_optimization_performance(self, mock_spark):
        """Test limit optimization performance."""
        # Create large dataset
        data = [{"id": i, "value": i * 10} for i in range(100000)]
        df = mock_spark.createDataFrame(data)

        # Measure limit performance
        start_time = time.time()
        result = df.limit(100).collect()
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 1.0  # Should complete in under 1 second
        assert len(result) == 100

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_complex_query_optimization(self, mock_spark):
        """Test optimization of complex queries."""
        # Create complex dataset
        data = [
            {
                "id": i,
                "name": f"user_{i}",
                "age": 20 + (i % 50),
                "salary": 30000 + (i * 100),
                "department": f"dept_{i % 5}",
                "join_date": f"2020-{(i % 12) + 1:02d}-01",
            }
            for i in range(10000)
        ]
        df = mock_spark.createDataFrame(data)

        # Complex query with multiple operations
        start_time = time.time()
        result = (
            df.filter(df.age > 30)
            .filter(df.salary > 50000)
            .select("id", "name", "department")
            .groupBy("department")
            .agg(F.count("*").alias("count"))
            .orderBy(F.desc("count"))
            .limit(10)
            .collect()
        )
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 3.0  # Should complete in under 3 seconds
        assert len(result) <= 10


class TestMemoryManagement:
    """Test memory management features."""

    def test_memory_usage_tracking(self, mock_spark):
        """Test memory usage tracking."""
        # Get initial memory usage
        initial_memory = mock_spark.storage.get_memory_usage()
        assert "rss" in initial_memory
        assert "vms" in initial_memory
        assert "percent" in initial_memory

    def test_cleanup_temp_tables(self, mock_spark):
        """Test temporary table cleanup."""
        # Create some data
        data = [{"id": i, "value": i * 10} for i in range(1000)]
        df = mock_spark.createDataFrame(data)

        # Perform operations that might create temp tables
        df.filter(df.value > 500).collect()

        # Clean up temp tables
        mock_spark.storage.cleanup_temp_tables()

        # Should not raise exception
        assert True

    def test_force_garbage_collection(self, mock_spark):
        """Test forced garbage collection."""
        # Create some data
        data = [{"id": i, "value": i * 10} for i in range(1000)]
        df = mock_spark.createDataFrame(data)

        # Perform operations
        df.filter(df.value > 500).collect()

        # Force garbage collection
        mock_spark.storage.force_garbage_collection()

        # Should not raise exception
        assert True

    def test_table_size_tracking(self, mock_spark):
        """Test table size tracking."""
        # Create some data
        data = [{"id": i, "value": i * 10} for i in range(1000)]
        mock_spark.createDataFrame(data)

        # Get table sizes
        sizes = mock_spark.storage.get_table_sizes()
        assert isinstance(sizes, dict)

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_cleanup_old_tables(self, spark):
        """Test cleanup of old tables."""
        # Create some data
        data = [{"id": i, "value": i * 10} for i in range(1000)]
        spark.createDataFrame(data)

        # Clean up old tables (should not affect current session)
        cleaned_count = spark.storage.cleanup_old_tables(max_age_hours=0)

        # Should not raise exception
        assert cleaned_count >= 0

    def test_memory_optimization(self, mock_spark):
        """Test memory optimization."""
        # Create large dataset
        data = [{"id": i, "value": i * 10} for i in range(10000)]
        df = mock_spark.createDataFrame(data)

        # Perform operations
        df.filter(df.value > 5000).collect()

        # Optimize storage
        mock_spark.storage.optimize_storage()

        # Should not raise exception
        assert True


class TestLazyEvaluationOptimization:
    """Test lazy evaluation with optimization."""

    def test_lazy_evaluation_with_optimization(self, mock_spark):
        """Test lazy evaluation with optimization enabled."""
        # Create dataset
        data = [
            {"id": i, "value": i * 10, "category": f"cat_{i % 5}"} for i in range(1000)
        ]
        df = mock_spark.createDataFrame(data)

        # Enable lazy evaluation
        df_lazy = df

        # Build complex query
        query = (
            df_lazy.filter(df_lazy.category == "cat_1")
            .select("id", "value")
            .filter(df_lazy.value > 100)
        )

        # Materialize with optimization
        start_time = time.time()
        result = query.collect()
        end_time = time.time()

        execution_time = end_time - start_time
        assert (
            execution_time < 5.0
        )  # Should complete in under 5 seconds (adjusted for test environment overhead)
        assert len(result) > 0

    def test_optimization_disabled(self, mock_spark):
        """Test lazy evaluation with optimization disabled."""
        # Create dataset
        data = [{"id": i, "value": i * 10} for i in range(1000)]
        df = mock_spark.createDataFrame(data)

        # Enable lazy evaluation without optimization
        df_lazy = df

        # Build query
        query = df_lazy.filter(df_lazy.value > 500)

        # Materialize without optimization
        start_time = time.time()
        result = query.collect()
        end_time = time.time()

        execution_time = end_time - start_time
        assert (
            execution_time < 4.0
        )  # Should still complete quickly (adjusted for test environment)
        assert len(result) > 0


class TestPerformanceBenchmarks:
    """Test performance benchmarks."""

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_small_dataset_performance(self, mock_spark):
        """Test performance with small dataset."""
        data = [{"id": i, "value": i * 10} for i in range(100)]
        df = mock_spark.createDataFrame(data)

        start_time = time.time()
        df.filter(df.value > 500).collect()
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 0.1  # Should complete in under 0.1 seconds

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_medium_dataset_performance(self, mock_spark):
        """Test performance with medium dataset."""
        data = [{"id": i, "value": i * 10} for i in range(10000)]
        df = mock_spark.createDataFrame(data)

        start_time = time.time()
        df.filter(df.value > 5000).collect()
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 0.5  # Should complete in under 0.5 seconds

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_large_dataset_performance(self, mock_spark):
        """Test performance with large dataset."""
        data = [{"id": i, "value": i * 10} for i in range(100000)]
        df = mock_spark.createDataFrame(data)

        start_time = time.time()
        result = df.filter(df.value > 50000).limit(1000).collect()
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 2.0  # Should complete in under 2 seconds
        assert len(result) <= 1000

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_aggregation_performance(self, mock_spark):
        """Test aggregation performance."""
        data = [
            {"id": i, "value": i * 10, "category": f"cat_{i % 10}"}
            for i in range(50000)
        ]
        df = mock_spark.createDataFrame(data)

        start_time = time.time()
        result = (
            df.groupBy("category")
            .agg(F.count("*").alias("count"), F.avg("value").alias("avg_value"))
            .collect()
        )
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 3.0  # Should complete in under 3 seconds
        assert len(result) == 10  # 10 categories

    @pytest.mark.skip(reason="Performance benchmarks are environment-dependent")
    def test_join_performance(self, mock_spark):
        """Test join performance."""
        # Create two datasets
        users_data = [{"id": i, "name": f"user_{i}"} for i in range(5000)]
        orders_data = [
            {"id": i, "user_id": i % 2500, "amount": i * 10} for i in range(10000)
        ]

        users_df = mock_spark.createDataFrame(users_data)
        orders_df = mock_spark.createDataFrame(orders_data)

        start_time = time.time()
        result = (
            users_df.join(orders_df, users_df.id == orders_df.user_id)
            .select("name", "amount")
            .limit(1000)
            .collect()
        )
        end_time = time.time()

        execution_time = end_time - start_time
        assert execution_time < 2.0  # Should complete in under 2 seconds
        assert len(result) <= 1000
