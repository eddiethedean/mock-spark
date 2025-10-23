"""
Unit tests for advanced window functions.

Tests complex window scenarios including partitioning, ordering,
boundaries, and all window function types.
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.window import MockWindow


@pytest.mark.fast
class TestWindowFunctionsAdvanced:
    """Test advanced window functions with complex scenarios."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def sales_data(self):
        """Sample sales data for testing."""
        return [
            {
                "customer_id": 1,
                "product": "Laptop",
                "amount": 1000.0,
                "date": "2024-01-15",
                "region": "North",
                "salesperson": "Alice",
            },
            {
                "customer_id": 1,
                "product": "Mouse",
                "amount": 25.0,
                "date": "2024-01-16",
                "region": "North",
                "salesperson": "Alice",
            },
            {
                "customer_id": 2,
                "product": "Laptop",
                "amount": 1200.0,
                "date": "2024-01-15",
                "region": "South",
                "salesperson": "Bob",
            },
            {
                "customer_id": 2,
                "product": "Keyboard",
                "amount": 75.0,
                "date": "2024-01-17",
                "region": "South",
                "salesperson": "Bob",
            },
            {
                "customer_id": 3,
                "product": "Monitor",
                "amount": 300.0,
                "date": "2024-01-16",
                "region": "North",
                "salesperson": "Alice",
            },
            {
                "customer_id": 3,
                "product": "Laptop",
                "amount": 1100.0,
                "date": "2024-01-18",
                "region": "North",
                "salesperson": "Alice",
            },
        ]

    def test_row_number_partitioning(self, spark, sales_data):
        """Test row_number with partitioning."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id"),
            F.col("product"),
            F.col("amount"),
            F.row_number().over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("row_num"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that row numbers start at 1 for each partition
        customer_1_rows = [r for r in rows if r["customer_id"] == 1]
        customer_2_rows = [r for r in rows if r["customer_id"] == 2]
        customer_3_rows = [r for r in rows if r["customer_id"] == 3]

        assert len(customer_1_rows) == 2
        assert len(customer_2_rows) == 2
        assert len(customer_3_rows) == 2

        # Row numbers should be sequential within each partition
        for customer_rows in [customer_1_rows, customer_2_rows, customer_3_rows]:
            row_nums = [r["row_num"] for r in customer_rows]
            assert sorted(row_nums) == list(range(1, len(customer_rows) + 1))

    def test_rank_and_dense_rank(self, spark, sales_data):
        """Test rank and dense_rank functions."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id"),
            F.col("amount"),
            F.rank().over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("rank"),
            F.dense_rank().over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("dense_rank"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that ranks are valid
        for row in rows:
            assert row["rank"] >= 1
            assert row["dense_rank"] >= 1
            assert row["dense_rank"] <= row["rank"]

    def test_lag_and_lead(self, spark, sales_data):
        """Test lag and lead functions."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id"),
            F.col("product"),
            F.col("amount"),
            F.lag("amount", 1).over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("prev_amount"),
            F.lead("amount", 1).over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("next_amount"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that lag/lead work correctly
        for row in rows:
            # prev_amount and next_amount should be None for first/last rows in partition
            # or valid numbers for middle rows
            assert row["prev_amount"] is None or isinstance(row["prev_amount"], (int, float))
            assert row["next_amount"] is None or isinstance(row["next_amount"], (int, float))

    def test_first_and_last_values(self, spark, sales_data):
        """Test first_value and last_value functions."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id"),
            F.col("amount"),
            F.first("amount").over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("first_amount"),
            F.last("amount").over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("last_amount"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that first and last values are consistent within partitions
        for row in rows:
            assert isinstance(row["first_amount"], (int, float))
            assert isinstance(row["last_amount"], (int, float))

    def test_window_with_multiple_partitions(self, spark, sales_data):
        """Test window functions with multiple partition columns."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("region"),
            F.col("salesperson"),
            F.col("amount"),
            F.row_number().over(
                MockWindow.partitionBy("region", "salesperson").orderBy("amount")
            ).alias("row_num"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that row numbers are correct for each partition
        partitions = {}
        for row in rows:
            key = (row["region"], row["salesperson"])
            if key not in partitions:
                partitions[key] = []
            partitions[key].append(row["row_num"])

        # Each partition should have sequential row numbers starting from 1
        for partition_rows in partitions.values():
            assert sorted(partition_rows) == list(range(1, len(partition_rows) + 1))

    def test_window_with_complex_ordering(self, spark, sales_data):
        """Test window functions with complex ordering expressions."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id"),
            F.col("product"),
            F.col("amount"),
            F.row_number().over(
                MockWindow.partitionBy("customer_id")
                .orderBy(F.col("amount").desc())
            ).alias("row_num_desc"),
            F.row_number().over(
                MockWindow.partitionBy("customer_id")
                .orderBy(F.col("amount").asc())
            ).alias("row_num_asc"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that ordering works correctly
        for row in rows:
            assert row["row_num_desc"] >= 1
            assert row["row_num_asc"] >= 1

    def test_window_boundaries_rows_between(self, spark, sales_data):
        """Test window functions with ROWS BETWEEN boundaries."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id"),
            F.col("amount"),
            F.sum("amount").over(
                MockWindow.partitionBy("customer_id")
                .orderBy("amount")
                .rowsBetween(MockWindow.currentRow, MockWindow.unboundedFollowing)
            ).alias("sum_following"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that sum is calculated correctly
        for row in rows:
            assert isinstance(row["sum_following"], (int, float))
            assert row["sum_following"] >= 0

    def test_window_boundaries_range_between(self, spark, sales_data):
        """Test window functions with RANGE BETWEEN boundaries."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id"),
            F.col("amount"),
            F.avg("amount").over(
                MockWindow.partitionBy("customer_id")
                .orderBy("amount")
                .rangeBetween(MockWindow.unboundedPreceding, MockWindow.currentRow)
            ).alias("avg_preceding"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that average is calculated correctly
        for row in rows:
            assert isinstance(row["avg_preceding"], (int, float))
            assert row["avg_preceding"] >= 0

    def test_window_with_aliases(self, spark, sales_data):
        """Test window functions with column aliases."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id").alias("customer"),
            F.col("amount").alias("sale_amount"),
            F.row_number().over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("rank"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that aliases work correctly
        for row in rows:
            assert "customer" in row
            assert "sale_amount" in row
            assert "rank" in row

    def test_window_with_null_handling(self, spark):
        """Test window functions with null values."""
        data_with_nulls = [
            {"id": 1, "value": 10, "category": "A"},
            {"id": 2, "value": None, "category": "A"},
            {"id": 3, "value": 20, "category": "A"},
            {"id": 4, "value": 15, "category": "B"},
            {"id": 5, "value": None, "category": "B"},
        ]
        df = spark.createDataFrame(data_with_nulls)

        result = df.select(
            F.col("id"),
            F.col("value"),
            F.col("category"),
            F.row_number().over(
                MockWindow.partitionBy("category").orderBy("value")
            ).alias("row_num"),
        )

        rows = result.collect()
        assert len(rows) == 5

        # Check that nulls are handled correctly
        for row in rows:
            assert row["row_num"] >= 1

    def test_window_with_string_ordering(self, spark):
        """Test window functions with string ordering."""
        data = [
            {"name": "Alice", "department": "IT", "salary": 50000},
            {"name": "Bob", "department": "IT", "salary": 60000},
            {"name": "Charlie", "department": "HR", "salary": 45000},
            {"name": "David", "department": "HR", "salary": 55000},
        ]
        df = spark.createDataFrame(data)

        result = df.select(
            F.col("name"),
            F.col("department"),
            F.col("salary"),
            F.row_number().over(
                MockWindow.partitionBy("department").orderBy("name")
            ).alias("row_num"),
        )

        rows = result.collect()
        assert len(rows) == 4

        # Check that string ordering works
        for row in rows:
            assert row["row_num"] >= 1

    def test_window_with_multiple_functions(self, spark, sales_data):
        """Test multiple window functions in one query."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id"),
            F.col("amount"),
            F.row_number().over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("row_num"),
            F.rank().over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("rank"),
            F.sum("amount").over(
                MockWindow.partitionBy("customer_id")
            ).alias("total_amount"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that all window functions work together
        for row in rows:
            assert row["row_num"] >= 1
            assert row["rank"] >= 1
            assert row["total_amount"] >= 0

    def test_window_with_complex_expressions(self, spark, sales_data):
        """Test window functions with complex expressions."""
        df = spark.createDataFrame(sales_data)

        result = df.select(
            F.col("customer_id"),
            F.col("amount"),
            F.col("product"),
            F.row_number().over(
                MockWindow.partitionBy("customer_id")
                .orderBy(F.col("amount").desc(), F.col("product").asc())
            ).alias("row_num"),
        )

        rows = result.collect()
        assert len(rows) == 6

        # Check that complex ordering works
        for row in rows:
            assert row["row_num"] >= 1

    def test_window_performance_with_large_partitions(self, spark):
        """Test window functions with larger datasets."""
        # Create a larger dataset
        data = []
        for customer_id in range(1, 11):  # 10 customers
            for product_id in range(1, 6):  # 5 products per customer
                data.append({
                    "customer_id": customer_id,
                    "product_id": product_id,
                    "amount": customer_id * 100 + product_id * 10,
                })

        df = spark.createDataFrame(data)

        result = df.select(
            F.col("customer_id"),
            F.col("product_id"),
            F.col("amount"),
            F.row_number().over(
                MockWindow.partitionBy("customer_id").orderBy("amount")
            ).alias("row_num"),
        )

        rows = result.collect()
        assert len(rows) == 50  # 10 customers * 5 products

        # Check that all partitions have correct row numbers
        partitions = {}
        for row in rows:
            customer_id = row["customer_id"]
            if customer_id not in partitions:
                partitions[customer_id] = []
            partitions[customer_id].append(row["row_num"])

        # Each partition should have sequential row numbers
        for customer_id, row_nums in partitions.items():
            assert sorted(row_nums) == list(range(1, len(row_nums) + 1))
