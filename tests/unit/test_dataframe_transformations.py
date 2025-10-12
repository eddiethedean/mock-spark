"""Unit tests for DataFrame transformation operations."""

import pytest
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_transformations")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_distinct(spark):
    """Test distinct operation removes duplicates."""
    data = [{"id": 1}, {"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    result = df.distinct()
    assert result.count() == 2


def test_distinct_multiple_columns(spark):
    """Test distinct with multiple columns."""
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
    df = spark.createDataFrame(data)
    
    result = df.distinct()
    assert result.count() == 2


def test_dropDuplicates(spark):
    """Test dropDuplicates (alias for distinct)."""
    data = [{"id": 1}, {"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    result = df.dropDuplicates()
    assert result.count() == 2


def test_dropDuplicates_subset(spark):
    """Test dropDuplicates on subset of columns."""
    data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 1, "name": "Bob", "age": 30},
        {"id": 2, "name": "Charlie", "age": 35}
    ]
    df = spark.createDataFrame(data)
    
    result = df.dropDuplicates(["id"])
    assert result.count() == 2


def test_drop_duplicates_alias(spark):
    """Test drop_duplicates (snake_case alias)."""
    data = [{"id": 1}, {"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    result = df.drop_duplicates()
    assert result.count() == 2


def test_sample_without_replacement(spark):
    """Test sample without replacement."""
    data = [{"id": i} for i in range(100)]
    df = spark.createDataFrame(data)
    
    result = df.sample(fraction=0.1, seed=42)
    # Should return approximately 10% of rows
    assert result.count() < 100


def test_sample_with_replacement(spark):
    """Test sample with replacement."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    result = df.sample(withReplacement=True, fraction=0.5, seed=42)
    assert result.count() <= 10


def test_sample_with_seed(spark):
    """Test sample produces consistent results with seed."""
    data = [{"id": i} for i in range(100)]
    df = spark.createDataFrame(data)
    
    result1 = df.sample(fraction=0.3, seed=42)
    result2 = df.sample(fraction=0.3, seed=42)
    
    # Same seed should produce same count
    assert result1.count() == result2.count()


def test_randomSplit(spark):
    """Test randomSplit divides DataFrame."""
    data = [{"id": i} for i in range(100)]
    df = spark.createDataFrame(data)
    
    splits = df.randomSplit([0.7, 0.3], seed=42)
    assert len(splits) == 2
    assert splits[0].count() + splits[1].count() == 100


def test_randomSplit_equal_weights(spark):
    """Test randomSplit with equal weights."""
    data = [{"id": i} for i in range(50)]
    df = spark.createDataFrame(data)
    
    splits = df.randomSplit([0.5, 0.5], seed=42)
    assert len(splits) == 2


def test_orderBy_ascending(spark):
    """Test orderBy with ascending order."""
    data = [{"id": 3}, {"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    result = df.orderBy("id")
    rows = result.collect()
    assert rows[0]["id"] == 1
    assert rows[1]["id"] == 2
    assert rows[2]["id"] == 3


def test_orderBy_descending(spark):
    """Test orderBy with descending order."""
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    df = spark.createDataFrame(data)
    
    result = df.orderBy(F.col("id").desc())
    rows = result.collect()
    assert rows[0]["id"] == 3


def test_orderBy_multiple_columns(spark):
    """Test orderBy with multiple columns."""
    data = [
        {"category": "A", "value": 2},
        {"category": "A", "value": 1},
        {"category": "B", "value": 3}
    ]
    df = spark.createDataFrame(data)
    
    result = df.orderBy("category", "value")
    rows = result.collect()
    assert rows[0]["value"] == 1


def test_sort_alias(spark):
    """Test sort (alias for orderBy)."""
    data = [{"id": 3}, {"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    result = df.sort("id")
    rows = result.collect()
    assert rows[0]["id"] == 1


def test_sortWithinPartitions(spark):
    """Test sortWithinPartitions."""
    data = [{"id": 3}, {"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data)
    
    result = df.sortWithinPartitions("id")
    assert result.count() == 3


def test_repartition(spark):
    """Test repartition."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    result = df.repartition(4)
    assert result.count() == 10


def test_repartition_by_column(spark):
    """Test repartition by column."""
    data = [{"category": "A"}, {"category": "B"}]
    df = spark.createDataFrame(data)
    
    result = df.repartition("category")
    assert result.count() == 2


def test_repartition_with_num_and_column(spark):
    """Test repartition with number and column."""
    data = [{"category": "A"}, {"category": "B"}]
    df = spark.createDataFrame(data)
    
    result = df.repartition(2, "category")
    assert result.count() == 2


def test_coalesce(spark):
    """Test coalesce to reduce partitions."""
    data = [{"id": i} for i in range(10)]
    df = spark.createDataFrame(data)
    
    result = df.coalesce(1)
    assert result.count() == 10


def test_fillna_all_columns(spark):
    """Test fillna for all columns."""
    data = [{"name": "Alice", "age": None}, {"name": None, "age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.fillna(0)
    rows = result.collect()
    # Numeric columns should be filled
    assert rows[0]["age"] == 0


def test_fillna_specific_columns(spark):
    """Test fillna for specific columns."""
    data = [{"name": None, "age": None}]
    df = spark.createDataFrame(data)
    
    result = df.fillna({"name": "Unknown", "age": 0})
    rows = result.collect()
    assert rows[0]["name"] == "Unknown"
    assert rows[0]["age"] == 0


def test_na_fill_alias(spark):
    """Test na.fill (alias for fillna)."""
    data = [{"value": None}]
    df = spark.createDataFrame(data)
    
    result = df.na.fill(0)
    assert result.count() == 1


def test_dropna_default(spark):
    """Test dropna with default parameters."""
    data = [{"name": "Alice", "age": 25}, {"name": None, "age": 30}]
    df = spark.createDataFrame(data)
    
    result = df.dropna()
    # Should drop rows with any null
    assert result.count() == 1


def test_dropna_how_all(spark):
    """Test dropna with how='all'."""
    data = [
        {"name": "Alice", "age": 25},
        {"name": None, "age": None}
    ]
    df = spark.createDataFrame(data)
    
    result = df.dropna(how="all")
    # Should only drop rows where all values are null
    assert result.count() == 1


def test_dropna_subset(spark):
    """Test dropna on subset of columns."""
    data = [
        {"name": "Alice", "age": None, "city": "NYC"},
        {"name": None, "age": 30, "city": "LA"}
    ]
    df = spark.createDataFrame(data)
    
    result = df.dropna(subset=["name"])
    assert result.count() == 1


def test_na_drop_alias(spark):
    """Test na.drop (alias for dropna)."""
    data = [{"name": None}, {"name": "Alice"}]
    df = spark.createDataFrame(data)
    
    result = df.na.drop()
    assert result.count() == 1


def test_replace(spark):
    """Test replace to substitute values."""
    data = [{"status": "active"}, {"status": "inactive"}]
    df = spark.createDataFrame(data)
    
    result = df.replace("active", "ACTIVE")
    rows = result.collect()
    assert rows[0]["status"] == "ACTIVE"


def test_replace_with_dict(spark):
    """Test replace with dictionary mapping."""
    data = [{"status": "A"}, {"status": "B"}]
    df = spark.createDataFrame(data)
    
    result = df.replace({"A": "Active", "B": "Blocked"})
    rows = result.collect()
    assert rows[0]["status"] == "Active"


def test_replace_subset_columns(spark):
    """Test replace on subset of columns."""
    data = [{"col1": "X", "col2": "X"}]
    df = spark.createDataFrame(data)
    
    result = df.replace("X", "Y", subset=["col1"])
    rows = result.collect()
    assert rows[0]["col1"] == "Y"
    assert rows[0]["col2"] == "X"


def test_na_replace_alias(spark):
    """Test na.replace (alias for replace)."""
    data = [{"value": 1}]
    df = spark.createDataFrame(data)
    
    result = df.na.replace(1, 2)
    rows = result.collect()
    assert rows[0]["value"] == 2


def test_transform_custom_function(spark):
    """Test transform with custom function."""
    data = [{"value": 10}]
    df = spark.createDataFrame(data)
    
    def add_column(df):
        return df.withColumn("doubled", F.col("value") * 2)
    
    result = df.transform(add_column)
    assert "doubled" in result.columns

