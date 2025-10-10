"""
Unit tests for complex column expressions (AND/OR combinations).

Based on exploration/complex_columns.py findings:
- Column expressions support & (AND) and | (OR) operations
- isNull() and isNotNull() can be combined with AND/OR
- Complex expressions can be used in filter() and withColumn()
- alias() is the correct method (label() is deprecated)
"""

from mock_spark import MockSparkSession
import mock_spark.functions as F


class TestComplexColumns:
    """Test complex column expressions with AND/OR operations."""

    def test_simple_and_expression(self):
        """Test simple AND expression with two conditions."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [
            {"id": 1, "value": 100, "active": True},
            {"id": 2, "value": 200, "active": False},
            {"id": 3, "value": 150, "active": True},
        ]
        df = spark.createDataFrame(data)
        
        # Filter with AND: value > 100 AND active = True
        result = df.filter((F.col("value") > 100) & (F.col("active") == True))
        
        # id=1: (100 > 100) & True = False & True = False
        # id=2: (200 > 100) & False = True & False = False  
        # id=3: (150 > 100) & True = True & True = True
        # Only id=3 matches
        assert result.count() == 1
        row = result.collect()[0]
        assert row["id"] == 3
        assert row["value"] == 150
        assert row["active"] == True
        
        spark.stop()

    def test_simple_or_expression(self):
        """Test simple OR expression with two conditions."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [
            {"id": 1, "value": 50},
            {"id": 2, "value": 150},
            {"id": 3, "value": 250},
        ]
        df = spark.createDataFrame(data)
        
        # Filter with OR: value < 100 OR value > 200
        result = df.filter((F.col("value") < 100) | (F.col("value") > 200))
        
        assert result.count() == 2
        ids = {row["id"] for row in result.collect()}
        assert ids == {1, 3}  # id=1 (50 < 100) and id=3 (250 > 200)
        
        spark.stop()

    def test_and_or_combination(self):
        """Test combination of AND and OR operations."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [
            {"id": 1, "value": 50, "category": "A"},
            {"id": 2, "value": 150, "category": "B"},
            {"id": 3, "value": 250, "category": "A"},
            {"id": 4, "value": 100, "category": "C"},
        ]
        df = spark.createDataFrame(data)
        
        # (value < 100 AND category = 'A') OR value > 200
        result = df.filter(
            ((F.col("value") < 100) & (F.col("category") == "A")) | (F.col("value") > 200)
        )
        
        assert result.count() == 2
        ids = {row["id"] for row in result.collect()}
        assert ids == {1, 3}  # id=1 (50 < 100 AND cat=A) and id=3 (250 > 200)
        
        spark.stop()

    def test_is_null_with_and(self):
        """Test isNull() combined with AND."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [
            {"id": 1, "value": 100, "optional": "data"},
            {"id": 2, "value": 200, "optional": None},
            {"id": 3, "value": 150, "optional": None},
        ]
        df = spark.createDataFrame(data)
        
        # value > 100 AND optional IS NULL
        result = df.filter((F.col("value") > 100) & F.col("optional").isNull())
        
        assert result.count() == 2  # id=2 and id=3
        ids = {row["id"] for row in result.collect()}
        assert ids == {2, 3}
        
        spark.stop()

    def test_is_not_null_with_or(self):
        """Test isNotNull() combined with OR."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [
            {"id": 1, "value": 50, "optional": "data"},
            {"id": 2, "value": 150, "optional": None},
            {"id": 3, "value": 250, "optional": None},
        ]
        df = spark.createDataFrame(data)
        
        # value < 100 OR optional IS NOT NULL
        result = df.filter((F.col("value") < 100) | F.col("optional").isNotNull())
        
        # id=1: (value=50<100) OR (optional='data' IS NOT NULL) = True OR True = True ✓
        # id=2: (value=150>100) OR (optional=NULL IS NOT NULL) = False OR False = False ✗
        # id=3: (value=250>100) OR (optional=NULL IS NOT NULL) = False OR False = False ✗
        # So only id=1 should match
        assert result.count() == 1
        
        ids = {row["id"] for row in result.collect()}
        assert ids == {1}
        
        spark.stop()

    def test_with_column_complex_expression(self):
        """Test using complex expressions in withColumn."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()
        
        data = [
            {"id": 1, "value": 100, "active": True},
            {"id": 2, "value": 200, "active": False},
            {"id": 3, "value": 50, "active": True},
        ]
        df = spark.createDataFrame(data)
        
        # Create flag column: (value > 100 AND active) OR value < 100
        result = df.withColumn(
            "flag",
            ((F.col("value") > 100) & F.col("active")) | (F.col("value") < 100)
        )
        
        assert "flag" in result.columns
        rows_dict = {row["id"]: row for row in result.collect()}
        
        # DuckDB might return boolean as string, so handle both
        def is_truthy(val):
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.lower() in ('true', '1', 't')
            return bool(val)
        
        # id=1: (100>100 AND True) OR (100<100) = False OR False = False
        assert not is_truthy(rows_dict[1]["flag"])
        
        # id=2: (200>100 AND False) OR (200<100) = False OR False = False
        assert not is_truthy(rows_dict[2]["flag"])
        
        # id=3: (50>100 AND True) OR (50<100) = False OR True = True
        assert is_truthy(rows_dict[3]["flag"])
        
        spark.stop()

