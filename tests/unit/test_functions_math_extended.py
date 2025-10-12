"""Extended unit tests for math functions."""

import pytest
import math
from mock_spark import MockSparkSession, F


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_math_extended")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_abs_function(spark):
    """Test abs (absolute value) function."""
    data = [{"value": -10}, {"value": 10}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("absolute", F.abs(F.col("value")))
    rows = result.collect()
    assert rows[0]["absolute"] == 10
    assert rows[1]["absolute"] == 10


def test_sqrt_function(spark):
    """Test sqrt (square root) function."""
    data = [{"value": 16}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("square_root", F.sqrt(F.col("value")))
    rows = result.collect()
    assert rows[0]["square_root"] == 4.0


def test_pow_function(spark):
    """Test pow (power) function."""
    data = [{"base": 2, "exp": 3}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("power", F.pow(F.col("base"), F.col("exp")))
    rows = result.collect()
    assert rows[0]["power"] == 8.0


def test_round_function(spark):
    """Test round function."""
    data = [{"value": 3.14159}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("rounded", F.round(F.col("value"), 2))
    rows = result.collect()
    assert rows[0]["rounded"] == 3.14


def test_ceil_function(spark):
    """Test ceil (ceiling) function."""
    data = [{"value": 3.2}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("ceiling", F.ceil(F.col("value")))
    rows = result.collect()
    assert rows[0]["ceiling"] == 4


def test_floor_function(spark):
    """Test floor function."""
    data = [{"value": 3.8}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("floored", F.floor(F.col("value")))
    rows = result.collect()
    assert rows[0]["floored"] == 3


def test_exp_function(spark):
    """Test exp (exponential) function."""
    data = [{"value": 1}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("exponential", F.exp(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["exponential"] - math.e) < 0.0001


def test_log_function(spark):
    """Test log (natural logarithm) function."""
    data = [{"value": math.e}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("logarithm", F.log(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["logarithm"] - 1.0) < 0.0001


def test_log10_function(spark):
    """Test log10 (base 10 logarithm) function."""
    data = [{"value": 100}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("log10_val", F.log10(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["log10_val"] - 2.0) < 0.0001


def test_sin_function(spark):
    """Test sin (sine) function."""
    data = [{"value": 0}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("sine", F.sin(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["sine"]) < 0.0001


def test_cos_function(spark):
    """Test cos (cosine) function."""
    data = [{"value": 0}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("cosine", F.cos(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["cosine"] - 1.0) < 0.0001


def test_tan_function(spark):
    """Test tan (tangent) function."""
    data = [{"value": 0}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("tangent", F.tan(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["tangent"]) < 0.0001


def test_asin_function(spark):
    """Test asin (arcsine) function."""
    data = [{"value": 0}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("arcsine", F.asin(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["arcsine"]) < 0.0001


def test_acos_function(spark):
    """Test acos (arccosine) function."""
    data = [{"value": 1}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("arccosine", F.acos(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["arccosine"]) < 0.0001


def test_atan_function(spark):
    """Test atan (arctangent) function."""
    data = [{"value": 0}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("arctangent", F.atan(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["arctangent"]) < 0.0001


def test_degrees_function(spark):
    """Test degrees (radians to degrees) function."""
    data = [{"value": math.pi}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("degrees", F.degrees(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["degrees"] - 180.0) < 0.01


def test_radians_function(spark):
    """Test radians (degrees to radians) function."""
    data = [{"value": 180}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("radians", F.radians(F.col("value")))
    rows = result.collect()
    assert abs(rows[0]["radians"] - math.pi) < 0.0001


def test_signum_function(spark):
    """Test signum (sign) function."""
    data = [{"neg": -10}, {"zero": 0}, {"pos": 10}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("sign_neg", F.signum(F.col("neg"))) \
               .withColumn("sign_zero", F.signum(F.col("zero"))) \
               .withColumn("sign_pos", F.signum(F.col("pos")))
    rows = result.collect()
    assert rows[0]["sign_neg"] == -1.0
    assert rows[0]["sign_zero"] == 0.0
    assert rows[0]["sign_pos"] == 1.0


def test_factorial_function(spark):
    """Test factorial function."""
    data = [{"value": 5}]
    df = spark.createDataFrame(data)
    
    result = df.withColumn("fact", F.factorial(F.col("value")))
    rows = result.collect()
    assert rows[0]["fact"] == 120

