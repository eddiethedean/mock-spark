import pytest

from mock_spark.sql import functions as F
from mock_spark.errors import PySparkTypeError, PySparkValueError


def test_call_function_invokes_registered_function(spark):
    df = spark.createDataFrame([{"name": "alice"}])

    result = df.select(F.call_function("upper", "name").alias("upper_name")).collect()

    assert result[0]["upper_name"] == "ALICE"


def test_call_function_propagates_type_error():
    with pytest.raises(PySparkTypeError):
        F.call_function("upper")


def test_call_function_missing_name():
    with pytest.raises(PySparkValueError):
        F.call_function("does_not_exist", "name")
