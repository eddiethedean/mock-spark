"""Tests for SchemaManager class."""

from mock_spark.dataframe.schema.schema_manager import SchemaManager
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType,
    MapType,
)
from mock_spark.functions import MockColumn, MockLiteral


class TestSchemaManager:
    """Test cases for SchemaManager class."""

    def test_parse_cast_type_string_primitive_types(self):
        """Test parsing primitive type strings."""
        assert isinstance(SchemaManager.parse_cast_type_string("int"), IntegerType)
        assert isinstance(SchemaManager.parse_cast_type_string("integer"), IntegerType)
        assert isinstance(SchemaManager.parse_cast_type_string("long"), LongType)
        assert isinstance(SchemaManager.parse_cast_type_string("bigint"), LongType)
        assert isinstance(SchemaManager.parse_cast_type_string("double"), DoubleType)
        assert isinstance(SchemaManager.parse_cast_type_string("float"), DoubleType)
        assert isinstance(SchemaManager.parse_cast_type_string("string"), StringType)
        assert isinstance(SchemaManager.parse_cast_type_string("varchar"), StringType)
        assert isinstance(SchemaManager.parse_cast_type_string("boolean"), BooleanType)
        assert isinstance(SchemaManager.parse_cast_type_string("bool"), BooleanType)
        assert isinstance(SchemaManager.parse_cast_type_string("date"), DateType)
        assert isinstance(
            SchemaManager.parse_cast_type_string("timestamp"), TimestampType
        )

    def test_parse_cast_type_string_decimal(self):
        """Test parsing decimal type strings."""
        decimal_type = SchemaManager.parse_cast_type_string("decimal(10,2)")
        assert isinstance(decimal_type, DecimalType)
        assert decimal_type.precision == 10
        assert decimal_type.scale == 2

        # Test default decimal
        default_decimal = SchemaManager.parse_cast_type_string("decimal")
        assert isinstance(default_decimal, DecimalType)
        assert default_decimal.precision == 10
        assert default_decimal.scale == 2

    def test_parse_cast_type_string_array(self):
        """Test parsing array type strings."""
        array_type = SchemaManager.parse_cast_type_string("array<int>")
        assert isinstance(array_type, ArrayType)
        assert isinstance(array_type.element_type, IntegerType)

        # Nested array
        nested_array = SchemaManager.parse_cast_type_string("array<array<string>>")
        assert isinstance(nested_array, ArrayType)
        assert isinstance(nested_array.element_type, ArrayType)
        assert isinstance(nested_array.element_type.element_type, StringType)

    def test_parse_cast_type_string_map(self):
        """Test parsing map type strings."""
        map_type = SchemaManager.parse_cast_type_string("map<string,int>")
        assert isinstance(map_type, MapType)
        assert isinstance(map_type.key_type, StringType)
        assert isinstance(map_type.value_type, IntegerType)

        # Complex map
        complex_map = SchemaManager.parse_cast_type_string("map<string,array<int>>")
        assert isinstance(complex_map, MapType)
        assert isinstance(complex_map.key_type, StringType)
        assert isinstance(complex_map.value_type, ArrayType)
        assert isinstance(complex_map.value_type.element_type, IntegerType)

    def test_parse_cast_type_string_unknown(self):
        """Test parsing unknown type strings defaults to StringType."""
        unknown_type = SchemaManager.parse_cast_type_string("unknown_type")
        assert isinstance(unknown_type, StringType)

    def test_project_schema_with_operations_filter(self):
        """Test schema projection with filter operation (no schema change)."""
        base_schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )
        operations = [("filter", MockColumn("age") > 25)]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        assert result_schema == base_schema

    def test_project_schema_with_operations_select_all(self):
        """Test schema projection with select * operation."""
        base_schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )
        operations = [("select", ("*",))]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        assert result_schema == base_schema

    def test_project_schema_with_operations_select_columns(self):
        """Test schema projection with select specific columns."""
        base_schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("city", StringType()),
            ]
        )
        operations = [("select", ("name", "age"))]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        expected_schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )
        assert result_schema == expected_schema

    def test_project_schema_with_operations_withcolumn_cast(self):
        """Test schema projection with withColumn cast operation."""
        base_schema = MockStructType([MockStructField("age", StringType())])
        operations = [("withColumn", ("age_int", MockColumn("age").cast("int")))]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        expected_schema = MockStructType(
            [
                MockStructField("age", StringType()),
                MockStructField("age_int", IntegerType()),
            ]
        )
        assert result_schema == expected_schema

    def test_project_schema_with_operations_withcolumn_arithmetic(self):
        """Test schema projection with withColumn arithmetic operation."""
        base_schema = MockStructType([MockStructField("age", IntegerType())])
        # Create a mock arithmetic operation
        arithmetic_op = MockColumn("age")
        arithmetic_op.operation = "+"
        arithmetic_op.value = MockLiteral(5, IntegerType())
        operations = [("withColumn", ("age_plus_5", arithmetic_op))]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        expected_schema = MockStructType(
            [
                MockStructField("age", IntegerType()),
                MockStructField("age_plus_5", LongType()),
            ]
        )
        assert result_schema == expected_schema

    def test_project_schema_with_operations_withcolumn_string_function(self):
        """Test schema projection with withColumn string function."""
        base_schema = MockStructType([MockStructField("name", StringType())])
        # Create a mock string function operation
        string_op = MockColumn("name")
        string_op.operation = "upper"
        operations = [("withColumn", ("name_upper", string_op))]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        expected_schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("name_upper", StringType()),
            ]
        )
        assert result_schema == expected_schema

    def test_project_schema_with_operations_withcolumn_datetime_function(self):
        """Test schema projection with withColumn datetime function."""
        base_schema = MockStructType([MockStructField("timestamp", TimestampType())])
        # Create a mock datetime function operation
        datetime_op = MockColumn("timestamp")
        datetime_op.operation = "hour"
        operations = [("withColumn", ("hour", datetime_op))]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        expected_schema = MockStructType(
            [
                MockStructField("timestamp", TimestampType()),
                MockStructField("hour", IntegerType()),
            ]
        )
        assert result_schema == expected_schema

    def test_project_schema_with_operations_withcolumn_literal(self):
        """Test schema projection with withColumn literal value."""
        base_schema = MockStructType([MockStructField("name", StringType())])
        literal = MockLiteral("constant", StringType())
        operations = [("withColumn", ("constant_col", literal))]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        expected_schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField(
                    "constant_col", StringType(nullable=False), nullable=False
                ),
            ]
        )
        assert result_schema == expected_schema

    def test_project_schema_with_operations_join(self):
        """Test schema projection with join operation."""
        base_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )

        # Mock other DataFrame
        class MockDataFrame:
            def __init__(self):
                self.schema = MockStructType(
                    [
                        MockStructField("id", IntegerType()),
                        MockStructField("city", StringType()),
                    ]
                )

        other_df = MockDataFrame()
        operations = [("join", (other_df, "id", "inner"))]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        expected_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("right_id", IntegerType()),
                MockStructField("city", StringType()),
            ]
        )
        assert result_schema == expected_schema

    def test_project_schema_with_operations_join_with_conflict(self):
        """Test schema projection with join operation that has field name conflicts."""
        base_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
            ]
        )

        # Mock other DataFrame with conflicting field name
        class MockDataFrame:
            def __init__(self):
                self.schema = MockStructType(
                    [
                        MockStructField("id", IntegerType()),
                        MockStructField("name", StringType()),
                    ]
                )

        other_df = MockDataFrame()
        operations = [("join", (other_df, "id", "inner"))]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        # Should have original fields plus prefixed conflicting fields
        field_names = [field.name for field in result_schema.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "right_id" in field_names
        assert "right_name" in field_names

    def test_project_schema_with_operations_multiple_operations(self):
        """Test schema projection with multiple operations."""
        base_schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("city", StringType()),
            ]
        )

        # Create operations: select name and age, then add a new column
        string_op = MockColumn("name")
        string_op.operation = "upper"
        operations = [
            ("select", ("name", "age")),
            ("withColumn", ("name_upper", string_op)),
        ]

        result_schema = SchemaManager.project_schema_with_operations(
            base_schema, operations
        )

        expected_schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("name_upper", StringType()),
            ]
        )
        assert result_schema == expected_schema

    def test_infer_expression_type_datediff(self):
        """Test type inference for datediff expression."""
        col = MockColumn("timestamp")
        col.operation = "datediff"

        field = SchemaManager._infer_expression_type(col)
        assert field.name == "timestamp"
        assert isinstance(field.dataType, IntegerType)

    def test_infer_expression_type_months_between(self):
        """Test type inference for months_between expression."""
        col = MockColumn("date1")
        col.operation = "months_between"

        field = SchemaManager._infer_expression_type(col)
        assert field.name == "date1"
        assert isinstance(field.dataType, DoubleType)

    def test_infer_expression_type_datetime_function(self):
        """Test type inference for datetime function expression."""
        col = MockColumn("timestamp")
        col.operation = "hour"

        field = SchemaManager._infer_expression_type(col)
        assert field.name == "timestamp"
        assert isinstance(field.dataType, IntegerType)

    def test_infer_expression_type_unknown_operation(self):
        """Test type inference for unknown operation defaults to StringType."""
        col = MockColumn("value")
        col.operation = "unknown_function"

        field = SchemaManager._infer_expression_type(col)
        assert field.name == "value"
        assert isinstance(field.dataType, StringType)

    def test_infer_arithmetic_type_double_operand(self):
        """Test arithmetic type inference with double operand."""
        base_schema = MockStructType(
            [
                MockStructField("age", DoubleType()),
                MockStructField("bonus", IntegerType()),
            ]
        )

        # Create arithmetic operation
        col = MockColumn("age")
        col.operation = "+"
        col.value = MockColumn("bonus")

        data_type = SchemaManager._infer_arithmetic_type(col, base_schema)
        assert isinstance(data_type, DoubleType)

    def test_infer_arithmetic_type_integer_operands(self):
        """Test arithmetic type inference with integer operands."""
        base_schema = MockStructType(
            [
                MockStructField("age", IntegerType()),
                MockStructField("bonus", IntegerType()),
            ]
        )

        # Create arithmetic operation
        col = MockColumn("age")
        col.operation = "+"
        col.value = MockColumn("bonus")

        data_type = SchemaManager._infer_arithmetic_type(col, base_schema)
        assert isinstance(data_type, LongType)

    def test_infer_round_type_cast_to_int(self):
        """Test round type inference for cast to int."""
        col = MockColumn("value")
        col.operation = "round"
        col.column = MockColumn("value")
        col.column.operation = "cast"
        col.column.value = "int"

        data_type = SchemaManager._infer_round_type(col)
        assert isinstance(data_type, LongType)

    def test_infer_round_type_default(self):
        """Test round type inference defaults to DoubleType."""
        col = MockColumn("value")
        col.operation = "round"
        col.column = MockColumn("value")

        data_type = SchemaManager._infer_round_type(col)
        assert isinstance(data_type, DoubleType)

    def test_infer_literal_type_integer(self):
        """Test literal type inference for integer."""
        col = 42

        data_type = SchemaManager._infer_literal_type(col)
        assert isinstance(data_type, LongType)

    def test_infer_literal_type_float(self):
        """Test literal type inference for float."""
        col = 3.14

        data_type = SchemaManager._infer_literal_type(col)
        assert isinstance(data_type, DoubleType)

    def test_infer_literal_type_string(self):
        """Test literal type inference for string."""
        col = "hello"

        data_type = SchemaManager._infer_literal_type(col)
        assert isinstance(data_type, StringType)
