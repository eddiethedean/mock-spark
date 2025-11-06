"""Schema management and inference for DataFrame operations."""

from typing import Any, Dict, List, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ...dataframe import DataFrame

from ...spark_types import (
    StructType,
    StructField,
    DataType,
    BooleanType,
    LongType,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType,
    MapType,
)
from ...functions import Literal, Column, ColumnOperation


class SchemaManager:
    """Manages schema projection and type inference for DataFrame operations.

    This class handles:
    - Schema projection after queued lazy operations
    - Type inference for select operations
    - Type inference for withColumn operations
    - Type inference for join operations
    - Cast type string parsing
    """

    @staticmethod
    def project_schema_with_operations(
        base_schema: StructType, operations_queue: List[Tuple[str, Any]]
    ) -> StructType:
        """Compute schema after applying queued lazy operations.

        Iterates through operations queue and projects resulting schema
        without materializing data.

        Preserves base schema fields even when data is empty.
        """
        # Ensure base_schema has fields attribute
        if not hasattr(base_schema, "fields"):
            # Fallback to empty schema if fields attribute missing
            fields_map: Dict[str, StructField] = {}
        else:
            # Preserve base schema fields - this works even for empty DataFrames with schemas
            fields_map = {f.name: f for f in base_schema.fields}

        for op_name, op_val in operations_queue:
            if op_name == "filter":
                # no schema change
                continue
            elif op_name == "select":
                fields_map = SchemaManager._handle_select_operation(fields_map, op_val)
            elif op_name == "withColumn":
                col_name, col = op_val
                fields_map = SchemaManager._handle_withcolumn_operation(
                    fields_map, col_name, col, base_schema
                )
            elif op_name == "drop":
                fields_map = SchemaManager._handle_drop_operation(fields_map, op_val)
            elif op_name == "join":
                other_df, on, how = op_val
                # For joins, return immediately to preserve duplicate column names (PySpark behavior)
                # Build a list instead of dict to allow duplicates
                fields_list = list(fields_map.values())
                # Add all fields from right DataFrame (may create duplicates - that's PySpark behavior)
                for field in other_df.schema.fields:
                    fields_list.append(field)
                # For semi/anti joins, only return left DataFrame columns
                if how and how.lower() in ("semi", "anti", "left_semi", "left_anti"):
                    return StructType(list(fields_map.values()))
                return StructType(fields_list)

        return StructType(list(fields_map.values()))

    @staticmethod
    def _handle_select_operation(
        fields_map: Dict[str, StructField], columns: Tuple[Any, ...]
    ) -> Dict[str, StructField]:
        """Handle select operation schema changes."""
        new_fields_map = {}

        for col in columns:
            if isinstance(col, str):
                if col == "*":
                    # Add all existing fields
                    new_fields_map.update(fields_map)
                elif col in fields_map:
                    new_fields_map[col] = fields_map[col]
            elif hasattr(col, "name"):
                col_name = col.name
                if col_name == "*":
                    # Add all existing fields
                    new_fields_map.update(fields_map)
                elif col_name in fields_map:
                    new_fields_map[col_name] = fields_map[col_name]
                elif hasattr(col, "value") and hasattr(col, "column_type"):
                    # For Literal objects - literals are never nullable
                    new_fields_map[col_name] = SchemaManager._create_literal_field(col)
                else:
                    # New column from expression - infer type based on operation
                    new_fields_map[col_name] = SchemaManager._infer_expression_type(col)

        return new_fields_map

    @staticmethod
    def _handle_withcolumn_operation(
        fields_map: Dict[str, StructField],
        col_name: str,
        col: Union[Column, ColumnOperation, Literal, Any],
        base_schema: StructType,
    ) -> Dict[str, StructField]:
        """Handle withColumn operation schema changes."""
        if hasattr(col, "operation") and hasattr(col, "name"):
            if getattr(col, "operation", None) == "cast":
                # Cast operation - use the target data type from col.value
                cast_type = col.value
                if isinstance(cast_type, str):
                    fields_map[col_name] = StructField(
                        col_name, SchemaManager.parse_cast_type_string(cast_type)
                    )
                else:
                    # Already a DataType object
                    if isinstance(cast_type, DataType):
                        # Create a new instance with nullable=True
                        type_class = type(cast_type)
                        if type_class in (
                            StringType,
                            IntegerType,
                            LongType,
                            DoubleType,
                            BooleanType,
                            DateType,
                            TimestampType,
                        ):
                            # Simple types that take nullable parameter
                            new_type = type_class(nullable=True)
                        elif isinstance(cast_type, ArrayType):
                            new_type = ArrayType(cast_type.element_type, nullable=True)
                        elif isinstance(cast_type, MapType):
                            new_type = MapType(
                                cast_type.key_type, cast_type.value_type, nullable=True
                            )
                        else:
                            new_type = cast_type
                        fields_map[col_name] = StructField(col_name, new_type)
                    else:
                        fields_map[col_name] = StructField(col_name, cast_type)
            elif getattr(col, "operation", None) in ["+", "-", "*", "/", "%"]:
                # Arithmetic operations - infer type from operands
                data_type = SchemaManager._infer_arithmetic_type(col, base_schema)
                fields_map[col_name] = StructField(col_name, data_type)
            elif getattr(col, "operation", None) in ["abs"]:
                fields_map[col_name] = StructField(col_name, LongType())
            elif getattr(col, "operation", None) in ["length"]:
                fields_map[col_name] = StructField(col_name, IntegerType())
            elif getattr(col, "operation", None) in ["round"]:
                data_type = SchemaManager._infer_round_type(col)
                fields_map[col_name] = StructField(col_name, data_type)
            elif getattr(col, "operation", None) in ["upper", "lower"]:
                fields_map[col_name] = StructField(col_name, StringType())
            elif getattr(col, "operation", None) == "datediff":
                fields_map[col_name] = StructField(col_name, IntegerType())
            elif getattr(col, "operation", None) == "months_between":
                fields_map[col_name] = StructField(col_name, DoubleType())
            elif getattr(col, "operation", None) in [
                "hour",
                "minute",
                "second",
                "day",
                "dayofmonth",
                "month",
                "year",
                "quarter",
                "dayofweek",
                "dayofyear",
                "weekofyear",
            ]:
                fields_map[col_name] = StructField(col_name, IntegerType())
            else:
                fields_map[col_name] = StructField(col_name, StringType())
        elif hasattr(col, "value") and hasattr(col, "column_type"):
            # For Literal objects - literals are never nullable
            field = SchemaManager._create_literal_field(col)
            fields_map[col_name] = StructField(col_name, field.dataType, field.nullable)
        else:
            # fallback literal inference
            data_type = SchemaManager._infer_literal_type(col)
            fields_map[col_name] = StructField(col_name, data_type)

        return fields_map

    @staticmethod
    def _handle_drop_operation(
        fields_map: Dict[str, StructField],
        columns_to_drop: Union[str, List[str], Tuple[str, ...]],
    ) -> Dict[str, StructField]:
        """Handle drop operation schema changes.

        Args:
            fields_map: Current schema fields map
            columns_to_drop: Column name(s) to drop (string, list, or tuple)

        Returns:
            Updated fields_map with dropped columns removed
        """
        # Handle different formats for columns_to_drop
        if isinstance(columns_to_drop, str):
            # Single column name
            columns_to_drop = [columns_to_drop]
        elif isinstance(columns_to_drop, tuple):
            # Convert tuple to list
            columns_to_drop = list(columns_to_drop)

        # Remove columns from fields_map
        for col_name in columns_to_drop:
            if col_name in fields_map:
                del fields_map[col_name]

        return fields_map

    @staticmethod
    def _handle_join_operation(
        fields_map: Dict[str, StructField],
        other_df: "DataFrame",
        how: str = "inner",
    ) -> Dict[str, StructField]:
        """Handle join operation schema changes."""
        # For semi/anti joins, only return left DataFrame columns
        if how and how.lower() in ["semi", "anti", "left_semi", "left_anti"]:
            # Don't add right DataFrame fields for semi/anti joins
            return fields_map

        # Add fields from the other DataFrame to the schema
        for field in other_df.schema.fields:
            # Avoid duplicate field names
            if field.name not in fields_map:
                fields_map[field.name] = field
            else:
                # Handle field name conflicts by prefixing
                new_field = StructField(
                    f"right_{field.name}", field.dataType, field.nullable
                )
                fields_map[f"right_{field.name}"] = new_field

        return fields_map

    @staticmethod
    def _create_literal_field(col: Literal) -> StructField:
        """Create a field for a Literal object."""
        col_type = col.column_type
        if isinstance(col_type, BooleanType):
            data_type: DataType = BooleanType(nullable=False)
        elif isinstance(col_type, IntegerType):
            data_type = IntegerType(nullable=False)
        elif isinstance(col_type, LongType):
            data_type = LongType(nullable=False)
        elif isinstance(col_type, DoubleType):
            data_type = DoubleType(nullable=False)
        elif isinstance(col_type, StringType):
            data_type = StringType(nullable=False)
        else:
            # For other types, create a new instance with nullable=False
            data_type = col_type.__class__(nullable=False)

        return StructField(col.name, data_type, nullable=False)

    @staticmethod
    def _infer_expression_type(
        col: Union[Column, ColumnOperation, Literal, Any],
    ) -> StructField:
        """Infer type for an expression column."""
        if hasattr(col, "operation"):
            operation = getattr(col, "operation", None)
            if operation == "datediff":
                return StructField(col.name, IntegerType())
            elif operation == "months_between":
                return StructField(col.name, DoubleType())
            elif operation in [
                "hour",
                "minute",
                "second",
                "day",
                "dayofmonth",
                "month",
                "year",
                "quarter",
                "dayofweek",
                "dayofyear",
                "weekofyear",
            ]:
                return StructField(col.name, IntegerType())
            else:
                # Default to StringType for unknown operations
                return StructField(col.name, StringType())
        else:
            # No operation attribute - default to StringType
            return StructField(col.name, StringType())

    @staticmethod
    def _infer_arithmetic_type(
        col: Union[Column, ColumnOperation, Any], base_schema: StructType
    ) -> DataType:
        """Infer type for arithmetic operations."""
        left_type = None
        right_type = None

        # Get left operand type (the column itself)
        if hasattr(col, "name"):
            for field in base_schema.fields:
                if field.name == col.name:
                    left_type = field.dataType
                    break

        # Get right operand type
        if (
            hasattr(col, "value")
            and col.value is not None
            and hasattr(col.value, "name")
        ):
            for field in base_schema.fields:
                if field.name == col.value.name:
                    right_type = field.dataType
                    break

        # If either operand is DoubleType, result is DoubleType
        if (left_type and isinstance(left_type, DoubleType)) or (
            right_type and isinstance(right_type, DoubleType)
        ):
            return DoubleType()
        else:
            return LongType()

    @staticmethod
    def _infer_round_type(
        col: Union[Column, ColumnOperation, Any],
    ) -> DataType:
        """Infer type for round operation."""
        # round() should return the same type as its input
        if hasattr(col.column, "operation") and col.column.operation == "cast":
            # If the input is a cast operation, check the target type
            cast_type = getattr(col.column, "value", "string")
            if isinstance(cast_type, str) and cast_type.lower() in ["int", "integer"]:
                return LongType()
            else:
                return DoubleType()
        else:
            # Default to DoubleType for other cases
            return DoubleType()

    @staticmethod
    def _infer_literal_type(
        col: Union[Literal, int, float, str, bool, Any],
    ) -> DataType:
        """Infer type for literal values."""
        if isinstance(col, (int, float)):
            if isinstance(col, float):
                return DoubleType()
            else:
                return LongType()
        else:
            return StringType()

    @staticmethod
    def parse_cast_type_string(type_str: str) -> DataType:
        """Parse a cast type string to DataType."""
        type_str = type_str.strip().lower()

        # Primitive types
        if type_str in ["int", "integer"]:
            return IntegerType()
        elif type_str in ["long", "bigint"]:
            return LongType()
        elif type_str in ["double", "float"]:
            return DoubleType()
        elif type_str in ["string", "varchar"]:
            return StringType()
        elif type_str in ["boolean", "bool"]:
            return BooleanType()
        elif type_str == "date":
            return DateType()
        elif type_str == "timestamp":
            return TimestampType()
        elif type_str.startswith("decimal"):
            import re

            match = re.match(r"decimal\((\d+),(\d+)\)", type_str)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return DecimalType(precision, scale)
            return DecimalType(10, 2)
        elif type_str.startswith("array<"):
            element_type_str = type_str[6:-1]
            return ArrayType(SchemaManager.parse_cast_type_string(element_type_str))
        elif type_str.startswith("map<"):
            types = type_str[4:-1].split(",", 1)
            key_type = SchemaManager.parse_cast_type_string(types[0].strip())
            value_type = SchemaManager.parse_cast_type_string(types[1].strip())
            return MapType(key_type, value_type)
        else:
            return StringType()  # Default fallback
