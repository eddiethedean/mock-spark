"""
DataFrame factory service for MockSparkSession.

This service handles DataFrame creation, schema inference, and validation
following the Single Responsibility Principle.
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
)
from mock_spark.dataframe import MockDataFrame
from mock_spark.session.config import MockSparkConfig
from mock_spark.core.exceptions import IllegalArgumentException


class DataFrameFactory:
    """Factory for creating DataFrames with validation and coercion."""

    def create_dataframe(
        self,
        data: Union[List[Dict[str, Any]], List[Any]],
        schema: Optional[Union[MockStructType, List[str], str]],
        engine_config: MockSparkConfig,
        storage: Any,
    ) -> MockDataFrame:
        """Create a DataFrame from data.

        Args:
            data: List of dictionaries or tuples representing rows.
            schema: Optional schema definition (MockStructType or list of column names).
            engine_config: Engine configuration for validation and coercion.
            storage: Storage manager for the DataFrame.

        Returns:
            MockDataFrame instance with the specified data and schema.

        Raises:
            IllegalArgumentException: If data is not in the expected format.

        Example:
            >>> data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
            >>> df = factory.create_dataframe(data, None, config, storage)
        """
        if not isinstance(data, list):
            raise IllegalArgumentException(
                "Data must be a list of dictionaries or tuples"
            )

        # Handle DDL schema strings
        if isinstance(schema, str):
            from mock_spark.core.ddl_adapter import parse_ddl_schema

            schema = parse_ddl_schema(schema)

        # Handle list of column names as schema
        if isinstance(schema, list):
            # Convert tuples to dictionaries using provided column names first
            if data and isinstance(data[0], tuple):
                reordered_data = []
                column_names = schema
                for row in data:
                    if isinstance(row, tuple):
                        row_dict = {column_names[i]: row[i] for i in range(len(row))}
                        reordered_data.append(row_dict)
                    else:
                        reordered_data.append(row)
                data = reordered_data

                # Now infer schema from the converted data
                from mock_spark.core.schema_inference import SchemaInferenceEngine

                schema, data = SchemaInferenceEngine.infer_from_data(data)
            else:
                # For non-tuple data with column names, use StringType as default
                fields = [MockStructField(name, StringType()) for name in schema]
                schema = MockStructType(fields)

        if schema is None:
            # Infer schema from data using SchemaInferenceEngine
            if not data:
                # For empty dataset, create empty schema
                schema = MockStructType([])
            else:
                # Check if data is in expected format
                sample_row = data[0]
                if not isinstance(sample_row, (dict, tuple)):
                    raise IllegalArgumentException(
                        "Data must be a list of dictionaries or tuples"
                    )

                if isinstance(sample_row, dict):
                    # Use SchemaInferenceEngine for dictionary data
                    from mock_spark.core.schema_inference import SchemaInferenceEngine

                    schema, data = SchemaInferenceEngine.infer_from_data(data)
                elif isinstance(sample_row, tuple):
                    # For tuples, we need column names - this should have been handled earlier
                    # If we get here, it's an error
                    raise IllegalArgumentException(
                        "Cannot infer schema from tuples without column names. "
                        "Please provide schema or use list of column names."
                    )

        # Apply validation and optional type coercion per mode
        # Note: When an explicit schema is provided with empty data, we still need to preserve
        # the schema even though validation is skipped (since there's no data to validate)
        if isinstance(schema, MockStructType) and data:
            from mock_spark.core.data_validation import DataValidator

            validator = DataValidator(
                schema,
                validation_mode=engine_config.validation_mode,
                enable_coercion=engine_config.enable_type_coercion,
            )

            # Validate if in strict mode
            if engine_config.validation_mode == "strict":
                validator.validate(data)

            # Coerce if enabled
            if engine_config.enable_type_coercion:
                data = validator.coerce(data)

        # Ensure schema is always MockStructType at this point
        # IMPORTANT: When explicit schema is provided with empty data, preserve it!
        if not isinstance(schema, MockStructType):
            # This should never happen, but provide a fallback
            schema = MockStructType([])

        # Validate that schema is properly initialized with fields attribute
        # This ensures empty DataFrames with explicit schemas preserve column information
        if isinstance(schema, MockStructType):
            # Ensure fields attribute exists (should always exist for MockStructType)
            if not hasattr(schema, "fields"):
                # This shouldn't happen, but handle edge case
                schema = MockStructType([])
            # fields can be empty list, but that's valid for empty schemas
            # If schema was provided explicitly, trust it even if fields is empty

        return MockDataFrame(data, schema, storage)

    def _handle_schema_inference(
        self, data: List[Dict[str, Any]], schema: Optional[Any]
    ) -> Tuple[MockStructType, List[Dict[str, Any]]]:
        """Handle schema inference or conversion.

        Args:
            data: List of dictionaries representing rows.
            schema: Optional schema definition.

        Returns:
            Tuple of (inferred_schema, normalized_data).
        """
        if schema is None:
            from mock_spark.core.schema_inference import SchemaInferenceEngine

            return SchemaInferenceEngine.infer_from_data(data)
        else:
            # Schema provided, return as-is
            return schema, data

    def _apply_validation_and_coercion(
        self,
        data: List[Dict[str, Any]],
        schema: MockStructType,
        engine_config: MockSparkConfig,
    ) -> List[Dict[str, Any]]:
        """Apply validation and type coercion.

        Args:
            data: List of dictionaries representing rows.
            schema: Schema to validate against.
            engine_config: Engine configuration.

        Returns:
            Validated and coerced data.
        """
        from mock_spark.core.data_validation import DataValidator

        validator = DataValidator(
            schema,
            validation_mode=engine_config.validation_mode,
            enable_coercion=engine_config.enable_type_coercion,
        )

        # Validate if in strict mode
        if engine_config.validation_mode == "strict":
            validator.validate(data)

        # Coerce if enabled
        if engine_config.enable_type_coercion:
            data = validator.coerce(data)

        return data
