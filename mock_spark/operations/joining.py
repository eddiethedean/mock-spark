"""
Joining operations for MockDataFrame.
"""

from typing import Any, Dict, List, Optional, Union


class JoiningOperations:
    """Handles joining operations for DataFrames."""
    
    def __init__(self, dataframe):
        """Initialize with a reference to the DataFrame."""
        self.df = dataframe
    
    def join(
        self, other: "MockDataFrame", on: Union[str, List[str]], how: str = "inner"
    ) -> "MockDataFrame":
        """Join with another DataFrame."""
        if isinstance(on, str):
            on = [on]
        
        # Simple join implementation
        joined_data = []
        for left_row in self.df.data:
            for right_row in other.data:
                # Check if join condition matches
                if all(left_row.get(col) == right_row.get(col) for col in on):
                    joined_row = left_row.copy()
                    joined_row.update(right_row)
                    joined_data.append(joined_row)
        
        from mock_spark.dataframe import MockDataFrame
        from mock_spark.spark_types import MockStructType
        
        # Create new schema
        new_fields = self.df.schema.fields.copy()
        for field in other.schema.fields:
            if not any(f.name == field.name for f in new_fields):
                new_fields.append(field)
        
        new_schema = MockStructType(new_fields)
        return MockDataFrame(joined_data, new_schema, self.df.storage)
