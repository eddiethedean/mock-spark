"""
Grouping operations for MockDataFrame.
"""

from typing import Any, Dict, List, Optional, Union
from mock_spark.functions import MockColumn, MockColumnOperation
from mock_spark.errors import raise_column_not_found


class GroupingOperations:
    """Handles grouping operations for DataFrames."""
    
    def __init__(self, dataframe):
        """Initialize with a reference to the DataFrame."""
        self.df = dataframe
    
    def groupBy(self, *columns: Union[str, MockColumn]) -> "MockGroupedData":
        """Group by columns."""
        col_names = []
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
            else:
                col_names.append(col)
        
        # Validate that all columns exist
        for col_name in col_names:
            if col_name not in [field.name for field in self.df.schema.fields]:
                raise_column_not_found(col_name)
        
        from mock_spark.dataframe import MockGroupedData
        return MockGroupedData(self.df, col_names)
