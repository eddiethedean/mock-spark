"""
Aggregation operations for MockDataFrame.
"""

from typing import Any, Dict, List, Optional, Union


class AggregationOperations:
    """Handles aggregation operations for DataFrames."""
    
    def __init__(self, dataframe):
        """Initialize with a reference to the DataFrame."""
        self.df = dataframe
    
    def count(self) -> int:
        """Count the number of rows in the DataFrame."""
        return len(self.df.data)
    
    def distinct(self) -> "MockDataFrame":
        """Return distinct rows."""
        from mock_spark.dataframe import MockDataFrame
        
        seen = set()
        distinct_data = []
        for row in self.df.data:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple not in seen:
                seen.add(row_tuple)
                distinct_data.append(row)
        
        return MockDataFrame(distinct_data, self.df.schema, self.df.storage)
    
    def limit(self, n: int) -> "MockDataFrame":
        """Limit the number of rows."""
        from mock_spark.dataframe import MockDataFrame
        
        limited_data = self.df.data[:n]
        return MockDataFrame(limited_data, self.df.schema, self.df.storage)
