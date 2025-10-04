"""
Ordering operations for MockDataFrame.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from mock_spark.functions import MockColumn, MockColumnOperation


class OrderingOperations:
    """Handles ordering operations for DataFrames."""
    
    def __init__(self, dataframe):
        """Initialize with a reference to the DataFrame."""
        self.df = dataframe
    
    def orderBy(self, *columns: Union[str, MockColumn]) -> "MockDataFrame":
        """Order by columns."""
        col_names: List[str] = []
        sort_orders: List[bool] = []
        
        for col in columns:
            if isinstance(col, MockColumn):
                col_names.append(col.name)
                sort_orders.append(True)  # Default ascending
            elif hasattr(col, "operation") and hasattr(col, "column"):
                # Handle MockColumnOperation (e.g., col.desc())
                if col.operation == "desc":
                    col_names.append(col.column.name)
                    sort_orders.append(False)  # Descending
                elif col.operation == "asc":
                    col_names.append(col.column.name)
                    sort_orders.append(True)  # Ascending
                else:
                    col_names.append(col.column.name)
                    sort_orders.append(True)  # Default ascending
            else:
                col_names.append(col)
                sort_orders.append(True)  # Default ascending
        
        # Sort data by columns with proper ordering
        def sort_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
            key_values = []
            for col_name in col_names:
                value = row.get(col_name)
                # Handle None values - put them at the end for ascending, beginning for descending
                if value is None:
                    # Use a special value that sorts appropriately
                    key_values.append(float('inf') if sort_orders[len(key_values)] else float('-inf'))
                else:
                    key_values.append(value)
            return tuple(key_values)
        
        # Sort the data
        sorted_data = sorted(self.df.data, key=sort_key, reverse=False)
        
        from mock_spark.dataframe import MockDataFrame
        return MockDataFrame(sorted_data, self.df.schema, self.df.storage)
