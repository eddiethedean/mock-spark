"""XML functions for PySpark 3.2+ compatibility."""

from typing import Union
from mock_spark.functions.core.column import MockColumn
from mock_spark.functions.base import MockColumnOperation


class XMLFunctions:
    """XML parsing and manipulation functions."""

    @staticmethod
    def from_xml(col: Union[MockColumn, str], schema: str) -> MockColumnOperation:
        """Parse XML string to struct based on schema.

        Args:
            col: Column containing XML strings.
            schema: Schema definition string.

        Returns:
            MockColumnOperation representing the from_xml function.

        Example:
            >>> df.select(F.from_xml(F.col("xml"), "name STRING, age INT"))
        """
        if isinstance(col, str):
            col = MockColumn(col)

        return MockColumnOperation(
            col,
            "from_xml",
            schema,
            name=f"from_xml({col.name}, '{schema}')",
        )

    @staticmethod
    def to_xml(col: Union[MockColumn, MockColumnOperation]) -> MockColumnOperation:
        """Convert struct column to XML string.

        Args:
            col: Struct column to convert.

        Returns:
            MockColumnOperation representing the to_xml function.

        Example:
            >>> df.select(F.to_xml(F.struct(F.col("name"), F.col("age"))))
        """
        if isinstance(col, str):
            col = MockColumn(col)

        return MockColumnOperation(
            col if isinstance(col, MockColumn) else col.column,
            "to_xml",
            col if isinstance(col, MockColumnOperation) else None,
            name=f"to_xml({getattr(col, 'name', 'struct')})",
        )

    @staticmethod
    def schema_of_xml(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Infer schema from XML string.

        Args:
            col: Column containing XML strings.

        Returns:
            MockColumnOperation representing the schema_of_xml function.

        Example:
            >>> df.select(F.schema_of_xml(F.col("xml")))
        """
        if isinstance(col, str):
            col = MockColumn(col)

        return MockColumnOperation(
            col,
            "schema_of_xml",
            name=f"schema_of_xml({col.name})",
        )

    @staticmethod
    def xpath(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract array of values from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            MockColumnOperation representing the xpath function.

        Example:
            >>> df.select(F.xpath(F.col("xml"), "/root/item"))
        """
        if isinstance(xml, str):
            xml = MockColumn(xml)

        return MockColumnOperation(
            xml,
            "xpath",
            path,
            name=f"xpath({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_boolean(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Evaluate XPath expression to boolean.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            MockColumnOperation representing the xpath_boolean function.

        Example:
            >>> df.select(F.xpath_boolean(F.col("xml"), "/root/active='true'"))
        """
        if isinstance(xml, str):
            xml = MockColumn(xml)

        return MockColumnOperation(
            xml,
            "xpath_boolean",
            path,
            name=f"xpath_boolean({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_double(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract double value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            MockColumnOperation representing the xpath_double function.

        Example:
            >>> df.select(F.xpath_double(F.col("xml"), "/root/value"))
        """
        if isinstance(xml, str):
            xml = MockColumn(xml)

        return MockColumnOperation(
            xml,
            "xpath_double",
            path,
            name=f"xpath_double({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_float(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract float value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            MockColumnOperation representing the xpath_float function.

        Example:
            >>> df.select(F.xpath_float(F.col("xml"), "/root/price"))
        """
        if isinstance(xml, str):
            xml = MockColumn(xml)

        return MockColumnOperation(
            xml,
            "xpath_float",
            path,
            name=f"xpath_float({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_int(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract integer value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            MockColumnOperation representing the xpath_int function.

        Example:
            >>> df.select(F.xpath_int(F.col("xml"), "/root/age"))
        """
        if isinstance(xml, str):
            xml = MockColumn(xml)

        return MockColumnOperation(
            xml,
            "xpath_int",
            path,
            name=f"xpath_int({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_long(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract long value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            MockColumnOperation representing the xpath_long function.

        Example:
            >>> df.select(F.xpath_long(F.col("xml"), "/root/value"))
        """
        if isinstance(xml, str):
            xml = MockColumn(xml)

        return MockColumnOperation(
            xml,
            "xpath_long",
            path,
            name=f"xpath_long({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_short(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract short value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            MockColumnOperation representing the xpath_short function.

        Example:
            >>> df.select(F.xpath_short(F.col("xml"), "/root/count"))
        """
        if isinstance(xml, str):
            xml = MockColumn(xml)

        return MockColumnOperation(
            xml,
            "xpath_short",
            path,
            name=f"xpath_short({xml.name}, '{path}')",
        )

    @staticmethod
    def xpath_string(xml: Union[MockColumn, str], path: str) -> MockColumnOperation:
        """Extract string value from XML using XPath.

        Args:
            xml: Column containing XML strings.
            path: XPath expression.

        Returns:
            MockColumnOperation representing the xpath_string function.

        Example:
            >>> df.select(F.xpath_string(F.col("xml"), "/root/name"))
        """
        if isinstance(xml, str):
            xml = MockColumn(xml)

        return MockColumnOperation(
            xml,
            "xpath_string",
            path,
            name=f"xpath_string({xml.name}, '{path}')",
        )

