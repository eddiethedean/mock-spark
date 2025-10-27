"""
Unit tests for XML functions.
"""

import pytest
from mock_spark import F
from mock_spark.functions.xml import XMLFunctions


@pytest.mark.unit
class TestXMLFunctions:
    """Test XML parsing and manipulation functions."""

    def test_from_xml_with_schema(self):
        """Test from_xml with schema."""
        schema = "name STRING, age INT"
        result = XMLFunctions.from_xml(F.col("xml"), schema)
        assert result.operation == "from_xml"
        assert result.value == schema

    def test_from_xml_with_string_column(self):
        """Test from_xml with string column name."""
        result = XMLFunctions.from_xml("xml", "schema")
        assert result.operation == "from_xml"

    def test_to_xml_with_column(self):
        """Test to_xml with column."""
        result = XMLFunctions.to_xml(F.col("struct_col"))
        assert result.operation == "to_xml"

    def test_to_xml_with_column_operation(self):
        """Test to_xml with column operation."""
        struct_expr = F.struct(F.col("name"), F.col("age"))
        result = XMLFunctions.to_xml(struct_expr)
        assert result.operation == "to_xml"

    def test_schema_of_xml(self):
        """Test schema_of_xml."""
        result = XMLFunctions.schema_of_xml(F.col("xml"))
        assert result.operation == "schema_of_xml"

    def test_schema_of_xml_with_string(self):
        """Test schema_of_xml with string."""
        result = XMLFunctions.schema_of_xml("xml")
        assert result.operation == "schema_of_xml"

    def test_xpath(self):
        """Test xpath extraction."""
        result = XMLFunctions.xpath(F.col("xml"), "/root/item")
        assert result.operation == "xpath"
        assert result.value == "/root/item"

    def test_xpath_with_string(self):
        """Test xpath with string column."""
        result = XMLFunctions.xpath("xml", "/root/item")
        assert result.operation == "xpath"

    def test_xpath_boolean(self):
        """Test xpath_boolean."""
        result = XMLFunctions.xpath_boolean(F.col("xml"), "/root/active='true'")
        assert result.operation == "xpath_boolean"

    def test_xpath_double(self):
        """Test xpath_double."""
        result = XMLFunctions.xpath_double(F.col("xml"), "/root/value")
        assert result.operation == "xpath_double"

    def test_xpath_float(self):
        """Test xpath_float."""
        result = XMLFunctions.xpath_float(F.col("xml"), "/root/price")
        assert result.operation == "xpath_float"

    def test_xpath_int(self):
        """Test xpath_int."""
        result = XMLFunctions.xpath_int(F.col("xml"), "/root/age")
        assert result.operation == "xpath_int"

    def test_xpath_long(self):
        """Test xpath_long."""
        result = XMLFunctions.xpath_long(F.col("xml"), "/root/value")
        assert result.operation == "xpath_long"

    def test_xpath_short(self):
        """Test xpath_short."""
        result = XMLFunctions.xpath_short(F.col("xml"), "/root/count")
        assert result.operation == "xpath_short"

    def test_xpath_string(self):
        """Test xpath_string."""
        result = XMLFunctions.xpath_string(F.col("xml"), "/root/name")
        assert result.operation == "xpath_string"

    def test_xpath_functions_with_complex_paths(self):
        """Test xpath functions with complex XPath expressions."""
        # Test nested paths
        result = XMLFunctions.xpath_string(F.col("xml"), "/root/person/name")
        assert result.operation == "xpath_string"

        # Test with wildcards
        result = XMLFunctions.xpath(F.col("xml"), "/root/*/value")
        assert result.operation == "xpath"

        # Test with predicates
        result = XMLFunctions.xpath_int(F.col("xml"), "/root/item[@id='1']/count")
        assert result.operation == "xpath_int"
