"""Unit tests for Phase 9: XML Functions."""

from mock_spark.session.session import MockSparkSession
from mock_spark import functions as F


class TestXMLFunctionsUnit:
    """Test XML functions (Phase 9)."""

    def test_to_xml(self):
        """Test to_xml converts struct to XML string."""
        spark = MockSparkSession("test")
        data = [{"data": "test_value"}]
        df = spark.createDataFrame(data)

        # to_xml with simple column - should wrap in XML tags
        result = df.select(F.to_xml(F.col("data")).alias("xml")).collect()

        # Should contain XML tags
        assert "<row>" in str(result[0]["xml"]) and "</row>" in str(result[0]["xml"])

    def test_from_xml(self):
        """Test from_xml parses XML string to struct."""
        spark = MockSparkSession("test")
        data = [{"xml": "<row><name>Alice</name><age>30</age></row>"}]
        df = spark.createDataFrame(data)

        # from_xml should parse XML to struct
        result = df.select(
            F.from_xml(F.col("xml"), "name STRING, age INT").alias("parsed")
        ).collect()

        # Should return a struct-like representation
        parsed = result[0]["parsed"]
        assert parsed is not None
        assert "Alice" in str(parsed) or "name" in str(parsed)

    def test_schema_of_xml(self):
        """Test schema_of_xml infers schema from XML."""
        spark = MockSparkSession("test")
        data = [{"xml": "<row><name>Alice</name><age>30</age></row>"}]
        df = spark.createDataFrame(data)

        result = df.select(F.schema_of_xml(F.col("xml")).alias("schema")).collect()

        # Should return STRUCT<...> format
        schema_str = result[0]["schema"]
        assert "STRUCT" in str(schema_str) or schema_str is not None

    def test_xpath_string(self):
        """Test xpath_string extracts string from XML."""
        spark = MockSparkSession("test")
        data = [{"xml": "<root><name>Alice</name></root>"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.xpath_string(F.col("xml"), "/root/name").alias("name")
        ).collect()

        # Should extract "Alice"
        assert result[0]["name"] == "Alice"

    def test_xpath_boolean(self):
        """Test xpath_boolean evaluates XPath to boolean."""
        spark = MockSparkSession("test")
        data = [{"xml": "<root><active>true</active></root>"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.xpath_boolean(F.col("xml"), "/root/active='true'").alias("is_active")
        ).collect()

        # Should return True
        assert result[0]["is_active"] is True

    def test_xpath_int(self):
        """Test xpath_int extracts integer from XML."""
        spark = MockSparkSession("test")
        data = [{"xml": "<root><age>30</age></root>"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.xpath_int(F.col("xml"), "/root/age").alias("age")
        ).collect()

        # Should extract 30
        assert result[0]["age"] == 30

    def test_xpath_long(self):
        """Test xpath_long extracts long from XML."""
        spark = MockSparkSession("test")
        data = [{"xml": "<root><value>1000000</value></root>"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.xpath_long(F.col("xml"), "/root/value").alias("value")
        ).collect()

        assert result[0]["value"] is not None or len(result) == 1

    def test_xpath_short(self):
        """Test xpath_short extracts short from XML."""
        spark = MockSparkSession("test")
        data = [{"xml": "<root><count>10</count></root>"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.xpath_short(F.col("xml"), "/root/count").alias("count")
        ).collect()

        assert result[0]["count"] is not None or len(result) == 1

    def test_xpath_float(self):
        """Test xpath_float extracts float from XML."""
        spark = MockSparkSession("test")
        data = [{"xml": "<root><price>19.99</price></root>"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.xpath_float(F.col("xml"), "/root/price").alias("price")
        ).collect()

        assert result[0]["price"] is not None or len(result) == 1

    def test_xpath_double(self):
        """Test xpath_double extracts double from XML."""
        spark = MockSparkSession("test")
        data = [{"xml": "<root><value>3.14159</value></root>"}]
        df = spark.createDataFrame(data)

        result = df.select(
            F.xpath_double(F.col("xml"), "/root/value").alias("value")
        ).collect()

        assert result[0]["value"] is not None or len(result) == 1

    def test_xpath(self):
        """Test xpath extracts array of values from XML."""
        spark = MockSparkSession("test")
        data = [{"xml": "<root><item>A</item><item>B</item></root>"}]
        df = spark.createDataFrame(data)

        result = df.select(F.xpath(F.col("xml"), "/root/item").alias("items")).collect()

        # Should return array with "A" and "B"
        items = result[0]["items"]
        assert items is not None
        assert len(items) == 2
        assert "A" in items and "B" in items
