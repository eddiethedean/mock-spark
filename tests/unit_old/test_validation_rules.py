"""
Unit tests for validation rules.

Tests string-based and list-based validation rule expressions.
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.functions.conditional import validate_rule


@pytest.mark.fast
class TestValidationRules:
    """Test validation rule expressions."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {
                "id": 1,
                "name": "Alice",
                "age": 25,
                "salary": 50000,
                "email": "alice@example.com",
            },
            {
                "id": 2,
                "name": "Bob",
                "age": 30,
                "salary": 60000,
                "email": "bob@test.com",
            },
            {
                "id": 3,
                "name": "Charlie",
                "age": 35,
                "salary": 70000,
                "email": "charlie@company.org",
            },
            {
                "id": 4,
                "name": "David",
                "age": 40,
                "salary": 80000,
                "email": "david@corp.net",
            },
            {
                "id": 5,
                "name": "Eve",
                "age": 45,
                "salary": 90000,
                "email": "eve@business.com",
            },
        ]

    def test_string_validation_rules(self, spark, sample_data):
        """Test string-based validation rules."""
        df = spark.createDataFrame(sample_data)

        # Test not_null rule
        not_null_rule = validate_rule("name", "not_null")
        result = df.filter(not_null_rule)
        assert result.count() == 5  # All names are not null

        # Test positive rule
        positive_rule = validate_rule("age", "positive")
        result = df.filter(positive_rule)
        assert result.count() == 5  # All ages are positive

        # Test non_negative rule
        non_negative_rule = validate_rule("salary", "non_negative")
        result = df.filter(non_negative_rule)
        assert result.count() == 5  # All salaries are non-negative

        # Test negative rule (should return 0 results)
        negative_rule = validate_rule("age", "negative")
        result = df.filter(negative_rule)
        assert result.count() == 0  # No ages are negative

        # Test non_positive rule (should return 0 results)
        non_positive_rule = validate_rule("salary", "non_positive")
        result = df.filter(non_positive_rule)
        assert result.count() == 0  # No salaries are non-positive

        # Test non_zero rule
        non_zero_rule = validate_rule("id", "non_zero")
        result = df.filter(non_zero_rule)
        assert result.count() == 5  # All IDs are non-zero

        # Test zero rule (should return 0 results)
        zero_rule = validate_rule("id", "zero")
        result = df.filter(zero_rule)
        assert result.count() == 0  # No IDs are zero

    def test_list_validation_rules(self, spark, sample_data):
        """Test list-based validation rules."""
        df = spark.createDataFrame(sample_data)

        # Test gt (greater than) rule
        gt_rule = validate_rule("age", ["gt", 30])
        result = df.filter(gt_rule)
        assert result.count() == 3  # 3 people are older than 30

        # Test gte (greater than or equal) rule
        gte_rule = validate_rule("age", ["gte", 30])
        result = df.filter(gte_rule)
        assert result.count() == 4  # 4 people are 30 or older

        # Test lt (less than) rule
        lt_rule = validate_rule("age", ["lt", 40])
        result = df.filter(lt_rule)
        assert result.count() == 3  # 3 people are younger than 40

        # Test lte (less than or equal) rule
        lte_rule = validate_rule("age", ["lte", 35])
        result = df.filter(lte_rule)
        assert result.count() == 3  # 3 people are 35 or younger

        # Test eq (equal) rule
        eq_rule = validate_rule("age", ["eq", 30])
        result = df.filter(eq_rule)
        assert result.count() == 1  # 1 person is exactly 30

        # Test ne (not equal) rule
        ne_rule = validate_rule("age", ["ne", 30])
        result = df.filter(ne_rule)
        assert result.count() == 4  # 4 people are not 30

        # Test between rule
        between_rule = validate_rule("age", ["between", 30, 40])
        result = df.filter(between_rule)
        assert result.count() == 3  # 3 people are between 30 and 40

        # Test in rule
        in_rule = validate_rule("age", ["in", [25, 30, 35]])
        result = df.filter(in_rule)
        assert result.count() == 3  # 3 people have ages in the list

        # Test not_in rule
        not_in_rule = validate_rule("age", ["not_in", [25, 30]])
        result = df.filter(not_in_rule)
        assert result.count() == 3  # 3 people have ages not in the list

    def test_string_validation_rules_with_columns(self, spark, sample_data):
        """Test string-based validation rules with column operations."""
        df = spark.createDataFrame(sample_data)

        # Test contains rule
        contains_rule = validate_rule("email", ["contains", "@example.com"])
        result = df.filter(contains_rule)
        assert result.count() == 1  # 1 email contains @example.com

        # Test starts_with rule
        starts_with_rule = validate_rule("name", ["starts_with", "A"])
        result = df.filter(starts_with_rule)
        assert result.count() == 1  # 1 name starts with A

        # Test ends_with rule
        ends_with_rule = validate_rule("name", ["ends_with", "e"])
        result = df.filter(ends_with_rule)
        assert result.count() == 3  # 3 names end with e (Alice, Charlie, Eve)

        # Test regex rule
        regex_rule = validate_rule("email", ["regex", r".*@.*\.com"])
        result = df.filter(regex_rule)
        assert result.count() == 3  # 3 emails match the pattern

    def test_validation_rule_errors(self, spark, sample_data):
        """Test validation rule error handling."""
        spark.createDataFrame(sample_data)

        # Test unknown string rule
        with pytest.raises(ValueError, match="Unknown string validation rule"):
            validate_rule("age", "unknown_rule")

        # Test empty list rule
        with pytest.raises(ValueError, match="Empty rule list"):
            validate_rule("age", [])

        # Test unknown list rule
        with pytest.raises(ValueError, match="Unknown list validation rule"):
            validate_rule("age", ["unknown_op"])

        # Test gt rule without value
        with pytest.raises(ValueError, match="gt rule requires a value"):
            validate_rule("age", ["gt"])

        # Test between rule without enough values
        with pytest.raises(ValueError, match="between rule requires two values"):
            validate_rule("age", ["between", 30])

        # Test in rule without values
        with pytest.raises(ValueError, match="in rule requires a list of values"):
            validate_rule("age", ["in"])

        # Test contains rule without value
        with pytest.raises(ValueError, match="contains rule requires a value"):
            validate_rule("name", ["contains"])

        # Test regex rule without pattern
        with pytest.raises(ValueError, match="regex rule requires a pattern"):
            validate_rule("email", ["regex"])

    def test_validation_rule_with_mock_column(self, spark, sample_data):
        """Test validation rules with MockColumn objects."""
        df = spark.createDataFrame(sample_data)

        # Test with MockColumn object
        age_col = F.col("age")
        gt_rule = validate_rule(age_col, ["gt", 30])
        result = df.filter(gt_rule)
        assert result.count() == 3

        # Test with string column name
        name_rule = validate_rule("name", "not_null")
        result = df.filter(name_rule)
        assert result.count() == 5

    def test_complex_validation_rules(self, spark, sample_data):
        """Test complex validation rule combinations."""
        df = spark.createDataFrame(sample_data)

        # Test multiple conditions
        age_rule = validate_rule("age", ["between", 25, 40])
        salary_rule = validate_rule("salary", ["gte", 60000])

        result = df.filter(age_rule & salary_rule)
        assert result.count() == 3  # 3 people meet both criteria (Bob, Charlie, David)

        # Test OR conditions
        name_rule = validate_rule("name", ["starts_with", "A"])
        age_rule = validate_rule("age", ["gt", 40])

        result = df.filter(name_rule | age_rule)
        assert result.count() == 2  # 1 name starts with A, 1 age > 40

    def test_validation_rule_performance(self, spark):
        """Test validation rule performance with larger dataset."""
        # Create a larger dataset
        data = []
        for i in range(100):
            data.append(
                {
                    "id": i,
                    "value": i * 10,
                    "category": f"cat_{i % 10}",
                    "score": i * 0.1,
                }
            )

        df = spark.createDataFrame(data)

        # Test various rules
        rules = [
            ("value", "positive"),
            ("value", ["gt", 500]),
            ("value", ["between", 200, 800]),
            ("category", ["in", ["cat_0", "cat_1", "cat_2"]]),
            ("score", ["gte", 5.0]),
        ]

        for column, rule in rules:
            validation_rule = validate_rule(column, rule)
            result = df.filter(validation_rule)
            assert result.count() >= 0  # Should not crash

    def test_validation_rule_edge_cases(self, spark):
        """Test validation rule edge cases."""
        # Test with null values
        data_with_nulls = [
            {"id": 1, "value": 10, "name": "Alice"},
            {"id": 2, "value": None, "name": "Bob"},
            {"id": 3, "value": 20, "name": None},
            {"id": 4, "value": 30, "name": "Charlie"},
        ]
        df = spark.createDataFrame(data_with_nulls)

        # Test not_null rule
        not_null_rule = validate_rule("value", "not_null")
        result = df.filter(not_null_rule)
        assert result.count() == 3  # 3 non-null values

        # Test with zero values
        zero_data = [
            {"id": 1, "value": 0, "name": "Zero"},
            {"id": 2, "value": 10, "name": "Positive"},
            {"id": 3, "value": -5, "name": "Negative"},
        ]
        df = spark.createDataFrame(zero_data)

        # Test zero rule
        zero_rule = validate_rule("value", "zero")
        result = df.filter(zero_rule)
        assert result.count() == 1  # 1 zero value

        # Test non_zero rule
        non_zero_rule = validate_rule("value", "non_zero")
        result = df.filter(non_zero_rule)
        assert result.count() == 2  # 2 non-zero values

    def test_validation_rule_type_safety(self, spark):
        """Test validation rule type safety."""
        data = [
            {"id": 1, "value": "10", "name": "Alice"},
            {"id": 2, "value": "20", "name": "Bob"},
            {"id": 3, "value": "30", "name": "Charlie"},
        ]
        df = spark.createDataFrame(data)

        # Test with string values
        string_rule = validate_rule("value", ["eq", "20"])
        result = df.filter(string_rule)
        assert result.count() == 1

        # Test with numeric comparison (should work with string values)
        numeric_rule = validate_rule("value", ["gt", "15"])
        result = df.filter(numeric_rule)
        assert result.count() == 2  # "20" and "30" are > "15" lexicographically
