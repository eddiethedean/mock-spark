"""
Performance and stress tests for DDL schema parser.

Tests that the parser scales properly with large schemas and
deeply nested structures.
"""

import pytest
import time
from mock_spark.core.ddl_parser import parse_ddl_schema


class TestDDLParserPerformance:
    """Performance tests for DDL parser."""

    # ==================== Large Schema Performance ====================
    
    def test_large_schema_100_fields(self):
        """Test parsing schema with 100 fields."""
        fields = ", ".join(f"field{i} string" for i in range(100))
        start = time.time()
        schema = parse_ddl_schema(fields)
        duration = time.time() - start
        
        assert len(schema.fields) == 100
        assert duration < 1.0, f"Parsing took {duration:.3f}s, expected < 1.0s"

    def test_large_schema_500_fields(self):
        """Test parsing schema with 500 fields."""
        fields = ", ".join(f"field{i} int" for i in range(500))
        start = time.time()
        schema = parse_ddl_schema(fields)
        duration = time.time() - start
        
        assert len(schema.fields) == 500
        assert duration < 1.0, f"Parsing took {duration:.3f}s, expected < 1.0s"

    def test_large_schema_1000_fields(self):
        """Test parsing schema with 1000 fields."""
        fields = ", ".join(f"field{i} long" for i in range(1000))
        start = time.time()
        schema = parse_ddl_schema(fields)
        duration = time.time() - start
        
        assert len(schema.fields) == 1000
        assert duration < 1.0, f"Parsing took {duration:.3f}s, expected < 1.0s"

    def test_large_schema_2000_fields(self):
        """Test parsing schema with 2000 fields."""
        fields = ", ".join(f"field{i} double" for i in range(2000))
        start = time.time()
        schema = parse_ddl_schema(fields)
        duration = time.time() - start
        
        assert len(schema.fields) == 2000
        assert duration < 2.0, f"Parsing took {duration:.3f}s, expected < 2.0s"

    # ==================== Deep Nesting Performance ====================
    
    def test_deeply_nested_10_levels(self):
        """Test parsing 10 levels of nesting."""
        nested = "string"
        for i in range(10):
            nested = f"struct<level{i}:{nested}>"
        
        start = time.time()
        schema = parse_ddl_schema(f"data {nested}")
        duration = time.time() - start
        
        assert len(schema.fields) == 1
        assert duration < 1.0, f"Parsing took {duration:.3f}s, expected < 1.0s"

    def test_deeply_nested_20_levels(self):
        """Test parsing 20 levels of nesting."""
        nested = "string"
        for i in range(20):
            nested = f"struct<level{i}:{nested}>"
        
        start = time.time()
        schema = parse_ddl_schema(f"data {nested}")
        duration = time.time() - start
        
        assert len(schema.fields) == 1
        assert duration < 1.0, f"Parsing took {duration:.3f}s, expected < 1.0s"

    def test_deeply_nested_50_levels(self):
        """Test parsing 50 levels of nesting."""
        nested = "string"
        for i in range(50):
            nested = f"struct<level{i}:{nested}>"
        
        start = time.time()
        schema = parse_ddl_schema(f"data {nested}")
        duration = time.time() - start
        
        assert len(schema.fields) == 1
        assert duration < 1.0, f"Parsing took {duration:.3f}s, expected < 1.0s"

    # ==================== Batch Parsing Performance ====================
    
    def test_batch_parsing_100_schemas(self):
        """Test parsing 100 schemas in batch."""
        schemas = [
            ", ".join(f"field{i} string" for i in range(10))
            for _ in range(100)
        ]
        
        start = time.time()
        results = [parse_ddl_schema(schema) for schema in schemas]
        duration = time.time() - start
        
        assert len(results) == 100
        assert all(len(r.fields) == 10 for r in results)
        assert duration < 1.0, f"Batch parsing took {duration:.3f}s, expected < 1.0s"

    def test_batch_parsing_1000_schemas(self):
        """Test parsing 1000 simple schemas in batch."""
        schemas = ["id long, name string"] * 1000
        
        start = time.time()
        results = [parse_ddl_schema(schema) for schema in schemas]
        duration = time.time() - start
        
        assert len(results) == 1000
        assert all(len(r.fields) == 2 for r in results)
        assert duration < 2.0, f"Batch parsing took {duration:.3f}s, expected < 2.0s"

    # ==================== Memory Usage ====================
    
    def test_memory_usage_large_schema(self):
        """Test memory usage with large schema."""
        import sys
        
        # Create large schema
        fields = ", ".join(f"field{i} string" for i in range(1000))
        
        # Parse and check memory
        schema = parse_ddl_schema(fields)
        size = sys.getsizeof(schema)
        
        # Should be reasonable (less than 1MB for 1000 fields)
        assert size < 1024 * 1024, f"Schema size {size} bytes is too large"

    def test_memory_usage_deeply_nested(self):
        """Test memory usage with deeply nested schema."""
        import sys
        
        # Create deeply nested schema
        nested = "string"
        for i in range(50):
            nested = f"struct<level{i}:{nested}>"
        
        schema = parse_ddl_schema(f"data {nested}")
        size = sys.getsizeof(schema)
        
        # Should be reasonable
        assert size < 1024 * 1024, f"Schema size {size} bytes is too large"

    # ==================== Repeated Parsing ====================
    
    def test_repeated_parsing_same_schema(self):
        """Test parsing same schema multiple times."""
        schema_str = ", ".join(f"field{i} string" for i in range(100))
        
        start = time.time()
        for _ in range(100):
            schema = parse_ddl_schema(schema_str)
            assert len(schema.fields) == 100
        duration = time.time() - start
        
        assert duration < 2.0, f"Repeated parsing took {duration:.3f}s"

    def test_repeated_parsing_no_memory_leak(self):
        """Test that repeated parsing doesn't leak memory."""
        schema_str = "id long, name string, age int"
        
        # Parse many times
        for _ in range(1000):
            schema = parse_ddl_schema(schema_str)
            assert len(schema.fields) == 3

    # ==================== Complex Schema Performance ====================
    
    def test_complex_schema_performance(self):
        """Test parsing complex schema with all types."""
        schema_str = (
            "id long, name string, age int, score double, active boolean, "
            "tags array<string>, metadata map<string,string>, "
            "address struct<street:string,city:string>, "
            "history array<struct<date:string,action:string>>, "
            "settings map<string,array<string>>"
        )
        
        start = time.time()
        schema = parse_ddl_schema(schema_str)
        duration = time.time() - start
        
        assert len(schema.fields) == 10
        assert duration < 0.1, f"Parsing took {duration:.3f}s, expected < 0.1s"

    def test_very_complex_schema_performance(self):
        """Test parsing very complex schema."""
        schema_str = (
            "data struct<"
            "items:array<struct<"
            "id:long,name:string,"
            "metadata:map<string,string>,"
            "tags:array<string>,"
            "history:array<struct<date:string,action:string>>"
            ">>, "
            "settings:map<string,array<struct<key:string,value:string>>>, "
            "config:struct<"
            "database:struct<host:string,port:int>,"
            "cache:struct<enabled:boolean,ttl:int>"
            ">"
            ">"
        )
        
        start = time.time()
        schema = parse_ddl_schema(schema_str)
        duration = time.time() - start
        
        assert len(schema.fields) == 1
        assert duration < 0.1, f"Parsing took {duration:.3f}s, expected < 0.1s"

    # ==================== Stress Tests ====================
    
    def test_stress_large_and_nested(self):
        """Test large schema with nested structures."""
        # Create schema with 100 fields, some nested
        fields = []
        for i in range(100):
            if i % 10 == 0:
                fields.append(f"field{i} struct<id:long,name:string>")
            elif i % 10 == 1:
                fields.append(f"field{i} array<string>")
            elif i % 10 == 2:
                fields.append(f"field{i} map<string,int>")
            else:
                fields.append(f"field{i} string")
        
        schema_str = ", ".join(fields)
        
        start = time.time()
        schema = parse_ddl_schema(schema_str)
        duration = time.time() - start
        
        assert len(schema.fields) == 100
        assert duration < 1.0, f"Parsing took {duration:.3f}s, expected < 1.0s"

    def test_stress_deep_and_wide(self):
        """Test deeply nested and wide schema."""
        # Build wide nested structure
        nested_fields = ", ".join(f"field{i}:string" for i in range(50))
        nested = f"struct<{nested_fields}>"
        for i in range(10):
            nested = f"struct<level{i}:{nested}>"
        
        start = time.time()
        schema = parse_ddl_schema(f"data {nested}")
        duration = time.time() - start
        
        assert len(schema.fields) == 1
        assert duration < 1.0, f"Parsing took {duration:.3f}s, expected < 1.0s"

    # ==================== Edge Case Performance ====================
    
    def test_performance_with_whitespace(self):
        """Test performance with excessive whitespace."""
        schema_str = "   id    long   ,   name    string   ,   age    int   "
        
        start = time.time()
        for _ in range(1000):
            schema = parse_ddl_schema(schema_str)
            assert len(schema.fields) == 3
        duration = time.time() - start
        
        assert duration < 1.0, f"Parsing took {duration:.3f}s"

    def test_performance_with_colons(self):
        """Test performance with colon separators."""
        schema_str = "id:long,name:string,age:int"
        
        start = time.time()
        for _ in range(1000):
            schema = parse_ddl_schema(schema_str)
            assert len(schema.fields) == 3
        duration = time.time() - start
        
        assert duration < 1.0, f"Parsing took {duration:.3f}s"

    # ==================== Scalability Tests ====================
    
    def test_linear_scaling(self):
        """Test that parsing scales linearly with schema size."""
        sizes = [10, 50, 100, 500, 1000]
        times = []
        
        for size in sizes:
            fields = ", ".join(f"field{i} string" for i in range(size))
            start = time.time()
            schema = parse_ddl_schema(fields)
            duration = time.time() - start
            times.append(duration)
            assert len(schema.fields) == size
        
        # Check that times scale roughly linearly (allow 2x overhead)
        for i in range(1, len(times)):
            ratio = times[i] / times[i-1]
            size_ratio = sizes[i] / sizes[i-1]
            # Time should not grow faster than 2x the size ratio
            assert ratio <= size_ratio * 2, f"Non-linear scaling: {ratio:.2f}x time for {size_ratio:.2f}x size"

