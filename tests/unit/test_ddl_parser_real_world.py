"""
Real-world DDL schema tests for DDL parser.

Tests with DDL strings from real-world PySpark usage patterns,
AWS Glue, Databricks, and common data engineering scenarios.
"""

import pytest
from mock_spark.core.ddl_parser import parse_ddl_schema
from mock_spark.spark_types import (
    MockStructType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
)


class TestDDLParserRealWorld:
    """Test DDL parser with real-world schemas."""

    # ==================== E-commerce Schemas ====================
    
    def test_product_schema(self):
        """Test product catalog schema."""
        schema = parse_ddl_schema(
            "product_id long, product_name string, price double, "
            "category string, in_stock boolean, tags array<string>, "
            "created_at timestamp, updated_at timestamp"
        )
        assert len(schema.fields) == 8
        assert schema.fields[0].name == "product_id"
        assert schema.fields[5].name == "tags"

    def test_order_schema(self):
        """Test order schema."""
        schema = parse_ddl_schema(
            "order_id long, customer_id long, order_date date, "
            "total_amount double, status string, "
            "items array<struct<product_id:long,quantity:int,price:double>>"
        )
        assert len(schema.fields) == 6

    def test_customer_schema(self):
        """Test customer schema."""
        schema = parse_ddl_schema(
            "customer_id long, first_name string, last_name string, "
            "email string, phone string, registration_date timestamp, "
            "address struct<street:string,city:string,state:string,zip:string>"
        )
        assert len(schema.fields) == 7

    # ==================== Financial Schemas ====================
    
    def test_transaction_schema(self):
        """Test financial transaction schema."""
        schema = parse_ddl_schema(
            "transaction_id long, account_id long, transaction_date date, "
            "amount decimal(18,2), transaction_type string, "
            "description string, category string"
        )
        assert len(schema.fields) == 7

    def test_account_schema(self):
        """Test account schema."""
        schema = parse_ddl_schema(
            "account_id long, account_type string, balance decimal(18,2), "
            "currency string, opened_date date, status string"
        )
        assert len(schema.fields) == 6

    # ==================== IoT/Streaming Schemas ====================
    
    def test_sensor_data_schema(self):
        """Test IoT sensor data schema."""
        schema = parse_ddl_schema(
            "sensor_id string, timestamp timestamp, temperature double, "
            "humidity double, pressure double, location struct<lat:double,lon:double>"
        )
        assert len(schema.fields) == 6

    def test_device_event_schema(self):
        """Test device event schema."""
        schema = parse_ddl_schema(
            "event_id long, device_id string, event_type string, "
            "event_time timestamp, metadata map<string,string>, "
            "metrics map<string,double>"
        )
        assert len(schema.fields) == 6

    # ==================== Log/Event Schemas ====================
    
    def test_web_log_schema(self):
        """Test web server log schema."""
        schema = parse_ddl_schema(
            "timestamp timestamp, ip_address string, user_agent string, "
            "request_method string, request_path string, status_code int, "
            "response_time_ms int, referer string"
        )
        assert len(schema.fields) == 8

    def test_application_log_schema(self):
        """Test application log schema."""
        schema = parse_ddl_schema(
            "log_id long, timestamp timestamp, level string, "
            "logger string, message string, exception string, "
            "context map<string,string>"
        )
        assert len(schema.fields) == 7

    # ==================== ML Feature Schemas ====================
    
    def test_feature_vector_schema(self):
        """Test ML feature vector schema."""
        schema = parse_ddl_schema(
            "user_id long, features array<double>, feature_names array<string>, "
            "created_at timestamp, model_version string"
        )
        assert len(schema.fields) == 5

    def test_training_data_schema(self):
        """Test training data schema."""
        schema = parse_ddl_schema(
            "label double, features struct<"
            "feature1:double,feature2:double,feature3:double,"
            "categorical:string,numerical:array<double>"
            ">, metadata map<string,string>"
        )
        assert len(schema.fields) == 3

    # ==================== Social Media Schemas ====================
    
    def test_post_schema(self):
        """Test social media post schema."""
        schema = parse_ddl_schema(
            "post_id long, user_id long, content string, "
            "created_at timestamp, likes int, comments array<struct<"
            "user_id:long,comment:string,timestamp:timestamp"
            ">>, hashtags array<string>"
        )
        assert len(schema.fields) == 7

    def test_user_profile_schema(self):
        """Test user profile schema."""
        schema = parse_ddl_schema(
            "user_id long, username string, email string, "
            "profile struct<"
            "bio:string,location:string,website:string,"
            "joined_date:date,followers_count:int,following_count:int"
            ">, interests array<string>"
        )
        assert len(schema.fields) == 5

    # ==================== Scientific Data Schemas ====================
    
    def test_experiment_data_schema(self):
        """Test scientific experiment data schema."""
        schema = parse_ddl_schema(
            "experiment_id long, timestamp timestamp, "
            "measurements struct<"
            "temperature:double,pressure:double,humidity:double,"
            "sensor_readings:array<double>"
            ">, conditions map<string,string>"
        )
        assert len(schema.fields) == 4

    # ==================== Time Series Schemas ====================
    
    def test_time_series_schema(self):
        """Test time series data schema."""
        schema = parse_ddl_schema(
            "timestamp timestamp, value double, metric_name string, "
            "tags map<string,string>, metadata struct<"
            "source:string,quality:double,unit:string"
            ">"
        )
        assert len(schema.fields) == 5

    # ==================== Nested JSON-like Schemas ====================
    
    def test_nested_json_schema(self):
        """Test deeply nested JSON-like schema."""
        schema = parse_ddl_schema(
            "id long, data struct<"
            "user:struct<id:long,name:string,email:string>,"
            "session:struct<id:string,start:timestamp,duration:int>,"
            "events:array<struct<type:string,timestamp:timestamp,data:map<string,string>>>"
            ">"
        )
        assert len(schema.fields) == 2

    def test_complex_nested_schema(self):
        """Test complex nested schema."""
        schema = parse_ddl_schema(
            "root struct<"
            "level1:struct<"
            "level2:struct<"
            "level3:struct<"
            "level4:struct<"
            "value:string"
            ">"
            ">"
            ">"
            ">"
            ">"
        )
        assert len(schema.fields) == 1

    # ==================== AWS Glue Style Schemas ====================
    
    def test_glue_catalog_schema(self):
        """Test AWS Glue catalog style schema."""
        schema = parse_ddl_schema(
            "id bigint, name string, created_date date, "
            "metadata struct<"
            "source:string,version:string,"
            "tags:array<string>,properties:map<string,string>"
            ">"
        )
        assert len(schema.fields) == 4

    # ==================== Databricks Delta Schemas ====================
    
    def test_delta_table_schema(self):
        """Test Databricks Delta table schema."""
        schema = parse_ddl_schema(
            "id long, name string, value double, "
            "created_at timestamp, updated_at timestamp, "
            "partition_date date, metadata map<string,string>"
        )
        assert len(schema.fields) == 7

    # ==================== Data Warehouse Schemas ====================
    
    def test_fact_table_schema(self):
        """Test data warehouse fact table schema."""
        schema = parse_ddl_schema(
            "fact_id long, date_key int, product_key int, "
            "customer_key int, sales_amount decimal(18,2), "
            "quantity int, discount decimal(5,2)"
        )
        assert len(schema.fields) == 7

    def test_dimension_table_schema(self):
        """Test data warehouse dimension table schema."""
        schema = parse_ddl_schema(
            "dim_key int, natural_key string, "
            "attributes struct<name:string,description:string,category:string>, "
            "effective_date date, expiry_date date, current_flag boolean"
        )
        assert len(schema.fields) == 6

    # ==================== Event Sourcing Schemas ====================
    
    def test_event_store_schema(self):
        """Test event store schema."""
        schema = parse_ddl_schema(
            "event_id long, aggregate_id string, event_type string, "
            "event_version int, timestamp timestamp, "
            "payload map<string,string>, metadata map<string,string>"
        )
        assert len(schema.fields) == 7

    # ==================== API Data Schemas ====================
    
    def test_api_response_schema(self):
        """Test API response schema."""
        schema = parse_ddl_schema(
            "status int, message string, data struct<"
            "items:array<struct<id:long,name:string,value:double>>,"
            "pagination:struct<page:int,per_page:int,total:int>"
            ">, errors array<string>"
        )
        assert len(schema.fields) == 3

    # ==================== Document Store Schemas ====================
    
    def test_document_schema(self):
        """Test document store schema."""
        schema = parse_ddl_schema(
            "doc_id string, title string, content string, "
            "metadata map<string,string>, tags array<string>, "
            "created_at timestamp, updated_at timestamp, version int"
        )
        assert len(schema.fields) == 8

    # ==================== Analytics Schemas ====================
    
    def test_pageview_schema(self):
        """Test web analytics pageview schema."""
        schema = parse_ddl_schema(
            "pageview_id long, session_id string, user_id long, "
            "page_path string, referrer string, timestamp timestamp, "
            "duration_seconds int, device_info struct<"
            "type:string,os:string,browser:string"
            ">"
        )
        assert len(schema.fields) == 8

    def test_conversion_schema(self):
        """Test conversion tracking schema."""
        schema = parse_ddl_schema(
            "conversion_id long, user_id long, conversion_type string, "
            "value decimal(10,2), timestamp timestamp, "
            "attribution struct<source:string,medium:string,campaign:string>, "
            "properties map<string,string>"
        )
        assert len(schema.fields) == 7

    # ==================== Inventory Schemas ====================
    
    def test_inventory_schema(self):
        """Test inventory management schema."""
        schema = parse_ddl_schema(
            "sku string, product_name string, quantity int, "
            "warehouse string, location struct<aisle:string,shelf:string>, "
            "last_updated timestamp, reorder_level int"
        )
        assert len(schema.fields) == 7

    # ==================== Healthcare Schemas ====================
    
    def test_patient_schema(self):
        """Test healthcare patient schema."""
        schema = parse_ddl_schema(
            "patient_id long, name struct<first:string,last:string>, "
            "date_of_birth date, gender string, "
            "contact struct<phone:string,email:string,address:string>, "
            "medical_history array<struct<condition:string,diagnosed_date:date>>"
        )
        assert len(schema.fields) == 6

    def test_medical_record_schema(self):
        """Test medical record schema."""
        schema = parse_ddl_schema(
            "record_id long, patient_id long, visit_date date, "
            "diagnosis string, medications array<string>, "
            "vitals struct<temperature:double,blood_pressure:string,heart_rate:int>, "
            "notes string"
        )
        assert len(schema.fields) == 7

    # ==================== Supply Chain Schemas ====================
    
    def test_shipment_schema(self):
        """Test supply chain shipment schema."""
        schema = parse_ddl_schema(
            "shipment_id long, order_id long, carrier string, "
            "tracking_number string, status string, "
            "origin struct<name:string,address:string>, "
            "destination struct<name:string,address:string>, "
            "estimated_delivery timestamp, actual_delivery timestamp"
        )
        assert len(schema.fields) == 9

    # ==================== Gaming Schemas ====================
    
    def test_game_event_schema(self):
        """Test gaming event schema."""
        schema = parse_ddl_schema(
            "event_id long, player_id long, game_id string, "
            "event_type string, timestamp timestamp, "
            "game_state struct<level:int,score:int,lives:int>, "
            "actions array<struct<action:string,timestamp:timestamp>>"
        )
        assert len(schema.fields) == 7

    # ==================== Streaming Data Schemas ====================
    
    def test_streaming_event_schema(self):
        """Test streaming event schema."""
        schema = parse_ddl_schema(
            "event_id long, event_type string, timestamp timestamp, "
            "source string, payload map<string,string>, "
            "enrichment struct<user_id:long,session_id:string,device:string>"
        )
        assert len(schema.fields) == 6

    # ==================== TPC-H Style Schemas ====================
    
    def test_lineitem_schema(self):
        """Test TPC-H lineitem schema."""
        schema = parse_ddl_schema(
            "l_orderkey long, l_partkey long, l_suppkey long, "
            "l_linenumber int, l_quantity decimal(12,2), "
            "l_extendedprice decimal(12,2), l_discount decimal(12,2), "
            "l_tax decimal(12,2), l_returnflag string, l_linestatus string, "
            "l_shipdate date, l_commitdate date, l_receiptdate date, "
            "l_shipinstruct string, l_shipmode string, l_comment string"
        )
        assert len(schema.fields) == 16

    def test_orders_schema(self):
        """Test TPC-H orders schema."""
        schema = parse_ddl_schema(
            "o_orderkey long, o_custkey long, o_orderstatus string, "
            "o_totalprice decimal(12,2), o_orderdate date, "
            "o_orderpriority string, o_clerk string, o_shippriority int, "
            "o_comment string"
        )
        assert len(schema.fields) == 9

    # ==================== Graph Data Schemas ====================
    
    def test_graph_node_schema(self):
        """Test graph node schema."""
        schema = parse_ddl_schema(
            "node_id string, node_type string, "
            "properties map<string,string>, "
            "attributes struct<name:string,label:string,weight:double>"
        )
        assert len(schema.fields) == 4

    def test_graph_edge_schema(self):
        """Test graph edge schema."""
        schema = parse_ddl_schema(
            "edge_id string, source_node string, target_node string, "
            "edge_type string, weight double, "
            "properties map<string,string>"
        )
        assert len(schema.fields) == 6

    # ==================== Multi-tenant Schemas ====================
    
    def test_tenant_data_schema(self):
        """Test multi-tenant data schema."""
        schema = parse_ddl_schema(
            "tenant_id string, entity_id long, "
            "data struct<"
            "custom_fields:map<string,string>,"
            "tags:array<string>,"
            "metadata:struct<created_by:string,created_at:timestamp>>"
            ", version int"
        )
        assert len(schema.fields) == 4

    # ==================== Audit Log Schemas ====================
    
    def test_audit_log_schema(self):
        """Test audit log schema."""
        schema = parse_ddl_schema(
            "audit_id long, entity_type string, entity_id string, "
            "action string, user_id long, timestamp timestamp, "
            "changes struct<before:map<string,string>,after:map<string,string>>, "
            "ip_address string, user_agent string"
        )
        assert len(schema.fields) == 9

    # ==================== Configuration Schemas ====================
    
    def test_config_schema(self):
        """Test configuration schema."""
        schema = parse_ddl_schema(
            "config_id string, config_type string, "
            "settings map<string,string>, "
            "nested_config struct<"
            "database:struct<host:string,port:int,name:string>,"
            "cache:struct<enabled:boolean,ttl:int>,"
            "features:array<string>"
            ">, version string"
        )
        assert len(schema.fields) == 5

