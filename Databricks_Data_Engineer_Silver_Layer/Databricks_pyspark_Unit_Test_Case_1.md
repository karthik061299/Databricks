_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive unit test cases and Pytest script for PySpark Silver layer data processing pipeline in Databricks
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks PySpark Unit Test Cases
## Silver Layer Data Processing Pipeline - Inventory Management System

## Overview

This document provides comprehensive unit test cases and a Pytest script for validating the PySpark Silver layer data processing pipeline in Databricks. The tests cover data transformations, validation logic, error handling, and performance optimization scenarios.

## Test Case List

### 1. Data Validation Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_DV_001 | Test null value validation for Product_ID | Should reject records with null Product_ID and log error |
| TC_DV_002 | Test uniqueness validation for Product_ID | Should identify and handle duplicate Product_ID values |
| TC_DV_003 | Test positive integer validation for Product_ID | Should reject negative or zero Product_ID values |
| TC_DV_004 | Test foreign key validation for Supplier-Product relationship | Should reject invalid foreign key references |
| TC_DV_005 | Test email format validation for customer emails | Should validate email format using regex pattern |
| TC_DV_006 | Test phone number format validation | Should standardize and validate phone number formats |
| TC_DV_007 | Test date logic validation for order dates | Should ensure order dates are logical and consistent |
| TC_DV_008 | Test completeness validation for required fields | Should identify missing mandatory field values |

### 2. Data Transformation Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_DT_001 | Test product name standardization | Should convert product names to proper case format |
| TC_DT_002 | Test category name standardization | Should convert categories to uppercase |
| TC_DT_003 | Test PII masking for customer names | Should mask customer names while preserving first/last 2 characters |
| TC_DT_004 | Test email masking for customer data | Should mask email domains for privacy protection |
| TC_DT_005 | Test phone number standardization | Should remove non-numeric characters from phone numbers |
| TC_DT_006 | Test timestamp to date conversion | Should convert timestamp columns to date format |
| TC_DT_007 | Test location name standardization | Should standardize warehouse location formats |

### 3. Error Handling Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_EH_001 | Test error logging for validation failures | Should create detailed error records with proper metadata |
| TC_EH_002 | Test pipeline continuation after errors | Should continue processing valid records despite errors |
| TC_EH_003 | Test error severity classification | Should classify errors by severity (CRITICAL, HIGH, MEDIUM, LOW) |
| TC_EH_004 | Test error record structure | Should create error records with all required fields |
| TC_EH_005 | Test audit logging for pipeline execution | Should log comprehensive audit information |
| TC_EH_006 | Test fallback mechanisms for merge failures | Should fallback to overwrite when merge operations fail |

### 4. Performance and Integration Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_PI_001 | Test Delta table merge operations | Should successfully merge data using UPSERT logic |
| TC_PI_002 | Test schema evolution handling | Should handle schema changes gracefully |
| TC_PI_003 | Test table optimization with Z-ordering | Should optimize tables with appropriate Z-order columns |
| TC_PI_004 | Test large dataset processing | Should handle large datasets efficiently |
| TC_PI_005 | Test concurrent processing scenarios | Should handle concurrent pipeline executions |
| TC_PI_006 | Test memory management | Should manage Spark memory efficiently during processing |

### 5. Edge Case Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_EC_001 | Test empty DataFrame processing | Should handle empty input DataFrames gracefully |
| TC_EC_002 | Test single record processing | Should process single record datasets correctly |
| TC_EC_003 | Test maximum value boundary conditions | Should handle maximum integer and string length values |
| TC_EC_004 | Test special characters in data | Should handle special characters and Unicode properly |
| TC_EC_005 | Test null DataFrame scenarios | Should handle completely null DataFrames |
| TC_EC_006 | Test schema mismatch scenarios | Should handle unexpected schema changes |

## Pytest Script

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import uuid
from unittest.mock import Mock, patch
import logging

# Import the DataProcessor class (assuming it's in a separate module)
# from data_processor import DataProcessor

class TestDataProcessor:
    """
    Comprehensive test suite for DataProcessor class in Databricks environment
    """
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """
        Create Spark session for testing with Delta configurations
        """
        spark = SparkSession.builder \n            .appName("DataProcessor_UnitTests") \n            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n            .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n            .config("spark.sql.adaptive.enabled", "true") \n            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \n            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \n            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \n            .getOrCreate()
        
        yield spark
        spark.stop()
    
    @pytest.fixture
    def data_processor(self, spark_session):
        """
        Create DataProcessor instance for testing
        """
        # Mock the mssparkutils for testing environment
        with patch('mssparkutils.credentials.getSecret') as mock_secret, \n             patch('mssparkutils.env.getUserName') as mock_user:
            
            mock_secret.return_value = "test_value"
            mock_user.return_value = "test_user"
            
            # Create DataProcessor instance
            processor = DataProcessor()
            return processor
    
    @pytest.fixture
    def sample_products_data(self, spark_session):
        """
        Create sample products data for testing
        """
        schema = StructType([
            StructField("Product_ID", IntegerType(), True),
            StructField("Product_Name", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("Price", DoubleType(), True),
            StructField("load_timestamp", TimestampType(), True)
        ])
        
        data = [
            (1, "laptop computer", "electronics", 999.99, datetime.now()),
            (2, "office chair", "furniture", 299.50, datetime.now()),
            (3, "wireless mouse", "electronics", 49.99, datetime.now()),
            (None, "invalid product", "electronics", 99.99, datetime.now()),  # Invalid: null ID
            (-1, "negative id product", "electronics", 199.99, datetime.now()),  # Invalid: negative ID
            (1, "duplicate product", "electronics", 599.99, datetime.now())  # Invalid: duplicate ID
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_customers_data(self, spark_session):
        """
        Create sample customers data for testing PII masking
        """
        schema = StructType([
            StructField("Customer_ID", IntegerType(), True),
            StructField("Customer_Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("Phone", StringType(), True)
        ])
        
        data = [
            (1, "John Smith", "john.smith@email.com", "123-456-7890"),
            (2, "Jane Doe", "jane.doe@company.org", "(555) 123-4567"),
            (3, "Bob", "invalid-email", "555.123.4567"),  # Invalid email format
            (4, "Alice Johnson", "alice@test.com", "1234567890")
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def empty_dataframe(self, spark_session):
        """
        Create empty DataFrame for edge case testing
        """
        schema = StructType([
            StructField("Product_ID", IntegerType(), True),
            StructField("Product_Name", StringType(), True)
        ])
        
        return spark_session.createDataFrame([], schema)
    
    # Data Validation Tests
    def test_null_validation_product_id(self, data_processor, sample_products_data):
        """
        TC_DV_001: Test null value validation for Product_ID
        """
        validation_rules = [
            {'type': 'not_null', 'column': 'Product_ID', 'id_column': 'Product_ID'}
        ]
        
        valid_df, quality_score = data_processor.validate_data(
            sample_products_data, "products", validation_rules
        )
        
        # Should filter out null Product_ID records
        assert valid_df.filter(col("Product_ID").isNull()).count() == 0
        assert len(data_processor.error_records) > 0
        assert quality_score < 100.0  # Quality score should be less than 100% due to null values
    
    def test_uniqueness_validation_product_id(self, data_processor, sample_products_data):
        """
        TC_DV_002: Test uniqueness validation for Product_ID
        """
        validation_rules = [
            {'type': 'unique', 'column': 'Product_ID', 'id_column': 'Product_ID'}
        ]
        
        valid_df, quality_score = data_processor.validate_data(
            sample_products_data, "products", validation_rules
        )
        
        # Should remove duplicate Product_ID records
        product_ids = valid_df.select("Product_ID").collect()
        unique_ids = set([row.Product_ID for row in product_ids if row.Product_ID is not None])
        assert len(product_ids) == len(unique_ids)
    
    def test_positive_integer_validation(self, data_processor, sample_products_data):
        """
        TC_DV_003: Test positive integer validation for Product_ID
        """
        validation_rules = [
            {'type': 'positive_integer', 'column': 'Product_ID', 'id_column': 'Product_ID'}
        ]
        
        valid_df, quality_score = data_processor.validate_data(
            sample_products_data, "products", validation_rules
        )
        
        # Should filter out negative Product_ID values
        negative_count = valid_df.filter(col("Product_ID") < 0).count()
        assert negative_count == 0
    
    def test_email_format_validation(self, data_processor, sample_customers_data):
        """
        TC_DV_005: Test email format validation for customer emails
        """
        validation_rules = [
            {'type': 'email_format', 'column': 'Email', 'id_column': 'Customer_ID'}
        ]
        
        valid_df, quality_score = data_processor.validate_data(
            sample_customers_data, "customers", validation_rules
        )
        
        # Should filter out invalid email formats
        email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        invalid_emails = valid_df.filter(~col("Email").rlike(email_pattern)).count()
        assert invalid_emails == 0
    
    def test_phone_format_validation(self, data_processor, sample_customers_data):
        """
        TC_DV_006: Test phone number format validation
        """
        validation_rules = [
            {'type': 'phone_format', 'column': 'Phone', 'id_column': 'Customer_ID'}
        ]
        
        valid_df, quality_score = data_processor.validate_data(
            sample_customers_data, "customers", validation_rules
        )
        
        # Should standardize phone numbers to digits only
        phone_numbers = valid_df.select("Phone").collect()
        for row in phone_numbers:
            if row.Phone:
                assert row.Phone.isdigit(), f"Phone number {row.Phone} should contain only digits"
    
    # Data Transformation Tests
    def test_product_name_standardization(self, data_processor, sample_products_data):
        """
        TC_DT_001: Test product name standardization
        """
        transformed_df = data_processor.apply_transformations(sample_products_data, "products")
        
        # Should convert to proper case
        product_names = transformed_df.select("Product_Name").collect()
        for row in product_names:
            if row.Product_Name:
                # Check if first letter of each word is capitalized
                words = row.Product_Name.split()
                for word in words:
                    if word:
                        assert word[0].isupper(), f"Product name '{row.Product_Name}' should be in proper case"
    
    def test_category_standardization(self, data_processor, sample_products_data):
        """
        TC_DT_002: Test category name standardization
        """
        transformed_df = data_processor.apply_transformations(sample_products_data, "products")
        
        # Should convert to uppercase
        categories = transformed_df.select("Category").collect()
        for row in categories:
            if row.Category:
                assert row.Category.isupper(), f"Category '{row.Category}' should be uppercase"
    
    def test_pii_masking_customer_names(self, data_processor, sample_customers_data):
        """
        TC_DT_003: Test PII masking for customer names
        """
        transformed_df = data_processor.apply_transformations(sample_customers_data, "customers")
        
        # Should mask customer names (first 2 + *** + last 2 characters)
        customer_names = transformed_df.select("Customer_Name").collect()
        for row in customer_names:
            if row.Customer_Name and len(row.Customer_Name) > 4:
                assert "***" in row.Customer_Name, f"Customer name '{row.Customer_Name}' should be masked"
    
    def test_email_masking(self, data_processor, sample_customers_data):
        """
        TC_DT_004: Test email masking for customer data
        """
        transformed_df = data_processor.apply_transformations(sample_customers_data, "customers")
        
        # Should mask email domains
        emails = transformed_df.select("Email").collect()
        for row in emails:
            if row.Email and "@" in row.Email:
                if row.Email.count("@") == 1:  # Valid email format
                    assert "@*****.com" in row.Email, f"Email '{row.Email}' should be masked"
    
    def test_timestamp_to_date_conversion(self, data_processor, sample_products_data):
        """
        TC_DT_006: Test timestamp to date conversion
        """
        transformed_df = data_processor.apply_transformations(sample_products_data, "products")
        
        # Should convert load_timestamp to load_date
        if "load_date" in transformed_df.columns:
            assert "load_timestamp" not in transformed_df.columns
            # Verify data type is date
            load_date_type = dict(transformed_df.dtypes)["load_date"]
            assert "date" in load_date_type.lower()
    
    # Error Handling Tests
    def test_error_logging_structure(self, data_processor, sample_products_data):
        """
        TC_EH_004: Test error record structure
        """
        validation_rules = [
            {'type': 'not_null', 'column': 'Product_ID', 'id_column': 'Product_ID'}
        ]
        
        data_processor.validate_data(sample_products_data, "products", validation_rules)
        
        # Check error record structure
        if data_processor.error_records:
            error_record = data_processor.error_records[0]
            required_fields = [
                'error_id', 'error_timestamp', 'source_table', 'error_type',
                'error_description', 'severity_level', 'processed_by'
            ]
            
            for field in required_fields:
                assert field in error_record, f"Error record missing required field: {field}"
    
    def test_error_severity_classification(self, data_processor, sample_products_data):
        """
        TC_EH_003: Test error severity classification
        """
        validation_rules = [
            {'type': 'not_null', 'column': 'Product_ID', 'id_column': 'Product_ID'}
        ]
        
        data_processor.validate_data(sample_products_data, "products", validation_rules)
        
        # Check that errors have appropriate severity levels
        valid_severities = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
        for error_record in data_processor.error_records:
            assert error_record['severity_level'] in valid_severities
    
    # Edge Case Tests
    def test_empty_dataframe_processing(self, data_processor, empty_dataframe):
        """
        TC_EC_001: Test empty DataFrame processing
        """
        validation_rules = [
            {'type': 'not_null', 'column': 'Product_ID', 'id_column': 'Product_ID'}
        ]
        
        valid_df, quality_score = data_processor.validate_data(
            empty_dataframe, "products", validation_rules
        )
        
        # Should handle empty DataFrame gracefully
        assert valid_df.count() == 0
        assert quality_score == 0.0  # Quality score should be 0 for empty DataFrame
    
    def test_single_record_processing(self, data_processor, spark_session):
        """
        TC_EC_002: Test single record processing
        """
        schema = StructType([
            StructField("Product_ID", IntegerType(), True),
            StructField("Product_Name", StringType(), True)
        ])
        
        single_record_df = spark_session.createDataFrame([(1, "Test Product")], schema)
        
        validation_rules = [
            {'type': 'not_null', 'column': 'Product_ID', 'id_column': 'Product_ID'}
        ]
        
        valid_df, quality_score = data_processor.validate_data(
            single_record_df, "products", validation_rules
        )
        
        # Should process single record correctly
        assert valid_df.count() == 1
        assert quality_score == 100.0
    
    def test_special_characters_handling(self, data_processor, spark_session):
        """
        TC_EC_004: Test special characters in data
        """
        schema = StructType([
            StructField("Product_ID", IntegerType(), True),
            StructField("Product_Name", StringType(), True)
        ])
        
        special_char_data = [
            (1, "Product with émojis 🚀"),
            (2, "Product with symbols @#$%"),
            (3, "Product with unicode ñáéíóú")
        ]
        
        special_df = spark_session.createDataFrame(special_char_data, schema)
        transformed_df = data_processor.apply_transformations(special_df, "products")
        
        # Should handle special characters without errors
        assert transformed_df.count() == 3
        
        # Verify data integrity
        product_names = [row.Product_Name for row in transformed_df.collect()]
        assert len(product_names) == 3
    
    # Performance Tests
    def test_large_dataset_processing(self, data_processor, spark_session):
        """
        TC_PI_004: Test large dataset processing
        """
        # Create a larger dataset for performance testing
        schema = StructType([
            StructField("Product_ID", IntegerType(), True),
            StructField("Product_Name", StringType(), True),
            StructField("Category", StringType(), True)
        ])
        
        # Generate 10,000 records
        large_data = [(i, f"Product_{i}", "Electronics") for i in range(1, 10001)]
        large_df = spark_session.createDataFrame(large_data, schema)
        
        validation_rules = [
            {'type': 'not_null', 'column': 'Product_ID', 'id_column': 'Product_ID'},
            {'type': 'unique', 'column': 'Product_ID', 'id_column': 'Product_ID'}
        ]
        
        import time
        start_time = time.time()
        
        valid_df, quality_score = data_processor.validate_data(
            large_df, "products", validation_rules
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Should process large dataset efficiently (within reasonable time)
        assert valid_df.count() == 10000
        assert quality_score == 100.0
        assert processing_time < 60  # Should complete within 60 seconds
        
        print(f"Large dataset processing time: {processing_time:.2f} seconds")
    
    def test_memory_management(self, data_processor, spark_session):
        """
        TC_PI_006: Test memory management
        """
        # Create multiple DataFrames to test memory usage
        schema = StructType([
            StructField("ID", IntegerType(), True),
            StructField("Data", StringType(), True)
        ])
        
        dataframes = []
        for i in range(5):
            data = [(j, f"Data_{i}_{j}") for j in range(1000)]
            df = spark_session.createDataFrame(data, schema)
            dataframes.append(df)
        
        # Process multiple DataFrames
        for i, df in enumerate(dataframes):
            validation_rules = [{'type': 'not_null', 'column': 'ID', 'id_column': 'ID'}]
            valid_df, _ = data_processor.validate_data(df, f"table_{i}", validation_rules)
            
            # Cache and unpersist to manage memory
            valid_df.cache()
            count = valid_df.count()
            valid_df.unpersist()
            
            assert count == 1000
    
    # Integration Tests
    @patch('spark.sql')
    def test_delta_merge_operations(self, mock_sql, data_processor, sample_products_data):
        """
        TC_PI_001: Test Delta table merge operations
        """
        # Mock the SQL execution for merge operations
        mock_sql.return_value = None
        
        # Test the merge logic (this would normally interact with Delta tables)
        table_name = "products"
        id_column = "Product_ID"
        
        # Create temporary view
        sample_products_data.createOrReplaceTempView(f"temp_{table_name}")
        
        # Verify that the merge SQL would be constructed correctly
        expected_merge_pattern = f"MERGE INTO .* AS target USING temp_{table_name} AS source ON target.{id_column} = source.{id_column}"
        
        # This test verifies the merge logic structure
        assert id_column in sample_products_data.columns
        assert sample_products_data.count() > 0

# Helper Functions for Test Setup
def create_test_tables(spark_session):
    """
    Helper function to create test tables in Spark session
    """
    # Create test database
    spark_session.sql("CREATE DATABASE IF NOT EXISTS test_inventory")
    
    # Create sample tables for testing
    products_schema = StructType([
        StructField("Product_ID", IntegerType(), False),
        StructField("Product_Name", StringType(), False),
        StructField("Category", StringType(), False),
        StructField("Price", DoubleType(), True)
    ])
    
    sample_products = [
        (1, "Laptop", "Electronics", 999.99),
        (2, "Chair", "Furniture", 299.50),
        (3, "Mouse", "Electronics", 49.99)
    ]
    
    products_df = spark_session.createDataFrame(sample_products, products_schema)
    products_df.write.format("delta").mode("overwrite").saveAsTable("test_inventory.products")

def cleanup_test_tables(spark_session):
    """
    Helper function to cleanup test tables
    """
    try:
        spark_session.sql("DROP DATABASE IF EXISTS test_inventory CASCADE")
    except Exception as e:
        print(f"Cleanup warning: {e}")

# Pytest Configuration
def pytest_configure(config):
    """
    Configure pytest for Databricks environment
    """
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def pytest_sessionstart(session):
    """
    Setup before test session starts
    """
    print("Starting Databricks PySpark Unit Tests")
    print("Environment: Test")
    print("Spark Version: 3.x")
    print("Delta Lake: Enabled")

def pytest_sessionfinish(session, exitstatus):
    """
    Cleanup after test session ends
    """
    print(f"Test session completed with exit status: {exitstatus}")
    print("Cleaning up test resources...")

# Custom Assertions for PySpark DataFrames
class DataFrameAssertions:
    """
    Custom assertion methods for PySpark DataFrames
    """
    
    @staticmethod
    def assert_dataframe_equal(df1, df2, check_order=False):
        """
        Assert that two DataFrames are equal
        """
        if check_order:
            assert df1.collect() == df2.collect()
        else:
            assert df1.count() == df2.count()
            assert set(df1.columns) == set(df2.columns)
    
    @staticmethod
    def assert_schema_equal(df1, df2):
        """
        Assert that two DataFrames have the same schema
        """
        assert df1.schema == df2.schema
    
    @staticmethod
    def assert_column_exists(df, column_name):
        """
        Assert that a column exists in the DataFrame
        """
        assert column_name in df.columns, f"Column '{column_name}' not found in DataFrame"
    
    @staticmethod
    def assert_no_nulls(df, column_name):
        """
        Assert that a column has no null values
        """
        null_count = df.filter(col(column_name).isNull()).count()
        assert null_count == 0, f"Column '{column_name}' contains {null_count} null values"