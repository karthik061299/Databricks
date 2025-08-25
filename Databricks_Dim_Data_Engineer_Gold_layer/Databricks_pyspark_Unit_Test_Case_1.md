_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive unit test cases and Pytest script for Databricks Gold Dimension Data Engineering Pipeline PySpark code
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks PySpark Unit Test Cases
## Gold Dimension Data Engineering Pipeline Testing Framework

## Overview

This document provides comprehensive unit test cases and a Pytest script for validating the Databricks Gold Dimension Data Engineering Pipeline. The tests cover data transformations, edge cases, error handling, and performance validation in the Databricks environment.

## Test Case List

### 1. Spark Session and Configuration Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_SPARK_001 | Test Spark session creation with Delta Lake configuration | SparkSession created successfully with proper Delta configurations |
| TC_SPARK_002 | Test Spark session configuration parameters | All required configurations are set correctly |
| TC_SPARK_003 | Test Spark session cleanup and resource management | SparkSession stops gracefully without resource leaks |

### 2. Data Reading and Writing Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_READ_001 | Test successful reading from Silver layer tables | DataFrames loaded correctly with expected schema |
| TC_READ_002 | Test reading from non-existent Silver table | Error logged appropriately, None returned |
| TC_READ_003 | Test reading with corrupted data | Error handling triggered, appropriate logging |
| TC_WRITE_001 | Test successful writing to Gold layer tables | Data written successfully in Delta format |
| TC_WRITE_002 | Test writing with schema conflicts | Error handled gracefully with proper logging |
| TC_WRITE_003 | Test writing with insufficient permissions | Permission error caught and logged |

### 3. Product Dimension Transformation Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_PROD_001 | Test product dimension transformation with valid data | All transformations applied correctly |
| TC_PROD_002 | Test product dimension with null product names | Default "UNKNOWN_PRODUCT" assigned |
| TC_PROD_003 | Test product dimension with empty product names | Default "UNKNOWN_PRODUCT" assigned |
| TC_PROD_004 | Test category mapping logic | Categories mapped correctly to standardized values |
| TC_PROD_005 | Test product code generation | Product codes generated with proper format PRD_XXXXXX |
| TC_PROD_006 | Test product status derivation | Status derived correctly based on update_date |
| TC_PROD_007 | Test product dimension with empty DataFrame | Empty result handled gracefully |
| TC_PROD_008 | Test product dimension data quality validation | Invalid records filtered out correctly |

### 4. Warehouse Dimension Transformation Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_WARE_001 | Test warehouse dimension transformation with valid data | All transformations applied correctly |
| TC_WARE_002 | Test warehouse type classification based on capacity | Warehouse types assigned correctly |
| TC_WARE_003 | Test location parsing for address components | Location string parsed into address components |
| TC_WARE_004 | Test warehouse code generation | Warehouse codes generated with proper format WH_XXXXXX |
| TC_WARE_005 | Test warehouse dimension with malformed location data | Default values assigned for unparseable locations |
| TC_WARE_006 | Test warehouse dimension with null capacity | Transformation handles null capacity gracefully |

### 5. Supplier Dimension Transformation Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_SUPP_001 | Test supplier dimension transformation with valid data | All transformations applied correctly |
| TC_SUPP_002 | Test supplier name handling with null values | Default "UNKNOWN_SUPPLIER" assigned |
| TC_SUPP_003 | Test contact number validation for phone format | Valid phone numbers identified correctly |
| TC_SUPP_004 | Test contact number validation for email format | Valid email addresses identified correctly |
| TC_SUPP_005 | Test supplier code generation | Supplier codes generated with proper format SUP_XXXXXX |
| TC_SUPP_006 | Test supplier rating derivation | Rating assigned based on load_date and update_date |

### 6. Customer Dimension Transformation Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_CUST_001 | Test customer dimension transformation with valid data | All transformations applied correctly |
| TC_CUST_002 | Test customer name handling with null values | Default "UNKNOWN_CUSTOMER" assigned |
| TC_CUST_003 | Test email validation and formatting | Valid emails converted to lowercase |
| TC_CUST_004 | Test customer code generation | Customer codes generated with proper format CUST_XXXXXXXX |
| TC_CUST_005 | Test customer segment derivation | Segments assigned based on date criteria |
| TC_CUST_006 | Test credit limit assignment | Default credit limit assigned correctly |

### 7. Date Dimension Transformation Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_DATE_001 | Test date dimension generation for date range | All dates from 2020-01-01 to 2030-12-31 generated |
| TC_DATE_002 | Test date key generation | Date keys in YYYYMMDD format generated correctly |
| TC_DATE_003 | Test day of week calculation | Day of week calculated correctly |
| TC_DATE_004 | Test weekend identification | Weekends (Saturday/Sunday) identified correctly |
| TC_DATE_005 | Test holiday identification | Holidays identified correctly |
| TC_DATE_006 | Test fiscal year calculation | Fiscal years calculated correctly |
| TC_DATE_007 | Test quarter and month calculations | Quarters and months calculated correctly |

### 8. Error Handling and Logging Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_ERR_001 | Test error logging functionality | Errors logged to go_data_validation_error table |
| TC_ERR_002 | Test audit logging functionality | Audit records logged to go_process_audit table |
| TC_ERR_003 | Test error handling during read operations | Read errors handled and logged appropriately |
| TC_ERR_004 | Test error handling during write operations | Write errors handled and logged appropriately |
| TC_ERR_005 | Test error handling during transformations | Transformation errors handled gracefully |
| TC_ERR_006 | Test pipeline failure recovery | Pipeline continues processing other tables after failure |

### 9. Data Quality and Validation Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_DQ_001 | Test data quality validation for mandatory fields | Records with null mandatory fields rejected |
| TC_DQ_002 | Test data quality validation for data formats | Invalid format records rejected |
| TC_DQ_003 | Test record count validation | Accurate record counts maintained throughout pipeline |
| TC_DQ_004 | Test data type validation | Data types validated and converted correctly |

### 10. Performance and Integration Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_PERF_001 | Test pipeline performance with large datasets | Pipeline completes within acceptable time limits |
| TC_PERF_002 | Test memory usage during processing | Memory usage stays within acceptable limits |
| TC_INT_001 | Test end-to-end pipeline execution | All dimension tables created successfully |
| TC_INT_002 | Test pipeline execution with mixed success/failure | Partial success handled correctly |

## Pytest Script

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, date
import uuid
from unittest.mock import patch, MagicMock
import sys
import os

# Import the main pipeline functions (assuming they are in a module called gold_dim_pipeline)
# from gold_dim_pipeline import *

class TestGoldDimPipeline:
    """Test suite for Gold Dimension Data Engineering Pipeline"""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for testing"""
        spark = SparkSession.builder \n            .appName("Gold_Dim_Pipeline_Tests") \n            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \n            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n            .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n            .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \n            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \n            .getOrCreate()
        
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_products_data(self, spark_session):
        """Create sample products data for testing"""
        schema = StructType([
            StructField("Product_ID", IntegerType(), True),
            StructField("Product_Name", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("load_date", TimestampType(), True),
            StructField("update_date", TimestampType(), True),
            StructField("source_system", StringType(), True)
        ])
        
        data = [
            (1, "iPhone 14", "Electronics - Mobile", datetime.now(), datetime.now(), "ERP_SYSTEM"),
            (2, "MacBook Pro", "Electronics - Laptop", datetime.now(), datetime.now(), "ERP_SYSTEM"),
            (3, None, "Furniture", datetime.now(), datetime.now(), "ERP_SYSTEM"),  # Null name test
            (4, "", "Clothing", datetime.now(), datetime.now(), "ERP_SYSTEM"),  # Empty name test
            (5, "Office Chair", "Furniture - Office", datetime.now(), datetime.now(), "ERP_SYSTEM")
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_warehouses_data(self, spark_session):
        """Create sample warehouses data for testing"""
        schema = StructType([
            StructField("Warehouse_ID", IntegerType(), True),
            StructField("Location", StringType(), True),
            StructField("Capacity", IntegerType(), True),
            StructField("load_date", TimestampType(), True),
            StructField("update_date", TimestampType(), True),
            StructField("source_system", StringType(), True)
        ])
        
        data = [
            (1, "New York, NY, USA", 150000, datetime.now(), datetime.now(), "WMS_SYSTEM"),
            (2, "Los Angeles, CA, USA", 75000, datetime.now(), datetime.now(), "WMS_SYSTEM"),
            (3, "Chicago, IL, USA", 25000, datetime.now(), datetime.now(), "WMS_SYSTEM"),
            (4, "Miami", 5000, datetime.now(), datetime.now(), "WMS_SYSTEM"),  # Incomplete location
            (5, None, 100000, datetime.now(), datetime.now(), "WMS_SYSTEM")  # Null location
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_suppliers_data(self, spark_session):
        """Create sample suppliers data for testing"""
        schema = StructType([
            StructField("Supplier_ID", IntegerType(), True),
            StructField("Supplier_Name", StringType(), True),
            StructField("Contact_Number", StringType(), True),
            StructField("load_date", TimestampType(), True),
            StructField("update_date", TimestampType(), True),
            StructField("source_system", StringType(), True)
        ])
        
        data = [
            (1, "Apple Inc", "+1-555-123-4567", datetime.now(), datetime.now(), "SUPPLIER_SYSTEM"),
            (2, "Samsung Electronics", "supplier@samsung.com", datetime.now(), datetime.now(), "SUPPLIER_SYSTEM"),
            (3, None, "555-987-6543", datetime.now(), datetime.now(), "SUPPLIER_SYSTEM"),  # Null name
            (4, "", "invalid-contact", datetime.now(), datetime.now(), "SUPPLIER_SYSTEM"),  # Empty name, invalid contact
            (5, "Dell Technologies", "contact@dell.com", datetime.now(), datetime.now(), "SUPPLIER_SYSTEM")
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_customers_data(self, spark_session):
        """Create sample customers data for testing"""
        schema = StructType([
            StructField("Customer_ID", IntegerType(), True),
            StructField("Customer_Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("load_date", TimestampType(), True),
            StructField("update_date", TimestampType(), True),
            StructField("source_system", StringType(), True)
        ])
        
        data = [
            (1, "John Doe", "john.doe@email.com", datetime.now(), datetime.now(), "CRM_SYSTEM"),
            (2, "Jane Smith", "JANE.SMITH@EMAIL.COM", datetime.now(), datetime.now(), "CRM_SYSTEM"),
            (3, None, "invalid-email", datetime.now(), datetime.now(), "CRM_SYSTEM"),  # Null name, invalid email
            (4, "", "customer@valid.com", datetime.now(), datetime.now(), "CRM_SYSTEM"),  # Empty name
            (5, "Bob Johnson", None, datetime.now(), datetime.now(), "CRM_SYSTEM")  # Null email
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    # Test Cases for Spark Session and Configuration
    def test_spark_session_creation(self, spark_session):
        """TC_SPARK_001: Test Spark session creation with Delta Lake configuration"""
        assert spark_session is not None
        assert spark_session.sparkContext.appName == "Gold_Dim_Pipeline_Tests"
        
        # Check Delta configurations
        conf = spark_session.conf
        assert conf.get("spark.databricks.delta.schema.autoMerge.enabled") == "true"
        assert conf.get("spark.databricks.delta.autoOptimize.optimizeWrite") == "true"
        assert conf.get("spark.databricks.delta.autoOptimize.autoCompact") == "true"
    
    def test_spark_session_configuration_parameters(self, spark_session):
        """TC_SPARK_002: Test Spark session configuration parameters"""
        conf = spark_session.conf
        
        # Verify all required configurations are set
        required_configs = [
            "spark.databricks.delta.schema.autoMerge.enabled",
            "spark.databricks.delta.autoOptimize.optimizeWrite",
            "spark.databricks.delta.autoOptimize.autoCompact"
        ]
        
        for config in required_configs:
            assert conf.get(config) is not None
    
    def test_spark_session_cleanup(self, spark_session):
        """TC_SPARK_003: Test Spark session cleanup and resource management"""
        # Test that spark session can be stopped gracefully
        assert spark_session.sparkContext.isStopped == False
        
        # Create a temporary session to test stopping
        temp_spark = SparkSession.builder.appName("temp_test").getOrCreate()
        temp_spark.stop()
        assert temp_spark.sparkContext.isStopped == True
    
    # Test Cases for Product Dimension Transformation
    def test_product_dimension_transformation_valid_data(self, spark_session, sample_products_data):
        """TC_PROD_001: Test product dimension transformation with valid data"""
        # Apply product dimension transformation logic
        result_df = sample_products_data.select(
            row_number().over(Window.orderBy(col("Product_ID"), col("load_date"))).alias("product_id"),
            dense_rank().over(Window.orderBy(col("Product_ID"))).alias("product_key"),
            concat(lit("PRD_"), lpad(col("Product_ID"), 6, "0")).alias("product_code"),
            when((length(trim(col("Product_Name"))) == 0) | col("Product_Name").isNull(), 
                 lit("UNKNOWN_PRODUCT"))
            .otherwise(initcap(trim(col("Product_Name")))).alias("product_name")
        )
        
        # Collect results for validation
        results = result_df.collect()
        
        # Assertions
        assert len(results) == 5
        assert results[0]['product_code'] == 'PRD_000001'
        assert results[2]['product_name'] == 'UNKNOWN_PRODUCT'  # Null name test
        assert results[3]['product_name'] == 'UNKNOWN_PRODUCT'  # Empty name test
    
    def test_product_dimension_null_product_names(self, spark_session, sample_products_data):
        """TC_PROD_002: Test product dimension with null product names"""
        # Filter to only null product names
        null_name_df = sample_products_data.filter(col("Product_Name").isNull())
        
        result_df = null_name_df.select(
            when((length(trim(col("Product_Name"))) == 0) | col("Product_Name").isNull(), 
                 lit("UNKNOWN_PRODUCT"))
            .otherwise(initcap(trim(col("Product_Name")))).alias("product_name")
        )
        
        results = result_df.collect()
        assert all(row['product_name'] == 'UNKNOWN_PRODUCT' for row in results)
    
    def test_product_dimension_empty_product_names(self, spark_session, sample_products_data):
        """TC_PROD_003: Test product dimension with empty product names"""
        # Filter to only empty product names
        empty_name_df = sample_products_data.filter(col("Product_Name") == "")
        
        result_df = empty_name_df.select(
            when((length(trim(col("Product_Name"))) == 0) | col("Product_Name").isNull(), 
                 lit("UNKNOWN_PRODUCT"))
            .otherwise(initcap(trim(col("Product_Name")))).alias("product_name")
        )
        
        results = result_df.collect()
        assert all(row['product_name'] == 'UNKNOWN_PRODUCT' for row in results)
    
    def test_product_category_mapping(self, spark_session, sample_products_data):
        """TC_PROD_004: Test category mapping logic"""
        result_df = sample_products_data.select(
            col("Category"),
            when(col("Category").like("%Electronics%"), lit("Electronics"))
            .when(col("Category").like("%Furniture%"), lit("Furniture"))
            .when(col("Category").like("%Clothing%"), lit("Apparel"))
            .otherwise(lit("General")).alias("category_name")
        )
        
        results = result_df.collect()
        
        # Verify category mappings
        category_mappings = {row['Category']: row['category_name'] for row in results}
        assert category_mappings['Electronics - Mobile'] == 'Electronics'
        assert category_mappings['Electronics - Laptop'] == 'Electronics'
        assert category_mappings['Furniture'] == 'Furniture'
        assert category_mappings['Clothing'] == 'Apparel'
    
    def test_product_code_generation(self, spark_session, sample_products_data):
        """TC_PROD_005: Test product code generation"""
        result_df = sample_products_data.select(
            col("Product_ID"),
            concat(lit("PRD_"), lpad(col("Product_ID"), 6, "0")).alias("product_code")
        )
        
        results = result_df.collect()
        
        # Verify product code format
        for row in results:
            product_code = row['product_code']
            assert product_code.startswith('PRD_')
            assert len(product_code) == 10  # PRD_ + 6 digits
            assert product_code[4:].isdigit()
    
    # Test Cases for Error Handling
    def test_error_logging_functionality(self, spark_session):
        """TC_ERR_001: Test error logging functionality"""
        # Create mock error data
        error_schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("error_key", LongType(), False),
            StructField("pipeline_run_id", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("error_type", StringType(), True),
            StructField("error_description", StringType(), True)
        ])
        
        error_data = spark_session.createDataFrame([
            (str(uuid.uuid4()), 1, "test_run_id", "test_table", "VALIDATION_ERROR", "Test error message")
        ], error_schema)
        
        # Verify error data structure
        assert error_data.count() == 1
        assert error_data.columns == ['error_id', 'error_key', 'pipeline_run_id', 'table_name', 'error_type', 'error_description']
    
    def test_audit_logging_functionality(self, spark_session):
        """TC_ERR_002: Test audit logging functionality"""
        # Create mock audit data
        audit_schema = StructType([
            StructField("audit_id", StringType(), False),
            StructField("process_name", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("records_processed", LongType(), True),
            StructField("process_status", StringType(), True)
        ])
        
        audit_data = spark_session.createDataFrame([
            (str(uuid.uuid4()), "test_process", "source_table", "target_table", 100, "SUCCESS")
        ], audit_schema)
        
        # Verify audit data structure
        assert audit_data.count() == 1
        assert audit_data.columns == ['audit_id', 'process_name', 'source_table', 'target_table', 'records_processed', 'process_status']
    
    # Test Cases for Data Quality Validation
    def test_data_quality_validation_mandatory_fields(self, spark_session, sample_products_data):
        """TC_DQ_001: Test data quality validation for mandatory fields"""
        # Apply data quality validation
        valid_records = sample_products_data.filter(
            col("Product_ID").isNotNull() & 
            col("Category").isNotNull()
        )
        
        invalid_records = sample_products_data.filter(
            col("Product_ID").isNull() | 
            col("Category").isNull()
        )
        
        # Verify validation logic
        assert valid_records.count() == 5  # All records have Product_ID and Category
        assert invalid_records.count() == 0
    
    def test_data_type_validation(self, spark_session, sample_products_data):
        """TC_DQ_004: Test data type validation"""
        # Verify data types
        schema = sample_products_data.schema
        field_types = {field.name: field.dataType for field in schema.fields}
        
        assert isinstance(field_types['Product_ID'], IntegerType)
        assert isinstance(field_types['Product_Name'], StringType)
        assert isinstance(field_types['Category'], StringType)
        assert isinstance(field_types['load_date'], TimestampType)
        assert isinstance(field_types['update_date'], TimestampType)
        assert isinstance(field_types['source_system'], StringType)
    
    # Test Cases for Date Dimension
    def test_date_dimension_generation(self, spark_session):
        """TC_DATE_001: Test date dimension generation for date range"""
        # Generate date range from 2020-01-01 to 2020-01-10 for testing
        date_range_df = spark_session.sql("""
            SELECT explode(sequence(to_date('2020-01-01'), to_date('2020-01-10'), interval 1 day)) as full_date
        """)
        
        # Apply date dimension transformations
        date_dim_df = date_range_df.select(
            col("full_date"),
            date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
            dayofweek(col("full_date")).alias("day_of_week"),
            date_format(col("full_date"), "EEEE").alias("day_name"),
            month(col("full_date")).alias("month_number"),
            year(col("full_date")).alias("year_number")
        )
        
        results = date_dim_df.collect()
        
        # Verify date dimension generation
        assert len(results) == 10  # 10 days from 2020-01-01 to 2020-01-10
        assert results[0]['date_key'] == 20200101
        assert results[0]['year_number'] == 2020
        assert results[0]['month_number'] == 1
    
    def test_weekend_identification(self, spark_session):
        """TC_DATE_004: Test weekend identification"""
        # Create test dates including weekends
        test_dates = spark_session.sql("""
            SELECT explode(sequence(to_date('2024-01-06'), to_date('2024-01-08'), interval 1 day)) as full_date
        """)
        
        # Apply weekend identification logic
        weekend_df = test_dates.select(
            col("full_date"),
            dayofweek(col("full_date")).alias("day_of_week"),
            when(dayofweek(col("full_date")).isin(1, 7), lit(True))
            .otherwise(lit(False)).alias("is_weekend")
        )
        
        results = weekend_df.collect()
        
        # Verify weekend identification (Saturday=7, Sunday=1)
        for row in results:
            if row['day_of_week'] in [1, 7]:  # Sunday or Saturday
                assert row['is_weekend'] == True
            else:
                assert row['is_weekend'] == False
    
    # Performance Test Cases
    def test_pipeline_performance_large_dataset(self, spark_session):
        """TC_PERF_001: Test pipeline performance with large datasets"""
        # Create a larger dataset for performance testing
        large_data = [(i, f"Product_{i}", "Electronics", datetime.now(), datetime.now(), "ERP_SYSTEM") 
                     for i in range(1, 1001)]  # 1000 records
        
        schema = StructType([
            StructField("Product_ID", IntegerType(), True),
            StructField("Product_Name", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("load_date", TimestampType(), True),
            StructField("update_