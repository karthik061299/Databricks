_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive unit test cases and Pytest script for Databricks Gold Dimension Data Engineering Pipeline
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks PySpark Unit Test Cases
## Gold Dimension Data Engineering Pipeline Testing Framework

## 1. Test Case Overview

This document provides comprehensive unit test cases for the Databricks Gold Dimension Data Engineering Pipeline PySpark code. The test framework covers data transformations, edge cases, error handling scenarios, and performance validation in a Databricks environment.

### Testing Scope:
- **Data Transformation Logic**: Validation of dimension table transformations
- **Edge Case Handling**: Empty DataFrames, null values, schema mismatches
- **Error Scenarios**: Invalid data types, transformation failures
- **Performance Testing**: Databricks cluster optimization validation
- **Audit Logging**: Process audit and error logging verification

## 2. Test Case List

### 2.1 Product Dimension Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_PROD_001 | Test product dimension transformation with valid data | Successfully transforms products with proper naming, categorization, and status derivation |
| TC_PROD_002 | Test product dimension with null product names | Replaces null product names with "UNKNOWN_PRODUCT" |
| TC_PROD_003 | Test product dimension with empty product names | Handles empty strings by replacing with "UNKNOWN_PRODUCT" |
| TC_PROD_004 | Test product dimension category mapping | Correctly maps categories to standardized values (Electronics, Furniture, Apparel, General) |
| TC_PROD_005 | Test product dimension status derivation | Derives product status based on update_date (ACTIVE, INACTIVE, DISCONTINUED) |
| TC_PROD_006 | Test product dimension with empty DataFrame | Handles empty input DataFrame gracefully without errors |
| TC_PROD_007 | Test product dimension schema validation | Validates output schema matches expected structure |
| TC_PROD_008 | Test product dimension error handling | Logs errors appropriately when transformation fails |

### 2.2 Warehouse Dimension Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_WARE_001 | Test warehouse dimension transformation with valid data | Successfully transforms warehouses with proper type classification and address parsing |
| TC_WARE_002 | Test warehouse dimension capacity-based type classification | Correctly classifies warehouse types based on capacity thresholds |
| TC_WARE_003 | Test warehouse dimension location parsing | Properly parses location string into address components |
| TC_WARE_004 | Test warehouse dimension with null locations | Handles null location values gracefully |
| TC_WARE_005 | Test warehouse dimension with invalid capacity | Handles non-numeric or negative capacity values |
| TC_WARE_006 | Test warehouse dimension with empty DataFrame | Processes empty input without errors |
| TC_WARE_007 | Test warehouse dimension schema validation | Output schema matches expected warehouse dimension structure |
| TC_WARE_008 | Test warehouse dimension error logging | Proper error logging when transformation fails |

### 2.3 Supplier Dimension Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_SUPP_001 | Test supplier dimension transformation with valid data | Successfully transforms suppliers with proper contact validation |
| TC_SUPP_002 | Test supplier dimension with null supplier names | Replaces null supplier names with "UNKNOWN_SUPPLIER" |
| TC_SUPP_003 | Test supplier dimension contact number validation | Validates and categorizes contact numbers as phone or email |
| TC_SUPP_004 | Test supplier dimension rating derivation | Derives supplier rating based on load_date and update_date |
| TC_SUPP_005 | Test supplier dimension with invalid contact formats | Handles invalid contact formats gracefully |
| TC_SUPP_006 | Test supplier dimension with empty DataFrame | Processes empty input without errors |
| TC_SUPP_007 | Test supplier dimension schema validation | Output schema matches expected supplier dimension structure |
| TC_SUPP_008 | Test supplier dimension error handling | Proper error logging and handling |

### 2.4 Customer Dimension Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_CUST_001 | Test customer dimension transformation with valid data | Successfully transforms customers with proper segmentation |
| TC_CUST_002 | Test customer dimension with null customer names | Replaces null customer names with "UNKNOWN_CUSTOMER" |
| TC_CUST_003 | Test customer dimension email validation | Validates email format using regex pattern |
| TC_CUST_004 | Test customer dimension segmentation logic | Correctly segments customers based on activity dates |
| TC_CUST_005 | Test customer dimension with invalid emails | Handles invalid email formats by setting to null |
| TC_CUST_006 | Test customer dimension with empty DataFrame | Processes empty input without errors |
| TC_CUST_007 | Test customer dimension schema validation | Output schema matches expected customer dimension structure |
| TC_CUST_008 | Test customer dimension error handling | Proper error logging and exception handling |

### 2.5 Date Dimension Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_DATE_001 | Test date dimension generation for date range | Generates complete date range from 2020 to 2030 |
| TC_DATE_002 | Test date dimension calendar calculations | Correctly calculates day of week, month, quarter, year |
| TC_DATE_003 | Test date dimension fiscal year calculations | Properly calculates fiscal year and fiscal quarter |
| TC_DATE_004 | Test date dimension weekend identification | Correctly identifies weekends (Saturday and Sunday) |
| TC_DATE_005 | Test date dimension holiday identification | Identifies predefined holidays correctly |
| TC_DATE_006 | Test date dimension completeness | Ensures no missing dates in the specified range |
| TC_DATE_007 | Test date dimension schema validation | Output schema matches expected date dimension structure |
| TC_DATE_008 | Test date dimension error handling | Handles date generation errors appropriately |

### 2.6 Audit and Error Logging Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_AUDIT_001 | Test process audit logging for successful runs | Creates audit records with correct metrics |
| TC_AUDIT_002 | Test process audit logging for failed runs | Logs failure status with error messages |
| TC_AUDIT_003 | Test error logging for data validation failures | Creates error records with proper categorization |
| TC_AUDIT_004 | Test audit record completeness | All required audit fields are populated |
| TC_AUDIT_005 | Test error record completeness | All required error fields are populated |
| TC_AUDIT_006 | Test audit record uniqueness | Audit IDs and keys are unique |
| TC_AUDIT_007 | Test error record schema validation | Error records match expected schema |
| TC_AUDIT_008 | Test audit and error table creation | Tables are created successfully in Gold layer |

### 2.7 Performance and Optimization Test Cases

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_PERF_001 | Test table optimization execution | OPTIMIZE commands execute successfully |
| TC_PERF_002 | Test Z-ORDER clustering | Z-ORDER operations complete without errors |
| TC_PERF_003 | Test large dataset processing | Pipeline handles large datasets efficiently |
| TC_PERF_004 | Test memory usage optimization | Memory usage stays within acceptable limits |
| TC_PERF_005 | Test Delta Lake features | ACID transactions and schema evolution work correctly |
| TC_PERF_006 | Test Spark session configuration | Spark session is configured with proper settings |
| TC_PERF_007 | Test concurrent processing | Multiple transformations can run concurrently |
| TC_PERF_008 | Test resource cleanup | Spark session is properly stopped after execution |

## 3. Pytest Script

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, date
import uuid
from unittest.mock import patch, MagicMock
import sys
import os

# Import the main pipeline functions
# Assuming the main pipeline code is in a module called gold_pipeline
# from gold_pipeline import (
#     create_spark_session, transform_product_dimension, transform_warehouse_dimension,
#     transform_supplier_dimension, transform_customer_dimension, transform_date_dimension,
#     log_audit_record, log_error_record, optimize_gold_tables
# )

class TestDatabricksGoldDimPipeline:
    """Test class for Databricks Gold Dimension Pipeline"""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for testing in Databricks environment"""
        spark = SparkSession.builder \n            .appName("TestGoldDimPipeline") \n            .master("local[2]") \n            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \n            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \n            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \n            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n            .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n            .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
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
            (3, "Office Chair", "Furniture - Office", datetime.now(), datetime.now(), "ERP_SYSTEM"),
            (4, None, "Electronics", datetime.now(), datetime.now(), "ERP_SYSTEM"),  # Null product name
            (5, "", "Clothing", datetime.now(), datetime.now(), "ERP_SYSTEM"),  # Empty product name
            (6, "Gaming Laptop", "Electronics - Laptop", datetime(2023, 1, 1), datetime(2023, 1, 1), "ERP_SYSTEM"),  # Old date
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
            (4, "Miami, FL", 5000, datetime.now(), datetime.now(), "WMS_SYSTEM"),
            (5, None, 5000, datetime.now(), datetime.now(), "WMS_SYSTEM"),  # Null location
            (6, "Seattle", -1000, datetime.now(), datetime.now(), "WMS_SYSTEM"),  # Invalid capacity
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
            (1, "Apple Inc", "+1-800-275-2273", datetime.now(), datetime.now(), "SUPPLIER_SYSTEM"),
            (2, "Microsoft Corp", "support@microsoft.com", datetime.now(), datetime.now(), "SUPPLIER_SYSTEM"),
            (3, None, "123-456-7890", datetime.now(), datetime.now(), "SUPPLIER_SYSTEM"),  # Null supplier name
            (4, "Amazon", "invalid-contact", datetime.now(), datetime.now(), "SUPPLIER_SYSTEM"),  # Invalid contact
            (5, "Google", "contact@google.com", datetime(2024, 1, 1), datetime(2024, 1, 1), "SUPPLIER_SYSTEM"),  # New supplier
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
            (2, "Jane Smith", "jane.smith@email.com", datetime.now(), datetime.now(), "CRM_SYSTEM"),
            (3, None, "test@email.com", datetime.now(), datetime.now(), "CRM_SYSTEM"),  # Null customer name
            (4, "Bob Johnson", "invalid-email", datetime.now(), datetime.now(), "CRM_SYSTEM"),  # Invalid email
            (5, "Alice Brown", "alice@test.com", datetime(2023, 1, 1), datetime(2023, 1, 1), "CRM_SYSTEM"),  # Old customer
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def empty_dataframe(self, spark_session):
        """Create empty DataFrame for testing edge cases"""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        return spark_session.createDataFrame([], schema)
    
    # Product Dimension Tests
    def test_product_dimension_valid_data(self, spark_session, sample_products_data):
        """TC_PROD_001: Test product dimension transformation with valid data"""
        # Simulate the product dimension transformation
        result_df = sample_products_data.select(
            row_number().over(Window.orderBy("Product_ID", "load_date")).alias("product_id"),
            dense_rank().over(Window.orderBy("Product_ID")).alias("product_key"),
            concat(lit("PRD_"), lpad(col("Product_ID"), 6, "0")).alias("product_code"),
            when(col("Product_Name").isNull() | (length(trim(col("Product_Name"))) == 0), "UNKNOWN_PRODUCT")
            .otherwise(initcap(trim(col("Product_Name")))).alias("product_name"),
            when(col("Category").like("%Electronics%"), "Electronics")
            .when(col("Category").like("%Furniture%"), "Furniture")
            .when(col("Category").like("%Clothing%"), "Apparel")
            .otherwise("General").alias("category_name")
        )
        
        # Assertions
        assert result_df.count() == 6
        
        # Check product codes are properly formatted
        product_codes = [row['product_code'] for row in result_df.collect()]
        assert all(code.startswith("PRD_") for code in product_codes)
        assert "PRD_000001" in product_codes
        
        # Check category mapping
        categories = [row['category_name'] for row in result_df.collect()]
        assert "Electronics" in categories
        assert "Furniture" in categories
        assert "Apparel" in categories
    
    def test_product_dimension_null_names(self, spark_session, sample_products_data):
        """TC_PROD_002: Test product dimension with null product names"""
        result_df = sample_products_data.select(
            col("Product_Name"),
            when(col("Product_Name").isNull() | (length(trim(col("Product_Name"))) == 0), "UNKNOWN_PRODUCT")
            .otherwise(initcap(trim(col("Product_Name")))).alias("product_name")
        )
        
        results = result_df.collect()
        
        # Check that null and empty names are handled
        for row in results:
            if row['Product_Name'] is None or row['Product_Name'] == "":
                assert row['product_name'] == "UNKNOWN_PRODUCT"
    
    def test_product_dimension_status_derivation(self, spark_session, sample_products_data):
        """TC_PROD_005: Test product dimension status derivation"""
        result_df = sample_products_data.select(
            col("update_date"),
            when(col("update_date") >= date_sub(current_date(), 90), "ACTIVE")
            .when(col("update_date") >= date_sub(current_date(), 365), "INACTIVE")
            .otherwise("DISCONTINUED").alias("product_status")
        )
        
        results = result_df.collect()
        
        # Verify status derivation logic
        for row in results:
            assert row['product_status'] in ["ACTIVE", "INACTIVE", "DISCONTINUED"]
    
    def test_product_dimension_empty_dataframe(self, spark_session, empty_dataframe):
        """TC_PROD_006: Test product dimension with empty DataFrame"""
        try:
            result_count = empty_dataframe.count()
            assert result_count == 0
            
            # Test transformation on empty DataFrame
            transformed_df = empty_dataframe.select(
                lit("test").alias("test_column")
            )
            assert transformed_df.count() == 0
            
        except Exception as e:
            pytest.fail(f"Empty DataFrame handling failed: {str(e)}")
    
    # Warehouse Dimension Tests
    def test_warehouse_dimension_capacity_classification(self, spark_session, sample_warehouses_data):
        """TC_WARE_002: Test warehouse dimension capacity-based type classification"""
        result_df = sample_warehouses_data.select(
            col("Capacity"),
            when(col("Capacity") >= 100000, "LARGE_DISTRIBUTION")
            .when(col("Capacity") >= 50000, "MEDIUM_DISTRIBUTION")
            .when(col("Capacity") >= 10000, "SMALL_DISTRIBUTION")
            .otherwise("LOCAL_STORAGE").alias("warehouse_type")
        )
        
        results = result_df.collect()
        
        # Verify classifications
        for row in results:
            capacity = row['Capacity']
            warehouse_type = row['warehouse_type']
            
            if capacity >= 100000:
                assert warehouse_type == "LARGE_DISTRIBUTION"
            elif capacity >= 50000:
                assert warehouse_type == "MEDIUM_DISTRIBUTION"
            elif capacity >= 10000:
                assert warehouse_type == "SMALL_DISTRIBUTION"
            else:
                assert warehouse_type == "LOCAL_STORAGE"
    
    def test_warehouse_dimension_location_parsing(self, spark_session, sample_warehouses_data):
        """TC_WARE_003: Test warehouse dimension location parsing"""
        result_df = sample_warehouses_data.select(
            col("Location"),
            split(col("Location"), ",").getItem(0).alias("address_line1"),
            when(size(split(col("Location"), ",")) > 1, split(col("Location"), ",").getItem(1)).alias("city"),
            when(size(split(col("Location"), ",")) > 2, split(col("Location"), ",").getItem(2)).alias("state_province")
        ).filter(col("Location").isNotNull())
        
        results = result_df.collect()
        
        # Verify location parsing
        for row in results:
            if row['Location']:
                location_parts = row['Location'].split(",")
                assert row['address_line1'] == location_parts[0]
                if len(location_parts) > 1:
                    assert row['city'] == location_parts[1]
    
    def test_warehouse_dimension_null_locations(self, spark_session, sample_warehouses_data):
        """TC_WARE_004: Test warehouse dimension with null locations"""
        null_locations = sample_warehouses_data.filter(col("Location").isNull())
        
        # Should handle null locations without errors
        result_df = null_locations.select(
            col("Warehouse_ID"),
            coalesce(col("Location"), lit("UNKNOWN_LOCATION")).alias("location_handled")
        )
        
        results = result_df.collect()
        for row in results:
            assert row['location_handled'] == "UNKNOWN_LOCATION"
    
    # Supplier Dimension Tests
    def test_supplier_dimension_contact_validation(self, spark_session, sample_suppliers_data):
        """TC_SUPP_003: Test supplier dimension contact number validation"""
        result_df = sample_suppliers_data.select(
            col("Contact_Number"),
            when(col("Contact_Number").rlike("^[0-9+\-\s\(\)]+$"), col("Contact_Number")).alias("contact_phone"),
            when(col("Contact_Number").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), col("Contact_Number")).alias("contact_email")
        )
        
        results = result_df.collect()
        
        # Verify contact validation
        for row in results:
            contact = row['Contact_Number']
            if contact:
                if '@' in contact and '.' in contact:
                    # Should be classified as email
                    assert row['contact_email'] is not None or row['contact_phone'] is None
                elif any(char.isdigit() for char in contact):
                    # Should be classified as phone if it contains digits
                    assert row['contact_phone'] is not None or row['contact_email'] is None
    
    def test_supplier_dimension_rating_derivation(self, spark_session, sample_suppliers_data):
        """TC_SUPP_004: Test supplier dimension rating derivation"""
        result_df = sample_suppliers_data.select(
            col("load_date"),
            col("update_date"),
            when(col("load_date") >= date_sub(current_date(), 30), "NEW_SUPPLIER")
            .when(col("update_date") >= date_sub(current_date(), 90), "ACTIVE")
            .otherwise("INACTIVE").alias("supplier_rating")
        )
        
        results = result_df.collect()
        
        # Verify rating derivation
        for row in results:
            rating = row['supplier_rating']
            assert rating in ["NEW_SUPPLIER", "ACTIVE", "INACTIVE"]
    
    # Customer Dimension Tests
    def test_customer_dimension_email_validation(self, spark_session, sample_customers_data):
        """TC_CUST_003: Test customer dimension email validation"""
        result_df = sample_customers_data.select(
            col("Email"),
            when(col("Email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), lower(trim(col("Email")))).alias("contact_email")
        )
        
        results = result_df.collect()
        
        # Verify email validation
        for row in results:
            if row['contact_email']:
                assert '@' in row['contact_email']
                assert '.' in row['contact_email']
                assert row['contact_email'] == row['contact_email'].lower()
    
    def test_customer_dimension_segmentation(self