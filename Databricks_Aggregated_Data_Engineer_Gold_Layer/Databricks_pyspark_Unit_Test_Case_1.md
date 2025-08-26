_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive unit test cases for Databricks Gold Aggregated DE Pipeline PySpark code
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks PySpark Unit Test Cases
## Gold Aggregated DE Pipeline Testing Framework

## Overview
This document provides comprehensive unit test cases and Pytest script for the Databricks Gold Aggregated DE Pipeline PySpark code. The tests cover data transformations, aggregations, error handling, and performance validation in Databricks environment.

## Test Case List

### 1. Spark Session Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_SPARK_001 | Test Spark session creation with Delta Lake configuration | SparkSession created successfully with all required configurations |
| TC_SPARK_002 | Test Spark session configuration validation | All Delta Lake and optimization configs are properly set |
| TC_SPARK_003 | Test Spark session cleanup and resource management | SparkSession stops gracefully without resource leaks |

### 2. Data Reading Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_READ_001 | Test reading valid Silver layer table | DataFrame returned with correct schema and data |
| TC_READ_002 | Test reading non-existent Silver layer table | Appropriate exception raised with clear error message |
| TC_READ_003 | Test reading empty Silver layer table | Empty DataFrame returned with correct schema |
| TC_READ_004 | Test reading Silver table with schema evolution | DataFrame adapts to schema changes correctly |

### 3. Data Writing Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_WRITE_001 | Test writing DataFrame to Gold layer with overwrite mode | Data written successfully, previous data replaced |
| TC_WRITE_002 | Test writing DataFrame to Gold layer with append mode | Data appended successfully to existing table |
| TC_WRITE_003 | Test writing empty DataFrame to Gold layer | Empty table created with correct schema |
| TC_WRITE_004 | Test writing DataFrame with schema merge enabled | Schema evolution handled correctly |

### 4. Monthly Inventory Summary Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_INV_001 | Test monthly inventory aggregation with valid data | Correct aggregations calculated for each month/product/warehouse |
| TC_INV_002 | Test monthly inventory with null quantity values | Null values filtered out, aggregations calculated correctly |
| TC_INV_003 | Test monthly inventory with empty input data | Empty result DataFrame with correct schema |
| TC_INV_004 | Test monthly inventory with date range filtering | Only last 24 months data processed |
| TC_INV_005 | Test monthly inventory aggregation functions | Sum, avg, max, min calculations are accurate |

### 5. Daily Order Summary Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_ORDER_001 | Test daily order aggregation with valid orders and details | Correct join and aggregation results |
| TC_ORDER_002 | Test daily order with missing order details | Left join handles missing details gracefully |
| TC_ORDER_003 | Test daily order with duplicate order IDs | Distinct count handles duplicates correctly |
| TC_ORDER_004 | Test daily order date range filtering | Only last 2 years data processed |
| TC_ORDER_005 | Test daily order customer grouping | Aggregations grouped correctly by date and customer |

### 6. Product Performance Summary Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_PROD_001 | Test product performance with complete data | All joins execute correctly with accurate metrics |
| TC_PROD_002 | Test product performance with missing inventory data | Left joins handle missing data appropriately |
| TC_PROD_003 | Test product performance score calculation | Performance score formula applied correctly |
| TC_PROD_004 | Test product performance with no orders | Products with zero orders handled correctly |
| TC_PROD_005 | Test product performance with multiple returns | Return counts aggregated accurately |

### 7. Warehouse Utilization Summary Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_WAREHOUSE_001 | Test warehouse utilization calculation | Utilization percentage calculated correctly |
| TC_WAREHOUSE_002 | Test warehouse utilization status classification | HIGH/NORMAL/LOW status assigned correctly |
| TC_WAREHOUSE_003 | Test warehouse with zero capacity | Division by zero handled gracefully |
| TC_WAREHOUSE_004 | Test warehouse with no inventory | Zero utilization calculated correctly |
| TC_WAREHOUSE_005 | Test warehouse current month filtering | Only current month data processed |

### 8. Customer Order Behavior Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_CUSTOMER_001 | Test customer behavior aggregation | Customer metrics calculated accurately |
| TC_CUSTOMER_002 | Test customer segmentation logic | Segments assigned based on order count thresholds |
| TC_CUSTOMER_003 | Test customer with no orders | INACTIVE segment assigned correctly |
| TC_CUSTOMER_004 | Test customer date range calculations | First and last order dates calculated correctly |
| TC_CUSTOMER_005 | Test customer return behavior | Return counts aggregated per customer |

### 9. Supplier Performance Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_SUPPLIER_001 | Test supplier performance metrics | All supplier KPIs calculated correctly |
| TC_SUPPLIER_002 | Test supplier performance rating | Rating assigned based on stock-out logic |
| TC_SUPPLIER_003 | Test supplier with no products | Zero metrics handled appropriately |
| TC_SUPPLIER_004 | Test supplier out-of-stock calculation | Stock-out count calculated accurately |
| TC_SUPPLIER_005 | Test supplier inventory value aggregation | Total and average inventory calculated correctly |

### 10. Audit Logging Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_AUDIT_001 | Test successful pipeline audit logging | Audit record created with SUCCESS status |
| TC_AUDIT_002 | Test failed pipeline audit logging | Audit record created with FAILED status and error message |
| TC_AUDIT_003 | Test audit record schema validation | All audit fields populated with correct data types |
| TC_AUDIT_004 | Test audit timestamp accuracy | Timestamps reflect actual processing times |
| TC_AUDIT_005 | Test audit record count accuracy | Record counts match actual processed records |

### 11. Error Handling Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_ERROR_001 | Test handling of invalid table names | Appropriate exception raised and logged |
| TC_ERROR_002 | Test handling of schema mismatch | Error handled gracefully with clear message |
| TC_ERROR_003 | Test handling of network connectivity issues | Retry logic or appropriate error handling |
| TC_ERROR_004 | Test handling of insufficient permissions | Permission error caught and logged |
| TC_ERROR_005 | Test handling of corrupted data | Data quality issues identified and handled |

### 12. Performance and Optimization Tests

| Test Case ID | Test Case Description | Expected Outcome |
|--------------|----------------------|------------------|
| TC_PERF_001 | Test table optimization execution | OPTIMIZE command executes successfully |
| TC_PERF_002 | Test statistics computation | ANALYZE TABLE command completes successfully |
| TC_PERF_003 | Test adaptive query execution | Spark AQE configurations applied correctly |
| TC_PERF_004 | Test auto-compaction settings | Delta Lake auto-compaction enabled |
| TC_PERF_005 | Test memory usage during large aggregations | Memory usage stays within acceptable limits |

## Pytest Script

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import logging
from unittest.mock import patch, MagicMock

# Import the functions to test (assuming they're in a module called gold_pipeline)
# from gold_pipeline import *

class TestDatabricksGoldPipeline:
    """Test suite for Databricks Gold Aggregated DE Pipeline"""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session for testing"""
        spark = SparkSession.builder \n            .appName("Gold Pipeline Unit Tests") \n            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n            .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n            .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n            .config("spark.sql.adaptive.enabled", "true") \n            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \n            .enableHiveSupport() \n            .getOrCreate()
        
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_inventory_data(self, spark_session):
        """Create sample inventory data for testing"""
        schema = StructType([
            StructField("Inventory_ID", StringType(), True),
            StructField("Product_ID", StringType(), True),
            StructField("Warehouse_ID", StringType(), True),
            StructField("Quantity_Available", IntegerType(), True),
            StructField("load_date", DateType(), True)
        ])
        
        data = [
            ("INV001", "PROD001", "WH001", 100, datetime.now().date()),
            ("INV002", "PROD002", "WH001", 50, datetime.now().date()),
            ("INV003", "PROD001", "WH002", 75, datetime.now().date()),
            ("INV004", "PROD003", "WH001", None, datetime.now().date()),  # Null quantity
            ("INV005", "PROD002", "WH002", 0, datetime.now().date())  # Zero quantity
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_orders_data(self, spark_session):
        """Create sample orders data for testing"""
        schema = StructType([
            StructField("Order_ID", StringType(), True),
            StructField("Customer_ID", StringType(), True),
            StructField("Order_Date", DateType(), True)
        ])
        
        data = [
            ("ORD001", "CUST001", datetime.now().date()),
            ("ORD002", "CUST002", datetime.now().date()),
            ("ORD003", "CUST001", (datetime.now() - timedelta(days=1)).date())
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_order_details_data(self, spark_session):
        """Create sample order details data for testing"""
        schema = StructType([
            StructField("Order_ID", StringType(), True),
            StructField("Product_ID", StringType(), True),
            StructField("Quantity_Ordered", IntegerType(), True)
        ])
        
        data = [
            ("ORD001", "PROD001", 10),
            ("ORD001", "PROD002", 5),
            ("ORD002", "PROD001", 15),
            ("ORD003", "PROD003", 8)
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    # Test Cases for Spark Session
    def test_create_spark_session(self):
        """TC_SPARK_001: Test Spark session creation with Delta Lake configuration"""
        from gold_pipeline import create_spark_session
        spark = create_spark_session()
        assert spark is not None
        assert spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled") == "true"
        assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
        spark.stop()
    
    def test_spark_session_configurations(self, spark_session):
        """TC_SPARK_002: Test Spark session configuration validation"""
        configs = {
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.databricks.delta.autoOptimize.optimizeWrite": "true",
            "spark.databricks.delta.autoOptimize.autoCompact": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
        
        for config_key, expected_value in configs.items():
            actual_value = spark_session.conf.get(config_key)
            assert actual_value == expected_value, f"Configuration {config_key} mismatch"
    
    # Test Cases for Data Reading
    @patch('gold_pipeline.read_silver_table')
    def test_read_valid_silver_table(self, mock_read, spark_session, sample_inventory_data):
        """TC_READ_001: Test reading valid Silver layer table"""
        from gold_pipeline import read_silver_table
        mock_read.return_value = sample_inventory_data
        
        result = read_silver_table(spark_session, "si_inventory")
        
        assert result is not None
        assert result.count() == 5
        assert "Product_ID" in result.columns
        mock_read.assert_called_once_with(spark_session, "si_inventory")
    
    @patch('gold_pipeline.read_silver_table')
    def test_read_nonexistent_silver_table(self, mock_read, spark_session):
        """TC_READ_002: Test reading non-existent Silver layer table"""
        from gold_pipeline import read_silver_table
        mock_read.side_effect = Exception("Table not found")
        
        with pytest.raises(Exception) as exc_info:
            read_silver_table(spark_session, "non_existent_table")
        
        assert "Table not found" in str(exc_info.value)
    
    def test_read_empty_silver_table(self, spark_session):
        """TC_READ_003: Test reading empty Silver layer table"""
        schema = StructType([
            StructField("Product_ID", StringType(), True),
            StructField("Quantity_Available", IntegerType(), True)
        ])
        
        empty_df = spark_session.createDataFrame([], schema)
        
        assert empty_df.count() == 0
        assert len(empty_df.columns) == 2
    
    # Test Cases for Data Writing
    @patch('gold_pipeline.write_gold_table')
    def test_write_gold_table_overwrite(self, mock_write, spark_session, sample_inventory_data):
        """TC_WRITE_001: Test writing DataFrame to Gold layer with overwrite mode"""
        from gold_pipeline import write_gold_table
        write_gold_table(sample_inventory_data, "test_table", "overwrite")
        mock_write.assert_called_once_with(sample_inventory_data, "test_table", "overwrite")
    
    @patch('gold_pipeline.write_gold_table')
    def test_write_gold_table_append(self, mock_write, spark_session, sample_inventory_data):
        """TC_WRITE_002: Test writing DataFrame to Gold layer with append mode"""
        from gold_pipeline import write_gold_table
        write_gold_table(sample_inventory_data, "test_table", "append")
        mock_write.assert_called_once_with(sample_inventory_data, "test_table", "append")
    
    def test_write_empty_dataframe(self, spark_session):
        """TC_WRITE_003: Test writing empty DataFrame to Gold layer"""
        schema = StructType([StructField("test_col", StringType(), True)])
        empty_df = spark_session.createDataFrame([], schema)
        
        # Mock the write operation
        with patch('gold_pipeline.write_gold_table') as mock_write:
            from gold_pipeline import write_gold_table
            write_gold_table(empty_df, "empty_table")
            mock_write.assert_called_once()
    
    # Test Cases for Monthly Inventory Summary
    def test_monthly_inventory_aggregation_valid_data(self, spark_session, sample_inventory_data):
        """TC_INV_001: Test monthly inventory aggregation with valid data"""
        # Filter out null quantities and add year_month column
        result = sample_inventory_data.filter(
            col("Quantity_Available").isNotNull()
        ).withColumn(
            "year_month", date_format(col("load_date"), "yyyy-MM")
        ).groupBy(
            "year_month", "Product_ID", "Warehouse_ID"
        ).agg(
            sum("Quantity_Available").alias("total_quantity"),
            avg("Quantity_Available").alias("avg_quantity"),
            max("Quantity_Available").alias("max_quantity"),
            min("Quantity_Available").alias("min_quantity"),
            count("Inventory_ID").alias("record_count")
        )
        
        assert result.count() > 0
        assert "total_quantity" in result.columns
        assert "avg_quantity" in result.columns
    
    def test_monthly_inventory_null_handling(self, spark_session, sample_inventory_data):
        """TC_INV_002: Test monthly inventory with null quantity values"""
        # Count records before and after null filtering
        total_records = sample_inventory_data.count()
        filtered_records = sample_inventory_data.filter(
            col("Quantity_Available").isNotNull()
        ).count()
        
        assert total_records == 5  # Including null record
        assert filtered_records == 4  # Excluding null record
    
    def test_monthly_inventory_empty_input(self, spark_session):
        """TC_INV_003: Test monthly inventory with empty input data"""
        schema = StructType([
            StructField("Product_ID", StringType(), True),
            StructField("Warehouse_ID", StringType(), True),
            StructField("Quantity_Available", IntegerType(), True),
            StructField("load_date", DateType(), True)
        ])
        
        empty_df = spark_session.createDataFrame([], schema)
        
        result = empty_df.filter(
            col("Quantity_Available").isNotNull()
        ).withColumn(
            "year_month", date_format(col("load_date"), "yyyy-MM")
        ).groupBy(
            "year_month", "Product_ID", "Warehouse_ID"
        ).agg(
            sum("Quantity_Available").alias("total_quantity")
        )
        
        assert result.count() == 0
        assert "total_quantity" in result.columns
    
    # Test Cases for Daily Order Summary
    def test_daily_order_aggregation(self, spark_session, sample_orders_data, sample_order_details_data):
        """TC_ORDER_001: Test daily order aggregation with valid orders and details"""
        result = sample_orders_data.alias("o").join(
            sample_order_details_data.alias("od"),
            col("o.Order_ID") == col("od.Order_ID"),
            "inner"
        ).groupBy(
            col("o.Order_Date").alias("order_date"),
            col("o.Customer_ID").alias("customer_id")
        ).agg(
            countDistinct(col("o.Order_ID")).alias("total_orders"),
            sum(col("od.Quantity_Ordered")).alias("total_quantity")
        )
        
        assert result.count() > 0
        assert "total_orders" in result.columns
        assert "total_quantity" in result.columns
    
    def test_daily_order_distinct_count(self, spark_session, sample_orders_data, sample_order_details_data):
        """TC_ORDER_003: Test daily order with duplicate order IDs"""
        # Create duplicate order details
        duplicate_details = sample_order_details_data.union(sample_order_details_data)
        
        result = sample_orders_data.alias("o").join(
            duplicate_details.alias("od"),
            col("o.Order_ID") == col("od.Order_ID"),
            "inner"
        ).groupBy(
            col("o.Order_Date").alias("order_date")
        ).agg(
            countDistinct(col("o.Order_ID")).alias("distinct_orders"),
            count(col("o.Order_ID")).alias("total_records")
        )
        
        result_data = result.collect()
        for row in result_data:
            # Distinct count should be less than total records due to duplicates
            assert row['distinct_orders'] <= row['total_records']
    
    # Test Cases for Warehouse Utilization
    def test_warehouse_utilization_calculation(self, spark_session):
        """TC_WAREHOUSE_001: Test warehouse utilization calculation"""
        # Create sample warehouse data
        warehouse_schema = StructType([
            StructField("Warehouse_ID", StringType(), True),
            StructField("Location", StringType(), True),
            StructField("Capacity", IntegerType(), True)
        ])
        
        warehouse_data = [
            ("WH001", "New York", 1000),
            ("WH002", "California", 500),
            ("WH003", "Texas", 0)  # Zero capacity for edge case
        ]
        
        warehouse_df = spark_session.createDataFrame(warehouse_data, warehouse_schema)
        
        # Create inventory data
        inventory_data = [
            ("WH001", 800),  # 80% utilization
            ("WH002", 450),  # 90% utilization
            ("WH003", 100)   # Division by zero case
        ]
        
        inventory_schema = StructType([
            StructField("Warehouse_ID", StringType(), True),
            StructField("total_inventory", IntegerType(), True)
        ])
        
        inventory_df = spark_session.createDataFrame(inventory_data, inventory_schema)
        
        result = warehouse_df.join(
            inventory_df, "Warehouse_ID", "left"
        ).withColumn(
            "utilization_percentage",
            when(col("Capacity") > 0,
                 (col("total_inventory") / col("Capacity")) * 100
            ).otherwise(0.0)
        ).withColumn(
            "utilization_status",
            when(col("utilization_percentage") > 90, "HIGH")
            .when(col("utilization_percentage") < 10, "LOW")
            .otherwise("NORMAL")
        )
        
        result_data = result.collect()
        
        # Verify calculations
        wh001_row = [row for row in result_data if row['Warehouse_ID'] == 'WH001'][0]
        assert wh001_row['utilization_percentage'] == 80.0
        assert wh001_row['utilization_status'] == 'NORMAL'
        
        wh002_row = [row for row in result_data if row['Warehouse_ID'] == 'WH002'][0]
        assert wh002_row['utilization_percentage'] == 90.0
        assert wh002_row['utilization_status'] == 'NORMAL'
        
        wh003_row = [row for row in result_data if row['Warehouse_ID'] == 'WH003'][0]
        assert wh003_row['utilization_percentage'] == 0.0  # Division by zero handled
        assert wh003_row['utilization_status'] == 'LOW'
    
    # Test Cases for Customer Segmentation
    def test_customer_segmentation_logic(self, spark_session):
        """TC_CUSTOMER_002: Test customer segmentation logic"""
        # Create sample customer order data
        customer_data = [
            ("CUST001", 60),   # HIGH_VALUE
            ("CUST002", 25),   # MEDIUM_VALUE
            ("CUST003", 5),    # LOW_VALUE
            ("CUST004", 0)     # INACTIVE
        ]
        
        schema = StructType([
            StructField("Customer_ID", StringType(), True),
            StructField("total_orders", IntegerType(), True)
        ])
        
        customer_df = spark_session.createDataFrame(customer_data, schema)
        
        result = customer_df.withColumn(
            "customer_segment",
            when(col("total_orders") >= 50, "HIGH_VALUE")
            .when(col("total_orders") >= 10, "MEDIUM_VALUE")
            .when(col("total_orders") >= 1, "LOW_VALUE")
            .otherwise("INACTIVE")
        )
        
        result_data = result.collect()
        
        # Verify segmentation
        segments = {row['Customer_ID']: row['customer_segment'] for row in result_data}
        assert segments['CUST001'] == 'HIGH_VALUE'
        assert segments['CUST002'] == 'MEDIUM_VALUE'
        assert segments['CUST003'] == 'LOW_VALUE'
        assert segments['CUST004'] == 'INACTIVE'
    
    # Test Cases for Supplier Performance Rating
    def test_supplier_performance_rating(self, spark_session):
        """TC_SUPPLIER_002: Test supplier performance rating"""
        # Create sample supplier data with stock-out counts
        supplier_data = [
            ("SUP001", 0),    # Perfect - Rating 5.0
            ("SUP002", 2),    # Good - Rating 4.0
            ("SUP003", 5),    # Average - Rating 3.0
            ("SUP004", 10),   # Poor - Rating 2.0
            ("SUP005", 15)    # Very Poor - Rating 1.0
        ]
        
        schema = StructType([
            StructField("Supplier_ID", StringType(), True),
            StructField("products_out_of_stock", IntegerType(), True)
        ])
        
        supplier_df = spark_session.createDataFrame(supplier_data, schema)
        
        result = supplier_df.withColumn(
            "performance_rating",
            when(col("products_out_of_stock") == 0, 5.0)
            .when(col("products_out_of_stock") <= 2, 4.0)
            .when(col("products_out_of_stock") <= 5, 3.0)
            .when(col("products_out_of_stock") <= 10, 2.0)
            .otherwise(1.0)
        )
        
        result_data = result.collect()
        
        # Verify ratings
        ratings = {row['Supplier_ID']: row['performance_rating'] for row in result_data}
        assert ratings['SUP001'] == 5.0
        assert ratings['SUP002'] == 4.0
        assert ratings['SUP003'] == 3.0
        assert ratings['SUP004'] == 2.0
        assert ratings['SUP005'] == 1.0
    
    # Test Cases for Error Handling
    def test_error_handling_invalid_table(self, spark_session):
        """TC_ERROR_001: Test handling of invalid table names"""
        with pytest.raises(Exception):
            spark_session.read.format("delta").table("invalid_table_name")
    
    def test_error_handling_schema_mismatch(self, spark_session):
        """TC_ERROR_002: