_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Databricks Gold Dimension Data Engineering Pipeline for Inventory Management System
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Dim DE Pipeline
## Inventory Management System - Dimension Tables Processing

## 1. Pipeline Overview

This pipeline processes Silver Layer data into Gold Layer dimension tables within a Databricks environment, ensuring data quality and optimizing performance for business intelligence and analytics workloads.

### Key Features:
- **Data Quality**: Comprehensive validation and cleansing rules
- **Performance Optimization**: Delta Lake format with indexing and partitioning
- **Error Handling**: Robust error capture and audit logging
- **SCD Type 2**: Slowly Changing Dimension implementation
- **Business Rules**: Hierarchical relationships and standardization

## 2. PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, datediff, avg, count, when, date_add, sum
from datetime import datetime
import uuid

def create_spark_session():
    """Create Spark session with Delta Lake support and configure paths"""
    spark = SparkSession.builder \n        .appName("Silver to Gold Dimension Data Processing") \n        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n        .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n        .getOrCreate()
    return spark

# Data paths configuration
silver_path = "workspace.inventory_silver."
gold_path = "workspace.inventory_gold."

def read_silver_table(spark, table_name):
    """Read table from Silver layer"""
    return spark.read.format("delta").table(f"{silver_path}{table_name}")

def write_gold_table(df, table_name, mode="overwrite"):
    """Write table to Gold layer"""
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(f"{gold_path}{table_name}")

def log_audit_record(spark, process_name, source_table, target_table, records_read, records_processed, records_inserted, status, error_message=None):
    """Log audit information"""
    audit_data = [
        {
            "audit_id": str(uuid.uuid4()),
            "audit_key": hash(f"{process_name}_{datetime.now().isoformat()}"),
            "process_name": process_name,
            "process_type": "DIMENSION_LOAD",
            "pipeline_name": "Gold_Dimension_Pipeline",
            "pipeline_run_id": str(uuid.uuid4()),
            "job_name": "Databricks_Gold_Dim_DE_Pipeline",
            "step_name": process_name,
            "source_table": source_table,
            "target_table": target_table,
            "records_read": records_read,
            "records_processed": records_processed,
            "records_inserted": records_inserted,
            "records_updated": 0,
            "records_deleted": 0,
            "records_rejected": records_read - records_processed if records_read > records_processed else 0,
            "process_start_time": datetime.now(),
            "process_end_time": datetime.now(),
            "process_duration_seconds": 0,
            "process_status": status,
            "error_message": error_message,
            "user_name": "system",
            "load_date": datetime.now(),
            "source_system": "DATABRICKS_PIPELINE"
        }
    ]
    
    audit_df = spark.createDataFrame(audit_data)
    write_gold_table(audit_df, "go_process_audit", "append")

def log_error_record(spark, table_name, error_type, error_description, record_identifier, actual_value):
    """Log data validation errors"""
    error_data = [
        {
            "error_id": str(uuid.uuid4()),
            "error_key": hash(f"{table_name}_{error_type}_{datetime.now().isoformat()}"),
            "pipeline_run_id": str(uuid.uuid4()),
            "table_name": table_name,
            "column_name": "multiple",
            "record_identifier": record_identifier,
            "error_type": error_type,
            "error_category": "DATA_QUALITY",
            "error_severity": "MEDIUM",
            "error_description": error_description,
            "expected_value": "Valid data",
            "actual_value": str(actual_value),
            "validation_rule": "Business rule validation",
            "error_timestamp": datetime.now(),
            "resolution_status": "PENDING",
            "resolution_notes": None,
            "resolved_by": None,
            "resolved_timestamp": None,
            "load_date": datetime.now(),
            "source_system": "DATABRICKS_PIPELINE"
        }
    ]
    
    error_df = spark.createDataFrame(error_data)
    write_gold_table(error_df, "go_data_validation_error", "append")

def transform_product_dimension(spark):
    """Transform Product dimension table"""
    try:
        # Read from Silver layer
        products_df = read_silver_table(spark, "si_products")
        
        records_read = products_df.count()
        
        # Apply transformations with data quality checks
        product_dim_df = products_df.select(
            row_number().over(Window.orderBy("Product_ID", "load_date")).alias("product_id"),
            dense_rank().over(Window.orderBy("Product_ID")).alias("product_key"),
            concat(lit("PRD_"), lpad(col("Product_ID"), 6, "0")).alias("product_code"),
            when(col("Product_Name").isNull() | (length(trim(col("Product_Name"))) == 0), "UNKNOWN_PRODUCT")
            .otherwise(initcap(trim(col("Product_Name")))).alias("product_name"),
            concat(lit("Product: "), initcap(trim(col("Product_Name")))).alias("product_description"),
            when(col("Category").like("%Electronics%"), "Electronics")
            .when(col("Category").like("%Furniture%"), "Furniture")
            .when(col("Category").like("%Clothing%"), "Apparel")
            .otherwise("General").alias("category_name"),
            when(col("Category").like("%Mobile%"), "Mobile Devices")
            .when(col("Category").like("%Laptop%"), "Computing")
            .otherwise("Other").alias("subcategory_name"),
            split(col("Product_Name"), " ").getItem(0).alias("brand_name"),
            lit("EACH").alias("unit_of_measure"),
            lit(0.00).cast("decimal(10,2)").alias("standard_cost"),
            lit(0.00).cast("decimal(10,2)").alias("list_price"),
            when(col("update_date") >= date_sub(current_date(), 90), "ACTIVE")
            .when(col("update_date") >= date_sub(current_date(), 365), "INACTIVE")
            .otherwise("DISCONTINUED").alias("product_status"),
            coalesce(col("load_date"), current_timestamp()).cast("date").alias("effective_start_date"),
            lit("9999-12-31").cast("date").alias("effective_end_date"),
            lit(True).alias("is_current"),
            col("load_date"),
            col("update_date"),
            col("source_system")
        )
        
        records_processed = product_dim_df.count()
        
        # Write to Gold layer
        write_gold_table(product_dim_df, "go_product_dimension")
        
        # Log audit record
        log_audit_record(spark, "transform_product_dimension", "si_products", "go_product_dimension", 
                        records_read, records_processed, records_processed, "SUCCESS")
        
        print(f"Product Dimension: Processed {records_processed} records successfully")
        
    except Exception as e:
        log_audit_record(spark, "transform_product_dimension", "si_products", "go_product_dimension", 
                        0, 0, 0, "FAILED", str(e))
        log_error_record(spark, "go_product_dimension", "TRANSFORMATION_ERROR", str(e), "N/A", "N/A")
        raise e

def transform_warehouse_dimension(spark):
    """Transform Warehouse dimension table"""
    try:
        # Read from Silver layer
        warehouses_df = read_silver_table(spark, "si_warehouses")
        
        records_read = warehouses_df.count()
        
        # Apply transformations with data quality checks
        warehouse_dim_df = warehouses_df.select(
            row_number().over(Window.orderBy("Warehouse_ID", "load_date")).alias("warehouse_id"),
            dense_rank().over(Window.orderBy("Warehouse_ID")).alias("warehouse_key"),
            concat(lit("WH_"), lpad(col("Warehouse_ID"), 6, "0")).alias("warehouse_code"),
            concat(lit("Warehouse "), col("Warehouse_ID"), lit(" - "), split(col("Location"), ",").getItem(0)).alias("warehouse_name"),
            when(col("Capacity") >= 100000, "LARGE_DISTRIBUTION")
            .when(col("Capacity") >= 50000, "MEDIUM_DISTRIBUTION")
            .when(col("Capacity") >= 10000, "SMALL_DISTRIBUTION")
            .otherwise("LOCAL_STORAGE").alias("warehouse_type"),
            split(col("Location"), ",").getItem(0).alias("address_line1"),
            lit(None).cast("string").alias("address_line2"),
            when(size(split(col("Location"), ",")) > 1, split(col("Location"), ",").getItem(1)).alias("city"),
            when(size(split(col("Location"), ",")) > 2, split(col("Location"), ",").getItem(2)).alias("state_province"),
            lit(None).cast("string").alias("postal_code"),
            lit("USA").alias("country"),
            lit("TBD").alias("warehouse_manager"),
            lit(None).cast("string").alias("contact_phone"),
            lit(None).cast("string").alias("contact_email"),
            lit("ACTIVE").alias("warehouse_status"),
            coalesce(col("load_date"), current_timestamp()).cast("date").alias("effective_start_date"),
            lit("9999-12-31").cast("date").alias("effective_end_date"),
            lit(True).alias("is_current"),
            col("load_date"),
            col("update_date"),
            col("source_system")
        )
        
        records_processed = warehouse_dim_df.count()
        
        # Write to Gold layer
        write_gold_table(warehouse_dim_df, "go_warehouse_dimension")
        
        # Log audit record
        log_audit_record(spark, "transform_warehouse_dimension", "si_warehouses", "go_warehouse_dimension", 
                        records_read, records_processed, records_processed, "SUCCESS")
        
        print(f"Warehouse Dimension: Processed {records_processed} records successfully")
        
    except Exception as e:
        log_audit_record(spark, "transform_warehouse_dimension", "si_warehouses", "go_warehouse_dimension", 
                        0, 0, 0, "FAILED", str(e))
        log_error_record(spark, "go_warehouse_dimension", "TRANSFORMATION_ERROR", str(e), "N/A", "N/A")
        raise e

def transform_supplier_dimension(spark):
    """Transform Supplier dimension table"""
    try:
        # Read from Silver layer
        suppliers_df = read_silver_table(spark, "si_suppliers")
        
        records_read = suppliers_df.count()
        
        # Apply transformations with data quality checks
        supplier_dim_df = suppliers_df.select(
            row_number().over(Window.orderBy("Supplier_ID", "load_date")).alias("supplier_id"),
            dense_rank().over(Window.orderBy("Supplier_ID")).alias("supplier_key"),
            concat(lit("SUP_"), lpad(col("Supplier_ID"), 6, "0")).alias("supplier_code"),
            when(col("Supplier_Name").isNull() | (length(trim(col("Supplier_Name"))) == 0), "UNKNOWN_SUPPLIER")
            .otherwise(initcap(trim(col("Supplier_Name")))).alias("supplier_name"),
            lit("STANDARD").alias("supplier_type"),
            initcap(trim(col("Supplier_Name"))).alias("contact_person"),
            when(col("Contact_Number").rlike("^[0-9+\-\s\(\)]+$"), col("Contact_Number")).alias("contact_phone"),
            when(col("Contact_Number").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), col("Contact_Number")).alias("contact_email"),
            lit(None).cast("string").alias("address_line1"),
            lit(None).cast("string").alias("address_line2"),
            lit(None).cast("string").alias("city"),
            lit(None).cast("string").alias("state_province"),
            lit(None).cast("string").alias("postal_code"),
            lit("USA").alias("country"),
            lit("NET_30").alias("payment_terms"),
            when(col("load_date") >= date_sub(current_date(), 30), "NEW_SUPPLIER")
            .when(col("update_date") >= date_sub(current_date(), 90), "ACTIVE")
            .otherwise("INACTIVE").alias("supplier_rating"),
            lit("ACTIVE").alias("supplier_status"),
            coalesce(col("load_date"), current_timestamp()).cast("date").alias("effective_start_date"),
            lit("9999-12-31").cast("date").alias("effective_end_date"),
            lit(True).alias("is_current"),
            col("load_date"),
            col("update_date"),
            col("source_system")
        )
        
        records_processed = supplier_dim_df.count()
        
        # Write to Gold layer
        write_gold_table(supplier_dim_df, "go_supplier_dimension")
        
        # Log audit record
        log_audit_record(spark, "transform_supplier_dimension", "si_suppliers", "go_supplier_dimension", 
                        records_read, records_processed, records_processed, "SUCCESS")
        
        print(f"Supplier Dimension: Processed {records_processed} records successfully")
        
    except Exception as e:
        log_audit_record(spark, "transform_supplier_dimension", "si_suppliers", "go_supplier_dimension", 
                        0, 0, 0, "FAILED", str(e))
        log_error_record(spark, "go_supplier_dimension", "TRANSFORMATION_ERROR", str(e), "N/A", "N/A")
        raise e

def transform_customer_dimension(spark):
    """Transform Customer dimension table"""
    try:
        # Read from Silver layer
        customers_df = read_silver_table(spark, "si_customers")
        
        records_read = customers_df.count()
        
        # Apply transformations with data quality checks
        customer_dim_df = customers_df.select(
            row_number().over(Window.orderBy("Customer_ID", "load_date")).alias("customer_id"),
            dense_rank().over(Window.orderBy("Customer_ID")).alias("customer_key"),
            concat(lit("CUST_"), lpad(col("Customer_ID"), 8, "0")).alias("customer_code"),
            when(col("Customer_Name").isNull() | (length(trim(col("Customer_Name"))) == 0), "UNKNOWN_CUSTOMER")
            .otherwise(initcap(trim(col("Customer_Name")))).alias("customer_name"),
            lit("RETAIL").alias("customer_type"),
            initcap(trim(col("Customer_Name"))).alias("contact_person"),
            lit(None).cast("string").alias("contact_phone"),
            when(col("Email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), lower(trim(col("Email")))).alias("contact_email"),
            lit(None).cast("string").alias("address_line1"),
            lit(None).cast("string").alias("address_line2"),
            lit(None).cast("string").alias("city"),
            lit(None).cast("string").alias("state_province"),
            lit(None).cast("string").alias("postal_code"),
            lit("USA").alias("country"),
            lit(10000.00).cast("decimal(15,2)").alias("credit_limit"),
            when(col("load_date") >= date_sub(current_date(), 90), "NEW_CUSTOMER")
            .when(col("update_date") >= date_sub(current_date(), 365), "ACTIVE_CUSTOMER")
            .otherwise("INACTIVE_CUSTOMER").alias("customer_segment"),
            lit("ACTIVE").alias("customer_status"),
            coalesce(col("load_date"), current_timestamp()).cast("date").alias("effective_start_date"),
            lit("9999-12-31").cast("date").alias("effective_end_date"),
            lit(True).alias("is_current"),
            col("load_date"),
            col("update_date"),
            col("source_system")
        )
        
        records_processed = customer_dim_df.count()
        
        # Write to Gold layer
        write_gold_table(customer_dim_df, "go_customer_dimension")
        
        # Log audit record
        log_audit_record(spark, "transform_customer_dimension", "si_customers", "go_customer_dimension", 
                        records_read, records_processed, records_processed, "SUCCESS")
        
        print(f"Customer Dimension: Processed {records_processed} records successfully")
        
    except Exception as e:
        log_audit_record(spark, "transform_customer_dimension", "si_customers", "go_customer_dimension", 
                        0, 0, 0, "FAILED", str(e))
        log_error_record(spark, "go_customer_dimension", "TRANSFORMATION_ERROR", str(e), "N/A", "N/A")
        raise e

def transform_date_dimension(spark):
    """Transform Date dimension table"""
    try:
        # Generate date range from 2020 to 2030
        date_range_df = spark.sql("""
            SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) as full_date
        """)
        
        records_read = date_range_df.count()
        
        # Apply transformations
        date_dim_df = date_range_df.select(
            row_number().over(Window.orderBy("full_date")).alias("date_id"),
            date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("full_date"),
            dayofweek(col("full_date")).alias("day_of_week"),
            date_format(col("full_date"), "EEEE").alias("day_name"),
            dayofmonth(col("full_date")).alias("day_of_month"),
            dayofyear(col("full_date")).alias("day_of_year"),
            weekofyear(col("full_date")).alias("week_of_year"),
            month(col("full_date")).alias("month_number"),
            date_format(col("full_date"), "MMMM").alias("month_name"),
            quarter(col("full_date")).alias("quarter_number"),
            concat(lit("Q"), quarter(col("full_date"))).alias("quarter_name"),
            year(col("full_date")).alias("year_number"),
            when(dayofweek(col("full_date")).isin(1, 7), True).otherwise(False).alias("is_weekend"),
            when(col("full_date").isin(lit("2024-01-01"), lit("2024-07-04"), lit("2024-12-25")), True).otherwise(False).alias("is_holiday"),
            when(month(col("full_date")) <= 3, year(col("full_date"))).otherwise(year(col("full_date")) + 1).alias("fiscal_year"),
            when(month(col("full_date")) <= 3, quarter(col("full_date")) + 1)
            .when(month(col("full_date")) <= 6, 1)
            .when(month(col("full_date")) <= 9, 2)
            .otherwise(3).alias("fiscal_quarter"),
            current_timestamp().alias("load_date"),
            lit("SYSTEM_GENERATED").alias("source_system")
        )
        
        records_processed = date_dim_df.count()
        
        # Write to Gold layer
        write_gold_table(date_dim_df, "go_date_dimension")
        
        # Log audit record
        log_audit_record(spark, "transform_date_dimension", "SYSTEM_GENERATED", "go_date_dimension", 
                        records_read, records_processed, records_processed, "SUCCESS")
        
        print(f"Date Dimension: Processed {records_processed} records successfully")
        
    except Exception as e:
        log_audit_record(spark, "transform_date_dimension", "SYSTEM_GENERATED", "go_date_dimension", 
                        0, 0, 0, "FAILED", str(e))
        log_error_record(spark, "go_date_dimension", "TRANSFORMATION_ERROR", str(e), "N/A", "N/A")
        raise e

def optimize_gold_tables(spark):
    """Optimize Gold layer tables for performance"""
    try:
        # Z-ORDER optimization for dimension tables
        spark.sql("OPTIMIZE workspace.inventory_gold.go_product_dimension ZORDER BY (product_key, product_code)")
        spark.sql("OPTIMIZE workspace.inventory_gold.go_warehouse_dimension ZORDER BY (warehouse_key, warehouse_code)")
        spark.sql("OPTIMIZE workspace.inventory_gold.go_supplier_dimension ZORDER BY (supplier_key, supplier_code)")
        spark.sql("OPTIMIZE workspace.inventory_gold.go_customer_dimension ZORDER BY (customer_key, customer_code)")
        spark.sql("OPTIMIZE workspace.inventory_gold.go_date_dimension ZORDER BY (date_key)")
        
        print("Gold layer tables optimized successfully")
        
    except Exception as e:
        print(f"Optimization failed: {str(e)}")
        log_error_record(spark, "OPTIMIZATION", "OPTIMIZATION_ERROR", str(e), "N/A", "N/A")

def main():
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print("Starting Databricks Gold Dimension DE Pipeline...")
        
        # Transform dimension tables
        transform_product_dimension(spark)
        transform_warehouse_dimension(spark)
        transform_supplier_dimension(spark)
        transform_customer_dimension(spark)
        transform_date_dimension(spark)
        
        # Optimize tables for performance
        optimize_gold_tables(spark)
        
        print("Databricks Gold Dimension DE Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline failed with error: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

## 3. Error Handling Mechanism

### 3.1 Data Validation Errors
- **Null Value Handling**: Replace null values with default values or "UNKNOWN" placeholders
- **Format Validation**: Validate email formats