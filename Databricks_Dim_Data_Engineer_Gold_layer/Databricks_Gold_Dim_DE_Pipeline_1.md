_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Databricks Gold Dimension Data Engineering Pipeline for transforming Silver layer data into Gold layer dimension tables with comprehensive data quality, audit logging, and error handling
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Dim DE Pipeline
## Inventory Management System - Dimension Tables Processing

## Overview

This pipeline processes data from the Silver layer into Gold layer dimension tables, implementing comprehensive transformations, data quality validations, audit logging, and error handling mechanisms. The pipeline is optimized for Databricks environment with Delta Lake storage format.

## Pipeline Architecture

### Source Layer: Silver (workspace.inventory_silver)
- si_products
- si_suppliers  
- si_warehouses
- si_customers

### Target Layer: Gold (workspace.inventory_gold)
- Go_Product_Dimension
- Go_Supplier_Dimension
- Go_Warehouse_Dimension
- Go_Customer_Dimension
- Go_Date_Dimension
- Go_Data_Validation_Error
- Go_Process_Audit

## PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, datediff, avg, count, when, date_add, sum
import uuid
from datetime import datetime

def create_spark_session():
    """Create Spark session with Delta Lake support and configure paths"""
    spark = SparkSession.builder \n        .appName("Silver to Gold Dimension Data Processing") \n        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n        .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n        .getOrCreate()
    return spark

# Path configurations from input credentials
silver_path = "workspace.inventory_silver."
gold_path = "workspace.inventory_gold."

# Global variables for audit tracking
pipeline_run_id = str(uuid.uuid4())
process_start_time = datetime.now()

def read_silver_table(spark, table_name):
    """Read table from Silver layer"""
    try:
        return spark.read.format("delta").table(f"{silver_path}{table_name}")
    except Exception as e:
        log_error(spark, f"Error reading {table_name}", str(e), "READ_ERROR")
        return None

def write_gold_table(spark, df, table_name, mode="overwrite"):
    """Write table to Gold layer"""
    try:
        df.write.format("delta").mode(mode).saveAsTable(f"{gold_path}{table_name}")
        return True
    except Exception as e:
        log_error(spark, f"Error writing {table_name}", str(e), "WRITE_ERROR")
        return False

def log_error(spark, table_name, error_message, error_type):
    """Log errors to error table"""
    error_data = spark.createDataFrame([
        (str(uuid.uuid4()), 
         1,
         pipeline_run_id,
         table_name,
         "N/A",
         "N/A",
         error_type,
         "SYSTEM",
         "HIGH",
         error_message,
         "N/A",
         "N/A",
         "SYSTEM_VALIDATION",
         datetime.now(),
         "OPEN",
         None,
         None,
         None,
         datetime.now(),
         "DATABRICKS_PIPELINE")
    ], ["error_id", "error_key", "pipeline_run_id", "table_name", "column_name", 
        "record_identifier", "error_type", "error_category", "error_severity", 
        "error_description", "expected_value", "actual_value", "validation_rule", 
        "error_timestamp", "resolution_status", "resolution_notes", "resolved_by", 
        "resolved_timestamp", "load_date", "source_system"])
    
    try:
        error_data.write.format("delta").mode("append").saveAsTable(f"{gold_path}go_data_validation_error")
    except:
        print(f"Failed to log error: {error_message}")

def log_audit(spark, process_name, source_table, target_table, records_read, records_processed, 
              records_inserted, records_rejected, status, error_message=None):
    """Log audit information"""
    process_end_time = datetime.now()
    duration = int((process_end_time - process_start_time).total_seconds())
    
    audit_data = spark.createDataFrame([
        (str(uuid.uuid4()),
         1,
         process_name,
         "DIMENSION_PROCESSING",
         "Gold_Dim_DE_Pipeline",
         pipeline_run_id,
         "Databricks_Job",
         process_name,
         source_table,
         target_table,
         records_read,
         records_processed,
         records_inserted,
         0,  # records_updated
         0,  # records_deleted
         records_rejected,
         process_start_time,
         process_end_time,
         duration,
         status,
         error_message,
         "system",
         datetime.now(),
         "DATABRICKS_PIPELINE")
    ], ["audit_id", "audit_key", "process_name", "process_type", "pipeline_name", 
        "pipeline_run_id", "job_name", "step_name", "source_table", "target_table", 
        "records_read", "records_processed", "records_inserted", "records_updated", 
        "records_deleted", "records_rejected", "process_start_time", "process_end_time", 
        "process_duration_seconds", "process_status", "error_message", "user_name", 
        "load_date", "source_system"])
    
    try:
        audit_data.write.format("delta").mode("append").saveAsTable(f"{gold_path}go_process_audit")
    except Exception as e:
        print(f"Failed to log audit: {str(e)}")

def transform_product_dimension(spark):
    """Transform Product dimension table"""
    try:
        # Read from Silver layer
        products_df = read_silver_table(spark, "si_products")
        
        if products_df is None:
            return False
            
        initial_count = products_df.count()
        
        # Apply transformations based on mapping specifications
        product_dim_df = products_df.select(
            row_number().over(Window.orderBy(col("Product_ID"), col("load_date"))).alias("product_id"),
            dense_rank().over(Window.orderBy(col("Product_ID"))).alias("product_key"),
            concat(lit("PRD_"), lpad(col("Product_ID"), 6, "0")).alias("product_code"),
            when((length(trim(col("Product_Name"))) == 0) | col("Product_Name").isNull(), 
                 lit("UNKNOWN_PRODUCT"))
            .otherwise(initcap(trim(col("Product_Name")))).alias("product_name"),
            concat(lit("Product: "), initcap(trim(col("Product_Name")))).alias("product_description"),
            when(col("Category").like("%Electronics%"), lit("Electronics"))
            .when(col("Category").like("%Furniture%"), lit("Furniture"))
            .when(col("Category").like("%Clothing%"), lit("Apparel"))
            .otherwise(lit("General")).alias("category_name"),
            when(col("Category").like("%Mobile%"), lit("Mobile Devices"))
            .when(col("Category").like("%Laptop%"), lit("Computing"))
            .otherwise(lit("Other")).alias("subcategory_name"),
            split(col("Product_Name"), " ").getItem(0).alias("brand_name"),
            lit("EACH").alias("unit_of_measure"),
            lit(0.00).cast("decimal(10,2)").alias("standard_cost"),
            lit(0.00).cast("decimal(10,2)").alias("list_price"),
            when(col("update_date") >= date_sub(current_date(), 90), lit("ACTIVE"))
            .when(col("update_date") >= date_sub(current_date(), 365), lit("INACTIVE"))
            .otherwise(lit("DISCONTINUED")).alias("product_status"),
            coalesce(col("load_date"), current_timestamp()).cast("date").alias("effective_start_date"),
            lit("9999-12-31").cast("date").alias("effective_end_date"),
            lit(True).alias("is_current"),
            col("load_date"),
            col("update_date"),
            col("source_system")
        )
        
        # Data quality validation
        valid_records = product_dim_df.filter(
            col("product_name").isNotNull() & 
            (col("product_name") != "") &
            col("category_name").isNotNull()
        )
        
        final_count = valid_records.count()
        rejected_count = initial_count - final_count
        
        # Write to Gold layer
        success = write_gold_table(spark, valid_records, "go_product_dimension")
        
        # Log audit
        status = "SUCCESS" if success else "FAILED"
        log_audit(spark, "transform_product_dimension", "si_products", "go_product_dimension", 
                 initial_count, final_count, final_count, rejected_count, status)
        
        return success
        
    except Exception as e:
        log_error(spark, "go_product_dimension", str(e), "TRANSFORMATION_ERROR")
        log_audit(spark, "transform_product_dimension", "si_products", "go_product_dimension", 
                 0, 0, 0, 0, "FAILED", str(e))
        return False

def transform_warehouse_dimension(spark):
    """Transform Warehouse dimension table"""
    try:
        # Read from Silver layer
        warehouses_df = read_silver_table(spark, "si_warehouses")
        
        if warehouses_df is None:
            return False
            
        initial_count = warehouses_df.count()
        
        # Apply transformations based on mapping specifications
        warehouse_dim_df = warehouses_df.select(
            row_number().over(Window.orderBy(col("Warehouse_ID"), col("load_date"))).alias("warehouse_id"),
            dense_rank().over(Window.orderBy(col("Warehouse_ID"))).alias("warehouse_key"),
            concat(lit("WH_"), lpad(col("Warehouse_ID"), 6, "0")).alias("warehouse_code"),
            concat(lit("Warehouse "), col("Warehouse_ID"), lit(" - "), 
                   split(col("Location"), ",").getItem(0)).alias("warehouse_name"),
            when(col("Capacity") >= 100000, lit("LARGE_DISTRIBUTION"))
            .when(col("Capacity") >= 50000, lit("MEDIUM_DISTRIBUTION"))
            .when(col("Capacity") >= 10000, lit("SMALL_DISTRIBUTION"))
            .otherwise(lit("LOCAL_STORAGE")).alias("warehouse_type"),
            split(col("Location"), ",").getItem(0).alias("address_line1"),
            lit(None).cast("string").alias("address_line2"),
            when(size(split(col("Location"), ",")) > 1, 
                 split(col("Location"), ",").getItem(1)).alias("city"),
            when(size(split(col("Location"), ",")) > 2, 
                 split(col("Location"), ",").getItem(2)).alias("state_province"),
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
        
        # Data quality validation
        valid_records = warehouse_dim_df.filter(
            col("warehouse_name").isNotNull() & 
            (col("warehouse_name") != "")
        )
        
        final_count = valid_records.count()
        rejected_count = initial_count - final_count
        
        # Write to Gold layer
        success = write_gold_table(spark, valid_records, "go_warehouse_dimension")
        
        # Log audit
        status = "SUCCESS" if success else "FAILED"
        log_audit(spark, "transform_warehouse_dimension", "si_warehouses", "go_warehouse_dimension", 
                 initial_count, final_count, final_count, rejected_count, status)
        
        return success
        
    except Exception as e:
        log_error(spark, "go_warehouse_dimension", str(e), "TRANSFORMATION_ERROR")
        log_audit(spark, "transform_warehouse_dimension", "si_warehouses", "go_warehouse_dimension", 
                 0, 0, 0, 0, "FAILED", str(e))
        return False

def transform_supplier_dimension(spark):
    """Transform Supplier dimension table"""
    try:
        # Read from Silver layer
        suppliers_df = read_silver_table(spark, "si_suppliers")
        
        if suppliers_df is None:
            return False
            
        initial_count = suppliers_df.count()
        
        # Apply transformations based on mapping specifications
        supplier_dim_df = suppliers_df.select(
            row_number().over(Window.orderBy(col("Supplier_ID"), col("load_date"))).alias("supplier_id"),
            dense_rank().over(Window.orderBy(col("Supplier_ID"))).alias("supplier_key"),
            concat(lit("SUP_"), lpad(col("Supplier_ID"), 6, "0")).alias("supplier_code"),
            when((col("Supplier_Name").isNull()) | (length(trim(col("Supplier_Name"))) == 0), 
                 lit("UNKNOWN_SUPPLIER"))
            .otherwise(initcap(trim(col("Supplier_Name")))).alias("supplier_name"),
            lit("STANDARD").alias("supplier_type"),
            initcap(trim(col("Supplier_Name"))).alias("contact_person"),
            when(col("Contact_Number").rlike("^[0-9+\-\s\(\)]+$"), col("Contact_Number"))
            .otherwise(lit(None)).alias("contact_phone"),
            when(col("Contact_Number").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), 
                 col("Contact_Number"))
            .otherwise(lit(None)).alias("contact_email"),
            lit(None).cast("string").alias("address_line1"),
            lit(None).cast("string").alias("address_line2"),
            lit(None).cast("string").alias("city"),
            lit(None).cast("string").alias("state_province"),
            lit(None).cast("string").alias("postal_code"),
            lit("USA").alias("country"),
            lit("NET_30").alias("payment_terms"),
            when(col("load_date") >= date_sub(current_date(), 30), lit("NEW_SUPPLIER"))
            .when(col("update_date") >= date_sub(current_date(), 90), lit("ACTIVE"))
            .otherwise(lit("INACTIVE")).alias("supplier_rating"),
            lit("ACTIVE").alias("supplier_status"),
            coalesce(col("load_date"), current_timestamp()).cast("date").alias("effective_start_date"),
            lit("9999-12-31").cast("date").alias("effective_end_date"),
            lit(True).alias("is_current"),
            col("load_date"),
            col("update_date"),
            col("source_system")
        )
        
        # Data quality validation
        valid_records = supplier_dim_df.filter(
            col("supplier_name").isNotNull() & 
            (col("supplier_name") != "")
        )
        
        final_count = valid_records.count()
        rejected_count = initial_count - final_count
        
        # Write to Gold layer
        success = write_gold_table(spark, valid_records, "go_supplier_dimension")
        
        # Log audit
        status = "SUCCESS" if success else "FAILED"
        log_audit(spark, "transform_supplier_dimension", "si_suppliers", "go_supplier_dimension", 
                 initial_count, final_count, final_count, rejected_count, status)
        
        return success
        
    except Exception as e:
        log_error(spark, "go_supplier_dimension", str(e), "TRANSFORMATION_ERROR")
        log_audit(spark, "transform_supplier_dimension", "si_suppliers", "go_supplier_dimension", 
                 0, 0, 0, 0, "FAILED", str(e))
        return False

def transform_customer_dimension(spark):
    """Transform Customer dimension table"""
    try:
        # Read from Silver layer
        customers_df = read_silver_table(spark, "si_customers")
        
        if customers_df is None:
            return False
            
        initial_count = customers_df.count()
        
        # Apply transformations based on mapping specifications
        customer_dim_df = customers_df.select(
            row_number().over(Window.orderBy(col("Customer_ID"), col("load_date"))).alias("customer_id"),
            dense_rank().over(Window.orderBy(col("Customer_ID"))).alias("customer_key"),
            concat(lit("CUST_"), lpad(col("Customer_ID"), 8, "0")).alias("customer_code"),
            when((col("Customer_Name").isNull()) | (length(trim(col("Customer_Name"))) == 0), 
                 lit("UNKNOWN_CUSTOMER"))
            .otherwise(initcap(trim(col("Customer_Name")))).alias("customer_name"),
            lit("RETAIL").alias("customer_type"),
            initcap(trim(col("Customer_Name"))).alias("contact_person"),
            lit(None).cast("string").alias("contact_phone"),
            when(col("Email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), 
                 lower(trim(col("Email"))))
            .otherwise(lit(None)).alias("contact_email"),
            lit(None).cast("string").alias("address_line1"),
            lit(None).cast("string").alias("address_line2"),
            lit(None).cast("string").alias("city"),
            lit(None).cast("string").alias("state_province"),
            lit(None).cast("string").alias("postal_code"),
            lit("USA").alias("country"),
            lit(10000.00).cast("decimal(15,2)").alias("credit_limit"),
            when(col("load_date") >= date_sub(current_date(), 90), lit("NEW_CUSTOMER"))
            .when(col("update_date") >= date_sub(current_date(), 365), lit("ACTIVE_CUSTOMER"))
            .otherwise(lit("INACTIVE_CUSTOMER")).alias("customer_segment"),
            lit("ACTIVE").alias("customer_status"),
            coalesce(col("load_date"), current_timestamp()).cast("date").alias("effective_start_date"),
            lit("9999-12-31").cast("date").alias("effective_end_date"),
            lit(True).alias("is_current"),
            col("load_date"),
            col("update_date"),
            col("source_system")
        )
        
        # Data quality validation
        valid_records = customer_dim_df.filter(
            col("customer_name").isNotNull() & 
            (col("customer_name") != "")
        )
        
        final_count = valid_records.count()
        rejected_count = initial_count - final_count
        
        # Write to Gold layer
        success = write_gold_table(spark, valid_records, "go_customer_dimension")
        
        # Log audit
        status = "SUCCESS" if success else "FAILED"
        log_audit(spark, "transform_customer_dimension", "si_customers", "go_customer_dimension", 
                 initial_count, final_count, final_count, rejected_count, status)
        
        return success
        
    except Exception as e:
        log_error(spark, "go_customer_dimension", str(e), "TRANSFORMATION_ERROR")
        log_audit(spark, "transform_customer_dimension", "si_customers", "go_customer_dimension", 
                 0, 0, 0, 0, "FAILED", str(e))
        return False

def transform_date_dimension(spark):
    """Transform Date dimension table"""
    try:
        # Generate date range from 2020-01-01 to 2030-12-31
        date_range_df = spark.sql("""
            SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) as full_date
        """)
        
        initial_count = date_range_df.count()
        
        # Apply transformations based on mapping specifications
        date_dim_df = date_range_df.select(
            row_number().over(Window.orderBy(col("full_date"))).alias("date_id"),
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
            when(dayofweek(col("full_date")).isin(1, 7), lit(True))
            .otherwise(lit(False)).alias("is_weekend"),
            when(col("full_date").isin(lit("2024-01-01"), lit("2024-07-04"), lit("2024-12-25")), lit(True))
            .otherwise(lit(False)).alias("is_holiday"),
            when(month(col("full_date")) <= 3, year(col("full_date")))
            .otherwise(year(col("full_date")) + 1).alias("fiscal_year"),
            when(month(col("full_date")) <= 3, quarter(col("full_date")) + 1)
            .when(month(col("full_date")) <= 6, lit(1))
            .when(month(col("full_date")) <= 9, lit(2))
            .otherwise(lit(3)).alias("fiscal_quarter"),
            current_timestamp().alias("load_date"),
            lit("SYSTEM_GENERATED").alias("source_system")
        )
        
        final_count = date_dim_df.count()
        
        # Write to Gold layer
        success = write_gold_table(spark, date_dim_df, "go_date_dimension")
        
        # Log audit
        status = "SUCCESS" if success else "FAILED"
        log_audit(spark, "transform_date_dimension", "SYSTEM_GENERATED", "go_date_dimension", 
                 initial_count, final_count, final_count, 0, status)
        
        return success
        
    except Exception as e:
        log_error(spark, "go_date_dimension", str(e), "TRANSFORMATION_ERROR")
        log_audit(spark, "transform_date_dimension", "SYSTEM_GENERATED", "go_date_dimension", 
                 0, 0, 0, 0, "FAILED", str(e))
        return False

def create_error_data_table(spark):
    """Create error data table structure in Gold layer"""
    try:
        # Create empty error table with proper schema if it doesn't exist
        error_schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("error_key", LongType(), False),
            StructField("pipeline_run_id", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("record_identifier", StringType(), True),
            StructField("error_type", StringType(), True),
            StructField("error_category", StringType(), True),
            StructField("error_severity", StringType(), True),
            StructField("error_description", StringType(), True),
            StructField("expected_value", StringType(), True),
            StructField("actual_value", StringType(), True),
            StructField("validation_rule", StringType(), True),
            StructField("error_timestamp", TimestampType(), True),
            StructField("resolution_status", StringType(), True),
            StructField("resolution_notes", StringType(), True),
            StructField("resolved_by", StringType(), True),
            StructField("resolved_timestamp", TimestampType(), True),
            StructField("load_date", TimestampType(), True),
            StructField("source_system", StringType(), True)
        ])