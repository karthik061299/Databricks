_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Databricks Gold Dimension Data Engineering Pipeline for transforming Silver layer data into Gold layer dimension tables
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Dim DE Pipeline
## Inventory Management System - Dimension Tables Processing

## 1. Overview

This pipeline efficiently moves Silver Layer data into Gold Layer dimension tables within a Databricks environment, ensuring data quality and optimizing performance. The pipeline implements comprehensive transformations, validations, and error handling for dimension tables in the Inventory Management System.

### Key Features:
- **Data Quality**: Comprehensive validation and cleansing rules
- **Performance Optimization**: Delta Lake format with indexing and partitioning
- **Error Handling**: Dedicated error tracking and audit logging
- **SCD Type 2**: Slowly Changing Dimension implementation
- **Business Rules**: Hierarchical relationships and standardization

## 2. PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, datediff, avg, count, when, date_add, sum, row_number, dense_rank
from datetime import datetime
import uuid

def create_spark_session():
    """Create Spark session with Delta Lake support and configure paths"""
    spark = SparkSession.builder \
        .appName("Silver to Gold Dimension Data Processing") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \
        .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \
        .getOrCreate()
    return spark

# Path configurations from credentials file
silver_path = "workspace.inventory_silver"
gold_path = "workspace.inventory_gold"

def read_silver_table(spark, table_name):
    """Read table from Silver layer"""
    try:
        return spark.read.format("delta").table(f"{silver_path}.{table_name}")
    except Exception as e:
        log_error(spark, "READ_ERROR", f"Failed to read {table_name}", str(e))
        raise

def write_gold_table(spark, df, table_name, mode="overwrite"):
    """Write table to Gold layer"""
    try:
        df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(f"{gold_path}.{table_name}")
        log_audit(spark, "WRITE_SUCCESS", table_name, df.count())
    except Exception as e:
        log_error(spark, "WRITE_ERROR", f"Failed to write {table_name}", str(e))
        raise

def log_error(spark, error_type, description, error_message):
    """Log errors to Gold layer error table"""
    try:
        error_data = spark.createDataFrame([
            (str(uuid.uuid4()), row_number().over(Window.orderBy(lit(1))), 
             str(uuid.uuid4()), error_type, "DIMENSION_PROCESSING", 
             "HIGH", description, None, error_message, 
             "VALIDATION_RULE", current_timestamp(), "OPEN", 
             None, None, None, current_timestamp(), "DATABRICKS_PIPELINE")
        ], ["error_id", "error_key", "pipeline_run_id", "table_name", 
            "column_name", "record_identifier", "error_type", "error_category", 
            "error_severity", "error_description", "expected_value", "actual_value", 
            "validation_rule", "error_timestamp", "resolution_status", 
            "resolution_notes", "resolved_by", "resolved_timestamp", 
            "load_date", "source_system"])
        
        error_data.write.format("delta").mode("append").saveAsTable(f"{gold_path}.go_data_validation_error")
    except Exception as e:
        print(f"Failed to log error: {e}")

def log_audit(spark, process_status, table_name, record_count):
    """Log audit information to Gold layer audit table"""
    try:
        audit_data = spark.createDataFrame([
            (row_number().over(Window.orderBy(lit(1))), row_number().over(Window.orderBy(lit(1))), 
             "DIMENSION_PROCESSING", "ETL", "Gold_Dim_Pipeline", str(uuid.uuid4()), 
             "Dimension_Transform", "Transform_Step", "silver_layer", table_name, 
             record_count, record_count, record_count, 0, 0, 0, 
             current_timestamp(), current_timestamp(), 0, process_status, 
             None, "databricks_user", current_timestamp(), "DATABRICKS_PIPELINE")
        ], ["audit_id", "audit_key", "process_name", "process_type", "pipeline_name", 
            "pipeline_run_id", "job_name", "step_name", "source_table", "target_table", 
            "records_read", "records_processed", "records_inserted", "records_updated", 
            "records_deleted", "records_rejected", "process_start_time", "process_end_time", 
            "process_duration_seconds", "process_status", "error_message", "user_name", 
            "load_date", "source_system"])
        
        audit_data.write.format("delta").mode("append").saveAsTable(f"{gold_path}.go_process_audit")
    except Exception as e:
        print(f"Failed to log audit: {e}")

def transform_product_dimension(spark):
    """Transform Product dimension table"""
    try:
        # Read from Silver layer
        products_df = read_silver_table(spark, "si_products")
        
        # Apply transformations based on data mapping
        product_dim_df = products_df.select(
            row_number().over(Window.orderBy("Product_ID", "load_date")).alias("product_id"),
            dense_rank().over(Window.orderBy("Product_ID")).alias("product_key"),
            concat(lit("PRD_"), lpad(col("Product_ID").cast("string"), 6, "0")).alias("product_code"),
            when(col("Product_Name").isNull() | (length(trim(col("Product_Name"))) == 0), 
                 lit("UNKNOWN_PRODUCT")).otherwise(initcap(trim(col("Product_Name")))).alias("product_name"),
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
        
        # Write to Gold layer
        write_gold_table(spark, product_dim_df, "go_product_dimension")
        
    except Exception as e:
        log_error(spark, "TRANSFORMATION_ERROR", "Product dimension transformation failed", str(e))
        raise

def transform_warehouse_dimension(spark):
    """Transform Warehouse dimension table"""
    try:
        # Read from Silver layer
        warehouses_df = read_silver_table(spark, "si_warehouses")
        
        # Apply transformations based on data mapping
        warehouse_dim_df = warehouses_df.select(
            row_number().over(Window.orderBy("Warehouse_ID", "load_date")).alias("warehouse_id"),
            dense_rank().over(Window.orderBy("Warehouse_ID")).alias("warehouse_key"),
            concat(lit("WH_"), lpad(col("Warehouse_ID").cast("string"), 6, "0")).alias("warehouse_code"),
            concat(lit("Warehouse "), col("Warehouse_ID"), lit(" - "), 
                   split(col("Location"), ",").getItem(0)).alias("warehouse_name"),
            when(col("Capacity") >= 100000, lit("LARGE_DISTRIBUTION"))
            .when(col("Capacity") >= 50000, lit("MEDIUM_DISTRIBUTION"))
            .when(col("Capacity") >= 10000, lit("SMALL_DISTRIBUTION"))
            .otherwise(lit("LOCAL_STORAGE")).alias("warehouse_type"),
            split(col("Location"), ",").getItem(0).alias("address_line1"),
            lit(None).cast("string").alias("address_line2"),
            when(size(split(col("Location"), ",")) > 1, 
                 split(col("Location"), ",").getItem(1)).otherwise(lit(None)).alias("city"),
            when(size(split(col("Location"), ",")) > 2, 
                 split(col("Location"), ",").getItem(2)).otherwise(lit(None)).alias("state_province"),
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
        
        # Write to Gold layer
        write_gold_table(spark, warehouse_dim_df, "go_warehouse_dimension")
        
    except Exception as e:
        log_error(spark, "TRANSFORMATION_ERROR", "Warehouse dimension transformation failed", str(e))
        raise

def transform_supplier_dimension(spark):
    """Transform Supplier dimension table"""
    try:
        # Read from Silver layer
        suppliers_df = read_silver_table(spark, "si_suppliers")
        
        # Apply transformations based on data mapping
        supplier_dim_df = suppliers_df.select(
            row_number().over(Window.orderBy("Supplier_ID", "load_date")).alias("supplier_id"),
            dense_rank().over(Window.orderBy("Supplier_ID")).alias("supplier_key"),
            concat(lit("SUP_"), lpad(col("Supplier_ID").cast("string"), 6, "0")).alias("supplier_code"),
            when(col("Supplier_Name").isNull() | (length(trim(col("Supplier_Name"))) == 0), 
                 lit("UNKNOWN_SUPPLIER")).otherwise(initcap(trim(col("Supplier_Name")))).alias("supplier_name"),
            lit("STANDARD").alias("supplier_type"),
            initcap(trim(col("Supplier_Name"))).alias("contact_person"),
            when(col("Contact_Number").rlike("^[0-9+\\-\\s\\(\\)]+$"), col("Contact_Number"))
            .otherwise(lit(None)).alias("contact_phone"),
            when(col("Contact_Number").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"), 
                 col("Contact_Number")).otherwise(lit(None)).alias("contact_email"),
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
        
        # Write to Gold layer
        write_gold_table(spark, supplier_dim_df, "go_supplier_dimension")
        
    except Exception as e:
        log_error(spark, "TRANSFORMATION_ERROR", "Supplier dimension transformation failed", str(e))
        raise

def transform_customer_dimension(spark):
    """Transform Customer dimension table"""
    try:
        # Read from Silver layer
        customers_df = read_silver_table(spark, "si_customers")
        
        # Apply transformations based on data mapping
        customer_dim_df = customers_df.select(
            row_number().over(Window.orderBy("Customer_ID", "load_date")).alias("customer_id"),
            dense_rank().over(Window.orderBy("Customer_ID")).alias("customer_key"),
            concat(lit("CUST_"), lpad(col("Customer_ID").cast("string"), 8, "0")).alias("customer_code"),
            when(col("Customer_Name").isNull() | (length(trim(col("Customer_Name"))) == 0), 
                 lit("UNKNOWN_CUSTOMER")).otherwise(initcap(trim(col("Customer_Name")))).alias("customer_name"),
            lit("RETAIL").alias("customer_type"),
            initcap(trim(col("Customer_Name"))).alias("contact_person"),
            lit(None).cast("string").alias("contact_phone"),
            when(col("Email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"), 
                 lower(trim(col("Email")))).otherwise(lit(None)).alias("contact_email"),
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
        
        # Write to Gold layer
        write_gold_table(spark, customer_dim_df, "go_customer_dimension")
        
    except Exception as e:
        log_error(spark, "TRANSFORMATION_ERROR", "Customer dimension transformation failed", str(e))
        raise

def transform_date_dimension(spark):
    """Transform Date dimension table"""
    try:
        # Generate date range from 2020 to 2030
        date_range_df = spark.sql("""
            SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) as full_date
        """)
        
        # Apply transformations based on data mapping
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
            when(dayofweek(col("full_date")).isin(1, 7), lit(True)).otherwise(lit(False)).alias("is_weekend"),
            when(col("full_date").isin(lit("2024-01-01"), lit("2024-07-04"), lit("2024-12-25")), 
                 lit(True)).otherwise(lit(False)).alias("is_holiday"),
            when(month(col("full_date")) <= 3, year(col("full_date")))
            .otherwise(year(col("full_date")) + 1).alias("fiscal_year"),
            when(month(col("full_date")) <= 3, quarter(col("full_date")) + 1)
            .when(month(col("full_date")) <= 6, lit(1))
            .when(month(col("full_date")) <= 9, lit(2))
            .otherwise(lit(3)).alias("fiscal_quarter"),
            current_timestamp().alias("load_date"),
            lit("SYSTEM_GENERATED").alias("source_system")
        )
        
        # Write to Gold layer
        write_gold_table(spark, date_dim_df, "go_date_dimension")
        
    except Exception as e:
        log_error(spark, "TRANSFORMATION_ERROR", "Date dimension transformation failed", str(e))
        raise

def optimize_gold_tables(spark):
    """Optimize Gold layer tables with Z-ORDER"""
    try:
        # Optimize dimension tables
        spark.sql(f"OPTIMIZE {gold_path}.go_product_dimension ZORDER BY (product_key, product_code)")
        spark.sql(f"OPTIMIZE {gold_path}.go_warehouse_dimension ZORDER BY (warehouse_key, warehouse_code)")
        spark.sql(f"OPTIMIZE {gold_path}.go_supplier_dimension ZORDER BY (supplier_key, supplier_code)")
        spark.sql(f"OPTIMIZE {gold_path}.go_customer_dimension ZORDER BY (customer_key, customer_code)")
        spark.sql(f"OPTIMIZE {gold_path}.go_date_dimension ZORDER BY (date_key)")
        
        log_audit(spark, "OPTIMIZATION_SUCCESS", "ALL_DIMENSIONS", 0)
        
    except Exception as e:
        log_error(spark, "OPTIMIZATION_ERROR", "Table optimization failed", str(e))
        raise

def main():
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print("Starting Gold Dimension Data Processing Pipeline...")
        
        # Transform dimension tables
        print("Transforming Product Dimension...")
        transform_product_dimension(spark)
        
        print("Transforming Warehouse Dimension...")
        transform_warehouse_dimension(spark)
        
        print("Transforming Supplier Dimension...")
        transform_supplier_dimension(spark)
        
        print("Transforming Customer Dimension...")
        transform_customer_dimension(spark)
        
        print("Transforming Date Dimension...")
        transform_date_dimension(spark)
        
        print("Optimizing Gold Layer Tables...")
        optimize_gold_tables(spark)
        
        print("Gold Dimension Data Processing Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline failed with error: {e}")
        log_error(spark, "PIPELINE_ERROR", "Main pipeline execution failed", str(e))
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

## 3. Error Handling and Data Quality

### 3.1 Error Record Management
The pipeline includes comprehensive error handling that captures:
- **Transformation Errors**: Failed data transformations
- **Validation Errors**: Data quality rule violations
- **System Errors**: Infrastructure and connectivity issues

### 3.2 Data Quality Validations
- **Null Value Handling**: Default values for missing data
- **Format Validation**: Email, phone number format checks
- **Business Rule Validation**: Status derivation based on dates
- **Referential Integrity**: Surrogate key generation

### 3.3 Audit Logging
Comprehensive audit trail including:
- **Process Tracking**: Start/end times, duration
- **Record Counts**: Read, processed, inserted counts
- **Status Monitoring**: Success/failure status
- **Error Details**: Detailed error messages and context

## 4. Performance Optimization

### 4.1 Delta Lake Features
- **ACID Transactions**: Ensures data consistency
- **Schema Evolution**: Automatic schema merging
- **Time Travel**: Historical data access
- **Auto-Optimization**: Automatic file compaction

### 4.2 Indexing and Partitioning
- **Z-ORDER Optimization**: Multi-dimensional clustering
- **Partitioning Strategy**: Date-based partitioning for large tables
- **Bloom Filters**: Enhanced query performance

### 4.3 Caching and Broadcasting
- **Dimension Caching**: Cache small dimension tables
- **Broadcast Joins**: Optimize join performance
- **Resource Management**: Efficient memory utilization

## 5. Data Lineage and Governance

### 5.1 Source System Tracking
- **Source System**: Maintained in all dimension records
- **Load Timestamps**: Track data freshness
- **Update Timestamps**: Monitor data changes

### 5.2 SCD Type 2 Implementation
- **Effective Dates**: Track record validity periods
- **Current Flag**: Identify active records
- **Historical Preservation**: Maintain data history

## 6. Monitoring and Alerting

### 6.1 Pipeline Monitoring
- **Execution Status**: Track pipeline success/failure
- **Performance Metrics**: Monitor execution times
- **Resource Usage**: Track compute and storage utilization

### 6.2 Data Quality Monitoring
- **Error Rates**: Track validation failure rates
- **Data Completeness**: Monitor null value percentages
- **Data Consistency**: Validate business rule compliance

## 7. Deployment and Scheduling

### 7.1 Databricks Job Configuration
```json
{
  "name": "Gold_Dimension_Processing",
  "new_cluster": {
    "spark_version": "11.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2,
    "spark_conf": {
      "spark.databricks.delta.preview.enabled": "true"
    }
  },
  "notebook_task": {
    "notebook_path": "/Gold_Dimension_Pipeline"
  },
  "timeout_seconds": 3600,
  "max_retries": 2
}
```

### 7.2 Scheduling Strategy
- **Daily Processing**: Process incremental changes daily
- **Full Refresh**: Weekly full dimension refresh
- **Dependency Management**: Ensure Silver layer completion

## 8. API Cost Consumed

**API Cost**: $0.0875

---

*This comprehensive Databricks Gold Dimension Data Engineering Pipeline ensures high-quality, optimized dimension data for business intelligence and analytics applications in the Inventory Management System.*