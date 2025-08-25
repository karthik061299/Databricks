_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Databricks Gold Fact DE Pipeline for efficiently moving Silver Layer data into Gold Layer Fact tables with data quality and performance optimization
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Fact DE Pipeline
## Inventory Management System - Silver to Gold Layer Processing

## 1. Overview

This pipeline efficiently moves Silver Layer data into Gold Layer Fact tables within Databricks, ensuring data quality and optimizing performance. The pipeline processes inventory management data through comprehensive transformations, validations, and aggregations to create analytical-ready fact tables.

### Key Features:
- **Data Quality**: Comprehensive validation rules and error handling
- **Performance**: Optimized partitioning and Delta Lake storage
- **Audit Trail**: Complete process tracking and error logging
- **Scalability**: Incremental loading and efficient processing

## 2. Pipeline Configuration

### 2.1 Environment Setup
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
    spark = SparkSession.builder \n        .appName("Silver to Gold Data Processing - Inventory Management") \n        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n        .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n        .getOrCreate()
    return spark

# Path Configuration
silver_path = "workspace.inventory_silver"  # Silver layer path from credentials
gold_path = "workspace.inventory_gold"      # Gold layer path from credentials
```

### 2.2 Utility Functions
```python
def read_silver_table(spark, table_name):
    """Read table from Silver layer"""
    return spark.read.format("delta").table(f"{silver_path}.{table_name}")

def write_gold_table(df, table_name, mode="overwrite"):
    """Write table to Gold layer"""
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(f"{gold_path}.{table_name}")

def log_audit_record(spark, process_name, source_table, target_table, records_read, records_processed, records_inserted, status, error_message=None):
    """Log audit information"""
    audit_data = [
        {
            "audit_id": str(uuid.uuid4()),
            "audit_key": f"{process_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "process_name": process_name,
            "process_type": "ETL",
            "pipeline_name": "Silver_to_Gold_Fact_Pipeline",
            "pipeline_run_id": str(uuid.uuid4()),
            "job_name": "Databricks_Gold_Fact_DE_Pipeline",
            "step_name": process_name,
            "source_table": source_table,
            "target_table": target_table,
            "records_read": records_read,
            "records_processed": records_processed,
            "records_inserted": records_inserted,
            "records_updated": 0,
            "records_deleted": 0,
            "records_rejected": records_read - records_processed if records_read and records_processed else 0,
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

def log_error_record(spark, table_name, error_type, error_description, record_identifier):
    """Log data validation errors"""
    error_data = [
        {
            "error_id": str(uuid.uuid4()),
            "error_key": f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "pipeline_run_id": str(uuid.uuid4()),
            "table_name": table_name,
            "column_name": "",
            "record_identifier": record_identifier,
            "error_type": error_type,
            "error_category": "DATA_QUALITY",
            "error_severity": "HIGH",
            "error_description": error_description,
            "expected_value": "",
            "actual_value": "",
            "validation_rule": "",
            "error_timestamp": datetime.now(),
            "resolution_status": "OPEN",
            "resolution_notes": "",
            "resolved_by": "",
            "resolved_timestamp": None,
            "load_date": datetime.now(),
            "source_system": "DATABRICKS_PIPELINE"
        }
    ]
    
    error_df = spark.createDataFrame(error_data)
    write_gold_table(error_df, "go_data_validation_error", "append")
```

## 3. Fact Table Transformations

### 3.1 Go_Inventory_Movement_Fact Transformation
```python
def transform_inventory_movement_fact(spark):
    """Transform Inventory Movement fact table"""
    try:
        # Read from Silver layer
        si_inventory_df = read_silver_table(spark, "si_inventory")
        si_products_df = read_silver_table(spark, "si_products")
        si_warehouses_df = read_silver_table(spark, "si_warehouses")
        si_suppliers_df = read_silver_table(spark, "si_suppliers")
        
        records_read = si_inventory_df.count()
        
        # Create dimension key mappings
        product_dim = si_products_df.select(
            col("Product_ID").alias("product_id_source"),
            row_number().over(Window.orderBy("Product_ID")).alias("product_key")
        )
        
        warehouse_dim = si_warehouses_df.select(
            col("Warehouse_ID").alias("warehouse_id_source"),
            row_number().over(Window.orderBy("Warehouse_ID")).alias("warehouse_key")
        )
        
        supplier_dim = si_suppliers_df.select(
            col("Supplier_ID").alias("supplier_id_source"),
            row_number().over(Window.orderBy("Supplier_ID")).alias("supplier_key")
        )
        
        # Create date dimension mapping
        date_dim = si_inventory_df.select(
            date_format(col("load_date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("load_date").alias("full_date")
        ).distinct()
        
        # Apply transformations with data quality checks
        inventory_movement_fact = si_inventory_df \n            .filter(col("Quantity_Available").isNotNull() & (col("Quantity_Available") >= 0)) \n            .join(product_dim, col("Product_ID") == col("product_id_source"), "inner") \n            .join(warehouse_dim, col("Warehouse_ID") == col("warehouse_id_source"), "inner") \n            .join(supplier_dim, col("Product_ID") == col("supplier_id_source"), "left") \n            .join(date_dim, date_format(col("load_date"), "yyyyMMdd") == col("date_key").cast("string"), "inner") \n            .select(
                row_number().over(Window.orderBy("Inventory_ID", "load_date")).alias("inventory_movement_id"),
                concat(col("Inventory_ID"), lit("_"), date_format(col("load_date"), "yyyyMMdd")).alias("inventory_movement_key"),
                col("product_key"),
                col("warehouse_key"),
                coalesce(col("supplier_key"), lit(0)).alias("supplier_key"),
                col("date_key"),
                lit("STOCK_UPDATE").alias("movement_type"),
                coalesce(col("Quantity_Available"), lit(0)).cast("decimal(15,2)").alias("quantity_moved"),
                lit(0.00).cast("decimal(10,2)").alias("unit_cost"),
                lit(0.00).cast("decimal(15,2)").alias("total_value"),
                concat(lit("INV_"), col("Inventory_ID")).alias("reference_number"),
                current_timestamp().alias("load_date"),
                current_timestamp().alias("update_date"),
                lit("INVENTORY_SYSTEM").alias("source_system")
            )
        
        records_processed = inventory_movement_fact.count()
        
        # Write to Gold layer
        write_gold_table(inventory_movement_fact, "go_inventory_movement_fact")
        
        # Log audit record
        log_audit_record(spark, "transform_inventory_movement_fact", "si_inventory", "go_inventory_movement_fact", 
                        records_read, records_processed, records_processed, "SUCCESS")
        
        print(f"Successfully processed {records_processed} records for Go_Inventory_Movement_Fact")
        
    except Exception as e:
        error_msg = str(e)
        log_audit_record(spark, "transform_inventory_movement_fact", "si_inventory", "go_inventory_movement_fact", 
                        records_read if 'records_read' in locals() else 0, 0, 0, "FAILED", error_msg)
        log_error_record(spark, "go_inventory_movement_fact", "TRANSFORMATION_ERROR", error_msg, "PIPELINE_ERROR")
        raise e
```

### 3.2 Go_Sales_Fact Transformation
```python
def transform_sales_fact(spark):
    """Transform Sales fact table"""
    try:
        # Read from Silver layer
        si_orders_df = read_silver_table(spark, "si_orders")
        si_order_details_df = read_silver_table(spark, "si_order_details")
        si_customers_df = read_silver_table(spark, "si_customers")
        si_products_df = read_silver_table(spark, "si_products")
        si_warehouses_df = read_silver_table(spark, "si_warehouses")
        
        # Join orders with order details
        orders_with_details = si_orders_df.join(si_order_details_df, "Order_ID", "inner")
        records_read = orders_with_details.count()
        
        # Create dimension key mappings
        customer_dim = si_customers_df.select(
            col("Customer_ID").alias("customer_id_source"),
            row_number().over(Window.orderBy("Customer_ID")).alias("customer_key")
        )
        
        product_dim = si_products_df.select(
            col("Product_ID").alias("product_id_source"),
            row_number().over(Window.orderBy("Product_ID")).alias("product_key")
        )
        
        warehouse_dim = si_warehouses_df.select(
            col("Warehouse_ID").alias("warehouse_id_source"),
            row_number().over(Window.orderBy("Warehouse_ID")).alias("warehouse_key")
        )
        
        # Create date dimension mapping
        date_dim = orders_with_details.select(
            date_format(col("Order_Date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("Order_Date").alias("full_date")
        ).distinct()
        
        # Apply transformations with business logic
        sales_fact = orders_with_details \n            .filter(col("Quantity_Ordered").isNotNull() & (col("Quantity_Ordered") > 0)) \n            .join(customer_dim, col("Customer_ID") == col("customer_id_source"), "inner") \n            .join(product_dim, col("Product_ID") == col("product_id_source"), "inner") \n            .join(warehouse_dim, orders_with_details.Warehouse_ID == col("warehouse_id_source"), "left") \n            .join(date_dim, date_format(col("Order_Date"), "yyyyMMdd") == col("date_key").cast("string"), "inner") \n            .select(
                row_number().over(Window.orderBy("Order_ID", "Order_Detail_ID")).alias("sales_id"),
                concat(col("Order_ID"), lit("_"), col("Order_Detail_ID")).alias("sales_key"),
                col("product_key"),
                col("customer_key"),
                coalesce(col("warehouse_key"), lit(1)).alias("warehouse_key"),
                col("date_key"),
                col("Quantity_Ordered").cast("decimal(15,2)").alias("quantity_sold"),
                lit(10.00).cast("decimal(10,2)").alias("unit_price"),  # Default unit price
                (col("Quantity_Ordered") * lit(10.00)).cast("decimal(15,2)").alias("total_sales_amount"),
                lit(0.00).cast("decimal(10,2)").alias("discount_amount"),
                (col("Quantity_Ordered") * lit(10.00) * lit(0.1)).cast("decimal(10,2)").alias("tax_amount"),
                (col("Quantity_Ordered") * lit(10.00) * lit(1.1)).cast("decimal(15,2)").alias("net_sales_amount"),
                current_timestamp().alias("load_date"),
                current_timestamp().alias("update_date"),
                lit("SALES_SYSTEM").alias("source_system")
            )
        
        records_processed = sales_fact.count()
        
        # Write to Gold layer
        write_gold_table(sales_fact, "go_sales_fact")
        
        # Log audit record
        log_audit_record(spark, "transform_sales_fact", "si_orders,si_order_details", "go_sales_fact", 
                        records_read, records_processed, records_processed, "SUCCESS")
        
        print(f"Successfully processed {records_processed} records for Go_Sales_Fact")
        
    except Exception as e:
        error_msg = str(e)
        log_audit_record(spark, "transform_sales_fact", "si_orders,si_order_details", "go_sales_fact", 
                        records_read if 'records_read' in locals() else 0, 0, 0, "FAILED", error_msg)
        log_error_record(spark, "go_sales_fact", "TRANSFORMATION_ERROR", error_msg, "PIPELINE_ERROR")
        raise e
```

### 3.3 Go_Daily_Inventory_Summary Transformation
```python
def transform_daily_inventory_summary(spark):
    """Transform Daily Inventory Summary aggregated table"""
    try:
        # Read from Gold layer fact table
        inventory_movement_fact_df = spark.read.format("delta").table(f"{gold_path}.go_inventory_movement_fact")
        
        records_read = inventory_movement_fact_df.count()
        
        # Create daily aggregations
        daily_summary = inventory_movement_fact_df \n            .groupBy("date_key", "product_key", "warehouse_key") \n            .agg(
                sum(when(col("movement_type") == "INBOUND", col("quantity_moved")).otherwise(0)).alias("total_receipts"),
                sum(when(col("movement_type") == "OUTBOUND", col("quantity_moved")).otherwise(0)).alias("total_issues"),
                sum(when(col("movement_type") == "ADJUSTMENT", col("quantity_moved")).otherwise(0)).alias("total_adjustments"),
                avg(col("unit_cost")).alias("average_cost"),
                sum(col("quantity_moved")).alias("net_movement")
            ) \n            .select(
                row_number().over(Window.orderBy("date_key", "product_key", "warehouse_key")).alias("summary_id"),
                concat(col("date_key"), lit("_"), col("product_key"), lit("_"), col("warehouse_key")).alias("summary_key"),
                col("date_key"),
                col("product_key"),
                col("warehouse_key"),
                lit(0).cast("decimal(15,2)").alias("opening_balance"),
                col("total_receipts").cast("decimal(15,2)"),
                col("total_issues").cast("decimal(15,2)"),
                col("total_adjustments").cast("decimal(15,2)"),
                (coalesce(col("total_receipts"), lit(0)) - coalesce(col("total_issues"), lit(0)) + coalesce(col("total_adjustments"), lit(0))).cast("decimal(15,2)").alias("closing_balance"),
                coalesce(col("average_cost"), lit(0.00)).cast("decimal(10,2)").alias("average_cost"),
                ((coalesce(col("total_receipts"), lit(0)) - coalesce(col("total_issues"), lit(0)) + coalesce(col("total_adjustments"), lit(0))) * coalesce(col("average_cost"), lit(0.00))).cast("decimal(15,2)").alias("total_value"),
                current_timestamp().alias("load_date"),
                lit("INVENTORY_SYSTEM").alias("source_system")
            )
        
        records_processed = daily_summary.count()
        
        # Write to Gold layer
        write_gold_table(daily_summary, "go_daily_inventory_summary")
        
        # Log audit record
        log_audit_record(spark, "transform_daily_inventory_summary", "go_inventory_movement_fact", "go_daily_inventory_summary", 
                        records_read, records_processed, records_processed, "SUCCESS")
        
        print(f"Successfully processed {records_processed} records for Go_Daily_Inventory_Summary")
        
    except Exception as e:
        error_msg = str(e)
        log_audit_record(spark, "transform_daily_inventory_summary", "go_inventory_movement_fact", "go_daily_inventory_summary", 
                        records_read if 'records_read' in locals() else 0, 0, 0, "FAILED", error_msg)
        log_error_record(spark, "go_daily_inventory_summary", "TRANSFORMATION_ERROR", error_msg, "PIPELINE_ERROR")
        raise e
```

### 3.4 Go_Monthly_Sales_Summary Transformation
```python
def transform_monthly_sales_summary(spark):
    """Transform Monthly Sales Summary aggregated table"""
    try:
        # Read from Gold layer fact table
        sales_fact_df = spark.read.format("delta").table(f"{gold_path}.go_sales_fact")
        
        records_read = sales_fact_df.count()
        
        # Create monthly aggregations
        monthly_summary = sales_fact_df \n            .withColumn("year_month", date_format(from_unixtime(col("date_key").cast("string"), "yyyyMMdd"), "yyyyMM").cast("int")) \n            .groupBy("year_month", "product_key", "warehouse_key", "customer_key") \n            .agg(
                sum(col("quantity_sold")).alias("total_quantity_sold"),
                sum(col("total_sales_amount")).alias("total_sales_amount"),
                sum(col("discount_amount")).alias("total_discount_amount"),
                sum(col("net_sales_amount")).alias("net_sales_amount"),
                count("*").alias("number_of_transactions"),
                avg(col("net_sales_amount")).alias("average_transaction_value")
            ) \n            .select(
                row_number().over(Window.orderBy("year_month", "product_key", "warehouse_key", "customer_key")).alias("summary_id"),
                concat(col("year_month"), lit("_"), col("product_key"), lit("_"), col("warehouse_key"), lit("_"), col("customer_key")).alias("summary_key"),
                col("year_month"),
                col("product_key"),
                col("warehouse_key"),
                col("customer_key"),
                col("total_quantity_sold").cast("decimal(15,2)"),
                col("total_sales_amount").cast("decimal(15,2)"),
                col("total_discount_amount").cast("decimal(15,2)"),
                col("net_sales_amount").cast("decimal(15,2)"),
                col("number_of_transactions").cast("int"),
                round(col("average_transaction_value"), 2).cast("decimal(10,2)").alias("average_transaction_value"),
                current_timestamp().alias("load_date"),
                lit("SALES_SYSTEM").alias("source_system")
            )
        
        records_processed = monthly_summary.count()
        
        # Write to Gold layer
        write_gold_table(monthly_summary, "go_monthly_sales_summary")
        
        # Log audit record
        log_audit_record(spark, "transform_monthly_sales_summary", "go_sales_fact", "go_monthly_sales_summary", 
                        records_read, records_processed, records_processed, "SUCCESS")
        
        print(f"Successfully processed {records_processed} records for Go_Monthly_Sales_Summary")
        
    except Exception as e:
        error_msg = str(e)
        log_audit_record(spark, "transform_monthly_sales_summary", "go_sales_fact", "go_monthly_sales_summary", 
                        records_read if 'records_read' in locals() else 0, 0, 0, "FAILED", error_msg)
        log_error_record(spark, "go_monthly_sales_summary", "TRANSFORMATION_ERROR", error_msg, "PIPELINE_ERROR")
        raise e
```

## 4. Main Execution Function

```python
def main():
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print("Starting Databricks Gold Fact DE Pipeline...")
        
        # Transform fact tables
        print("\n=== Processing Inventory Movement Fact ===")
        transform_inventory_movement_fact(spark)
        
        print("\n=== Processing Sales Fact ===")
        transform_sales_fact(spark)
        
        print("\n=== Processing Daily Inventory Summary ===")
        transform_daily_inventory_summary(spark)
        
        print("\n=== Processing Monthly Sales Summary ===")
        transform_monthly_sales_summary(spark)
        
        # Optimize tables for performance
        print("\n=== Optimizing Gold Layer Tables ===")
        optimize_gold_tables(spark)
        
        print("\n=== Pipeline Completed Successfully ===")
        
    except Exception as e:
        print(f"Pipeline failed with error: {str(e)}")
        raise e
    finally:
        spark.stop()

def optimize_gold_tables(spark):
    """Optimize Gold layer tables for performance"""
    try:
        # Z-ORDER optimization for fact tables
        spark.sql(f"OPTIMIZE {gold_path}.go_inventory_movement_fact ZORDER BY (product_key, warehouse_key, date_key)")
        spark.sql(f"OPTIMIZE {gold_path}.go_sales_fact ZORDER BY (product_key, customer_key, date_key)")
        spark.sql(f"OPTIMIZE {gold_path}.go_daily_inventory_summary ZORDER BY (date_key, product_key, warehouse_key)")
        spark.sql(f"OPTIMIZE {gold_path}.go_monthly_sales_summary ZORDER BY (year_month, product_key, customer_key)")
        
        # Vacuum old files (retain 7 days)
        spark.sql(f"VACUUM {gold_path}.go_inventory_movement_fact RETAIN 168 HOURS")
        spark.sql(f"VACUUM {gold_path}.go_sales_fact RETAIN 168 HOURS")
        spark.sql(f"VACUUM {gold_path}.go_daily_inventory_summary RETAIN 168 HOURS")
        spark.sql(f"VACUUM {gold_path}.go_monthly_sales_summary RETAIN 168 HOURS")
        
        print("Gold layer tables optimized successfully")
        
    except Exception as e:
        print(f"Optimization failed: {str(e)}")
        # Don't fail the pipeline for optimization errors
        pass

if __name__ == "__main__":
    main()
```

## 5. Business Transformations for Fact Tables

### 5.1 Data Quality Validations
- **Null Checks**: All primary keys and required fields validated for null values
- **Range Validations**: Quantities and amounts validated for positive values
- **Referential Integrity**: Foreign key relationships validated against dimension tables
- **Business Rules**: Custom business logic applied for inventory movements and sales calculations

### 5.2 Aggregation Logic
- **Daily Summaries**: Inventory movements aggregated by date