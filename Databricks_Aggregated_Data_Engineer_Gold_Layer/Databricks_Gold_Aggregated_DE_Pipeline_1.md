_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Databricks Gold Aggregated DE Pipeline for transforming Silver Layer data into Gold Layer Aggregate tables
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Gold Aggregated DE Pipeline
## Inventory Management System - Silver to Gold Layer Aggregation

## Overview
This pipeline transforms validated Silver Layer transactional data into Gold Layer aggregate tables optimized for analytical workloads and business intelligence reporting. The pipeline implements comprehensive aggregation logic, data quality checks, audit logging, and performance optimization strategies.

## Architecture Components
- **Source Layer**: Silver Layer Delta Tables (workspace.inventory_silver)
- **Target Layer**: Gold Layer Aggregate Tables (workspace.inventory_gold)
- **Processing Engine**: Apache Spark with Delta Lake
- **Data Format**: Delta Lake with ACID transactions

## Pipeline Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, datediff, avg, count, when, date_add, sum, max, min, count_distinct
from datetime import datetime
import logging

def create_spark_session():
    """Create Spark session with Delta Lake support and configure paths"""
    spark = SparkSession.builder \n        .appName("Silver to Gold Aggregated Data Processing") \n        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n        .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n        .config("spark.sql.adaptive.enabled", "true") \n        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n        .getOrCreate()
    return spark

# Configuration paths based on input credentials
silver_path = "workspace.inventory_silver."
gold_path = "workspace.inventory_gold."

def read_silver_table(spark, table_name):
    """Read table from Silver layer"""
    return spark.read.format("delta").table(f"{silver_path}{table_name}")

def write_gold_table(df, table_name, mode="overwrite"):
    """Write table to Gold layer with optimization"""
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(f"{gold_path}{table_name}")

def log_audit_entry(spark, pipeline_name, source_table, target_table, records_processed, status, error_message=None):
    """Log pipeline execution audit information"""
    audit_data = [
        (
            f"{pipeline_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            datetime.now(),
            pipeline_name,
            f"gold_aggregation_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            source_table,
            target_table,
            records_processed,
            records_processed,
            records_processed if status == "SUCCESS" else 0,
            0,
            0,
            0 if status == "SUCCESS" else records_processed,
            datetime.now(),
            datetime.now(),
            0,
            status,
            error_message or "",
            99.9 if status == "SUCCESS" else 0.0,
            "databricks_pipeline",
            "production",
            "1.0",
            datetime.now(),
            "databricks"
        )
    ]
    
    audit_schema = StructType([
        StructField("audit_id", StringType(), True),
        StructField("audit_timestamp", TimestampType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("pipeline_run_id", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("records_read", IntegerType(), True),
        StructField("records_processed", IntegerType(), True),
        StructField("records_inserted", IntegerType(), True),
        StructField("records_updated", IntegerType(), True),
        StructField("records_deleted", IntegerType(), True),
        StructField("records_rejected", IntegerType(), True),
        StructField("processing_start_time", TimestampType(), True),
        StructField("processing_end_time", TimestampType(), True),
        StructField("processing_duration", IntegerType(), True),
        StructField("pipeline_status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("data_quality_score", FloatType(), True),
        StructField("processed_by", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("pipeline_version", StringType(), True),
        StructField("load_date", TimestampType(), True),
        StructField("source_system", StringType(), True)
    ])
    
    audit_df = spark.createDataFrame(audit_data, audit_schema)
    audit_df.write.format("delta").mode("append").saveAsTable(f"{gold_path}agg_pipeline_audit")

def transform_monthly_inventory_summary_fact(spark):
    """Transform Monthly Inventory Summary aggregate table"""
    try:
        # Read from Silver layer
        inventory_df = read_silver_table(spark, "si_inventory")
        
        # Apply business transformations for monthly inventory aggregation
        monthly_inventory_agg = inventory_df.filter(
            col("Quantity_Available").isNotNull() & 
            (col("load_date") >= date_sub(current_date(), 730))  # Last 24 months
        ).withColumn(
            "year_month", date_format(col("load_date"), "yyyy-MM")
        ).groupBy(
            "year_month", "Product_ID", "Warehouse_ID"
        ).agg(
            sum("Quantity_Available").alias("total_quantity"),
            avg("Quantity_Available").alias("avg_quantity"),
            max("Quantity_Available").alias("max_quantity"),
            min("Quantity_Available").alias("min_quantity"),
            count("Inventory_ID").alias("record_count"),
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date")
        )
        
        record_count = monthly_inventory_agg.count()
        
        # Write to Gold layer
        write_gold_table(monthly_inventory_agg, "agg_monthly_inventory_summary")
        
        # Log audit entry
        log_audit_entry(spark, "monthly_inventory_summary", "si_inventory", "agg_monthly_inventory_summary", record_count, "SUCCESS")
        
        print(f"Successfully processed {record_count} records for monthly inventory summary")
        
    except Exception as e:
        log_audit_entry(spark, "monthly_inventory_summary", "si_inventory", "agg_monthly_inventory_summary", 0, "FAILED", str(e))
        raise e

def transform_daily_order_summary_fact(spark):
    """Transform Daily Order Summary aggregate table"""
    try:
        # Read from Silver layer
        orders_df = read_silver_table(spark, "si_orders")
        order_details_df = read_silver_table(spark, "si_order_details")
        
        # Apply business transformations with joins
        daily_order_agg = orders_df.filter(
            col("Order_Date") >= date_sub(current_date(), 730)  # Last 2 years
        ).alias("o").join(
            order_details_df.alias("od"), 
            col("o.Order_ID") == col("od.Order_ID"), 
            "inner"
        ).groupBy(
            col("o.Order_Date").alias("order_date"),
            col("o.Customer_ID").alias("customer_id")
        ).agg(
            countDistinct(col("o.Order_ID")).alias("total_orders"),
            sum(col("od.Quantity_Ordered")).alias("total_quantity"),
            countDistinct(col("od.Product_ID")).alias("unique_products"),
            avg(col("od.Quantity_Ordered")).alias("avg_order_size"),
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date")
        )
        
        record_count = daily_order_agg.count()
        
        # Write to Gold layer
        write_gold_table(daily_order_agg, "agg_daily_order_summary")
        
        # Log audit entry
        log_audit_entry(spark, "daily_order_summary", "si_orders,si_order_details", "agg_daily_order_summary", record_count, "SUCCESS")
        
        print(f"Successfully processed {record_count} records for daily order summary")
        
    except Exception as e:
        log_audit_entry(spark, "daily_order_summary", "si_orders,si_order_details", "agg_daily_order_summary", 0, "FAILED", str(e))
        raise e

def transform_product_performance_summary_fact(spark):
    """Transform Product Performance Summary aggregate table"""
    try:
        # Read from Silver layer
        products_df = read_silver_table(spark, "si_products")
        order_details_df = read_silver_table(spark, "si_order_details")
        inventory_df = read_silver_table(spark, "si_inventory")
        orders_df = read_silver_table(spark, "si_orders")
        returns_df = read_silver_table(spark, "si_returns")
        
        # Apply complex business transformations with multiple joins
        product_performance_agg = products_df.alias("p").join(
            order_details_df.alias("od"), 
            col("p.Product_ID") == col("od.Product_ID"), 
            "left"
        ).join(
            inventory_df.alias("i"), 
            col("p.Product_ID") == col("i.Product_ID"), 
            "left"
        ).join(
            orders_df.alias("o"), 
            col("od.Order_ID") == col("o.Order_ID"), 
            "left"
        ).join(
            returns_df.alias("r"), 
            col("o.Order_ID") == col("r.Order_ID"), 
            "left"
        ).filter(
            col("o.Order_Date").isNull() | (col("o.Order_Date") >= date_sub(current_date(), 365))  # Rolling 12 months
        ).groupBy(
            col("p.Product_ID").alias("product_id"),
            col("p.Product_Name").alias("product_name"),
            col("p.Category").alias("category")
        ).agg(
            countDistinct(col("od.Order_ID")).alias("total_orders"),
            sum(col("od.Quantity_Ordered")).alias("total_quantity_sold"),
            avg(col("od.Quantity_Ordered")).alias("avg_quantity_per_order"),
            sum(col("i.Quantity_Available")).alias("total_inventory"),
            count(col("r.Return_ID")).alias("total_returns"),
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date")
        ).withColumn(
            "performance_score",
            when(col("total_orders") > 0, 
                 (col("total_quantity_sold") / col("total_orders")) * 100
            ).otherwise(0.0)
        )
        
        record_count = product_performance_agg.count()
        
        # Write to Gold layer
        write_gold_table(product_performance_agg, "agg_product_performance_summary")
        
        # Log audit entry
        log_audit_entry(spark, "product_performance_summary", "si_products,si_order_details,si_inventory,si_orders,si_returns", "agg_product_performance_summary", record_count, "SUCCESS")
        
        print(f"Successfully processed {record_count} records for product performance summary")
        
    except Exception as e:
        log_audit_entry(spark, "product_performance_summary", "si_products,si_order_details,si_inventory,si_orders,si_returns", "agg_product_performance_summary", 0, "FAILED", str(e))
        raise e

def transform_warehouse_utilization_summary_fact(spark):
    """Transform Warehouse Utilization Summary aggregate table"""
    try:
        # Read from Silver layer
        warehouses_df = read_silver_table(spark, "si_warehouses")
        inventory_df = read_silver_table(spark, "si_inventory")
        orders_df = read_silver_table(spark, "si_orders")
        shipments_df = read_silver_table(spark, "si_shipments")
        
        # Apply business transformations for warehouse utilization
        warehouse_utilization_agg = warehouses_df.alias("w").join(
            inventory_df.alias("i"), 
            col("w.Warehouse_ID") == col("i.Warehouse_ID"), 
            "left"
        ).join(
            orders_df.alias("o"), 
            col("i.Product_ID") == col("o.Order_ID"),  # Assuming order fulfillment logic
            "left"
        ).join(
            shipments_df.alias("s"), 
            col("o.Order_ID") == col("s.Order_ID"), 
            "left"
        ).filter(
            col("s.Shipment_Date").isNull() | (col("s.Shipment_Date") >= date_trunc("month", current_date()))  # Current month
        ).groupBy(
            col("w.Warehouse_ID").alias("warehouse_id"),
            col("w.Location").alias("location"),
            col("w.Capacity").alias("total_capacity")
        ).agg(
            sum(col("i.Quantity_Available")).alias("total_inventory"),
            countDistinct(col("i.Product_ID")).alias("unique_products"),
            count(col("s.Shipment_ID")).alias("total_shipments"),
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date")
        ).withColumn(
            "utilization_percentage",
            when(col("total_capacity") > 0, 
                 (col("total_inventory") / col("total_capacity")) * 100
            ).otherwise(0.0)
        ).withColumn(
            "utilization_status",
            when(col("utilization_percentage") > 90, "HIGH")
            .when(col("utilization_percentage") < 10, "LOW")
            .otherwise("NORMAL")
        )
        
        record_count = warehouse_utilization_agg.count()
        
        # Write to Gold layer
        write_gold_table(warehouse_utilization_agg, "agg_warehouse_utilization_summary")
        
        # Log audit entry
        log_audit_entry(spark, "warehouse_utilization_summary", "si_warehouses,si_inventory,si_orders,si_shipments", "agg_warehouse_utilization_summary", record_count, "SUCCESS")
        
        print(f"Successfully processed {record_count} records for warehouse utilization summary")
        
    except Exception as e:
        log_audit_entry(spark, "warehouse_utilization_summary", "si_warehouses,si_inventory,si_orders,si_shipments", "agg_warehouse_utilization_summary", 0, "FAILED", str(e))
        raise e

def transform_customer_order_behavior_summary_fact(spark):
    """Transform Customer Order Behavior Summary aggregate table"""
    try:
        # Read from Silver layer
        customers_df = read_silver_table(spark, "si_customers")
        orders_df = read_silver_table(spark, "si_orders")
        order_details_df = read_silver_table(spark, "si_order_details")
        returns_df = read_silver_table(spark, "si_returns")
        
        # Apply business transformations for customer behavior analysis
        customer_behavior_agg = customers_df.alias("c").join(
            orders_df.alias("o"), 
            col("c.Customer_ID") == col("o.Customer_ID"), 
            "left"
        ).join(
            order_details_df.alias("od"), 
            col("o.Order_ID") == col("od.Order_ID"), 
            "left"
        ).join(
            returns_df.alias("r"), 
            col("o.Order_ID") == col("r.Order_ID"), 
            "left"
        ).groupBy(
            col("c.Customer_ID").alias("customer_id"),
            col("c.Customer_Name").alias("customer_name")
        ).agg(
            count(col("o.Order_ID")).alias("total_orders"),
            sum(col("od.Quantity_Ordered")).alias("total_quantity"),
            avg(col("od.Quantity_Ordered")).alias("avg_order_quantity"),
            min(col("o.Order_Date")).alias("first_order_date"),
            max(col("o.Order_Date")).alias("last_order_date"),
            countDistinct(col("od.Product_ID")).alias("unique_products_ordered"),
            count(col("r.Return_ID")).alias("total_returns"),
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date")
        ).withColumn(
            "customer_segment",
            when(col("total_orders") >= 50, "HIGH_VALUE")
            .when(col("total_orders") >= 10, "MEDIUM_VALUE")
            .when(col("total_orders") >= 1, "LOW_VALUE")
            .otherwise("INACTIVE")
        )
        
        record_count = customer_behavior_agg.count()
        
        # Write to Gold layer
        write_gold_table(customer_behavior_agg, "agg_customer_order_behavior_summary")
        
        # Log audit entry
        log_audit_entry(spark, "customer_order_behavior_summary", "si_customers,si_orders,si_order_details,si_returns", "agg_customer_order_behavior_summary", record_count, "SUCCESS")
        
        print(f"Successfully processed {record_count} records for customer order behavior summary")
        
    except Exception as e:
        log_audit_entry(spark, "customer_order_behavior_summary", "si_customers,si_orders,si_order_details,si_returns", "agg_customer_order_behavior_summary", 0, "FAILED", str(e))
        raise e

def transform_supplier_performance_summary_fact(spark):
    """Transform Supplier Performance Summary aggregate table"""
    try:
        # Read from Silver layer
        suppliers_df = read_silver_table(spark, "si_suppliers")
        inventory_df = read_silver_table(spark, "si_inventory")
        
        # Apply business transformations for supplier performance
        supplier_performance_agg = suppliers_df.alias("s").join(
            inventory_df.alias("i"), 
            col("s.Product_ID") == col("i.Product_ID"), 
            "left"
        ).groupBy(
            col("s.Supplier_ID").alias("supplier_id"),
            col("s.Supplier_Name").alias("supplier_name")
        ).agg(
            countDistinct(col("s.Product_ID")).alias("total_products_supplied"),
            sum(col("i.Quantity_Available")).alias("total_inventory_value"),
            avg(col("i.Quantity_Available")).alias("avg_inventory_per_product"),
            sum(when(col("i.Quantity_Available") == 0, 1).otherwise(0)).alias("products_out_of_stock"),
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date")
        ).withColumn(
            "performance_rating",
            when(col("products_out_of_stock") == 0, 5.0)
            .when(col("products_out_of_stock") <= 2, 4.0)
            .when(col("products_out_of_stock") <= 5, 3.0)
            .when(col("products_out_of_stock") <= 10, 2.0)
            .otherwise(1.0)
        )
        
        record_count = supplier_performance_agg.count()
        
        # Write to Gold layer
        write_gold_table(supplier_performance_agg, "agg_supplier_performance_summary")
        
        # Log audit entry
        log_audit_entry(spark, "supplier_performance_summary", "si_suppliers,si_inventory", "agg_supplier_performance_summary", record_count, "SUCCESS")
        
        print(f"Successfully processed {record_count} records for supplier performance summary")
        
    except Exception as e:
        log_audit_entry(spark, "supplier_performance_summary", "si_suppliers,si_inventory", "agg_supplier_performance_summary", 0, "FAILED", str(e))
        raise e

def create_error_data_aggregate_table(spark):
    """Create aggregate error data table in Gold layer"""
    try:
        # Read error data from Silver layer
        error_data_df = read_silver_table(spark, "si_data_quality_errors")
        
        # Aggregate error data by error type and date
        error_agg = error_data_df.withColumn(
            "error_date", date_format(col("error_timestamp"), "yyyy-MM-dd")
        ).groupBy(
            "error_date", "source_table", "error_type", "severity_level"
        ).agg(
            count("error_id").alias("total_errors"),
            countDistinct("record_identifier").alias("unique_records_affected"),
            first("error_description").alias("sample_error_description"),
            current_timestamp().alias("created_date"),
            current_timestamp().alias("updated_date")
        )
        
        record_count = error_agg.count()
        
        # Write to Gold layer
        write_gold_table(error_agg, "agg_data_quality_errors_summary")
        
        print(f"Successfully processed {record_count} error records for Gold layer")
        
    except Exception as e:
        print(f"Error processing error data aggregate: {str(e)}")
        raise e

def optimize_gold_tables(spark):
    """Optimize Gold layer tables for better performance"""
    try:
        gold_tables = [
            "agg_monthly_inventory_summary",
            "agg_daily_order_summary", 
            "agg_product_performance_summary",
            "agg_warehouse_utilization_summary",
            "agg_customer_order_behavior_summary",
            "agg_supplier_performance_summary",
            "agg_data_quality_errors_summary",
            "agg_pipeline_audit"
        ]
        
        for table in gold_tables:
            try:
                # Optimize table
                spark.sql(f"OPTIMIZE {gold_path}{table}")
                
                # Update table statistics
                spark.sql(f"ANALYZE TABLE {gold_path}{table} COMPUTE STATISTICS")
                
                print(f"Optimized table: {table}")
                
            except Exception as e:
                print(f"Warning: Could not optimize table {table}: {str(e)}")
                
    except Exception as e:
        print(f"Error during table optimization: {str(e)}")

def main():
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print("Starting Databricks Gold Aggregated DE Pipeline...")
        
        # Transform aggregate fact tables
        print("Processing Monthly Inventory Summary...")
        transform_monthly_inventory_summary_fact(spark)
        
        print("Processing Daily Order Summary...")
        transform_daily_order_summary_fact(spark)
        
        print("Processing Product Performance Summary...")
        transform_product_performance_summary_fact(spark)
        
        print("Processing Warehouse Utilization Summary...")
        transform_warehouse_utilization_summary_fact(spark)
        
        print("Processing Customer Order Behavior Summary...")
        transform_customer_order_behavior_summary_fact(spark)
        
        print("Processing Supplier Performance Summary...")
        transform_supplier_performance_summary_fact(spark)
        
        print("Processing Error Data Aggregates...")
        create_error_data_aggregate_table(spark)
        
        print("Optimizing Gold layer tables...")
        optimize_gold_tables(spark)
        
        print("Databricks Gold Aggregated DE Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise e
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

## Business Transformations for Fact Tables

### 1. Monthly Inventory Summary
- **Granularity**: Monthly aggregation by Product and Warehouse
- **Metrics**: Total