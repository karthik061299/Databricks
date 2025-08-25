_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Databricks Gold Aggregated DE Pipeline for transforming Silver Layer data to Gold Layer Fact and Aggregated tables
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Aggregated DE Pipeline
## Silver to Gold Layer Data Processing

## 1. Pipeline Overview

This pipeline transforms validated Silver Layer transactional data into Gold Layer aggregated fact tables and dimensional models for analytical reporting and business intelligence.

### 1.1 Data Flow Architecture
```
Silver Layer Tables → Business Transformations → Gold Layer Fact Tables → Aggregated Summary Tables
```

### 1.2 Processing Strategy
- **Extract**: Read from Silver Layer Delta tables
- **Transform**: Apply business logic and aggregations
- **Load**: Write to Gold Layer with optimized partitioning
- **Audit**: Track all transformations and data quality

## 2. PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, datediff, avg, count, when, date_add, sum, lag, row_number
from datetime import datetime
import uuid

def create_spark_session():
    """Create Spark session with Delta Lake support and configure paths"""
    spark = SparkSession.builder \n        .appName("Silver to Gold Data Processing") \n        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n        .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n        .getOrCreate()
    return spark

# Path configurations from credentials
silver_path = "workspace.inventory_silver"
gold_path = "workspace.inventory_gold"

def read_silver_table(spark, table_name):
    """Read table from Silver layer"""
    return spark.read.format("delta").table(f"{silver_path}.{table_name}")

def write_gold_table(df, table_name, mode="overwrite"):
    """Write table to Gold layer"""
    df.write.format("delta").mode(mode).saveAsTable(f"{gold_path}.{table_name}")

def log_audit_record(spark, process_name, source_table, target_table, records_read, records_processed, status, error_message=None):
    """Log audit information for pipeline tracking"""
    audit_data = [
        (
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            process_name,
            "AGGREGATION",
            "Gold_Layer_Pipeline",
            str(uuid.uuid4()),
            "Silver_to_Gold_Transform",
            "Data_Aggregation",
            source_table,
            target_table,
            records_read,
            records_processed,
            records_processed if status == "SUCCESS" else 0,
            0,
            0,
            records_read - records_processed if records_read > records_processed else 0,
            datetime.now(),
            datetime.now(),
            0,
            status,
            error_message,
            "databricks_user",
            datetime.now(),
            "DATABRICKS_GOLD"
        )
    ]
    
    audit_schema = StructType([
        StructField("audit_id", StringType(), True),
        StructField("audit_key", StringType(), True),
        StructField("process_name", StringType(), True),
        StructField("process_type", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("pipeline_run_id", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("step_name", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_processed", LongType(), True),
        StructField("records_inserted", LongType(), True),
        StructField("records_updated", LongType(), True),
        StructField("records_deleted", LongType(), True),
        StructField("records_rejected", LongType(), True),
        StructField("process_start_time", TimestampType(), True),
        StructField("process_end_time", TimestampType(), True),
        StructField("process_duration_seconds", IntegerType(), True),
        StructField("process_status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("load_date", TimestampType(), True),
        StructField("source_system", StringType(), True)
    ])
    
    audit_df = spark.createDataFrame(audit_data, audit_schema)
    write_gold_table(audit_df, "go_process_audit", "append")

def log_error_record(spark, table_name, error_type, error_description, record_identifier, actual_value):
    """Log data validation errors"""
    error_data = [
        (
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            table_name,
            "validation_column",
            record_identifier,
            error_type,
            "DATA_QUALITY",
            "ERROR",
            error_description,
            "expected_value",
            actual_value,
            "business_rule_validation",
            datetime.now(),
            "OPEN",
            "",
            "",
            None,
            datetime.now(),
            "DATABRICKS_GOLD"
        )
    ]
    
    error_schema = StructType([
        StructField("error_id", StringType(), True),
        StructField("error_key", StringType(), True),
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
    
    error_df = spark.createDataFrame(error_data, error_schema)
    write_gold_table(error_df, "go_data_validation_error", "append")

def transform_inventory_movement_fact(spark):
    """Transform Inventory Movement fact table"""
    try:
        # Read from Silver layer
        inventory_df = read_silver_table(spark, "si_inventory")
        products_df = read_silver_table(spark, "si_products")
        warehouses_df = read_silver_table(spark, "si_warehouses")
        suppliers_df = read_silver_table(spark, "si_suppliers")
        
        # Create window for movement type calculation
        window_spec = Window.partitionBy("Product_ID", "Warehouse_ID").orderBy("load_date")
        
        # Transform inventory movements
        inventory_movement_fact = inventory_df.alias("i") \n            .join(products_df.alias("p"), col("i.Product_ID") == col("p.Product_ID"), "inner") \n            .join(warehouses_df.alias("w"), col("i.Warehouse_ID") == col("w.Warehouse_ID"), "inner") \n            .join(suppliers_df.alias("s"), col("p.Product_ID") == col("s.Product_ID"), "left") \n            .select(
                row_number().over(Window.orderBy("i.load_date")).alias("inventory_movement_id"),
                concat(lit("INV_"), col("i.Inventory_ID"), lit("_"), date_format(col("i.load_date"), "yyyyMMdd")).alias("inventory_movement_key"),
                col("p.Product_ID").alias("product_key"),
                col("w.Warehouse_ID").alias("warehouse_key"),
                coalesce(col("s.Supplier_ID"), lit(0)).alias("supplier_key"),
                date_format(col("i.load_date"), "yyyyMMdd").cast("int").alias("date_key"),
                when(col("i.Quantity_Available") > 0, "RECEIPT").otherwise("ISSUE").alias("movement_type"),
                abs(col("i.Quantity_Available")).alias("quantity_moved"),
                lit(100.00).alias("unit_cost"),
                (abs(col("i.Quantity_Available")) * lit(100.00)).alias("total_value"),
                concat(lit("REF_"), col("i.Inventory_ID")).alias("reference_number"),
                col("i.load_date"),
                col("i.update_date"),
                col("i.source_system")
            )
        
        # Data quality validation
        error_records = inventory_movement_fact.filter(col("quantity_moved") < 0)
        if error_records.count() > 0:
            for row in error_records.collect():
                log_error_record(spark, "go_inventory_movement_fact", "NEGATIVE_QUANTITY", 
                               "Quantity moved cannot be negative", str(row.inventory_movement_key), str(row.quantity_moved))
        
        # Filter valid records
        valid_inventory_movement = inventory_movement_fact.filter(col("quantity_moved") >= 0)
        
        # Write to Gold layer
        write_gold_table(valid_inventory_movement, "go_inventory_movement_fact")
        
        # Log audit
        log_audit_record(spark, "transform_inventory_movement_fact", "si_inventory", "go_inventory_movement_fact", 
                        inventory_df.count(), valid_inventory_movement.count(), "SUCCESS")
        
        print(f"Successfully processed {valid_inventory_movement.count()} inventory movement records")
        
    except Exception as e:
        log_audit_record(spark, "transform_inventory_movement_fact", "si_inventory", "go_inventory_movement_fact", 
                        0, 0, "FAILED", str(e))
        raise e

def transform_sales_fact(spark):
    """Transform Sales fact table"""
    try:
        # Read from Silver layer
        orders_df = read_silver_table(spark, "si_orders")
        order_details_df = read_silver_table(spark, "si_order_details")
        products_df = read_silver_table(spark, "si_products")
        customers_df = read_silver_table(spark, "si_customers")
        warehouses_df = read_silver_table(spark, "si_warehouses")
        
        # Get default warehouse (first warehouse)
        default_warehouse = warehouses_df.select("Warehouse_ID").first()[0]
        
        # Transform sales data
        sales_fact = orders_df.alias("o") \n            .join(order_details_df.alias("od"), col("o.Order_ID") == col("od.Order_ID"), "inner") \n            .join(products_df.alias("p"), col("od.Product_ID") == col("p.Product_ID"), "inner") \n            .join(customers_df.alias("c"), col("o.Customer_ID") == col("c.Customer_ID"), "inner") \n            .select(
                row_number().over(Window.orderBy("o.Order_Date", "o.Order_ID")).alias("sales_id"),
                concat(lit("SALE_"), col("o.Order_ID"), lit("_"), col("p.Product_ID")).alias("sales_key"),
                col("p.Product_ID").alias("product_key"),
                col("c.Customer_ID").alias("customer_key"),
                lit(default_warehouse).alias("warehouse_key"),
                date_format(col("o.Order_Date"), "yyyyMMdd").cast("int").alias("date_key"),
                col("od.Quantity_Ordered").alias("quantity_sold"),
                lit(150.00).alias("unit_price"),
                (col("od.Quantity_Ordered") * lit(150.00)).alias("total_sales_amount"),
                (col("od.Quantity_Ordered") * lit(150.00) * lit(0.05)).alias("discount_amount"),
                ((col("od.Quantity_Ordered") * lit(150.00) * lit(0.95)) * lit(0.10)).alias("tax_amount"),
                ((col("od.Quantity_Ordered") * lit(150.00) * lit(0.95)) + 
                 ((col("od.Quantity_Ordered") * lit(150.00) * lit(0.95)) * lit(0.10))).alias("net_sales_amount"),
                col("o.load_date"),
                col("o.update_date"),
                col("o.source_system")
            )
        
        # Data quality validation
        error_records = sales_fact.filter((col("quantity_sold") <= 0) | (col("total_sales_amount") <= 0))
        if error_records.count() > 0:
            for row in error_records.collect():
                log_error_record(spark, "go_sales_fact", "INVALID_SALES_DATA", 
                               "Sales quantity and amount must be positive", str(row.sales_key), 
                               f"quantity: {row.quantity_sold}, amount: {row.total_sales_amount}")
        
        # Filter valid records
        valid_sales = sales_fact.filter((col("quantity_sold") > 0) & (col("total_sales_amount") > 0))
        
        # Write to Gold layer
        write_gold_table(valid_sales, "go_sales_fact")
        
        # Log audit
        log_audit_record(spark, "transform_sales_fact", "si_orders,si_order_details", "go_sales_fact", 
                        orders_df.count(), valid_sales.count(), "SUCCESS")
        
        print(f"Successfully processed {valid_sales.count()} sales records")
        
    except Exception as e:
        log_audit_record(spark, "transform_sales_fact", "si_orders,si_order_details", "go_sales_fact", 
                        0, 0, "FAILED", str(e))
        raise e

def transform_daily_inventory_summary(spark):
    """Transform Daily Inventory Summary aggregated table"""
    try:
        # Read from Silver layer
        inventory_df = read_silver_table(spark, "si_inventory")
        products_df = read_silver_table(spark, "si_products")
        warehouses_df = read_silver_table(spark, "si_warehouses")
        
        # Create window for lag calculation
        window_spec = Window.partitionBy("Product_ID", "Warehouse_ID").orderBy("load_date")
        
        # Transform daily inventory summary
        daily_summary = inventory_df.alias("i") \n            .join(products_df.alias("p"), col("i.Product_ID") == col("p.Product_ID"), "inner") \n            .join(warehouses_df.alias("w"), col("i.Warehouse_ID") == col("w.Warehouse_ID"), "inner") \n            .withColumn("previous_quantity", lag(col("i.Quantity_Available"), 1).over(window_spec)) \n            .groupBy(
                col("p.Product_ID").alias("product_key"),
                col("w.Warehouse_ID").alias("warehouse_key"),
                date_format(current_date(), "yyyyMMdd").cast("int").alias("date_key")
            ).agg(
                row_number().over(Window.orderBy("product_key", "warehouse_key")).alias("summary_id"),
                concat(date_format(current_date(), "yyyyMMdd"), lit("_"), col("product_key"), lit("_"), col("warehouse_key")).alias("summary_key"),
                coalesce(first("previous_quantity"), lit(0)).alias("opening_balance"),
                sum(when(col("i.Quantity_Available") > col("previous_quantity"), 
                        col("i.Quantity_Available") - coalesce(col("previous_quantity"), lit(0))).otherwise(0)).alias("total_receipts"),
                sum(when(col("i.Quantity_Available") < coalesce(col("previous_quantity"), lit(0)), 
                        coalesce(col("previous_quantity"), lit(0)) - col("i.Quantity_Available")).otherwise(0)).alias("total_issues"),
                sum(when(col("i.Quantity_Available") != coalesce(col("previous_quantity"), lit(0)), 
                        abs(col("i.Quantity_Available") - coalesce(col("previous_quantity"), lit(0)))).otherwise(0)).alias("total_adjustments"),
                max(col("i.Quantity_Available")).alias("closing_balance"),
                lit(100.00).alias("average_cost"),
                (max(col("i.Quantity_Available")) * lit(100.00)).alias("total_value"),
                current_timestamp().alias("load_date"),
                lit("DATABRICKS_GOLD").alias("source_system")
            )
        
        # Data quality validation
        error_records = daily_summary.filter((col("closing_balance") < 0) | (col("total_value") < 0))
        if error_records.count() > 0:
            for row in error_records.collect():
                log_error_record(spark, "go_daily_inventory_summary", "NEGATIVE_BALANCE", 
                               "Closing balance and total value cannot be negative", str(row.summary_key), 
                               f"balance: {row.closing_balance}, value: {row.total_value}")
        
        # Filter valid records
        valid_summary = daily_summary.filter((col("closing_balance") >= 0) & (col("total_value") >= 0))
        
        # Write to Gold layer
        write_gold_table(valid_summary, "go_daily_inventory_summary")
        
        # Log audit
        log_audit_record(spark, "transform_daily_inventory_summary", "si_inventory", "go_daily_inventory_summary", 
                        inventory_df.count(), valid_summary.count(), "SUCCESS")
        
        print(f"Successfully processed {valid_summary.count()} daily inventory summary records")
        
    except Exception as e:
        log_audit_record(spark, "transform_daily_inventory_summary", "si_inventory", "go_daily_inventory_summary", 
                        0, 0, "FAILED", str(e))
        raise e

def transform_monthly_sales_summary(spark):
    """Transform Monthly Sales Summary aggregated table"""
    try:
        # Read from Silver layer
        orders_df = read_silver_table(spark, "si_orders")
        order_details_df = read_silver_table(spark, "si_order_details")
        products_df = read_silver_table(spark, "si_products")
        customers_df = read_silver_table(spark, "si_customers")
        warehouses_df = read_silver_table(spark, "si_warehouses")
        
        # Get default warehouse
        default_warehouse = warehouses_df.select("Warehouse_ID").first()[0]
        
        # Transform monthly sales summary
        monthly_summary = orders_df.alias("o") \n            .join(order_details_df.alias("od"), col("o.Order_ID") == col("od.Order_ID"), "inner") \n            .join(products_df.alias("p"), col("od.Product_ID") == col("p.Product_ID"), "inner") \n            .join(customers_df.alias("c"), col("o.Customer_ID") == col("c.Customer_ID"), "inner") \n            .groupBy(
                date_format(col("o.Order_Date"), "yyyyMM").cast("int").alias("year_month"),
                col("p.Product_ID").alias("product_key"),
                lit(default_warehouse).alias("warehouse_key"),
                col("c.Customer_ID").alias("customer_key")
            ).agg(
                row_number().over(Window.orderBy("year_month", "product_key", "warehouse_key", "customer_key")).alias("summary_id"),
                concat(col("year_month"), lit("_"), col("product_key"), lit("_"), col("warehouse_key"), lit("_"), col("customer_key")).alias("summary_key"),
                sum(col("od.Quantity_Ordered")).alias("total_quantity_sold"),
                sum(col("od.Quantity_Ordered") * lit(150.00)).alias("total_sales_amount"),
                sum(col("od.Quantity_Ordered") * lit(150.00) * lit(0.05)).alias("total_discount_amount"),
                sum(col("od.Quantity_Ordered") * lit(150.00) * lit(0.95)).alias("net_sales_amount"),
                countDistinct(col("o.Order_ID")).alias("number_of_transactions"),
                (sum(col("od.Quantity_Ordered") * lit(150.00) * lit(0.95)) / countDistinct(col("o.Order_ID"))).alias("average_transaction_value"),
                current_timestamp().alias("load_date"),
                lit("DATABRICKS_GOLD").alias("source_system")
            )
        
        # Data quality validation
        error_records = monthly_summary.filter((col("total_quantity_sold") <= 0) | (col("total_sales_amount") <= 0))
        if error_records.count() > 0:
            for row in error_records.collect():
                log_error_record(spark, "go_monthly_sales_summary", "INVALID_SUMMARY_DATA", 
                               "Sales quantity and amount must be positive", str(row.summary_key), 
                               f"quantity: {row.total_quantity_sold}, amount: {row.total_sales_amount}")
        
        # Filter valid records
        valid_summary = monthly_summary.filter((col("total_quantity_sold") > 0) & (col("total_sales_amount") > 0))
        
        # Write to Gold layer
        write_gold_table(valid_summary, "go_monthly_sales_summary")
        
        # Log audit
        log_audit_record(spark, "transform_monthly_sales_summary", "si_orders,si_order_details", "go_monthly_sales_summary", 
                        orders_df.count(), valid_summary.count(), "SUCCESS")
        
        print(f"Successfully processed {valid_summary.count()} monthly sales summary records")
        
    except Exception as e:
        log_audit_record(spark, "transform_monthly_sales_summary", "si_orders,si_order_details", "go_monthly_sales_summary", 
                        0, 0, "FAILED", str(e))
        raise e

def optimize_gold_tables(spark):
    """Optimize Gold layer tables for better performance"""
    try:
        # Optimize fact tables
        spark.sql("OPTIMIZE workspace.inventory_gold.go_inventory_movement_fact ZORDER BY (product_key, warehouse_key, date_key)")
        spark.sql("OPTIMIZE workspace.inventory_gold.go_sales_fact ZORDER BY (product_key, customer_key, date_key)")
        
        # Optimize aggregated tables
        spark.sql("OPTIMIZE workspace.inventory_gold.go_daily_inventory_summary ZORDER BY (product_key, warehouse_key, date_key)")
        spark.sql("OPTIMIZE workspace.inventory_gold.go_monthly_sales_summary ZORDER BY (product_key, customer_key, year_month)")
        
        # Vacuum old files (retain 7 days)
        spark.sql("VACUUM workspace.inventory_gold.go_inventory_movement_fact RETAIN 168 HOURS")
        spark.sql("VACUUM workspace.inventory_gold.go_sales_fact RETAIN 168 HOURS")
        spark.sql("VACUUM workspace.inventory_gold.go_daily_inventory_summary RETAIN 168 HOURS")
        spark.sql("VACUUM workspace.inventory_gold.go_monthly_sales_summary RETAIN 168 HOURS")
        
        print("Successfully optimized all Gold layer tables")
        
    except Exception as e:
        print(f"Error optimizing tables: {str(e)}")
        raise e

def main():
    """Main execution function"""
    spark = create_spark_session()
    
    try:
        print("Starting Silver to Gold Layer transformation...")
        
        # Transform fact tables
        print("Transforming Inventory Movement Fact...")
        transform_inventory_movement_fact(spark)
        
        print("Transforming Sales Fact...")
        transform_sales_fact(spark)
        
        # Transform aggregated tables
        print("Transforming Daily Inventory Summary...")
        transform_daily_inventory_summary(spark)
        
        print("Transforming Monthly Sales Summary...")
        transform_monthly_sales_summary(spark)
        
        # Optimize tables for performance
        print("Optimizing Gold layer tables...")
        optimize_gold_tables(spark)
        
        print("Silver to Gold Layer transformation completed successfully!")
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

## 3. Business Transformations for Fact Tables

### 3.1 Inventory Movement Transformations
- **Movement Type Logic**: Determines RECEIPT/ISSUE based on quantity changes
- **Cost Calculation**: Applies standard unit cost of $100.00
- **Value Calculation**: Computes total value as quantity × unit cost
- **Reference Generation**: Creates unique reference numbers for tracking

### 3.2 Sales Fact Transformations
- **Pricing Logic**: Standard unit price of $150.00
- **Discount Calculation**: 5% standard discount rate
- **Tax Calculation**: 10% tax on net amount after discount
- **Net Amount**: Final amount including tax

### 3.3 Daily Inventory Summary Aggregations
- **Opening Balance**: Previous day's closing balance using LAG function
-