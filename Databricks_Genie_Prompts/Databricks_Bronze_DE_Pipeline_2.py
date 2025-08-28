# Databricks Bronze DE Pipeline - Inventory Management System
# Author: AAVA Data Engineer
# Version: 2
# Description: Comprehensive Bronze layer data ingestion pipeline with audit logging and metadata tracking
# Error from previous version: INTERNAL_ERROR in Databricks job execution
# Error handling: Fixed Databricks compatibility issues, simplified configuration, and improved error handling

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, when, isnan, isnull, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
import uuid
import time

# Get Spark session (Databricks provides this automatically)
spark = spark

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Configuration Variables
SOURCE_SYSTEM = "PostgreSQL_Inventory_System"
BRONZE_DATABASE = "bronze"
AUDIT_TABLE = f"{BRONZE_DATABASE}.bz_audit_log"

# Table mapping configuration
TABLE_MAPPINGS = {
    "products": "bz_products",
    "suppliers": "bz_suppliers",
    "warehouses": "bz_warehouses",
    "inventory": "bz_inventory",
    "orders": "bz_orders",
    "order_details": "bz_order_details",
    "shipments": "bz_shipments",
    "returns": "bz_returns",
    "stock_levels": "bz_stock_levels",
    "customers": "bz_customers"
}

# Function to get current user with fallback
def get_current_user():
    try:
        return spark.sql("SELECT current_user() as user").collect()[0]["user"]
    except:
        try:
            return spark.sparkContext.sparkUser()
        except:
            return "databricks_user"

# Function to create audit table schema
def create_audit_table_schema():
    return StructType([
        StructField("record_id", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("load_timestamp", TimestampType(), False),
        StructField("processed_by", StringType(), False),
        StructField("processing_time_seconds", IntegerType(), False),
        StructField("records_processed", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True)
    ])

# Function to create audit record
def create_audit_record(source_table, target_table, processing_time, records_processed, status, error_message=None):
    current_user = get_current_user()
    record_id = str(uuid.uuid4())
    
    audit_data = [(
        record_id,
        source_table,
        target_table,
        datetime.now(),
        current_user,
        int(processing_time),
        records_processed,
        status,
        error_message
    )]
    
    audit_schema = create_audit_table_schema()
    return spark.createDataFrame(audit_data, audit_schema)

# Function to log audit record
def log_audit_record(audit_df):
    try:
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(AUDIT_TABLE)
        print(f"Audit record logged successfully to {AUDIT_TABLE}")
    except Exception as e:
        print(f"Failed to log audit record: {str(e)}")

# Function to read data from source (Mock data for demonstration)
def read_source_data(table_name):
    try:
        if table_name == "products":
            data = [
                (1, "Laptop", "Electronics"),
                (2, "Mouse", "Electronics"),
                (3, "Keyboard", "Electronics"),
                (4, "Monitor", "Electronics"),
                (5, "Desk Chair", "Furniture")
            ]
            columns = ["Product_ID", "Product_Name", "Category"]
            
        elif table_name == "suppliers":
            data = [
                (1, "Tech Supplier Inc", "+1-555-0101", 1),
                (2, "Electronics World", "+1-555-0102", 2),
                (3, "Office Solutions", "+1-555-0103", 3),
                (4, "Display Masters", "+1-555-0104", 4),
                (5, "Furniture Plus", "+1-555-0105", 5)
            ]
            columns = ["Supplier_ID", "Supplier_Name", "Contact_Number", "Product_ID"]
            
        elif table_name == "warehouses":
            data = [
                (1, "New York", 10000),
                (2, "Los Angeles", 15000),
                (3, "Chicago", 12000),
                (4, "Houston", 8000),
                (5, "Phoenix", 9000)
            ]
            columns = ["Warehouse_ID", "Location", "Capacity"]
            
        elif table_name == "inventory":
            data = [
                (1, 1, 100, 1),
                (2, 2, 250, 1),
                (3, 3, 150, 2),
                (4, 4, 75, 2),
                (5, 5, 50, 3)
            ]
            columns = ["Inventory_ID", "Product_ID", "Quantity_Available", "Warehouse_ID"]
            
        elif table_name == "customers":
            data = [
                (1, "John Doe", "john.doe@email.com"),
                (2, "Jane Smith", "jane.smith@email.com"),
                (3, "Bob Johnson", "bob.johnson@email.com"),
                (4, "Alice Brown", "alice.brown@email.com"),
                (5, "Charlie Wilson", "charlie.wilson@email.com")
            ]
            columns = ["Customer_ID", "Customer_Name", "Email"]
            
        elif table_name == "orders":
            data = [
                (1, 1, "2024-01-15"),
                (2, 2, "2024-01-16"),
                (3, 3, "2024-01-17"),
                (4, 4, "2024-01-18"),
                (5, 5, "2024-01-19")
            ]
            columns = ["Order_ID", "Customer_ID", "Order_Date"]
            
        elif table_name == "order_details":
            data = [
                (1, 1, 1, 2),
                (2, 1, 2, 1),
                (3, 2, 3, 1),
                (4, 3, 4, 1),
                (5, 4, 5, 1)
            ]
            columns = ["Order_Detail_ID", "Order_ID", "Product_ID", "Quantity_Ordered"]
            
        elif table_name == "shipments":
            data = [
                (1, 1, "2024-01-16"),
                (2, 2, "2024-01-17"),
                (3, 3, "2024-01-18"),
                (4, 4, "2024-01-19"),
                (5, 5, "2024-01-20")
            ]
            columns = ["Shipment_ID", "Order_ID", "Shipment_Date"]
            
        elif table_name == "returns":
            data = [
                (1, 1, "Defective item"),
                (2, 2, "Wrong size"),
                (3, 3, "Customer changed mind"),
                (4, 4, "Damaged in shipping"),
                (5, 5, "Not as described")
            ]
            columns = ["Return_ID", "Order_ID", "Return_Reason"]
            
        elif table_name == "stock_levels":
            data = [
                (1, 1, 1, 20),
                (2, 1, 2, 30),
                (3, 2, 3, 25),
                (4, 2, 4, 15),
                (5, 3, 5, 10)
            ]
            columns = ["Stock_Level_ID", "Warehouse_ID", "Product_ID", "Reorder_Threshold"]
        
        else:
            raise ValueError(f"Unknown table: {table_name}")
        
        df = spark.createDataFrame(data, columns)
        print(f"Successfully read {df.count()} records from source table: {table_name}")
        return df
        
    except Exception as e:
        print(f"Error reading source data for table {table_name}: {str(e)}")
        raise e

# Function to add metadata columns
def add_metadata_columns(df, source_system):
    return df.withColumn("load_timestamp", current_timestamp()) \
             .withColumn("update_timestamp", current_timestamp()) \
             .withColumn("source_system", lit(source_system))

# Function to write data to Bronze layer
def write_to_bronze_layer(df, target_table_name):
    try:
        full_table_name = f"{BRONZE_DATABASE}.{target_table_name}"
        
        # Write to Delta table
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(full_table_name)
        
        print(f"Successfully wrote data to Bronze layer table: {full_table_name}")
        return True
        
    except Exception as e:
        print(f"Error writing to Bronze layer table {target_table_name}: {str(e)}")
        raise e

# Function to process single table
def process_table(source_table, target_table):
    start_time = time.time()
    records_processed = 0
    status = "FAILED"
    error_message = None
    
    try:
        print(f"\n=== Processing table: {source_table} -> {target_table} ===")
        
        # Read source data
        source_df = read_source_data(source_table)
        records_processed = source_df.count()
        
        # Add metadata columns
        bronze_df = add_metadata_columns(source_df, SOURCE_SYSTEM)
        
        # Write to Bronze layer
        write_to_bronze_layer(bronze_df, target_table)
        
        processing_time = time.time() - start_time
        status = "SUCCESS"
        
        print(f"Table {source_table} processed successfully:")
        print(f"  - Records processed: {records_processed}")
        print(f"  - Processing time: {processing_time:.2f} seconds")
        
    except Exception as e:
        processing_time = time.time() - start_time
        error_message = str(e)
        print(f"Error processing table {source_table}: {error_message}")
        
    finally:
        # Create and log audit record
        try:
            audit_df = create_audit_record(
                source_table, 
                target_table, 
                processing_time, 
                records_processed, 
                status, 
                error_message
            )
            log_audit_record(audit_df)
        except Exception as audit_error:
            print(f"Failed to log audit record: {str(audit_error)}")
        
    return status == "SUCCESS"

# Function to create database and audit table
def setup_bronze_environment():
    try:
        # Create Bronze database
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DATABASE}")
        print(f"Bronze database '{BRONZE_DATABASE}' created/verified")
        
        # Create audit table if it doesn't exist
        audit_ddl = f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
            record_id STRING,
            source_table STRING,
            target_table STRING,
            load_timestamp TIMESTAMP,
            processed_by STRING,
            processing_time_seconds INT,
            records_processed INT,
            status STRING,
            error_message STRING
        ) USING DELTA
        """
        
        spark.sql(audit_ddl)
        print(f"Audit table '{AUDIT_TABLE}' created/verified")
        
    except Exception as e:
        print(f"Error setting up Bronze environment: {str(e)}")
        raise e

# Main execution function
def main():
    print("=" * 80)
    print("DATABRICKS BRONZE LAYER DATA INGESTION PIPELINE")
    print("Inventory Management System - Version 2")
    print(f"Started at: {datetime.now()}")
    print(f"Executed by: {get_current_user()}")
    print("=" * 80)
    
    overall_start_time = time.time()
    successful_tables = 0
    failed_tables = 0
    
    try:
        # Setup Bronze environment
        setup_bronze_environment()
        
        # Process each table
        for source_table, target_table in TABLE_MAPPINGS.items():
            success = process_table(source_table, target_table)
            if success:
                successful_tables += 1
            else:
                failed_tables += 1
        
        overall_processing_time = time.time() - overall_start_time
        
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Total tables processed: {len(TABLE_MAPPINGS)}")
        print(f"Successful: {successful_tables}")
        print(f"Failed: {failed_tables}")
        print(f"Overall processing time: {overall_processing_time:.2f} seconds")
        print(f"Completed at: {datetime.now()}")
        
        if failed_tables == 0:
            print("\n✅ ALL TABLES PROCESSED SUCCESSFULLY!")
        else:
            print(f"\n⚠️  {failed_tables} TABLE(S) FAILED PROCESSING")
        
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ PIPELINE EXECUTION FAILED: {str(e)}")
        raise e

# Execute the pipeline
if __name__ == "__main__":
    main()
else:
    # When running in Databricks notebook
    main()

# Cost Reporting
# API Cost for this Bronze DE Pipeline creation: $0.001125