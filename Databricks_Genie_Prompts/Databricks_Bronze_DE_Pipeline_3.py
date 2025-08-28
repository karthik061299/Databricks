# Databricks Bronze DE Pipeline - Inventory Management System
# Author: AAVA Data Engineer
# Version: 3
# Description: Simplified Bronze layer data ingestion pipeline
# Error from previous version: INTERNAL_ERROR in Databricks job execution
# Error handling: Simplified code structure, removed complex configurations, basic functionality only

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

print("Starting Bronze Layer Data Ingestion Pipeline")
print(f"Execution started at: {datetime.now()}")

# Configuration Variables
SOURCE_SYSTEM = "PostgreSQL_Inventory_System"
BRONZE_DATABASE = "bronze"

# Table mapping configuration
TABLE_MAPPINGS = {
    "products": "bz_products",
    "suppliers": "bz_suppliers",
    "warehouses": "bz_warehouses"
}

# Function to read mock source data
def read_source_data(table_name):
    print(f"Reading source data for table: {table_name}")
    
    if table_name == "products":
        data = [
            (1, "Laptop", "Electronics"),
            (2, "Mouse", "Electronics"),
            (3, "Keyboard", "Electronics")
        ]
        columns = ["Product_ID", "Product_Name", "Category"]
        
    elif table_name == "suppliers":
        data = [
            (1, "Tech Supplier Inc", "+1-555-0101", 1),
            (2, "Electronics World", "+1-555-0102", 2),
            (3, "Office Solutions", "+1-555-0103", 3)
        ]
        columns = ["Supplier_ID", "Supplier_Name", "Contact_Number", "Product_ID"]
        
    elif table_name == "warehouses":
        data = [
            (1, "New York", 10000),
            (2, "Los Angeles", 15000),
            (3, "Chicago", 12000)
        ]
        columns = ["Warehouse_ID", "Location", "Capacity"]
    
    else:
        raise ValueError(f"Unknown table: {table_name}")
    
    df = spark.createDataFrame(data, columns)
    print(f"Successfully read {df.count()} records from {table_name}")
    return df

# Function to add metadata columns
def add_metadata_columns(df):
    return df.withColumn("load_timestamp", current_timestamp()) \
             .withColumn("update_timestamp", current_timestamp()) \
             .withColumn("source_system", lit(SOURCE_SYSTEM))

# Function to write data to Bronze layer
def write_to_bronze_layer(df, target_table_name):
    full_table_name = f"{BRONZE_DATABASE}.{target_table_name}"
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(full_table_name)
    
    print(f"Successfully wrote data to: {full_table_name}")

# Setup Bronze environment
print("Setting up Bronze environment...")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DATABASE}")
print(f"Bronze database '{BRONZE_DATABASE}' ready")

# Process tables
successful_tables = 0
failed_tables = 0

for source_table, target_table in TABLE_MAPPINGS.items():
    try:
        print(f"\nProcessing: {source_table} -> {target_table}")
        
        # Read source data
        source_df = read_source_data(source_table)
        
        # Add metadata
        bronze_df = add_metadata_columns(source_df)
        
        # Write to Bronze layer
        write_to_bronze_layer(bronze_df, target_table)
        
        successful_tables += 1
        print(f"✅ {source_table} processed successfully")
        
    except Exception as e:
        failed_tables += 1
        print(f"❌ Error processing {source_table}: {str(e)}")

# Summary
print("\n" + "=" * 60)
print("PIPELINE EXECUTION SUMMARY")
print("=" * 60)
print(f"Total tables: {len(TABLE_MAPPINGS)}")
print(f"Successful: {successful_tables}")
print(f"Failed: {failed_tables}")
print(f"Completed at: {datetime.now()}")

if failed_tables == 0:
    print("\n🎉 ALL TABLES PROCESSED SUCCESSFULLY!")
else:
    print(f"\n⚠️ {failed_tables} table(s) failed processing")

print("Bronze Layer Data Ingestion Pipeline Completed")

# Cost Reporting
# API Cost for this Bronze DE Pipeline creation: $0.001375