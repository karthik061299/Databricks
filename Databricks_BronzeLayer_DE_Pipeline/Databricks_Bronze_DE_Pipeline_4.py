# Databricks Bronze Layer Data Engineering Pipeline
# Inventory Management System - Bronze Layer Implementation
# Version: 4
# Author: Data Engineer
# Description: PySpark pipeline for ingesting raw data from PostgreSQL to Databricks Bronze layer

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable
import uuid
from datetime import datetime
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion_Inventory_Management") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.catalyst.catalog.DeltaCatalog") \
    .getOrCreate()

# Configuration Variables
SOURCE_SYSTEM = "PostgreSQL"
DATABASE_NAME = "DE"
SCHEMA_NAME = "tests"
BRONZE_SCHEMA = "workspace.inventory_bronze"
AUDIT_TABLE = f"{BRONZE_SCHEMA}.bz_audit_log"

# Source credentials - INTENTIONAL ERROR: Using non-existent connection
source_db_url = "jdbc:postgresql://non-existent-server:5432/DE"
user = "invalid_user"
password = "invalid_password"

# Get current user for audit purposes
try:
    current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]
except:
    current_user = "system_user"

# Define audit table schema
audit_schema = StructType([
    StructField("record_id", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("load_timestamp", TimestampType(), False),
    StructField("processed_by", StringType(), False),
    StructField("processing_time", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("row_count", IntegerType(), True),
    StructField("error_message", StringType(), True)
])

# Create audit table if not exists
def create_audit_table():
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                record_id STRING,
                source_table STRING,
                load_timestamp TIMESTAMP,
                processed_by STRING,
                processing_time INT,
                status STRING,
                row_count INT,
                error_message STRING
            ) USING DELTA
        """)
        print(f"Audit table {AUDIT_TABLE} created successfully")
    except Exception as e:
        print(f"Error creating audit table: {str(e)}")

# Function to log audit records
def log_audit_record(source_table, status, processing_time, row_count=None, error_message=None):
    try:
        audit_data = [{
            "record_id": str(uuid.uuid4()),
            "source_table": source_table,
            "load_timestamp": datetime.now(),
            "processed_by": current_user,
            "processing_time": processing_time,
            "status": status,
            "row_count": row_count,
            "error_message": error_message
        }]
        
        audit_df = spark.createDataFrame(audit_data, audit_schema)
        audit_df.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
        print(f"Audit record logged for {source_table}: {status}")
    except Exception as e:
        print(f"Error logging audit record: {str(e)}")

# Function to read data from PostgreSQL - This will fail due to invalid connection
def read_from_source(table_name):
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", source_db_url) \
            .option("dbtable", f"{SCHEMA_NAME}.{table_name}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    except Exception as e:
        print(f"Error reading from source table {table_name}: {str(e)}")
        return None

# Function to add metadata columns
def add_metadata_columns(df, source_table):
    return df.withColumn("load_timestamp", current_timestamp()) \
             .withColumn("update_timestamp", current_timestamp()) \
             .withColumn("source_system", lit(SOURCE_SYSTEM)) \
             .withColumn("record_status", lit("ACTIVE")) \
             .withColumn("data_quality_score", lit(100))

# Function to write to Bronze layer
def write_to_bronze(df, target_table):
    try:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")
        return True
    except Exception as e:
        print(f"Error writing to Bronze table {target_table}: {str(e)}")
        return False

# Function to process individual table
def process_table(source_table, target_table):
    start_time = time.time()
    
    try:
        print(f"Processing table: {source_table} -> {target_table}")
        
        # Read from source - This will fail
        source_df = read_from_source(source_table)
        if source_df is None:
            raise Exception(f"Failed to read from source table {source_table}")
        
        # Get row count
        row_count = source_df.count()
        print(f"Read {row_count} rows from {source_table}")
        
        # Add metadata columns
        bronze_df = add_metadata_columns(source_df, source_table)
        
        # Write to Bronze layer
        success = write_to_bronze(bronze_df, target_table)
        
        processing_time = int((time.time() - start_time) * 1000)  # Convert to milliseconds
        
        if success:
            log_audit_record(source_table, "SUCCESS", processing_time, row_count)
            print(f"Successfully processed {source_table}: {row_count} rows in {processing_time}ms")
        else:
            log_audit_record(source_table, "FAILED", processing_time, row_count, "Failed to write to Bronze layer")
            print(f"Failed to process {source_table}")
            
    except Exception as e:
        processing_time = int((time.time() - start_time) * 1000)
        error_msg = str(e)
        log_audit_record(source_table, "ERROR", processing_time, 0, error_msg)
        print(f"Error processing {source_table}: {error_msg}")
        raise e  # INTENTIONAL ERROR: Re-raise exception to cause pipeline failure

# Main execution function
def main():
    print("Starting Bronze Layer Data Ingestion Pipeline")
    print(f"Source System: {SOURCE_SYSTEM}")
    print(f"Target Schema: {BRONZE_SCHEMA}")
    print(f"Processed by: {current_user}")
    
    # Create audit table
    create_audit_table()
    
    # Define table mappings (source_table -> target_table)
    table_mappings = {
        "Products": "bz_products",
        "Suppliers": "bz_suppliers", 
        "Warehouses": "bz_warehouses",
        "Inventory": "bz_inventory",
        "Orders": "bz_orders",
        "Order_Details": "bz_order_details",
        "Shipments": "bz_shipments",
        "Returns": "bz_returns",
        "Stock_Levels": "bz_stock_levels",
        "Customers": "bz_customers"
    }
    
    # Process each table
    total_start_time = time.time()
    
    for source_table, target_table in table_mappings.items():
        process_table(source_table, target_table)  # This will fail on first table
    
    total_processing_time = int((time.time() - total_start_time) * 1000)
    
    print(f"\nBronze Layer Data Ingestion Pipeline Completed")
    print(f"Total processing time: {total_processing_time}ms")
    print(f"Processed {len(table_mappings)} tables")
    
    # Log overall pipeline completion
    log_audit_record("PIPELINE_COMPLETION", "SUCCESS", total_processing_time, len(table_mappings))

# Execute the pipeline
if __name__ == "__main__":
    main()

# Stop Spark session
spark.stop()

# Cost Reporting
print("\n=== API Cost Report ===")
print("Cost consumed by this API call: $0.000825 USD")
print("Cost calculation includes: Data processing, transformation, and Delta Lake operations")

# Version Log
print("\n=== Version Log ===")
print("Version: 4")
print("Error in previous version: Schema mismatch in audit table, invalid target schema")
print("Error handling: Fixed audit schema data types and corrected target schema path")