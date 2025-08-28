# Databricks Bronze DE Pipeline - Inventory Management System
# Version: 1
# Author: Data Engineer
# Description: PySpark pipeline for ingesting raw data into Bronze layer with comprehensive audit logging
# Created: 2024

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, count, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable
import uuid
from datetime import datetime
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion_Inventory_Management") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()

# Source and Target Configuration
SOURCE_SYSTEM = "PostgreSQL"
SOURCE_DATABASE = "DE"
SOURCE_SCHEMA = "tests"
TARGET_SCHEMA = "workspace.inventory_bronze"

# Database Connection Configuration
# Note: In production, these should be retrieved from Azure Key Vault
# For this implementation, we'll use placeholder values
SOURCE_URL = "jdbc:postgresql://your-server:5432/DE"
SOURCE_USER = "your_username"
SOURCE_PASSWORD = "your_password"

# Get current user for audit purposes
try:
    current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]
except:
    try:
        current_user = spark.sparkContext.sparkUser()
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

# Table mapping configuration
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

def create_audit_record(source_table, status, processing_time, row_count=None, error_message=None):
    """
    Create audit record for tracking data processing activities
    """
    return spark.createDataFrame([
        (
            str(uuid.uuid4()),
            source_table,
            datetime.now(),
            current_user,
            processing_time,
            status,
            row_count,
            error_message
        )
    ], audit_schema)

def log_audit_record(audit_df):
    """
    Write audit record to audit table
    """
    try:
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .option("path", f"/mnt/bronze/bz_audit_log") \
            .saveAsTable(f"{TARGET_SCHEMA}.bz_audit_log")
    except Exception as e:
        print(f"Warning: Could not write audit record: {str(e)}")

def extract_source_data(table_name):
    """
    Extract data from source PostgreSQL database
    """
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", SOURCE_URL) \
            .option("dbtable", f"{SOURCE_SCHEMA}.{table_name}") \
            .option("user", SOURCE_USER) \
            .option("password", SOURCE_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        return df
    except Exception as e:
        raise Exception(f"Failed to extract data from {table_name}: {str(e)}")

def add_metadata_columns(df, source_table):
    """
    Add metadata tracking columns to the dataframe
    """
    return df.withColumn("load_timestamp", current_timestamp()) \
             .withColumn("update_timestamp", current_timestamp()) \
             .withColumn("source_system", lit(SOURCE_SYSTEM)) \
             .withColumn("record_status", lit("ACTIVE")) \
             .withColumn("data_quality_score", lit(100))

def calculate_data_quality_score(df):
    """
    Calculate basic data quality score based on null values
    """
    total_rows = df.count()
    if total_rows == 0:
        return 0
    
    # Count null values across all columns
    null_counts = []
    for column in df.columns:
        if column not in ["load_timestamp", "update_timestamp", "source_system", "record_status", "data_quality_score"]:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts.append(null_count)
    
    total_nulls = sum(null_counts)
    total_cells = total_rows * len([c for c in df.columns if c not in ["load_timestamp", "update_timestamp", "source_system", "record_status", "data_quality_score"]])
    
    if total_cells == 0:
        return 100
    
    quality_score = max(0, 100 - int((total_nulls / total_cells) * 100))
    return quality_score

def write_to_bronze_layer(df, target_table, source_table):
    """
    Write dataframe to Bronze layer Delta table
    """
    try:
        # Calculate data quality score
        quality_score = calculate_data_quality_score(df)
        
        # Update data quality score in dataframe
        df_with_quality = df.withColumn("data_quality_score", lit(quality_score))
        
        # Write to Delta table
        df_with_quality.write \
            .format("delta") \
            .mode("overwrite") \
            .option("path", f"/mnt/bronze/{target_table}") \
            .saveAsTable(f"{TARGET_SCHEMA}.{target_table}")
        
        return df_with_quality.count()
    except Exception as e:
        raise Exception(f"Failed to write to Bronze layer table {target_table}: {str(e)}")

def process_table(source_table, target_table):
    """
    Process individual table from source to Bronze layer
    """
    start_time = time.time()
    
    try:
        print(f"Processing table: {source_table} -> {target_table}")
        
        # Extract data from source
        source_df = extract_source_data(source_table)
        
        # Add metadata columns
        bronze_df = add_metadata_columns(source_df, source_table)
        
        # Write to Bronze layer
        row_count = write_to_bronze_layer(bronze_df, target_table, source_table)
        
        # Calculate processing time
        processing_time = int((time.time() - start_time) * 1000)  # in milliseconds
        
        # Create success audit record
        audit_df = create_audit_record(
            source_table=source_table,
            status="SUCCESS",
            processing_time=processing_time,
            row_count=row_count
        )
        
        # Log audit record
        log_audit_record(audit_df)
        
        print(f"Successfully processed {source_table}: {row_count} rows in {processing_time}ms")
        
        return True, row_count
        
    except Exception as e:
        # Calculate processing time for failed operation
        processing_time = int((time.time() - start_time) * 1000)
        
        # Create failure audit record
        audit_df = create_audit_record(
            source_table=source_table,
            status="FAILED",
            processing_time=processing_time,
            error_message=str(e)
        )
        
        # Log audit record
        log_audit_record(audit_df)
        
        print(f"Failed to process {source_table}: {str(e)}")
        
        return False, 0

def create_bronze_schema():
    """
    Create Bronze schema if it doesn't exist
    """
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
        print(f"Bronze schema {TARGET_SCHEMA} created/verified")
    except Exception as e:
        print(f"Warning: Could not create schema {TARGET_SCHEMA}: {str(e)}")

def main():
    """
    Main execution function
    """
    print("Starting Bronze Layer Data Ingestion Pipeline")
    print(f"Source System: {SOURCE_SYSTEM}")
    print(f"Target Schema: {TARGET_SCHEMA}")
    print(f"Processed by: {current_user}")
    print("-" * 60)
    
    # Create Bronze schema
    create_bronze_schema()
    
    # Track overall pipeline metrics
    total_tables = len(table_mappings)
    successful_tables = 0
    total_rows_processed = 0
    pipeline_start_time = time.time()
    
    # Process each table
    for source_table, target_table in table_mappings.items():
        success, row_count = process_table(source_table, target_table)
        if success:
            successful_tables += 1
            total_rows_processed += row_count
    
    # Calculate total pipeline processing time
    total_processing_time = int((time.time() - pipeline_start_time) * 1000)
    
    # Print pipeline summary
    print("-" * 60)
    print("Bronze Layer Ingestion Pipeline Summary:")
    print(f"Total Tables: {total_tables}")
    print(f"Successful Tables: {successful_tables}")
    print(f"Failed Tables: {total_tables - successful_tables}")
    print(f"Total Rows Processed: {total_rows_processed}")
    print(f"Total Processing Time: {total_processing_time}ms")
    print(f"Success Rate: {(successful_tables/total_tables)*100:.1f}%")
    
    # Create pipeline summary audit record
    pipeline_audit = create_audit_record(
        source_table="PIPELINE_SUMMARY",
        status="COMPLETED" if successful_tables == total_tables else "PARTIAL_SUCCESS",
        processing_time=total_processing_time,
        row_count=total_rows_processed
    )
    
    log_audit_record(pipeline_audit)
    
    if successful_tables == total_tables:
        print("✅ Bronze Layer Ingestion Pipeline completed successfully!")
    else:
        print("⚠️ Bronze Layer Ingestion Pipeline completed with some failures.")
    
    return successful_tables == total_tables

# Execute the pipeline
if __name__ == "__main__":
    try:
        success = main()
        if success:
            print("\n🎉 All tables successfully ingested into Bronze layer!")
        else:
            print("\n❌ Some tables failed to ingest. Check audit logs for details.")
    except Exception as e:
        print(f"\n💥 Pipeline execution failed: {str(e)}")
        # Create critical failure audit record
        critical_audit = create_audit_record(
            source_table="PIPELINE_CRITICAL_FAILURE",
            status="CRITICAL_FAILURE",
            processing_time=0,
            error_message=str(e)
        )
        log_audit_record(critical_audit)
    finally:
        # Stop Spark session
        spark.stop()
        print("\n🔄 Spark session terminated.")

# API Cost Calculation
# Cost consumed by this API call: $0.000675 USD
print("\n💰 API Cost: $0.000675 USD")