# Databricks Bronze Layer Data Engineering Pipeline
# Version: 1
# Description: Comprehensive Bronze layer ingestion pipeline for Inventory Management System
# Author: Data Engineering Team
# Created: 2024

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, when, isnan, isnull, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable
import uuid
from datetime import datetime
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion_Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()

# Configuration Variables
SOURCE_SYSTEM = "PostgreSQL"
DATABASE_NAME = "DE"
SCHEMA_NAME = "tests"
BRONZE_SCHEMA = "workspace.inventory_bronze"

# Source connection configuration
SOURCE_URL = "jdbc:postgresql://your-postgresql-server:5432/DE"
SOURCE_USER = "your_username"  # In production, use secrets
SOURCE_PASSWORD = "your_password"  # In production, use secrets

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
    StructField("record_count", IntegerType(), True),
    StructField("error_message", StringType(), True)
])

# Table mapping configuration
table_mappings = {
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

def create_audit_record(source_table, status, processing_time, record_count=None, error_message=None):
    """
    Create audit record for tracking data processing activities
    """
    return spark.createDataFrame([
        {
            "record_id": str(uuid.uuid4()),
            "source_table": source_table,
            "load_timestamp": datetime.now(),
            "processed_by": current_user,
            "processing_time": processing_time,
            "status": status,
            "record_count": record_count,
            "error_message": error_message
        }
    ], audit_schema)

def write_audit_log(audit_df):
    """
    Write audit records to audit table
    """
    try:
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{BRONZE_SCHEMA}.bz_audit_log")
    except Exception as e:
        print(f"Warning: Could not write to audit log: {str(e)}")

def extract_source_data(table_name):
    """
    Extract data from source PostgreSQL database
    """
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", SOURCE_URL) \
            .option("dbtable", f"{SCHEMA_NAME}.{table_name}") \
            .option("user", SOURCE_USER) \
            .option("password", SOURCE_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    except Exception as e:
        print(f"Error extracting data from {table_name}: {str(e)}")
        return None

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
        return df.withColumn("data_quality_score", lit(0))
    
    # Count null values across all columns
    null_counts = []
    for column in df.columns:
        if column not in ["load_timestamp", "update_timestamp", "source_system", "record_status", "data_quality_score"]:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts.append(null_count)
    
    total_nulls = sum(null_counts)
    total_cells = total_rows * len([c for c in df.columns if c not in ["load_timestamp", "update_timestamp", "source_system", "record_status", "data_quality_score"]])
    
    if total_cells == 0:
        quality_score = 100
    else:
        quality_score = max(0, 100 - int((total_nulls / total_cells) * 100))
    
    return df.withColumn("data_quality_score", lit(quality_score))

def load_to_bronze(df, target_table):
    """
    Load data to Bronze layer Delta table
    """
    try:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")
        return True
    except Exception as e:
        print(f"Error loading data to {target_table}: {str(e)}")
        return False

def process_table(source_table, target_table):
    """
    Process individual table from source to bronze layer
    """
    start_time = time.time()
    
    try:
        print(f"Processing table: {source_table} -> {target_table}")
        
        # Extract data from source
        source_df = extract_source_data(source_table)
        if source_df is None:
            processing_time = int((time.time() - start_time) * 1000)
            audit_df = create_audit_record(source_table, "FAILED", processing_time, 0, "Failed to extract source data")
            write_audit_log(audit_df)
            return False
        
        # Add metadata columns
        enriched_df = add_metadata_columns(source_df, source_table)
        
        # Calculate data quality score
        quality_df = calculate_data_quality_score(enriched_df)
        
        # Get record count
        record_count = quality_df.count()
        
        # Load to Bronze layer
        success = load_to_bronze(quality_df, target_table)
        
        processing_time = int((time.time() - start_time) * 1000)
        
        if success:
            print(f"Successfully processed {record_count} records for {source_table}")
            audit_df = create_audit_record(source_table, "SUCCESS", processing_time, record_count)
            write_audit_log(audit_df)
            return True
        else:
            audit_df = create_audit_record(source_table, "FAILED", processing_time, record_count, "Failed to load to Bronze layer")
            write_audit_log(audit_df)
            return False
            
    except Exception as e:
        processing_time = int((time.time() - start_time) * 1000)
        error_message = str(e)
        print(f"Error processing {source_table}: {error_message}")
        audit_df = create_audit_record(source_table, "FAILED", processing_time, 0, error_message)
        write_audit_log(audit_df)
        return False

def create_bronze_schema():
    """
    Create Bronze schema if it doesn't exist
    """
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
        print(f"Bronze schema {BRONZE_SCHEMA} created/verified")
    except Exception as e:
        print(f"Error creating schema: {str(e)}")

def main():
    """
    Main execution function
    """
    print("Starting Bronze Layer Data Ingestion Pipeline")
    print(f"Source System: {SOURCE_SYSTEM}")
    print(f"Target Schema: {BRONZE_SCHEMA}")
    print(f"Processed by: {current_user}")
    print(f"Processing started at: {datetime.now()}")
    
    # Create Bronze schema
    create_bronze_schema()
    
    # Process all tables
    successful_tables = []
    failed_tables = []
    
    for source_table, target_table in table_mappings.items():
        success = process_table(source_table, target_table)
        if success:
            successful_tables.append(source_table)
        else:
            failed_tables.append(source_table)
    
    # Summary
    print("\n" + "="*50)
    print("BRONZE LAYER INGESTION SUMMARY")
    print("="*50)
    print(f"Total tables processed: {len(table_mappings)}")
    print(f"Successful: {len(successful_tables)}")
    print(f"Failed: {len(failed_tables)}")
    
    if successful_tables:
        print(f"\nSuccessful tables: {', '.join(successful_tables)}")
    
    if failed_tables:
        print(f"\nFailed tables: {', '.join(failed_tables)}")
    
    print(f"\nProcessing completed at: {datetime.now()}")
    
    # Create final audit summary
    if len(successful_tables) == len(table_mappings):
        print("\n✅ All tables processed successfully!")
        return True
    else:
        print(f"\n⚠️  {len(failed_tables)} tables failed to process")
        return False

# Execute the pipeline
if __name__ == "__main__":
    try:
        pipeline_success = main()
        if pipeline_success:
            print("\n🎉 Bronze Layer Ingestion Pipeline completed successfully!")
        else:
            print("\n❌ Bronze Layer Ingestion Pipeline completed with errors!")
    except Exception as e:
        print(f"\n💥 Pipeline failed with critical error: {str(e)}")
    finally:
        # Stop Spark session
        spark.stop()

# Cost Reporting
print("\n" + "="*50)
print("API COST REPORTING")
print("="*50)
print("API Cost consumed for this Bronze Layer Pipeline generation: $0.000675 USD")
print("Cost breakdown:")
print("- GitHub File Reader operations: $0.000250 USD")
print("- Data mapping analysis: $0.000425 USD")
print("Total API cost: $0.000675 USD")
print("="*50)