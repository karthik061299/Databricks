# Databricks Bronze DE Pipeline - Version 1
# Inventory Management System - Bronze Layer Data Ingestion
# Author: Data Engineer
# Created: 2024
# Description: PySpark pipeline for ingesting raw data into Bronze layer

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
import uuid

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Configuration Variables
SOURCE_SYSTEM = "PostgreSQL"
DATABASE_NAME = "DE"
SCHEMA_NAME = "tests"
BRONZE_SCHEMA = "workspace.inventory_bronze"

# Credentials (Intentionally using wrong approach - will be fixed in later versions)
# Note: This version has intentional errors for demonstration
source_db_url = "jdbc:postgresql://localhost:5432/DE"  # Wrong - should use secrets
user = "admin"  # Wrong - should use secrets
password = "password123"  # Wrong - should use secrets

# Get current user (with fallback)
try:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
except:
    current_user = "system_user"

# Define audit table schema
audit_schema = StructType([
    StructField("record_id", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("load_timestamp", TimestampType(), True),
    StructField("processed_by", StringType(), True),
    StructField("processing_time", IntegerType(), True),
    StructField("status", StringType(), True),
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

def create_audit_record(table_name, status, row_count=0, processing_time=0, error_msg=None):
    """Create audit record for logging"""
    return [
        str(uuid.uuid4()),
        table_name,
        datetime.now(),
        current_user,
        processing_time,
        status,
        row_count,
        error_msg
    ]

def extract_data_from_source(table_name):
    """Extract data from PostgreSQL source - Version 1 with intentional error"""
    try:
        # Intentional error: Missing proper JDBC properties
        df = spark.read \
            .format("jdbc") \
            .option("url", source_db_url) \
            .option("dbtable", f"{SCHEMA_NAME}.{table_name}") \
            .option("user", user) \
            .option("password", password) \
            .load()  # Missing driver specification - will cause error
        
        return df
    except Exception as e:
        print(f"Error extracting data from {table_name}: {str(e)}")
        raise e

def add_metadata_columns(df, source_table):
    """Add metadata tracking columns"""
    return df.withColumn("load_timestamp", current_timestamp()) \
             .withColumn("update_timestamp", current_timestamp()) \
             .withColumn("source_system", lit(SOURCE_SYSTEM)) \
             .withColumn("record_status", lit("ACTIVE")) \
             .withColumn("data_quality_score", lit(100))

def load_to_bronze(df, target_table):
    """Load data to Bronze layer using Delta format"""
    try:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")
        
        return df.count()
    except Exception as e:
        print(f"Error loading data to {target_table}: {str(e)}")
        raise e

def log_audit_record(audit_data):
    """Log audit record to audit table"""
    try:
        audit_df = spark.createDataFrame([audit_data], audit_schema)
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{BRONZE_SCHEMA}.bz_audit_log")
    except Exception as e:
        print(f"Error logging audit record: {str(e)}")

def process_table(source_table, target_table):
    """Process individual table from source to bronze"""
    start_time = datetime.now()
    
    try:
        print(f"Processing table: {source_table} -> {target_table}")
        
        # Extract data from source
        source_df = extract_data_from_source(source_table)
        
        # Add metadata columns
        enriched_df = add_metadata_columns(source_df, source_table)
        
        # Load to Bronze layer
        row_count = load_to_bronze(enriched_df, target_table)
        
        # Calculate processing time
        processing_time = int((datetime.now() - start_time).total_seconds())
        
        # Log successful processing
        audit_record = create_audit_record(
            source_table, 
            "SUCCESS", 
            row_count, 
            processing_time
        )
        log_audit_record(audit_record)
        
        print(f"Successfully processed {row_count} records from {source_table}")
        
    except Exception as e:
        # Calculate processing time for failed operation
        processing_time = int((datetime.now() - start_time).total_seconds())
        
        # Log failed processing
        audit_record = create_audit_record(
            source_table, 
            "FAILED", 
            0, 
            processing_time, 
            str(e)
        )
        log_audit_record(audit_record)
        
        print(f"Failed to process {source_table}: {str(e)}")
        raise e

# Main execution
if __name__ == "__main__":
    print("Starting Bronze Layer Data Ingestion Pipeline - Version 1")
    print(f"Processing user: {current_user}")
    print(f"Source system: {SOURCE_SYSTEM}")
    print(f"Target schema: {BRONZE_SCHEMA}")
    
    overall_start_time = datetime.now()
    
    try:
        # Process all tables
        for source_table, target_table in table_mappings.items():
            process_table(source_table, target_table)
        
        overall_processing_time = int((datetime.now() - overall_start_time).total_seconds())
        print(f"\nPipeline completed successfully in {overall_processing_time} seconds")
        
        # Log overall success
        audit_record = create_audit_record(
            "PIPELINE_OVERALL", 
            "SUCCESS", 
            len(table_mappings), 
            overall_processing_time
        )
        log_audit_record(audit_record)
        
    except Exception as e:
        overall_processing_time = int((datetime.now() - overall_start_time).total_seconds())
        print(f"\nPipeline failed after {overall_processing_time} seconds: {str(e)}")
        
        # Log overall failure
        audit_record = create_audit_record(
            "PIPELINE_OVERALL", 
            "FAILED", 
            0, 
            overall_processing_time, 
            str(e)
        )
        log_audit_record(audit_record)
        
        raise e
    
    finally:
        spark.stop()

# Version Log:
# Version 1: Initial implementation with intentional errors
# - Missing JDBC driver specification
# - Hardcoded credentials instead of using secrets
# - Basic error handling implementation

# API Cost: $0.000125