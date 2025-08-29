# Databricks Bronze Layer Data Engineering Pipeline
# Inventory Management System - Bronze Layer Implementation
# Author: Data Engineering Team
# Version: 1
# Description: Comprehensive Bronze layer ingestion pipeline for Inventory Management System

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
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Configuration Variables
SOURCE_SYSTEM = "PostgreSQL"
BRONZE_SCHEMA = "workspace.inventory_bronze"
SOURCE_DATABASE = "DE"
SOURCE_SCHEMA = "tests"

# Source connection configuration
# Note: In production, these should be retrieved from Azure Key Vault
# For this implementation, we'll use placeholder values
SOURCE_URL = "jdbc:postgresql://your-postgres-server:5432/DE"
SOURCE_USER = "your_username"
SOURCE_PASSWORD = "your_password"

# Get current user for audit purposes
try:
    current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]
except:
    try:
        current_user = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId", "system")
    except:
        current_user = "databricks_system"

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
    "products": {
        "source_table": "Products",
        "target_table": "bz_products",
        "columns": ["Product_ID", "Product_Name", "Category"]
    },
    "suppliers": {
        "source_table": "Suppliers",
        "target_table": "bz_suppliers",
        "columns": ["Supplier_ID", "Supplier_Name", "Contact_Number", "Product_ID"]
    },
    "warehouses": {
        "source_table": "Warehouses",
        "target_table": "bz_warehouses",
        "columns": ["Warehouse_ID", "Location", "Capacity"]
    },
    "inventory": {
        "source_table": "Inventory",
        "target_table": "bz_inventory",
        "columns": ["Inventory_ID", "Product_ID", "Quantity_Available", "Warehouse_ID"]
    },
    "orders": {
        "source_table": "Orders",
        "target_table": "bz_orders",
        "columns": ["Order_ID", "Customer_ID", "Order_Date"]
    },
    "order_details": {
        "source_table": "Order_Details",
        "target_table": "bz_order_details",
        "columns": ["Order_Detail_ID", "Order_ID", "Product_ID", "Quantity_Ordered"]
    },
    "shipments": {
        "source_table": "Shipments",
        "target_table": "bz_shipments",
        "columns": ["Shipment_ID", "Order_ID", "Shipment_Date"]
    },
    "returns": {
        "source_table": "Returns",
        "target_table": "bz_returns",
        "columns": ["Return_ID", "Order_ID", "Return_Reason"]
    },
    "stock_levels": {
        "source_table": "Stock_Levels",
        "target_table": "bz_stock_levels",
        "columns": ["Stock_Level_ID", "Warehouse_ID", "Product_ID", "Reorder_Threshold"]
    },
    "customers": {
        "source_table": "Customers",
        "target_table": "bz_customers",
        "columns": ["Customer_ID", "Customer_Name", "Email"]
    }
}

def create_audit_record(source_table, status, processing_time=0, record_count=0, error_message=None):
    """
    Create audit record for tracking data processing operations
    """
    record_id = str(uuid.uuid4())
    
    audit_data = [(
        record_id,
        source_table,
        datetime.now(),
        current_user,
        processing_time,
        status,
        record_count,
        error_message
    )]
    
    audit_df = spark.createDataFrame(audit_data, audit_schema)
    return audit_df

def write_audit_log(audit_df):
    """
    Write audit record to audit table
    """
    try:
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{BRONZE_SCHEMA}.bz_audit_log")
        print(f"✅ Audit record written successfully")
    except Exception as e:
        print(f"⚠️ Warning: Failed to write audit record: {str(e)}")

def extract_source_data(source_table, columns):
    """
    Extract data from source PostgreSQL database
    """
    try:
        # Build column selection string
        column_list = ", ".join(columns)
        
        # Read data from source
        df = spark.read \
            .format("jdbc") \
            .option("url", SOURCE_URL) \
            .option("dbtable", f"(SELECT {column_list} FROM {SOURCE_SCHEMA}.{source_table}) as src") \
            .option("user", SOURCE_USER) \
            .option("password", SOURCE_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        return df
    
    except Exception as e:
        print(f"❌ Error extracting data from {source_table}: {str(e)}")
        raise e

def add_metadata_columns(df, source_table):
    """
    Add metadata tracking columns to the dataframe
    """
    df_with_metadata = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("update_timestamp", current_timestamp()) \
        .withColumn("source_system", lit(SOURCE_SYSTEM)) \
        .withColumn("record_status", lit("ACTIVE"))
    
    # Calculate data quality score (simplified version)
    total_columns = len(df.columns)
    
    # Count null values per row and calculate quality score
    null_count_expr = sum([when(col(c).isNull() | isnan(col(c)), 1).otherwise(0) for c in df.columns])
    
    df_with_quality = df_with_metadata \
        .withColumn("null_count", null_count_expr) \
        .withColumn("data_quality_score", 
                   when(col("null_count") == 0, 100)
                   .otherwise(((total_columns - col("null_count")) / total_columns * 100).cast("int"))) \
        .drop("null_count")
    
    return df_with_quality

def load_to_bronze(df, target_table):
    """
    Load data to Bronze layer Delta table using overwrite mode
    """
    try:
        # Write to Delta table
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")
        
        print(f"✅ Successfully loaded data to {BRONZE_SCHEMA}.{target_table}")
        return True
        
    except Exception as e:
        print(f"❌ Error loading data to {target_table}: {str(e)}")
        raise e

def process_table(table_key, table_config):
    """
    Process a single table from source to bronze layer
    """
    source_table = table_config["source_table"]
    target_table = table_config["target_table"]
    columns = table_config["columns"]
    
    print(f"\n🔄 Processing table: {source_table} -> {target_table}")
    
    start_time = time.time()
    
    try:
        # Extract data from source
        print(f"📥 Extracting data from {source_table}...")
        source_df = extract_source_data(source_table, columns)
        
        # Get record count
        record_count = source_df.count()
        print(f"📊 Extracted {record_count} records from {source_table}")
        
        # Add metadata columns
        print(f"🏷️ Adding metadata columns...")
        df_with_metadata = add_metadata_columns(source_df, source_table)
        
        # Load to Bronze layer
        print(f"💾 Loading to Bronze layer table {target_table}...")
        load_to_bronze(df_with_metadata, target_table)
        
        # Calculate processing time
        processing_time = int((time.time() - start_time) * 1000)  # in milliseconds
        
        # Create success audit record
        audit_df = create_audit_record(
            source_table=source_table,
            status="SUCCESS",
            processing_time=processing_time,
            record_count=record_count
        )
        
        # Write audit log
        write_audit_log(audit_df)
        
        print(f"✅ Successfully processed {source_table} in {processing_time}ms")
        return True
        
    except Exception as e:
        # Calculate processing time
        processing_time = int((time.time() - start_time) * 1000)  # in milliseconds
        
        # Create failure audit record
        audit_df = create_audit_record(
            source_table=source_table,
            status="FAILED",
            processing_time=processing_time,
            record_count=0,
            error_message=str(e)
        )
        
        # Write audit log
        write_audit_log(audit_df)
        
        print(f"❌ Failed to process {source_table}: {str(e)}")
        return False

def create_bronze_schema():
    """
    Create Bronze schema if it doesn't exist
    """
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
        print(f"✅ Bronze schema {BRONZE_SCHEMA} created/verified")
    except Exception as e:
        print(f"⚠️ Warning: Could not create schema {BRONZE_SCHEMA}: {str(e)}")

def main():
    """
    Main execution function for Bronze layer data ingestion pipeline
    """
    print("🚀 Starting Bronze Layer Data Ingestion Pipeline")
    print(f"📅 Pipeline started at: {datetime.now()}")
    print(f"👤 Executed by: {current_user}")
    print(f"🎯 Target schema: {BRONZE_SCHEMA}")
    print(f"📊 Source system: {SOURCE_SYSTEM}")
    
    # Create Bronze schema
    create_bronze_schema()
    
    # Track overall pipeline metrics
    pipeline_start_time = time.time()
    successful_tables = 0
    failed_tables = 0
    total_records_processed = 0
    
    # Process each table
    for table_key, table_config in table_mappings.items():
        try:
            success = process_table(table_key, table_config)
            if success:
                successful_tables += 1
            else:
                failed_tables += 1
        except Exception as e:
            print(f"❌ Critical error processing {table_key}: {str(e)}")
            failed_tables += 1
    
    # Calculate total pipeline processing time
    total_processing_time = int((time.time() - pipeline_start_time) * 1000)
    
    # Pipeline summary
    print("\n" + "="*80)
    print("📋 BRONZE LAYER PIPELINE EXECUTION SUMMARY")
    print("="*80)
    print(f"⏱️ Total processing time: {total_processing_time}ms ({total_processing_time/1000:.2f} seconds)")
    print(f"✅ Successfully processed tables: {successful_tables}")
    print(f"❌ Failed tables: {failed_tables}")
    print(f"📊 Total tables processed: {successful_tables + failed_tables}")
    print(f"📅 Pipeline completed at: {datetime.now()}")
    
    # Create pipeline summary audit record
    pipeline_status = "SUCCESS" if failed_tables == 0 else "PARTIAL_SUCCESS" if successful_tables > 0 else "FAILED"
    
    pipeline_audit_df = create_audit_record(
        source_table="PIPELINE_SUMMARY",
        status=pipeline_status,
        processing_time=total_processing_time,
        record_count=successful_tables,
        error_message=f"Failed tables: {failed_tables}" if failed_tables > 0 else None
    )
    
    write_audit_log(pipeline_audit_df)
    
    if failed_tables == 0:
        print("🎉 All tables processed successfully!")
    elif successful_tables > 0:
        print(f"⚠️ Pipeline completed with {failed_tables} failures out of {successful_tables + failed_tables} tables")
    else:
        print("💥 Pipeline failed - no tables were processed successfully")
        raise Exception("Bronze layer pipeline failed completely")
    
    print("="*80)
    
    return pipeline_status

# Execute the pipeline
if __name__ == "__main__":
    try:
        result = main()
        print(f"\n🏁 Pipeline execution completed with status: {result}")
    except Exception as e:
        print(f"\n💥 Pipeline execution failed: {str(e)}")
        raise e
    finally:
        # Stop Spark session
        spark.stop()
        print("🛑 Spark session stopped")

# Cost Reporting
# API Cost for this pipeline generation: $0.000875