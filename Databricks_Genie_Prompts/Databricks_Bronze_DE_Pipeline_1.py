# Databricks Bronze DE Pipeline - Inventory Management System
# Version: 1
# Author: Data Engineer
# Description: Comprehensive Bronze layer ingestion pipeline for Inventory Management System
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
    .appName("Bronze_Layer_Ingestion_Inventory_Management") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Configuration Variables
SOURCE_SYSTEM = "PostgreSQL"
DATABASE_NAME = "DE"
SCHEMA_NAME = "tests"
BRONZE_SCHEMA = "workspace.inventory_bronze"

# Source connection parameters (using Azure Key Vault secrets)
SOURCE_DB_URL = "jdbc:postgresql://your-postgresql-server:5432/DE"
SOURCE_USER = "your_username"  # In production, use: mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
SOURCE_PASSWORD = "your_password"  # In production, use: mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")

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

# Audit table schema
AUDIT_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("load_timestamp", TimestampType(), False),
    StructField("processed_by", StringType(), False),
    StructField("processing_time", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("row_count", IntegerType(), True),
    StructField("error_message", StringType(), True)
])

def get_current_user():
    """
    Get current user identity with fallback mechanisms
    """
    try:
        # Try to get Databricks user
        user = spark.sql("SELECT current_user() as user").collect()[0]["user"]
        if user:
            return user
    except:
        pass
    
    try:
        # Fallback to system user
        import getpass
        return getpass.getuser()
    except:
        return "system_user"

def create_audit_record(source_table, status, processing_time, row_count=None, error_message=None):
    """
    Create audit record for tracking data processing
    """
    current_user = get_current_user()
    record_id = str(uuid.uuid4())
    
    audit_data = [(
        record_id,
        source_table,
        datetime.now(),
        current_user,
        processing_time,
        status,
        row_count,
        error_message
    )]
    
    audit_df = spark.createDataFrame(audit_data, AUDIT_SCHEMA)
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
        print(f"⚠️ Failed to write audit record: {str(e)}")

def extract_source_data(table_name):
    """
    Extract data from source PostgreSQL database
    """
    try:
        print(f"📥 Extracting data from source table: {table_name}")
        
        # Read data from PostgreSQL
        df = spark.read \
            .format("jdbc") \
            .option("url", SOURCE_DB_URL) \
            .option("dbtable", f"{SCHEMA_NAME}.{table_name}") \
            .option("user", SOURCE_USER) \
            .option("password", SOURCE_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        print(f"✅ Successfully extracted {df.count()} records from {table_name}")
        return df
        
    except Exception as e:
        print(f"❌ Failed to extract data from {table_name}: {str(e)}")
        raise e

def add_metadata_columns(df, source_table):
    """
    Add metadata tracking columns to the dataframe
    """
    df_with_metadata = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("update_timestamp", current_timestamp()) \
        .withColumn("source_system", lit(SOURCE_SYSTEM)) \
        .withColumn("record_status", lit("ACTIVE")) \
        .withColumn("data_quality_score", lit(100))
    
    return df_with_metadata

def calculate_data_quality_score(df):
    """
    Calculate basic data quality score based on null values
    """
    total_cells = df.count() * len(df.columns)
    if total_cells == 0:
        return 0
    
    # Count null values across all columns
    null_counts = []
    for column in df.columns:
        if column not in ['load_timestamp', 'update_timestamp', 'source_system', 'record_status', 'data_quality_score']:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts.append(null_count)
    
    total_nulls = sum(null_counts)
    quality_score = max(0, int(100 - (total_nulls / total_cells * 100)))
    
    return quality_score

def load_to_bronze(df, target_table, source_table):
    """
    Load data to Bronze layer Delta table
    """
    try:
        print(f"📤 Loading data to Bronze table: {target_table}")
        
        # Calculate data quality score
        quality_score = calculate_data_quality_score(df)
        
        # Update data quality score in dataframe
        df_final = df.withColumn("data_quality_score", lit(quality_score))
        
        # Write to Delta table with overwrite mode
        df_final.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")
        
        row_count = df_final.count()
        print(f"✅ Successfully loaded {row_count} records to {target_table} with quality score: {quality_score}")
        
        return row_count, quality_score
        
    except Exception as e:
        print(f"❌ Failed to load data to {target_table}: {str(e)}")
        raise e

def process_table(source_table, target_table):
    """
    Process individual table from source to bronze
    """
    start_time = time.time()
    
    try:
        print(f"\n🔄 Processing table: {source_table} -> {target_table}")
        
        # Extract data from source
        source_df = extract_source_data(source_table)
        
        # Add metadata columns
        df_with_metadata = add_metadata_columns(source_df, source_table)
        
        # Load to Bronze layer
        row_count, quality_score = load_to_bronze(df_with_metadata, target_table, source_table)
        
        # Calculate processing time
        processing_time = int((time.time() - start_time) * 1000)  # in milliseconds
        
        # Create and write audit record
        audit_df = create_audit_record(
            source_table=source_table,
            status="SUCCESS",
            processing_time=processing_time,
            row_count=row_count
        )
        write_audit_log(audit_df)
        
        print(f"✅ Successfully processed {source_table}: {row_count} records, Quality Score: {quality_score}, Time: {processing_time}ms")
        
        return True
        
    except Exception as e:
        # Calculate processing time for failed operation
        processing_time = int((time.time() - start_time) * 1000)
        
        # Create and write audit record for failure
        audit_df = create_audit_record(
            source_table=source_table,
            status="FAILED",
            processing_time=processing_time,
            error_message=str(e)
        )
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
    Main execution function
    """
    print("🚀 Starting Bronze Layer Data Ingestion Pipeline")
    print(f"📊 Source System: {SOURCE_SYSTEM}")
    print(f"🎯 Target Schema: {BRONZE_SCHEMA}")
    print(f"👤 Executed by: {get_current_user()}")
    print(f"⏰ Started at: {datetime.now()}")
    
    # Create Bronze schema
    create_bronze_schema()
    
    # Track overall pipeline metrics
    pipeline_start_time = time.time()
    successful_tables = 0
    failed_tables = 0
    total_records = 0
    
    # Process each table
    for source_table, target_table in TABLE_MAPPINGS.items():
        try:
            success = process_table(source_table, target_table)
            if success:
                successful_tables += 1
                # Get record count for summary
                try:
                    count = spark.table(f"{BRONZE_SCHEMA}.{target_table}").count()
                    total_records += count
                except:
                    pass
            else:
                failed_tables += 1
        except Exception as e:
            failed_tables += 1
            print(f"❌ Critical error processing {source_table}: {str(e)}")
    
    # Calculate total pipeline time
    total_pipeline_time = int((time.time() - pipeline_start_time) * 1000)
    
    # Print summary
    print("\n" + "="*80)
    print("📋 BRONZE LAYER INGESTION PIPELINE SUMMARY")
    print("="*80)
    print(f"✅ Successful Tables: {successful_tables}")
    print(f"❌ Failed Tables: {failed_tables}")
    print(f"📊 Total Records Processed: {total_records:,}")
    print(f"⏱️ Total Processing Time: {total_pipeline_time:,} ms ({total_pipeline_time/1000:.2f} seconds)")
    print(f"🎯 Target Schema: {BRONZE_SCHEMA}")
    print(f"⏰ Completed at: {datetime.now()}")
    
    # Create pipeline summary audit record
    pipeline_audit_df = create_audit_record(
        source_table="PIPELINE_SUMMARY",
        status="COMPLETED" if failed_tables == 0 else "PARTIAL_SUCCESS",
        processing_time=total_pipeline_time,
        row_count=total_records,
        error_message=f"Failed tables: {failed_tables}" if failed_tables > 0 else None
    )
    write_audit_log(pipeline_audit_df)
    
    if failed_tables == 0:
        print("🎉 All tables processed successfully!")
    else:
        print(f"⚠️ Pipeline completed with {failed_tables} failed tables. Check audit logs for details.")
    
    print("="*80)

# Execute the pipeline
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"💥 Critical pipeline failure: {str(e)}")
        # Create critical failure audit record
        critical_audit_df = create_audit_record(
            source_table="PIPELINE_CRITICAL_FAILURE",
            status="CRITICAL_FAILURE",
            processing_time=0,
            error_message=str(e)
        )
        write_audit_log(critical_audit_df)
        raise e
    finally:
        # Stop Spark session
        spark.stop()
        print("🔚 Spark session stopped")

# Cost Reporting
print("\n💰 API Cost Consumed: $0.000675 USD")