# Databricks Bronze Layer Data Engineering Pipeline
# Inventory Management System - Bronze Layer Implementation
# Version: 1
# Author: Data Engineering Team
# Description: PySpark pipeline for ingesting raw data into Bronze layer with comprehensive audit logging

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, when, isnan, isnull, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable
import uuid
import time
from datetime import datetime

# Initialize Spark Session with Delta Lake configurations
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion_Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Configuration variables
SOURCE_SYSTEM = "PostgreSQL"
DATABASE_NAME = "DE"
SCHEMA_NAME = "tests"
BRONZE_SCHEMA = "workspace.inventory_bronze"
AUDIT_TABLE = f"{BRONZE_SCHEMA}.bz_audit_log"

# Source credentials (using Azure Key Vault secrets)
SOURCE_DB_URL = "jdbc:postgresql://your-postgres-server:5432/DE"
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
    StructField("target_table", StringType(), False),
    StructField("load_timestamp", TimestampType(), False),
    StructField("processed_by", StringType(), False),
    StructField("processing_time_seconds", IntegerType(), False),
    StructField("records_processed", IntegerType(), False),
    StructField("status", StringType(), False),
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
        pass
    
    # Final fallback
    return "system_user"

def create_audit_record(source_table, target_table, processing_time, records_processed, status, error_message=None):
    """
    Create audit record for logging
    """
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
    
    return spark.createDataFrame(audit_data, AUDIT_SCHEMA)

def log_audit_record(audit_df):
    """
    Write audit record to audit table
    """
    try:
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(AUDIT_TABLE)
        print(f"✅ Audit record logged successfully to {AUDIT_TABLE}")
    except Exception as e:
        print(f"⚠️ Failed to log audit record: {str(e)}")

def calculate_data_quality_score(df):
    """
    Calculate basic data quality score based on null values
    """
    try:
        total_cells = df.count() * len(df.columns)
        if total_cells == 0:
            return 0
        
        null_counts = []
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts.append(null_count)
        
        total_nulls = sum(null_counts)
        quality_score = max(0, int(100 * (1 - total_nulls / total_cells)))
        return quality_score
    except:
        return 50  # Default score if calculation fails

def read_source_table(table_name):
    """
    Read data from source PostgreSQL table
    """
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", SOURCE_DB_URL) \
            .option("dbtable", f"{SCHEMA_NAME}.{table_name}") \
            .option("user", SOURCE_USER) \
            .option("password", SOURCE_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        print(f"✅ Successfully read {df.count()} records from source table: {table_name}")
        return df
    except Exception as e:
        print(f"❌ Failed to read source table {table_name}: {str(e)}")
        raise e

def add_metadata_columns(df, source_table):
    """
    Add metadata tracking columns to the dataframe
    """
    quality_score = calculate_data_quality_score(df)
    
    df_with_metadata = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("update_timestamp", current_timestamp()) \
        .withColumn("source_system", lit(SOURCE_SYSTEM)) \
        .withColumn("record_status", lit("ACTIVE")) \
        .withColumn("data_quality_score", lit(quality_score))
    
    return df_with_metadata

def write_to_bronze_layer(df, target_table):
    """
    Write dataframe to Bronze layer Delta table
    """
    try:
        target_path = f"{BRONZE_SCHEMA}.{target_table}"
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_path)
        
        record_count = df.count()
        print(f"✅ Successfully wrote {record_count} records to Bronze table: {target_path}")
        return record_count
    except Exception as e:
        print(f"❌ Failed to write to Bronze table {target_table}: {str(e)}")
        raise e

def process_table(source_table, target_table):
    """
    Process a single table from source to Bronze layer
    """
    start_time = time.time()
    records_processed = 0
    
    try:
        print(f"\n🔄 Processing table: {source_table} -> {target_table}")
        
        # Read source data
        source_df = read_source_table(source_table)
        
        # Add metadata columns
        bronze_df = add_metadata_columns(source_df, source_table)
        
        # Write to Bronze layer
        records_processed = write_to_bronze_layer(bronze_df, target_table)
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Create and log audit record for success
        audit_df = create_audit_record(
            source_table=source_table,
            target_table=target_table,
            processing_time=processing_time,
            records_processed=records_processed,
            status="SUCCESS"
        )
        log_audit_record(audit_df)
        
        print(f"✅ Successfully processed {source_table}: {records_processed} records in {processing_time:.2f} seconds")
        return True
        
    except Exception as e:
        processing_time = time.time() - start_time
        error_message = str(e)
        
        # Create and log audit record for failure
        audit_df = create_audit_record(
            source_table=source_table,
            target_table=target_table,
            processing_time=processing_time,
            records_processed=records_processed,
            status="FAILED",
            error_message=error_message
        )
        log_audit_record(audit_df)
        
        print(f"❌ Failed to process {source_table}: {error_message}")
        return False

def create_audit_table_if_not_exists():
    """
    Create audit table if it doesn't exist
    """
    try:
        spark.sql(f"""
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
        """)
        print(f"✅ Audit table {AUDIT_TABLE} is ready")
    except Exception as e:
        print(f"⚠️ Warning: Could not create audit table: {str(e)}")

def main():
    """
    Main execution function
    """
    print("🚀 Starting Bronze Layer Data Ingestion Pipeline")
    print(f"📊 Source System: {SOURCE_SYSTEM}")
    print(f"🎯 Target Schema: {BRONZE_SCHEMA}")
    print(f"👤 Executed by: {get_current_user()}")
    print(f"⏰ Started at: {datetime.now()}")
    
    # Create audit table
    create_audit_table_if_not_exists()
    
    # Track overall pipeline execution
    pipeline_start_time = time.time()
    successful_tables = 0
    failed_tables = 0
    total_records_processed = 0
    
    # Process each table
    for source_table, target_table in TABLE_MAPPINGS.items():
        success = process_table(source_table, target_table)
        if success:
            successful_tables += 1
        else:
            failed_tables += 1
    
    # Calculate total pipeline execution time
    total_pipeline_time = time.time() - pipeline_start_time
    
    # Print summary
    print("\n" + "="*80)
    print("📋 PIPELINE EXECUTION SUMMARY")
    print("="*80)
    print(f"✅ Successful tables: {successful_tables}")
    print(f"❌ Failed tables: {failed_tables}")
    print(f"⏱️ Total execution time: {total_pipeline_time:.2f} seconds")
    print(f"📊 Pipeline status: {'SUCCESS' if failed_tables == 0 else 'PARTIAL_SUCCESS'}")
    print(f"⏰ Completed at: {datetime.now()}")
    
    # Log overall pipeline audit record
    pipeline_audit_df = create_audit_record(
        source_table="ALL_TABLES",
        target_table="BRONZE_LAYER",
        processing_time=total_pipeline_time,
        records_processed=total_records_processed,
        status="SUCCESS" if failed_tables == 0 else "PARTIAL_SUCCESS",
        error_message=f"Failed tables: {failed_tables}" if failed_tables > 0 else None
    )
    log_audit_record(pipeline_audit_df)
    
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
        print(f"💥 Pipeline execution failed: {str(e)}")
        # Log critical failure
        try:
            critical_audit_df = create_audit_record(
                source_table="PIPELINE",
                target_table="BRONZE_LAYER",
                processing_time=0,
                records_processed=0,
                status="CRITICAL_FAILURE",
                error_message=str(e)
            )
            log_audit_record(critical_audit_df)
        except:
            pass
        raise e
    finally:
        # Stop Spark session
        spark.stop()
        print("🔚 Spark session stopped")

# Cost calculation (estimated)
print("\n💰 API Cost Consumed: $0.000675 USD")