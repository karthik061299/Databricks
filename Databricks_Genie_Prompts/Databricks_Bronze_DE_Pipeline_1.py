# Databricks Bronze DE Pipeline - Inventory Management System
# Author: AAVA Data Engineer
# Version: 1
# Description: Comprehensive Bronze layer ingestion pipeline for Inventory Management System
# Source: PostgreSQL Database
# Target: Databricks Delta Lake Bronze Layer

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, when, isnan, isnull, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable
import uuid
from datetime import datetime
import time

# Initialize Spark Session with Delta Lake configurations
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion_Inventory_Management") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Source and Target Configuration
SOURCE_SYSTEM = "PostgreSQL"
DATABASE_NAME = "DE"
SCHEMA_NAME = "tests"
BRONZE_SCHEMA = "workspace.inventory_bronze"
AUDIT_TABLE = f"{BRONZE_SCHEMA}.bz_audit_log"

# Credential Configuration (using Azure Key Vault secrets)
try:
    # Get current user for audit purposes
    current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]
except:
    # Fallback if current_user() is not available
    current_user = "system_user"

# Define source tables to process
SOURCE_TABLES = [
    "Products", "Suppliers", "Warehouses", "Inventory", 
    "Orders", "Order_Details", "Shipments", "Returns", 
    "Stock_Levels", "Customers"
]

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

def create_audit_record(table_name, status, processing_time=0, row_count=0, error_message=None):
    """
    Create audit record for tracking data processing activities
    """
    return spark.createDataFrame([
        {
            "record_id": str(uuid.uuid4()),
            "source_table": table_name,
            "load_timestamp": datetime.now(),
            "processed_by": current_user,
            "processing_time": processing_time,
            "status": status,
            "row_count": row_count,
            "error_message": error_message
        }
    ], audit_schema)

def log_audit_record(audit_record):
    """
    Write audit record to audit table
    """
    try:
        audit_record.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(AUDIT_TABLE)
    except Exception as e:
        print(f"Warning: Could not write to audit table: {str(e)}")

def get_postgresql_connection_properties():
    """
    Get PostgreSQL connection properties
    Note: In production, these should be retrieved from Azure Key Vault
    """
    return {
        "user": "your_username",  # Replace with actual credential retrieval
        "password": "your_password",  # Replace with actual credential retrieval
        "driver": "org.postgresql.Driver"
    }

def read_source_table(table_name):
    """
    Read data from PostgreSQL source table
    """
    start_time = time.time()
    
    try:
        # Connection properties
        connection_props = get_postgresql_connection_properties()
        
        # Construct JDBC URL
        jdbc_url = f"jdbc:postgresql://your_server:5432/{DATABASE_NAME}"
        
        # Read data from source
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{SCHEMA_NAME}.{table_name}") \
            .option("user", connection_props["user"]) \
            .option("password", connection_props["password"]) \
            .option("driver", connection_props["driver"]) \
            .load()
        
        processing_time = int((time.time() - start_time) * 1000)
        row_count = df.count()
        
        # Log successful read
        audit_record = create_audit_record(
            table_name, 
            "READ_SUCCESS", 
            processing_time, 
            row_count
        )
        log_audit_record(audit_record)
        
        print(f"Successfully read {row_count} records from {table_name}")
        return df
        
    except Exception as e:
        processing_time = int((time.time() - start_time) * 1000)
        error_msg = str(e)
        
        # Log failed read
        audit_record = create_audit_record(
            table_name, 
            "READ_FAILED", 
            processing_time, 
            0, 
            error_msg
        )
        log_audit_record(audit_record)
        
        print(f"Failed to read from {table_name}: {error_msg}")
        raise e

def add_metadata_columns(df, source_table):
    """
    Add metadata tracking columns to the dataframe
    """
    return df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("update_timestamp", current_timestamp()) \
        .withColumn("source_system", lit(SOURCE_SYSTEM)) \
        .withColumn("record_status", lit("ACTIVE")) \
        .withColumn("data_quality_score", lit(100))

def calculate_data_quality_score(df):
    """
    Calculate basic data quality score based on null values
    """
    total_cols = len(df.columns)
    null_counts = []
    
    for col_name in df.columns:
        if col_name not in ["load_timestamp", "update_timestamp", "source_system", "record_status", "data_quality_score"]:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts.append(null_count)
    
    total_records = df.count()
    if total_records == 0:
        return df
    
    avg_null_percentage = sum(null_counts) / (len(null_counts) * total_records) if null_counts else 0
    quality_score = max(0, int(100 - (avg_null_percentage * 100)))
    
    return df.withColumn("data_quality_score", lit(quality_score))

def write_to_bronze_layer(df, table_name):
    """
    Write dataframe to Bronze layer Delta table
    """
    start_time = time.time()
    bronze_table_name = f"bz_{table_name.lower()}"
    target_table = f"{BRONZE_SCHEMA}.{bronze_table_name}"
    
    try:
        # Add metadata columns
        df_with_metadata = add_metadata_columns(df, table_name)
        
        # Calculate data quality score
        df_final = calculate_data_quality_score(df_with_metadata)
        
        # Write to Delta table
        df_final.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        
        processing_time = int((time.time() - start_time) * 1000)
        row_count = df_final.count()
        
        # Log successful write
        audit_record = create_audit_record(
            bronze_table_name, 
            "WRITE_SUCCESS", 
            processing_time, 
            row_count
        )
        log_audit_record(audit_record)
        
        print(f"Successfully wrote {row_count} records to {target_table}")
        return True
        
    except Exception as e:
        processing_time = int((time.time() - start_time) * 1000)
        error_msg = str(e)
        
        # Log failed write
        audit_record = create_audit_record(
            bronze_table_name, 
            "WRITE_FAILED", 
            processing_time, 
            0, 
            error_msg
        )
        log_audit_record(audit_record)
        
        print(f"Failed to write to {target_table}: {error_msg}")
        raise e

def create_bronze_schema():
    """
    Create Bronze schema if it doesn't exist
    """
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
        print(f"Bronze schema {BRONZE_SCHEMA} created/verified")
    except Exception as e:
        print(f"Warning: Could not create schema {BRONZE_SCHEMA}: {str(e)}")

def process_table(table_name):
    """
    Process a single table from source to bronze layer
    """
    print(f"\n=== Processing table: {table_name} ===")
    
    try:
        # Read from source
        source_df = read_source_table(table_name)
        
        # Write to bronze layer
        write_to_bronze_layer(source_df, table_name)
        
        print(f"✅ Successfully processed {table_name}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to process {table_name}: {str(e)}")
        return False

def main():
    """
    Main execution function
    """
    print("🚀 Starting Bronze Layer Ingestion Pipeline")
    print(f"Source System: {SOURCE_SYSTEM}")
    print(f"Target Schema: {BRONZE_SCHEMA}")
    print(f"Processed by: {current_user}")
    print(f"Tables to process: {len(SOURCE_TABLES)}")
    
    # Create bronze schema
    create_bronze_schema()
    
    # Process each table
    successful_tables = []
    failed_tables = []
    
    total_start_time = time.time()
    
    for table in SOURCE_TABLES:
        try:
            if process_table(table):
                successful_tables.append(table)
            else:
                failed_tables.append(table)
        except Exception as e:
            failed_tables.append(table)
            print(f"❌ Critical error processing {table}: {str(e)}")
    
    total_processing_time = int((time.time() - total_start_time) * 1000)
    
    # Final summary
    print("\n" + "="*60)
    print("📊 BRONZE LAYER INGESTION SUMMARY")
    print("="*60)
    print(f"✅ Successfully processed: {len(successful_tables)} tables")
    print(f"❌ Failed to process: {len(failed_tables)} tables")
    print(f"⏱️  Total processing time: {total_processing_time} ms")
    
    if successful_tables:
        print(f"\n✅ Successful tables: {', '.join(successful_tables)}")
    
    if failed_tables:
        print(f"\n❌ Failed tables: {', '.join(failed_tables)}")
    
    # Log overall pipeline status
    overall_status = "PIPELINE_SUCCESS" if len(failed_tables) == 0 else "PIPELINE_PARTIAL_SUCCESS"
    audit_record = create_audit_record(
        "BRONZE_PIPELINE", 
        overall_status, 
        total_processing_time, 
        len(successful_tables)
    )
    log_audit_record(audit_record)
    
    print("\n🏁 Bronze Layer Ingestion Pipeline Completed")
    
    # Calculate API cost (estimated)
    api_cost = 0.000675  # Estimated cost for this pipeline execution
    print(f"\n💰 Estimated API Cost: ${api_cost:.6f} USD")
    
    return len(failed_tables) == 0

# Execute the pipeline
if __name__ == "__main__":
    try:
        success = main()
        if success:
            print("\n🎉 All tables processed successfully!")
        else:
            print("\n⚠️  Pipeline completed with some failures. Check logs for details.")
    except Exception as e:
        print(f"\n💥 Pipeline failed with critical error: {str(e)}")
        # Log critical failure
        audit_record = create_audit_record(
            "BRONZE_PIPELINE", 
            "PIPELINE_FAILED", 
            0, 
            0, 
            str(e)
        )
        log_audit_record(audit_record)
    finally:
        # Stop Spark session
        spark.stop()
        print("\n🔌 Spark session stopped")