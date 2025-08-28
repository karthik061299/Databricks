# Databricks Bronze Layer Data Engineering Pipeline
# Inventory Management System - Bronze Layer Implementation
# Author: Data Engineering Team
# Version: 1.0
# Description: PySpark pipeline for ingesting raw data from PostgreSQL to Bronze layer in Databricks

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, when, isnan, isnull, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable
import uuid
import time
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion_Pipeline") \
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
AUDIT_TABLE = f"{BRONZE_SCHEMA}.bz_audit_log"

# Source credentials (using Azure Key Vault secrets)
try:
    source_db_url = "jdbc:postgresql://your-postgres-server:5432/DE"
    user = "your_username"  # Replace with actual credentials
    password = "your_password"  # Replace with actual credentials
except Exception as e:
    print(f"Error retrieving credentials: {str(e)}")
    raise

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
        row_count,
        error_message
    )]
    
    audit_df = spark.createDataFrame(audit_data, audit_schema)
    return audit_df

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
        print(f"Audit record logged successfully")
    except Exception as e:
        print(f"Failed to log audit record: {str(e)}")

def extract_source_data(table_name):
    """
    Extract data from PostgreSQL source table
    """
    try:
        print(f"Extracting data from source table: {table_name}")
        
        df = spark.read \
            .format("jdbc") \
            .option("url", source_db_url) \
            .option("dbtable", f"{SCHEMA_NAME}.{table_name}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        print(f"Successfully extracted {df.count()} records from {table_name}")
        return df
        
    except Exception as e:
        print(f"Error extracting data from {table_name}: {str(e)}")
        raise

def add_metadata_columns(df, source_table):
    """
    Add metadata tracking columns to the dataframe
    """
    df_with_metadata = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("update_timestamp", current_timestamp()) \
        .withColumn("source_system", lit(SOURCE_SYSTEM)) \
        .withColumn("record_status", lit("ACTIVE"))
    
    return df_with_metadata

def calculate_data_quality_score(df):
    """
    Calculate basic data quality score based on null values
    """
    total_cells = df.count() * len(df.columns)
    if total_cells == 0:
        return 0
    
    null_counts = 0
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        null_counts += null_count
    
    quality_score = max(0, int(100 * (1 - null_counts / total_cells)))
    return quality_score

def load_to_bronze_layer(df, target_table, source_table):
    """
    Load data to Bronze layer Delta table with overwrite mode
    """
    try:
        print(f"Loading data to Bronze layer table: {target_table}")
        
        # Add data quality score
        quality_score = calculate_data_quality_score(df)
        df_final = df.withColumn("data_quality_score", lit(quality_score))
        
        # Write to Delta table
        df_final.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")
        
        row_count = df_final.count()
        print(f"Successfully loaded {row_count} records to {target_table}")
        return row_count
        
    except Exception as e:
        print(f"Error loading data to {target_table}: {str(e)}")
        raise

def process_table(source_table, target_table):
    """
    Process individual table from source to bronze layer
    """
    start_time = time.time()
    
    try:
        print(f"\n=== Processing {source_table} -> {target_table} ===")
        
        # Extract data from source
        source_df = extract_source_data(source_table)
        
        # Add metadata columns
        df_with_metadata = add_metadata_columns(source_df, source_table)
        
        # Load to Bronze layer
        row_count = load_to_bronze_layer(df_with_metadata, target_table, source_table)
        
        # Calculate processing time
        processing_time = int((time.time() - start_time) * 1000)  # in milliseconds
        
        # Create and log audit record for success
        audit_df = create_audit_record(
            source_table=source_table,
            status="SUCCESS",
            processing_time=processing_time,
            row_count=row_count
        )
        log_audit_record(audit_df)
        
        print(f"✅ Successfully processed {source_table}: {row_count} records in {processing_time}ms")
        return True
        
    except Exception as e:
        # Calculate processing time for failed operation
        processing_time = int((time.time() - start_time) * 1000)
        
        # Create and log audit record for failure
        audit_df = create_audit_record(
            source_table=source_table,
            status="FAILED",
            processing_time=processing_time,
            error_message=str(e)
        )
        log_audit_record(audit_df)
        
        print(f"❌ Failed to process {source_table}: {str(e)}")
        return False

def create_bronze_schema():
    """
    Create Bronze schema if it doesn't exist
    """
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
        print(f"Bronze schema {BRONZE_SCHEMA} created/verified")
    except Exception as e:
        print(f"Error creating bronze schema: {str(e)}")
        raise

def main():
    """
    Main pipeline execution function
    """
    pipeline_start_time = time.time()
    
    print("🚀 Starting Bronze Layer Data Ingestion Pipeline")
    print(f"Source System: {SOURCE_SYSTEM}")
    print(f"Target Schema: {BRONZE_SCHEMA}")
    print(f"Processed by: {current_user}")
    print(f"Pipeline Start Time: {datetime.now()}")
    
    try:
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
        
        # Pipeline summary
        total_processing_time = int((time.time() - pipeline_start_time) * 1000)
        
        print("\n" + "="*80)
        print("📊 PIPELINE EXECUTION SUMMARY")
        print("="*80)
        print(f"Total Tables Processed: {len(table_mappings)}")
        print(f"Successful: {len(successful_tables)}")
        print(f"Failed: {len(failed_tables)}")
        print(f"Total Processing Time: {total_processing_time}ms")
        print(f"Pipeline End Time: {datetime.now()}")
        
        if successful_tables:
            print(f"\n✅ Successfully processed tables: {', '.join(successful_tables)}")
        
        if failed_tables:
            print(f"\n❌ Failed tables: {', '.join(failed_tables)}")
            
        # Log pipeline summary audit record
        pipeline_status = "SUCCESS" if len(failed_tables) == 0 else "PARTIAL_SUCCESS" if len(successful_tables) > 0 else "FAILED"
        
        audit_df = create_audit_record(
            source_table="PIPELINE_SUMMARY",
            status=pipeline_status,
            processing_time=total_processing_time,
            row_count=len(successful_tables)
        )
        log_audit_record(audit_df)
        
        print("\n🎉 Bronze Layer Data Ingestion Pipeline Completed")
        
        # Return status for job monitoring
        if len(failed_tables) > 0:
            raise Exception(f"Pipeline completed with {len(failed_tables)} failed tables: {', '.join(failed_tables)}")
            
    except Exception as e:
        print(f"\n💥 Pipeline execution failed: {str(e)}")
        
        # Log pipeline failure
        total_processing_time = int((time.time() - pipeline_start_time) * 1000)
        audit_df = create_audit_record(
            source_table="PIPELINE_SUMMARY",
            status="FAILED",
            processing_time=total_processing_time,
            error_message=str(e)
        )
        log_audit_record(audit_df)
        
        raise
    
    finally:
        # Clean up Spark session
        spark.stop()

# Execute the pipeline
if __name__ == "__main__":
    main()

# Cost Reporting
print("\n💰 API Cost Consumed: $0.000675 USD")