# Databricks Bronze Layer Data Engineering Pipeline
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
import time
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Data_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true") \
    .getOrCreate()

# Configuration Variables
SOURCE_SYSTEM = "PostgreSQL"
DATABASE_NAME = "DE"
SCHEMA_NAME = "tests"
BRONZE_SCHEMA = "workspace.inventory_bronze"
AUDIT_TABLE = f"{BRONZE_SCHEMA}.bz_audit_log"

# Credential Configuration (using Azure Key Vault secrets)
try:
    source_db_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
    user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
    password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")
except Exception as e:
    print(f"Warning: Could not retrieve credentials from Key Vault: {e}")
    # Fallback for testing - replace with actual credentials
    source_db_url = "jdbc:postgresql://localhost:5432/DE"
    user = "test_user"
    password = "test_password"

# Get current user identity with fallback mechanisms
def get_current_user():
    try:
        return spark.sql("SELECT current_user() as user").collect()[0]["user"]
    except:
        try:
            return spark.sparkContext.sparkUser()
        except:
            return "system_user"

current_user = get_current_user()

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

# Create audit table if it doesn't exist
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
            record_count INT,
            error_message STRING
        ) USING DELTA
        """)
        print(f"Audit table {AUDIT_TABLE} created/verified successfully")
    except Exception as e:
        print(f"Error creating audit table: {e}")

# Function to log audit records
def log_audit_record(source_table, status, processing_time, record_count=None, error_message=None):
    try:
        audit_record = spark.createDataFrame([
            (str(uuid.uuid4()), source_table, datetime.now(), current_user, processing_time, status, record_count, error_message)
        ], audit_schema)
        
        audit_record.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
        print(f"Audit record logged for {source_table}: {status}")
    except Exception as e:
        print(f"Error logging audit record: {e}")

# Function to read data from source PostgreSQL
def read_source_data(table_name):
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
        print(f"Error reading from source table {table_name}: {e}")
        return None

# Function to add metadata columns
def add_metadata_columns(df, source_table):
    return df.withColumn("load_timestamp", current_timestamp()) \
             .withColumn("update_timestamp", current_timestamp()) \
             .withColumn("source_system", lit(SOURCE_SYSTEM)) \
             .withColumn("record_status", lit("ACTIVE")) \
             .withColumn("data_quality_score", lit(100))

# Function to calculate data quality score
def calculate_data_quality_score(df):
    total_rows = df.count()
    if total_rows == 0:
        return 0
    
    null_counts = 0
    for column in df.columns:
        if column not in ["load_timestamp", "update_timestamp", "source_system", "record_status", "data_quality_score"]:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts += null_count
    
    total_cells = total_rows * (len(df.columns) - 5)  # Exclude metadata columns
    quality_score = max(0, 100 - int((null_counts / total_cells) * 100)) if total_cells > 0 else 100
    return quality_score

# Function to write data to Bronze layer
def write_to_bronze(df, target_table):
    try:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")
        return True
    except Exception as e:
        print(f"Error writing to Bronze table {target_table}: {e}")
        return False

# Function to process individual table
def process_table(source_table, target_table):
    start_time = time.time()
    
    try:
        print(f"Processing table: {source_table} -> {target_table}")
        
        # Read source data
        source_df = read_source_data(source_table)
        if source_df is None:
            processing_time = int((time.time() - start_time) * 1000)
            log_audit_record(source_table, "FAILED", processing_time, 0, "Failed to read source data")
            return False
        
        # Add metadata columns
        bronze_df = add_metadata_columns(source_df, source_table)
        
        # Calculate and update data quality score
        quality_score = calculate_data_quality_score(bronze_df)
        bronze_df = bronze_df.withColumn("data_quality_score", lit(quality_score))
        
        # Get record count
        record_count = bronze_df.count()
        
        # Write to Bronze layer
        success = write_to_bronze(bronze_df, target_table)
        
        processing_time = int((time.time() - start_time) * 1000)
        
        if success:
            log_audit_record(source_table, "SUCCESS", processing_time, record_count)
            print(f"Successfully processed {record_count} records from {source_table}")
            return True
        else:
            log_audit_record(source_table, "FAILED", processing_time, record_count, "Failed to write to Bronze layer")
            return False
            
    except Exception as e:
        processing_time = int((time.time() - start_time) * 1000)
        log_audit_record(source_table, "FAILED", processing_time, 0, str(e))
        print(f"Error processing table {source_table}: {e}")
        return False

# Main processing function
def main():
    print("Starting Bronze Layer Data Ingestion Pipeline")
    print(f"Processed by: {current_user}")
    print(f"Source System: {SOURCE_SYSTEM}")
    print(f"Target Schema: {BRONZE_SCHEMA}")
    
    # Create audit table
    create_audit_table()
    
    # Define table mappings (source_table -> target_table)
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
    
    # Process each table
    successful_tables = []
    failed_tables = []
    
    for source_table, target_table in table_mappings.items():
        if process_table(source_table, target_table):
            successful_tables.append(source_table)
        else:
            failed_tables.append(source_table)
    
    # Summary
    print("\n=== PROCESSING SUMMARY ===")
    print(f"Total tables processed: {len(table_mappings)}")
    print(f"Successful: {len(successful_tables)}")
    print(f"Failed: {len(failed_tables)}")
    
    if successful_tables:
        print(f"Successfully processed tables: {', '.join(successful_tables)}")
    
    if failed_tables:
        print(f"Failed to process tables: {', '.join(failed_tables)}")
    
    # Log overall pipeline status
    overall_status = "SUCCESS" if len(failed_tables) == 0 else "PARTIAL_SUCCESS" if len(successful_tables) > 0 else "FAILED"
    log_audit_record("PIPELINE_OVERALL", overall_status, 0, len(successful_tables), 
                    f"Failed tables: {', '.join(failed_tables)}" if failed_tables else None)
    
    print("Bronze Layer Data Ingestion Pipeline completed")
    
    return len(failed_tables) == 0

# Execute main function
if __name__ == "__main__":
    try:
        success = main()
        if success:
            print("\n✅ Pipeline executed successfully!")
        else:
            print("\n⚠️ Pipeline completed with some failures. Check audit logs for details.")
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        log_audit_record("PIPELINE_OVERALL", "FAILED", 0, 0, str(e))
    finally:
        spark.stop()

# API Cost: $0.000875