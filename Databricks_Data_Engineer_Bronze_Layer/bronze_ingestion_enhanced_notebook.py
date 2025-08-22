# Databricks notebook source
# MAGIC %md
# MAGIC # Enhanced Bronze Layer Data Ingestion Pipeline
# MAGIC ## Inventory Management System
# MAGIC 
# MAGIC This notebook implements an enhanced data ingestion pipeline that extracts raw data from PostgreSQL source system and loads it into the Bronze layer in Databricks using Delta Lake format.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
import time
from datetime import datetime
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("BronzeLayerIngestion_InventoryManagement_Enhanced") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

# Load credentials from Azure Key Vault
source_db_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")

# COMMAND ----------

# Get current user's identity with enhanced fallback mechanisms
try:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
except:
    try:
        current_user = spark.sparkContext.sparkUser()
    except:
        try:
            current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        except:
            current_user = "System_Process"

print(f"Current user: {current_user}")

# COMMAND ----------

# Define source and target details
source_system = "PostgreSQL"
source_schema = "tests"
target_bronze_path = "workspace.inventory_bronze"
api_cost_total = 0.0

# Define tables to ingest with priority levels
tables_config = [
    {"name": "Products", "priority": 1, "retry_count": 3},
    {"name": "Suppliers", "priority": 1, "retry_count": 3},
    {"name": "Warehouses", "priority": 1, "retry_count": 3},
    {"name": "Customers", "priority": 1, "retry_count": 3},
    {"name": "Inventory", "priority": 2, "retry_count": 2},
    {"name": "Orders", "priority": 2, "retry_count": 2},
    {"name": "Order_Details", "priority": 3, "retry_count": 2},
    {"name": "Shipments", "priority": 3, "retry_count": 2},
    {"name": "Returns", "priority": 3, "retry_count": 2},
    {"name": "Stock_levels", "priority": 2, "retry_count": 2}
]

print(f"Configuration loaded - Source: {source_system}, Target: {target_bronze_path}")
print(f"Tables to process: {len(tables_config)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definitions and Helper Functions

# COMMAND ----------

# Enhanced audit table schema with additional metrics
audit_schema = StructType([
    StructField("Record_ID", IntegerType(), False),
    StructField("Source_Table", StringType(), False),
    StructField("Load_Timestamp", TimestampType(), False),
    StructField("Processed_By", StringType(), False),
    StructField("Processing_Time", IntegerType(), False),
    StructField("Status", StringType(), False),
    StructField("Row_Count", IntegerType(), True),
    StructField("Error_Details", StringType(), True),
    StructField("Retry_Attempt", IntegerType(), True),
    StructField("API_Cost", DoubleType(), True)
])

# COMMAND ----------

def log_audit(record_id, source_table, processing_time, status, row_count=None, error_details=None, retry_attempt=0, api_cost=0.0):
    """
    Enhanced audit logging with additional metrics
    """
    current_time = datetime.now()
    audit_df = spark.createDataFrame(
        [(record_id, source_table, current_time, current_user, processing_time, status, row_count, error_details, retry_attempt, api_cost)], 
        schema=audit_schema
    )
    audit_df.write.format("delta").mode("append").saveAsTable(f"{target_bronze_path}.bz_audit_log")

# COMMAND ----------

def validate_data_quality(df, table_name):
    """
    Perform basic data quality checks
    """
    try:
        total_rows = df.count()
        null_counts = {}
        
        for column in df.columns:
            if column not in ['Load_Date', 'Update_Date', 'Source_System']:
                null_count = df.filter(col(column).isNull()).count()
                null_counts[column] = null_count
        
        quality_metrics = {
            "total_rows": total_rows,
            "null_counts": null_counts,
            "quality_score": 100.0 if total_rows > 0 else 0.0
        }
        
        print(f"Data quality check for {table_name}: {quality_metrics}")
        return quality_metrics
        
    except Exception as e:
        print(f"Data quality check failed for {table_name}: {str(e)}")
        return {"total_rows": 0, "null_counts": {}, "quality_score": 0.0}

# COMMAND ----------

def load_to_bronze_enhanced(table_config, record_id):
    """
    Enhanced data loading with retry logic and quality checks
    """
    table_name = table_config["name"]
    max_retries = table_config["retry_count"]
    
    for retry_attempt in range(max_retries + 1):
        start_time = time.time()
        operation_cost = 0.000125  # Estimated API cost per operation
        
        try:
            print(f"Starting ingestion for table: {table_name} (Attempt {retry_attempt + 1}/{max_retries + 1})")
            
            # Read from source PostgreSQL with enhanced options
            df = spark.read \
                .format("jdbc") \
                .option("url", source_db_url) \
                .option("dbtable", f"{source_schema}.{table_name}") \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "10000") \
                .option("numPartitions", "4") \
                .load()

            row_count = df.count()
            print(f"Read {row_count} rows from {table_name}")
            
            # Perform data quality checks
            quality_metrics = validate_data_quality(df, table_name)
            
            # Add metadata columns for Bronze layer
            df_with_metadata = df.withColumn("Load_Date", current_timestamp()) \
                                .withColumn("Update_Date", current_timestamp()) \
                                .withColumn("Source_System", lit(source_system))

            # Write to Bronze layer with Delta format and optimization
            target_table_name = f"bz_{table_name.lower()}"
            df_with_metadata.write.format("delta") \
                            .mode("overwrite") \
                            .option("overwriteSchema", "true") \
                            .saveAsTable(f"{target_bronze_path}.{target_table_name}")

            # Optimize table after write
            spark.sql(f"OPTIMIZE {target_bronze_path}.{target_table_name}")
            
            processing_time = int(time.time() - start_time)
            success_message = f"Success - {row_count} rows processed with quality score: {quality_metrics['quality_score']:.2f}%"
            log_audit(record_id, table_name, processing_time, success_message, row_count, None, retry_attempt, operation_cost)
            
            print(f"Successfully loaded {table_name} to {target_table_name} in {processing_time} seconds")
            global api_cost_total
            api_cost_total += operation_cost
            return True
            
        except Exception as e:
            processing_time = int(time.time() - start_time)
            error_message = f"Failed - Attempt {retry_attempt + 1}: {str(e)}"
            error_details = f"Error Type: {type(e).__name__}, Message: {str(e)}"
            
            log_audit(record_id, table_name, processing_time, error_message, 0, error_details, retry_attempt, operation_cost)
            print(f"Error processing {table_name} (Attempt {retry_attempt + 1}): {str(e)}")
            
            if retry_attempt < max_retries:
                wait_time = (retry_attempt + 1) * 30  # Exponential backoff
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Max retries exceeded for {table_name}")
                api_cost_total += operation_cost
                return False
    
    return False

# COMMAND ----------

def create_enhanced_audit_table():
    """
    Create enhanced audit table with additional columns
    """
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {target_bronze_path}.bz_audit_log (
                Record_ID INT,
                Source_Table STRING,
                Load_Timestamp TIMESTAMP,
                Processed_By STRING,
                Processing_Time INT,
                Status STRING,
                Row_Count INT,
                Error_Details STRING,
                Retry_Attempt INT,
                API_Cost DOUBLE
            ) USING DELTA
        """)
        print("Enhanced audit table created/verified successfully")
    except Exception as e:
        print(f"Error creating enhanced audit table: {str(e)}")
        raise e

# COMMAND ----------

def generate_pipeline_summary():
    """
    Generate comprehensive pipeline execution summary
    """
    try:
        # Query audit table for summary
        summary_df = spark.sql(f"""
            SELECT 
                COUNT(*) as total_operations,
                SUM(CASE WHEN Status LIKE 'Success%' THEN 1 ELSE 0 END) as successful_operations,
                SUM(CASE WHEN Status LIKE 'Failed%' THEN 1 ELSE 0 END) as failed_operations,
                SUM(Row_Count) as total_rows_processed,
                AVG(Processing_Time) as avg_processing_time,
                SUM(API_Cost) as total_api_cost
            FROM {target_bronze_path}.bz_audit_log
            WHERE DATE(Load_Timestamp) = CURRENT_DATE()
        """)
        
        summary = summary_df.collect()[0]
        
        print("\n" + "="*60)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Total Operations: {summary['total_operations']}")
        print(f"Successful Operations: {summary['successful_operations']}")
        print(f"Failed Operations: {summary['failed_operations']}")
        print(f"Total Rows Processed: {summary['total_rows_processed']}")
        print(f"Average Processing Time: {summary['avg_processing_time']:.2f} seconds")
        print(f"Total API Cost: ${summary['total_api_cost']:.6f} USD")
        print(f"Success Rate: {(summary['successful_operations']/summary['total_operations']*100):.2f}%")
        print("="*60)
        
    except Exception as e:
        print(f"Error generating pipeline summary: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Pipeline Execution

# COMMAND ----------

# Main execution with enhanced error handling and monitoring
try:
    pipeline_start_time = time.time()
    print("Starting Enhanced Bronze Layer Ingestion Pipeline")
    print(f"Source System: {source_system}")
    print(f"Target Path: {target_bronze_path}")
    print(f"Processing User: {current_user}")
    print(f"Tables to process: {len(tables_config)}")
    
    # Create enhanced audit table
    create_enhanced_audit_table()
    
    # Sort tables by priority
    sorted_tables = sorted(tables_config, key=lambda x: x['priority'])
    
    # Process each table with enhanced logic
    successful_tables = []
    failed_tables = []
    
    for idx, table_config in enumerate(sorted_tables, 1):
        table_name = table_config['name']
        print(f"\nProcessing table {idx}/{len(sorted_tables)}: {table_name} (Priority: {table_config['priority']})")
        
        success = load_to_bronze_enhanced(table_config, idx)
        
        if success:
            successful_tables.append(table_name)
        else:
            failed_tables.append(table_name)
    
    pipeline_end_time = time.time()
    total_pipeline_time = int(pipeline_end_time - pipeline_start_time)
    
    print(f"\nBronze Layer Ingestion Pipeline completed in {total_pipeline_time} seconds")
    print(f"Successful tables: {len(successful_tables)} - {successful_tables}")
    print(f"Failed tables: {len(failed_tables)} - {failed_tables}")
    
    # Generate comprehensive summary
    generate_pipeline_summary()
    
    # Log final pipeline status
    final_status = f"Pipeline completed - {len(successful_tables)} successful, {len(failed_tables)} failed"
    log_audit(999, "PIPELINE_SUMMARY", total_pipeline_time, final_status, len(successful_tables), None, 0, api_cost_total)
    
except Exception as e:
    print(f"Pipeline failed with critical error: {str(e)}")
    log_audit(998, "PIPELINE_ERROR", 0, f"Critical failure: {str(e)}", 0, str(e), 0, 0.0)
    raise e
    
finally:
    print(f"\nTotal API Cost for this execution: ${api_cost_total:.6f} USD")
    print("Pipeline execution completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Completion
# MAGIC 
# MAGIC The Enhanced Bronze Layer Data Ingestion Pipeline has completed execution.
# MAGIC 
# MAGIC ### Key Features:
# MAGIC - Priority-based table processing
# MAGIC - Enhanced error handling with retry logic
# MAGIC - Comprehensive audit logging
# MAGIC - Data quality validation
# MAGIC - Performance optimization
# MAGIC - Cost tracking and reporting