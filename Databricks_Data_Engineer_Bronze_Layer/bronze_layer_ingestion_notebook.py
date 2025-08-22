# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion Pipeline - Enhanced Version
# MAGIC ## Inventory Management System
# MAGIC 
# MAGIC This notebook implements an enhanced data ingestion pipeline for loading data from PostgreSQL to Databricks Bronze layer.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Priority-based processing
# MAGIC - Data quality validation
# MAGIC - Enhanced error handling with retry logic
# MAGIC - Comprehensive audit logging
# MAGIC - Performance optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import Libraries and Initialize Spark Session

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, count, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
import time
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("BronzeLayerIngestion_InventoryManagement_Enhanced") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()

print("Spark session initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Credentials and Configuration

# COMMAND ----------

# Load credentials from Azure Key Vault
source_db_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")

# Get current user's identity with enhanced fallback mechanisms
try:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
except:
    try:
        current_user = spark.sparkContext.sparkUser()
    except:
        try:
            import getpass
            current_user = getpass.getuser()
        except:
            current_user = "System_Process"

# Define source and target details
source_system = "PostgreSQL"
source_schema = "tests"
target_bronze_path = "workspace.inventory_bronze"
pipeline_version = "2.0"

print(f"Configuration loaded - User: {current_user}, Source: {source_system}, Target: {target_bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Table Configuration and Schemas

# COMMAND ----------

# Define tables to ingest with priority levels
tables_config = {
    "Products": {"priority": 1, "partition_column": None, "quality_checks": ["not_null:Product_Name"]},
    "Suppliers": {"priority": 1, "partition_column": None, "quality_checks": ["not_null:Supplier_Name"]},
    "Warehouses": {"priority": 1, "partition_column": None, "quality_checks": ["not_null:Location"]},
    "Customers": {"priority": 1, "partition_column": None, "quality_checks": ["not_null:Customer_Name", "not_null:Email"]},
    "Inventory": {"priority": 2, "partition_column": None, "quality_checks": ["not_null:Quantity_Available"]},
    "Orders": {"priority": 2, "partition_column": "Order_Date", "quality_checks": ["not_null:Order_Date"]},
    "Order_Details": {"priority": 3, "partition_column": None, "quality_checks": ["not_null:Quantity_Ordered"]},
    "Shipments": {"priority": 3, "partition_column": "Shipment_Date", "quality_checks": ["not_null:Shipment_Date"]},
    "Returns": {"priority": 3, "partition_column": None, "quality_checks": ["not_null:Return_Reason"]},
    "Stock_levels": {"priority": 2, "partition_column": None, "quality_checks": ["not_null:Reorder_Threshold"]}
}

# Enhanced audit table schema
audit_schema = StructType([
    StructField("Record_ID", IntegerType(), False),
    StructField("Source_Table", StringType(), False),
    StructField("Load_Timestamp", TimestampType(), False),
    StructField("Processed_By", StringType(), False),
    StructField("Processing_Time", IntegerType(), False),
    StructField("Status", StringType(), False),
    StructField("Row_Count", IntegerType(), True),
    StructField("Quality_Score", DoubleType(), True),
    StructField("Pipeline_Version", StringType(), True),
    StructField("Error_Category", StringType(), True)
])

print(f"Table configuration loaded for {len(tables_config)} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Define Utility Functions

# COMMAND ----------

def calculate_data_quality_score(df, quality_checks):
    """
    Calculate data quality score based on defined checks
    
    Args:
        df: DataFrame to check
        quality_checks: List of quality check rules
    
    Returns:
        float: Quality score between 0 and 1
    """
    total_rows = df.count()
    if total_rows == 0:
        return 0.0
    
    quality_issues = 0
    
    for check in quality_checks:
        if check.startswith("not_null:"):
            column_name = check.split(":")[1]
            if column_name in df.columns:
                null_count = df.filter(col(column_name).isNull()).count()
                quality_issues += null_count
    
    quality_score = max(0.0, 1.0 - (quality_issues / (total_rows * len(quality_checks))))
    return round(quality_score, 4)

def log_audit(record_id, source_table, processing_time, status, row_count=None, quality_score=None, error_category=None):
    """
    Enhanced audit logging with additional metadata
    """
    current_time = datetime.now()
    audit_df = spark.createDataFrame(
        [(record_id, source_table, current_time, current_user, processing_time, status, 
          row_count, quality_score, pipeline_version, error_category)], 
        schema=audit_schema
    )
    audit_df.write.format("delta").mode("append").saveAsTable(f"{target_bronze_path}.bz_audit_log")

def validate_connection():
    """
    Validate database connection before processing
    """
    try:
        test_df = spark.read \
            .format("jdbc") \
            .option("url", source_db_url) \
            .option("query", "SELECT 1 as test_connection") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        test_df.count()
        print("Database connection validated successfully")
        return True
    except Exception as e:
        print(f"Database connection validation failed: {str(e)}")
        return False

print("Utility functions defined successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Define Main Processing Functions

# COMMAND ----------

def load_to_bronze_enhanced(table_name, record_id, config, max_retries=3):
    """
    Enhanced data loading with retry logic and quality checks
    """
    start_time = time.time()
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"Starting ingestion for table: {table_name} (Attempt {retry_count + 1})")
            
            # Read from source PostgreSQL with optimized settings
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

            # Cache for multiple operations
            df.cache()
            row_count = df.count()
            print(f"Read {row_count} rows from {table_name}")
            
            # Data quality validation
            quality_score = calculate_data_quality_score(df, config.get("quality_checks", []))
            print(f"Data quality score for {table_name}: {quality_score}")
            
            # Add enhanced metadata columns
            df_with_metadata = df.withColumn("Load_Date", current_timestamp()) \
                                .withColumn("Update_Date", current_timestamp()) \
                                .withColumn("Source_System", lit(source_system)) \
                                .withColumn("Pipeline_Version", lit(pipeline_version)) \
                                .withColumn("Data_Quality_Score", lit(quality_score))

            # Write to Bronze layer with Delta format and optimization
            target_table_name = f"bz_{table_name.lower()}"
            writer = df_with_metadata.write.format("delta").mode("overwrite")
            
            # Apply partitioning if specified
            if config.get("partition_column"):
                writer = writer.partitionBy(config["partition_column"])
            
            writer.saveAsTable(f"{target_bronze_path}.{target_table_name}")
            
            # Optimize table after write
            spark.sql(f"OPTIMIZE {target_bronze_path}.{target_table_name}")
            
            processing_time = int(time.time() - start_time)
            success_message = f"Success - {row_count} rows processed with quality score {quality_score}"
            log_audit(record_id, table_name, processing_time, success_message, row_count, quality_score)
            
            print(f"Successfully loaded {table_name} to {target_table_name} in {processing_time} seconds")
            
            # Unpersist cache
            df.unpersist()
            return
            
        except Exception as e:
            retry_count += 1
            processing_time = int(time.time() - start_time)
            
            # Categorize error
            error_category = "Unknown"
            if "connection" in str(e).lower():
                error_category = "Connection"
            elif "authentication" in str(e).lower():
                error_category = "Authentication"
            elif "permission" in str(e).lower():
                error_category = "Permission"
            elif "timeout" in str(e).lower():
                error_category = "Timeout"
            
            if retry_count < max_retries:
                print(f"Attempt {retry_count} failed for {table_name}: {str(e)}. Retrying...")
                time.sleep(retry_count * 5)  # Exponential backoff
            else:
                error_message = f"Failed after {max_retries} attempts - {str(e)}"
                log_audit(record_id, table_name, processing_time, error_message, 0, 0.0, error_category)
                print(f"Error processing {table_name}: {str(e)}")
                raise e

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
                Quality_Score DOUBLE,
                Pipeline_Version STRING,
                Error_Category STRING
            ) USING DELTA
        """)
        print("Enhanced audit table created/verified successfully")
    except Exception as e:
        print(f"Error creating audit table: {str(e)}")
        raise e

print("Main processing functions defined successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execute Pipeline

# COMMAND ----------

def process_tables_by_priority():
    """
    Process tables based on priority levels
    """
    # Group tables by priority
    priority_groups = {}
    for table, config in tables_config.items():
        priority = config["priority"]
        if priority not in priority_groups:
            priority_groups[priority] = []
        priority_groups[priority].append((table, config))
    
    record_id = 1
    
    # Process each priority group
    for priority in sorted(priority_groups.keys()):
        print(f"Processing priority {priority} tables")
        
        # Process tables sequentially for better error handling in notebook environment
        for table, config in priority_groups[priority]:
            try:
                load_to_bronze_enhanced(table, record_id, config)
                print(f"Completed processing {table}")
            except Exception as e:
                print(f"Failed to process {table}: {str(e)}")
            record_id += 1

# Main execution
try:
    print("Starting Enhanced Bronze Layer Ingestion Pipeline")
    print(f"Source System: {source_system}")
    print(f"Target Path: {target_bronze_path}")
    print(f"Processing User: {current_user}")
    print(f"Pipeline Version: {pipeline_version}")
    print(f"Tables to process: {len(tables_config)}")
    
    # Validate connection
    if not validate_connection():
        raise Exception("Database connection validation failed")
    
    # Create enhanced audit table
    create_enhanced_audit_table()
    
    # Process tables by priority
    process_tables_by_priority()
    
    print("Enhanced Bronze Layer Ingestion Pipeline completed successfully")
    
except Exception as e:
    print(f"Pipeline failed with error: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation and Summary

# COMMAND ----------

# Display audit log summary
print("\n=== Pipeline Execution Summary ===")
try:
    audit_summary = spark.sql(f"""
        SELECT 
            Source_Table,
            Status,
            Row_Count,
            Quality_Score,
            Processing_Time,
            Load_Timestamp
        FROM {target_bronze_path}.bz_audit_log 
        WHERE Pipeline_Version = '{pipeline_version}'
        ORDER BY Load_Timestamp DESC
        LIMIT 20
    """)
    
    audit_summary.show(truncate=False)
    
    # Show table counts
    print("\n=== Bronze Layer Table Counts ===")
    for table in tables_config.keys():
        target_table = f"bz_{table.lower()}"
        try:
            count_df = spark.sql(f"SELECT COUNT(*) as count FROM {target_bronze_path}.{target_table}")
            count = count_df.collect()[0][0]
            print(f"{target_table}: {count:,} records")
        except Exception as e:
            print(f"{target_table}: Error - {str(e)}")
            
except Exception as e:
    print(f"Error generating summary: {str(e)}")

print("\nPipeline execution completed. Check audit logs for detailed results.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cleanup

# COMMAND ----------

# Clear any cached DataFrames
spark.catalog.clearCache()
print("Cache cleared successfully")
print("Notebook execution completed")
