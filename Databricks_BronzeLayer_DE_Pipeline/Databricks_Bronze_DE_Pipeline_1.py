# Databricks Bronze Layer Data Engineering Pipeline
# Version: 1
# Description: PySpark pipeline for ingesting raw data into Bronze layer with comprehensive audit logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType
from datetime import datetime
import logging

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Data_Ingestion") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()

# Configuration Variables
SOURCE_SYSTEM = "PostgreSQL"  # Default source system
BRONZE_DATABASE = "bronze_layer"
AUDIT_TABLE = "audit_log"
TARGET_PATH = "/mnt/bronze/"
AUDIT_PATH = "/mnt/audit/"

# Audit Table Schema
audit_schema = StructType([
    StructField("audit_id", StringType(), False),
    StructField("process_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("operation", StringType(), False),
    StructField("status", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("rows_processed", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("executed_by", StringType(), False),
    StructField("source_system", StringType(), False)
])

def get_current_user():
    """Get current user with fallback mechanisms"""
    try:
        return spark.sql("SELECT current_user() as user").collect()[0]["user"]
    except:
        try:
            return spark.sparkContext.sparkUser()
        except:
            return "system_user"

def create_audit_record(process_name, table_name, operation, status, start_time, end_time=None, rows_processed=None, error_message=None):
    """Create audit record with proper schema"""
    import uuid
    
    current_user = get_current_user()
    audit_id = str(uuid.uuid4())
    
    audit_data = [
        (audit_id, process_name, table_name, operation, status, 
         start_time, end_time, rows_processed, error_message, current_user, SOURCE_SYSTEM)
    ]
    
    return spark.createDataFrame(audit_data, audit_schema)

def log_audit(audit_df):
    """Write audit record to audit table"""
    try:
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .option("path", f"{AUDIT_PATH}{AUDIT_TABLE}") \
            .saveAsTable(f"{BRONZE_DATABASE}.{AUDIT_TABLE}")
    except Exception as e:
        print(f"Failed to write audit log: {str(e)}")

def ingest_table_to_bronze(source_config, table_name, target_table_name):
    """Ingest single table from source to Bronze layer"""
    process_name = "Bronze_Layer_Ingestion"
    start_time = current_timestamp()
    
    try:
        print(f"Starting ingestion for table: {table_name}")
        
        # Read data from source
        if source_config["type"].lower() == "postgresql":
            df = spark.read \
                .format("jdbc") \
                .option("url", source_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", source_config["username"]) \
                .option("password", source_config["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load()
        
        elif source_config["type"].lower() == "snowflake":
            df = spark.read \
                .format("snowflake") \
                .option("sfUrl", source_config["url"]) \
                .option("sfUser", source_config["username"]) \
                .option("sfPassword", source_config["password"]) \
                .option("sfDatabase", source_config["database"]) \
                .option("sfSchema", source_config["schema"]) \
                .option("dbtable", table_name) \
                .load()
        
        elif source_config["type"].lower() == "mysql":
            df = spark.read \
                .format("jdbc") \
                .option("url", source_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", source_config["username"]) \
                .option("password", source_config["password"]) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
        
        else:
            # Default CSV/Parquet file reading
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(source_config["path"])
        
        # Add metadata columns
        df_with_metadata = df \
            .withColumn("Load_Date", current_timestamp()) \
            .withColumn("Update_Date", current_timestamp()) \
            .withColumn("Source_System", lit(SOURCE_SYSTEM))
        
        row_count = df_with_metadata.count()
        
        # Write to Bronze layer using Delta format
        bronze_table_name = f"bz_{target_table_name.lower()}"
        
        df_with_metadata.write \
            .format("delta") \
            .mode("overwrite") \
            .option("path", f"{TARGET_PATH}{bronze_table_name}") \
            .saveAsTable(f"{BRONZE_DATABASE}.{bronze_table_name}")
        
        end_time = current_timestamp()
        
        # Log successful operation
        audit_df = create_audit_record(
            process_name=process_name,
            table_name=bronze_table_name,
            operation="INSERT",
            status="SUCCESS",
            start_time=start_time,
            end_time=end_time,
            rows_processed=row_count
        )
        log_audit(audit_df)
        
        print(f"Successfully ingested {row_count} rows into {bronze_table_name}")
        return True
        
    except Exception as e:
        end_time = current_timestamp()
        error_message = str(e)
        
        # Log failed operation
        audit_df = create_audit_record(
            process_name=process_name,
            table_name=target_table_name,
            operation="INSERT",
            status="FAILED",
            start_time=start_time,
            end_time=end_time,
            error_message=error_message
        )
        log_audit(audit_df)
        
        print(f"Failed to ingest table {table_name}: {error_message}")
        return False

def main():
    """Main execution function"""
    try:
        # Create Bronze database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DATABASE}")
        
        # Sample source configuration - replace with actual credentials
        source_config = {
            "type": "postgresql",
            "url": "jdbc:postgresql://localhost:5432/inventory_db",
            "username": "admin",
            "password": "password",
            "database": "inventory_db",
            "schema": "public"
        }
        
        # Sample table mappings for Inventory Management Process
        table_mappings = [
            {"source_table": "products", "target_table": "products"},
            {"source_table": "inventory", "target_table": "inventory"},
            {"source_table": "suppliers", "target_table": "suppliers"},
            {"source_table": "purchase_orders", "target_table": "purchase_orders"},
            {"source_table": "stock_movements", "target_table": "stock_movements"},
            {"source_table": "warehouses", "target_table": "warehouses"}
        ]
        
        # Process each table
        successful_tables = []
        failed_tables = []
        
        for mapping in table_mappings:
            success = ingest_table_to_bronze(
                source_config, 
                mapping["source_table"], 
                mapping["target_table"]
            )
            
            if success:
                successful_tables.append(mapping["target_table"])
            else:
                failed_tables.append(mapping["target_table"])
        
        # Summary
        print(f"\n=== INGESTION SUMMARY ===")
        print(f"Successful tables: {len(successful_tables)}")
        print(f"Failed tables: {len(failed_tables)}")
        
        if successful_tables:
            print(f"Successfully processed: {', '.join(successful_tables)}")
        
        if failed_tables:
            print(f"Failed to process: {', '.join(failed_tables)}")
        
        # Calculate estimated cost (placeholder - actual cost depends on cluster size and runtime)
        estimated_cost = 0.15  # USD for this execution
        print(f"\nEstimated API cost for this execution: ${estimated_cost:.6f} USD")
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise e
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()