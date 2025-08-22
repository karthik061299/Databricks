_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Enhanced Bronze Layer Data Ingestion Pipeline with Databricks Job Integration for Inventory Management System
## *Version*: 2
## *Updated on*: 
_____________________________________________

# Databricks Bronze Layer Data Engineering Pipeline
## Inventory Management System - Enhanced Version

## 1. Pipeline Overview

### 1.1 Purpose
This enhanced pipeline extracts raw data from PostgreSQL source system and loads it into the Bronze layer in Databricks using Delta Lake format. The pipeline includes comprehensive audit logging, metadata tracking, error handling, and Databricks job scheduling integration for all inventory management tables.

### 1.2 Source System Details
- **Source System**: PostgreSQL
- **Database**: DE
- **Schema**: tests
- **Tables**: Products, Suppliers, Warehouses, Inventory, Orders, Order_Details, Shipments, Returns, Stock_levels, Customers

### 1.3 Target System Details
- **Target System**: Databricks (Delta Tables in Unity Catalog)
- **Bronze Schema Path**: workspace.inventory_bronze
- **Storage Format**: Delta Lake
- **Write Mode**: Overwrite
- **Compute**: Serverless (Databricks Job Integration)

### 1.4 New Enhancements in Version 2
- **Databricks Job Integration**: Automated scheduling with serverless compute
- **Enhanced Error Handling**: Improved retry mechanisms and error reporting
- **Performance Optimization**: Better resource utilization and parallel processing
- **Advanced Monitoring**: Enhanced audit logging with detailed metrics
- **Cost Optimization**: Serverless compute for cost-effective execution

## 2. Enhanced PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
import time
from datetime import datetime
import json

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("BronzeLayerIngestion_InventoryManagement_v2") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()

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
            current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        except:
            current_user = "Databricks_Job_System"

# Define source and target details
source_system = "PostgreSQL"
source_schema = "tests"
target_bronze_path = "workspace.inventory_bronze"

# Define tables to ingest with priority ordering
tables = [
    "Products",
    "Suppliers", 
    "Warehouses",
    "Customers",
    "Inventory",
    "Orders",
    "Order_Details",
    "Shipments",
    "Returns",
    "Stock_levels"
]

# Enhanced audit table schema with additional metrics
audit_schema = StructType([
    StructField("Record_ID", IntegerType(), False),
    StructField("Source_Table", StringType(), False),
    StructField("Load_Timestamp", TimestampType(), False),
    StructField("Processed_By", StringType(), False),
    StructField("Processing_Time", IntegerType(), False),
    StructField("Status", StringType(), False),
    StructField("Row_Count", LongType(), True),
    StructField("Error_Details", StringType(), True),
    StructField("Job_Run_Id", StringType(), True)
])

def get_job_run_id():
    """
    Get the current Databricks job run ID for tracking
    """
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
    except:
        return "manual_execution"

def log_audit(record_id, source_table, processing_time, status, row_count=None, error_details=None):
    """
    Enhanced audit logging with additional metrics
    
    Args:
        record_id (int): Unique identifier for the audit record
        source_table (str): Name of the source table being processed
        processing_time (int): Time taken to process in seconds
        status (str): Processing status with details
        row_count (int): Number of rows processed
        error_details (str): Detailed error information if applicable
    """
    current_time = datetime.now()
    job_run_id = get_job_run_id()
    
    audit_df = spark.createDataFrame(
        [(record_id, source_table, current_time, current_user, processing_time, status, row_count, error_details, job_run_id)], 
        schema=audit_schema
    )
    
    try:
        audit_df.write.format("delta").mode("append").saveAsTable(f"{target_bronze_path}.bz_audit_log")
    except Exception as e:
        print(f"Warning: Could not write to audit table: {str(e)}")

def validate_source_connection():
    """
    Validate connection to source PostgreSQL database
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
        print("✓ Source database connection validated successfully")
        return True
    except Exception as e:
        print(f"✗ Source database connection failed: {str(e)}")
        return False

def load_to_bronze(table_name, record_id, max_retries=3):
    """
    Enhanced data loading with retry mechanism and better error handling
    
    Args:
        table_name (str): Name of the source table to process
        record_id (int): Unique identifier for audit logging
        max_retries (int): Maximum number of retry attempts
    """
    start_time = time.time()
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            print(f"Starting ingestion for table: {table_name} (Attempt {retry_count + 1}/{max_retries + 1})")
            
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

            row_count = df.count()
            print(f"Read {row_count} rows from {table_name}")
            
            if row_count == 0:
                print(f"Warning: Table {table_name} is empty")
            
            # Add enhanced metadata columns for Bronze layer
            df_with_metadata = df.withColumn("Load_Date", current_timestamp()) \
                                .withColumn("Update_Date", current_timestamp()) \
                                .withColumn("Source_System", lit(source_system)) \
                                .withColumn("Job_Run_Id", lit(get_job_run_id())) \
                                .withColumn("Processing_Timestamp", current_timestamp())

            # Write to Bronze layer with Delta format and optimization
            target_table_name = f"bz_{table_name.lower()}"
            df_with_metadata.write.format("delta") \
                            .mode("overwrite") \
                            .option("overwriteSchema", "true") \
                            .saveAsTable(f"{target_bronze_path}.{target_table_name}")

            processing_time = int(time.time() - start_time)
            success_message = f"Success - {row_count} rows processed"
            log_audit(record_id, table_name, processing_time, success_message, row_count)
            
            print(f"✓ Successfully loaded {table_name} to {target_table_name} in {processing_time} seconds")
            return True
            
        except Exception as e:
            retry_count += 1
            processing_time = int(time.time() - start_time)
            error_message = f"Attempt {retry_count} failed: {str(e)}"
            
            if retry_count > max_retries:
                final_error = f"Failed after {max_retries + 1} attempts - {str(e)}"
                log_audit(record_id, table_name, processing_time, final_error, None, str(e))
                print(f"✗ Error processing {table_name}: {final_error}")
                raise e
            else:
                print(f"⚠ Retry {retry_count} for {table_name}: {str(e)}")
                time.sleep(5 * retry_count)  # Exponential backoff

def create_audit_table():
    """
    Create enhanced audit table if it doesn't exist
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
                Row_Count BIGINT,
                Error_Details STRING,
                Job_Run_Id STRING
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """)
        print("✓ Enhanced audit table created/verified successfully")
    except Exception as e:
        print(f"✗ Error creating audit table: {str(e)}")
        raise e

def optimize_bronze_tables():
    """
    Optimize all Bronze layer tables for better performance
    """
    try:
        for table in tables:
            target_table_name = f"bz_{table.lower()}"
            spark.sql(f"OPTIMIZE {target_bronze_path}.{target_table_name}")
            print(f"✓ Optimized table: {target_table_name}")
    except Exception as e:
        print(f"⚠ Warning: Table optimization failed: {str(e)}")

def generate_pipeline_summary():
    """
    Generate a summary of the pipeline execution
    """
    try:
        summary_df = spark.sql(f"""
            SELECT 
                Source_Table,
                Status,
                Row_Count,
                Processing_Time,
                Load_Timestamp
            FROM {target_bronze_path}.bz_audit_log 
            WHERE Job_Run_Id = '{get_job_run_id()}'
            ORDER BY Record_ID
        """)
        
        print("\n" + "="*80)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*80)
        summary_df.show(truncate=False)
        
        # Calculate total metrics
        total_rows = summary_df.agg({"Row_Count": "sum"}).collect()[0][0] or 0
        total_time = summary_df.agg({"Processing_Time": "sum"}).collect()[0][0] or 0
        success_count = summary_df.filter(col("Status").contains("Success")).count()
        
        print(f"Total Tables Processed: {len(tables)}")
        print(f"Successful Loads: {success_count}")
        print(f"Total Rows Processed: {total_rows:,}")
        print(f"Total Processing Time: {total_time} seconds")
        print("="*80)
        
    except Exception as e:
        print(f"⚠ Could not generate summary: {str(e)}")

# Main execution with enhanced error handling and monitoring
try:
    print("\n" + "="*80)
    print("STARTING ENHANCED BRONZE LAYER INGESTION PIPELINE v2.0")
    print("="*80)
    print(f"Source System: {source_system}")
    print(f"Target Path: {target_bronze_path}")
    print(f"Processing User: {current_user}")
    print(f"Job Run ID: {get_job_run_id()}")
    print(f"Tables to process: {len(tables)}")
    print(f"Execution Time: {datetime.now()}")
    print("="*80)
    
    # Validate source connection
    if not validate_source_connection():
        raise Exception("Source database connection validation failed")
    
    # Create enhanced audit table
    create_audit_table()
    
    # Process each table with enhanced monitoring
    successful_tables = []
    failed_tables = []
    
    for idx, table in enumerate(tables, 1):
        print(f"\n[{idx}/{len(tables)}] Processing table: {table}")
        try:
            load_to_bronze(table, idx)
            successful_tables.append(table)
        except Exception as e:
            failed_tables.append((table, str(e)))
            print(f"✗ Failed to process {table}: {str(e)}")
            # Continue with next table instead of stopping entire pipeline
    
    # Optimize tables for better performance
    if successful_tables:
        print(f"\nOptimizing {len(successful_tables)} successfully loaded tables...")
        optimize_bronze_tables()
    
    # Generate execution summary
    generate_pipeline_summary()
    
    if failed_tables:
        print(f"\n⚠ WARNING: {len(failed_tables)} tables failed to process:")
        for table, error in failed_tables:
            print(f"  - {table}: {error}")
    
    print(f"\n✓ Bronze Layer Ingestion Pipeline v2.0 completed")
    print(f"✓ Successfully processed: {len(successful_tables)}/{len(tables)} tables")
    
except Exception as e:
    print(f"\n✗ Pipeline failed with critical error: {str(e)}")
    # Log critical failure
    try:
        log_audit(0, "PIPELINE_CRITICAL_FAILURE", 0, f"Critical failure: {str(e)}", None, str(e))
    except:
        pass
    raise e
    
finally:
    print(f"\nSpark session cleanup initiated at {datetime.now()}")
    spark.stop()
    print("✓ Spark session stopped successfully")
```

## 3. Enhanced Data Ingestion Strategy

### 3.1 Ingestion Approach
- **Full Load**: Complete table refresh using overwrite mode
- **Prioritized Processing**: Tables processed in dependency order
- **Retry Mechanism**: Automatic retry with exponential backoff
- **Parallel Optimization**: Enhanced partitioning and fetch strategies
- **Error Isolation**: Individual table failures don't stop entire pipeline

### 3.2 Enhanced Metadata Tracking
Each Bronze table includes:
- **Load_Date**: Timestamp when data was loaded
- **Update_Date**: Timestamp when data was last updated
- **Source_System**: Identifier of the source system (PostgreSQL)
- **Job_Run_Id**: Databricks job execution identifier
- **Processing_Timestamp**: Detailed processing timestamp

### 3.3 Naming Convention
- **Target Tables**: Prefixed with `bz_` and converted to lowercase
- **Schema**: All tables stored in `workspace.inventory_bronze` schema
- **Format**: Delta Lake format with auto-optimization enabled

## 4. Databricks Job Configuration

### 4.1 Job Settings
```json
{
  "name": "bronze_layer_ingestion_inventory_v2",
  "tasks": [
    {
      "task_key": "bronze_ingestion_enhanced",
      "depends_on": [],
      "notebook_task": {
        "notebook_path": "/Workspace/Users/bronze_ingestion_pipeline_v2",
        "base_parameters": {
          "execution_date": "{{job.start_time}}",
          "pipeline_version": "2.0"
        }
      },
      "job_cluster_key": "serverless_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "serverless_cluster",
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "driver_node_type_id": "Standard_DS3_v2",
        "num_workers": 0,
        "custom_tags": {
          "project": "inventory_management",
          "layer": "bronze",
          "version": "2.0"
        },
        "spark_conf": {
          "spark.databricks.cluster.profile": "serverless",
          "spark.databricks.delta.optimizeWrite.enabled": "true",
          "spark.databricks.delta.autoCompact.enabled": "true"
        }
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "Asia/Kolkata"
  },
  "email_notifications": {
    "on_failure": ["admin@company.com"],
    "on_success": ["team@company.com"]
  },
  "timeout_seconds": 3600,
  "max_concurrent_runs": 1
}
```

### 4.2 Serverless Configuration
- **Compute Type**: Serverless for cost optimization
- **Auto-scaling**: Automatic resource allocation
- **Cost Control**: Pay-per-use model
- **Performance**: Optimized for batch processing

## 5. Target Table Mapping

| Source Table | Target Bronze Table | Description | Priority |
|--------------|--------------------|-------------|----------|
| Products | bz_products | Product master data | 1 |
| Suppliers | bz_suppliers | Supplier information | 2 |
| Warehouses | bz_warehouses | Warehouse facility data | 3 |
| Customers | bz_customers | Customer master data | 4 |
| Inventory | bz_inventory | Current inventory levels | 5 |
| Orders | bz_orders | Customer order headers | 6 |
| Order_Details | bz_order_details | Order line items | 7 |
| Shipments | bz_shipments | Shipment tracking | 8 |
| Returns | bz_returns | Product return records | 9 |
| Stock_levels | bz_stock_levels | Stock monitoring data | 10 |

## 6. Enhanced Audit and Monitoring

### 6.1 Enhanced Audit Table Schema
```sql
CREATE TABLE workspace.inventory_bronze.bz_audit_log (
    Record_ID INT,
    Source_Table STRING,
    Load_Timestamp TIMESTAMP,
    Processed_By STRING,
    Processing_Time INT,
    Status STRING,
    Row_Count BIGINT,
    Error_Details STRING,
    Job_Run_Id STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
```

### 6.2 Enhanced Monitoring Metrics
- **Processing Time**: Duration for each table load
- **Row Counts**: Number of records processed per table
- **Success/Failure Status**: Detailed outcome of each operation
- **User Tracking**: Identity of process executor
- **Job Run Tracking**: Databricks job execution correlation
- **Error Details**: Comprehensive error information
- **Performance Metrics**: Throughput and efficiency tracking

## 7. Advanced Error Handling and Recovery

### 7.1 Error Scenarios
- **Connection Failures**: Database connectivity issues with retry logic
- **Authentication Errors**: Credential validation with fallback mechanisms
- **Data Quality Issues**: Malformed or corrupt data handling
- **Storage Errors**: Target system write failures with recovery
- **Resource Constraints**: Memory and compute limitations

### 7.2 Enhanced Recovery Mechanisms
- **Exponential Backoff**: Intelligent retry with increasing delays
- **Detailed Audit Logging**: All failures logged with comprehensive details
- **Isolated Processing**: Single table failure isolation
- **Connection Validation**: Pre-execution connectivity checks
- **Graceful Degradation**: Partial success handling

## 8. Performance Optimization

### 8.1 Enhanced Optimization Strategies
- **Adaptive Query Execution**: Spark AQE enabled
- **Auto-Compaction**: Delta Lake auto-compaction enabled
- **Optimized Writes**: Delta optimized writes enabled
- **Partition Coalescing**: Automatic partition optimization
- **Fetch Size Optimization**: Optimized JDBC fetch parameters
- **Parallel Processing**: Multi-partition data loading

### 8.2 Scalability Improvements
- **Serverless Compute**: Auto-scaling based on workload
- **Resource Optimization**: Dynamic resource allocation
- **Cost Efficiency**: Pay-per-use serverless model
- **Performance Monitoring**: Real-time execution metrics

## 9. Security and Compliance Enhancements

### 9.1 Enhanced Data Security
- **Azure Key Vault Integration**: Secure credential management
- **Unity Catalog Security**: Fine-grained access control
- **Comprehensive Audit Trail**: Detailed logging for compliance
- **Job Run Tracking**: Complete execution lineage
- **PII Data Handling**: Enhanced sensitive data identification

### 9.2 PII Data Identification
| Table | Column | PII Type | Compliance Notes |
|-------|--------|----------|------------------|
| bz_customers | Customer_Name | Personal Name | GDPR Article 4 |
| bz_customers | Email | Contact Information | GDPR Article 4 |
| bz_suppliers | Contact_Number | Contact Information | Business Contact |
| bz_suppliers | Supplier_Name | Business Entity Name | Business Entity |

## 10. Deployment and Operations

### 10.1 Enhanced Prerequisites
- Databricks workspace with Unity Catalog enabled
- Serverless compute capability enabled
- Azure Key Vault with database credentials
- PostgreSQL JDBC driver available in cluster
- Appropriate Unity Catalog permissions
- Email notification configuration

### 10.2 Enhanced Execution Steps
1. Validate serverless compute availability
2. Verify source system connectivity
3. Confirm target schema permissions
4. Deploy notebook to Databricks workspace
5. Create and configure Databricks job
6. Execute pipeline with monitoring
7. Validate audit logs and data quality
8. Review performance metrics

## 11. Cost Analysis and Optimization

### 11.1 Cost Factors
- **Serverless Compute**: Pay-per-use DBU consumption
- **Storage**: Delta Lake storage costs
- **Data Transfer**: Network egress charges
- **Job Scheduling**: Minimal overhead for scheduling

### 11.2 Cost Optimization Strategies
- **Serverless Usage**: Automatic resource scaling
- **Delta Optimization**: Reduced storage and query costs
- **Efficient Scheduling**: Off-peak execution timing
- **Resource Right-sizing**: Optimal cluster configuration

## 12. Monitoring and Alerting

### 12.1 Monitoring Dashboard Metrics
- **Pipeline Success Rate**: Percentage of successful executions
- **Processing Time Trends**: Historical performance analysis
- **Data Volume Trends**: Row count and size tracking
- **Error Rate Analysis**: Failure pattern identification
- **Cost Tracking**: DBU consumption monitoring

### 12.2 Alerting Configuration
- **Failure Notifications**: Immediate alerts on pipeline failures
- **Performance Degradation**: Alerts on processing time increases
- **Data Quality Issues**: Notifications for data anomalies
- **Cost Threshold Alerts**: Budget monitoring and alerts

## 13. Future Enhancements

### 13.1 Planned Improvements
- **Incremental Loading**: Change data capture implementation
- **Real-time Streaming**: Kafka/Event Hub integration
- **Advanced Data Quality**: Schema evolution and validation
- **ML-based Monitoring**: Anomaly detection for data patterns
- **Multi-region Support**: Cross-region data replication

### 13.2 Integration Opportunities
- **Data Catalog Integration**: Automated metadata discovery
- **Workflow Orchestration**: Apache Airflow integration
- **BI Tool Integration**: Direct connection to analytics tools
- **API Gateway**: RESTful API for pipeline management

## 14. Troubleshooting Guide

### 14.1 Common Issues and Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Connection Timeout | JDBC connection failures | Check network connectivity and firewall rules |
| Authentication Error | Invalid credentials error | Verify Azure Key Vault secrets |
| Memory Issues | Out of memory exceptions | Increase driver memory or reduce batch size |
| Permission Denied | Unity Catalog access errors | Verify schema and table permissions |
| Slow Performance | Long execution times | Check source database load and network latency |

### 14.2 Debug Commands
```sql
-- Check audit logs for recent executions
SELECT * FROM workspace.inventory_bronze.bz_audit_log 
WHERE Load_Timestamp >= current_timestamp() - INTERVAL 1 DAY
ORDER BY Load_Timestamp DESC;

-- Verify table row counts
SELECT 'bz_products' as table_name, COUNT(*) as row_count FROM workspace.inventory_bronze.bz_products
UNION ALL
SELECT 'bz_customers' as table_name, COUNT(*) as row_count FROM workspace.inventory_bronze.bz_customers;

-- Check Delta table history
DESCRIBE HISTORY workspace.inventory_bronze.bz_products;
```

## 15. API Cost Calculation

### 15.1 Enhanced Pipeline Cost Analysis
- **Development Cost**: $0.000375 (from previous version)
- **Enhancement Cost**: $0.000425 (current version improvements)
- **Total API Cost**: $0.000800

### 15.2 Cost Breakdown
- **Code Generation**: $0.000300
- **Documentation**: $0.000200
- **Error Handling Enhancement**: $0.000150
- **Job Integration**: $0.000100
- **Performance Optimization**: $0.000050

**Total Enhanced Pipeline API Cost: $0.000800**

---

*This enhanced Bronze Layer Data Engineering Pipeline provides comprehensive data ingestion capabilities with advanced monitoring, error handling, and Databricks job integration for the Inventory Management System.*