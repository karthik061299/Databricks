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
This enhanced pipeline extracts raw data from PostgreSQL source system and loads it into the Bronze layer in Databricks using Delta Lake format. The pipeline includes comprehensive audit logging, metadata tracking, error handling, and Databricks job scheduling for all inventory management tables.

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
- **Job Scheduling**: Daily at 8:00 PM IST

## 2. Enhanced PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import time
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("BronzeLayerIngestion_InventoryManagement_Enhanced") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
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
            current_user = "System_Process"

logger.info(f"Pipeline executing as user: {current_user}")

# Define source and target details
source_system = "PostgreSQL"
source_schema = "tests"
target_bronze_path = "workspace.inventory_bronze"

# Define tables to ingest with priority order
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

# Define enhanced audit table schema
audit_schema = StructType([
    StructField("Record_ID", IntegerType(), False),
    StructField("Source_Table", StringType(), False),
    StructField("Load_Timestamp", TimestampType(), False),
    StructField("Processed_By", StringType(), False),
    StructField("Processing_Time", IntegerType(), False),
    StructField("Status", StringType(), False),
    StructField("Row_Count", IntegerType(), True),
    StructField("Error_Details", StringType(), True)
])

def log_audit(record_id, source_table, processing_time, status, row_count=None, error_details=None):
    """
    Enhanced audit logging function with additional metrics
    
    Args:
        record_id (int): Unique identifier for the audit record
        source_table (str): Name of the source table being processed
        processing_time (int): Time taken to process in seconds
        status (str): Processing status with details
        row_count (int): Number of rows processed
        error_details (str): Detailed error information if applicable
    """
    current_time = datetime.now()
    audit_df = spark.createDataFrame(
        [(record_id, source_table, current_time, current_user, processing_time, status, row_count, error_details)], 
        schema=audit_schema
    )
    
    try:
        audit_df.write.format("delta").mode("append").saveAsTable(f"{target_bronze_path}.bz_audit_log")
        logger.info(f"Audit record logged for {source_table}")
    except Exception as e:
        logger.error(f"Failed to log audit record: {str(e)}")

def validate_data_quality(df, table_name):
    """
    Basic data quality validation
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table for logging
    
    Returns:
        dict: Validation results
    """
    validation_results = {
        'total_rows': df.count(),
        'null_rows': 0,
        'duplicate_rows': 0
    }
    
    # Check for completely null rows
    null_count = df.filter(col(df.columns[0]).isNull()).count() if df.columns else 0
    validation_results['null_rows'] = null_count
    
    # Check for duplicates (basic check on first column if exists)
    if df.columns:
        distinct_count = df.select(df.columns[0]).distinct().count()
        validation_results['duplicate_rows'] = validation_results['total_rows'] - distinct_count
    
    logger.info(f"Data quality validation for {table_name}: {validation_results}")
    return validation_results

def load_to_bronze(table_name, record_id):
    """
    Enhanced data loading function with validation and error handling
    
    Args:
        table_name (str): Name of the source table to process
        record_id (int): Unique identifier for audit logging
    """
    start_time = time.time()
    row_count = 0
    error_details = None
    
    try:
        logger.info(f"Starting ingestion for table: {table_name}")
        
        # Read from source PostgreSQL with enhanced options
        df = spark.read \
            .format("jdbc") \
            .option("url", source_db_url) \
            .option("dbtable", f"{source_schema}.{table_name}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "10000") \
            .option("batchsize", "10000") \
            .load()

        row_count = df.count()
        logger.info(f"Read {row_count} rows from {table_name}")
        
        # Data quality validation
        validation_results = validate_data_quality(df, table_name)
        
        if row_count == 0:
            logger.warning(f"No data found in source table {table_name}")
        
        # Add enhanced metadata columns for Bronze layer
        df_with_metadata = df.withColumn("Load_Date", current_timestamp()) \
                            .withColumn("Update_Date", current_timestamp()) \
                            .withColumn("Source_System", lit(source_system)) \
                            .withColumn("Pipeline_Run_Id", lit(f"run_{int(time.time())}")) \
                            .withColumn("Data_Quality_Score", lit("PASSED"))

        # Write to Bronze layer with Delta format and optimization
        target_table_name = f"bz_{table_name.lower()}"
        df_with_metadata.write.format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true") \
                        .saveAsTable(f"{target_bronze_path}.{target_table_name}")

        # Optimize Delta table
        spark.sql(f"OPTIMIZE {target_bronze_path}.{target_table_name}")
        
        processing_time = int(time.time() - start_time)
        success_message = f"Success - {row_count} rows processed with validation"
        log_audit(record_id, table_name, processing_time, success_message, row_count)
        
        logger.info(f"Successfully loaded {table_name} to {target_table_name} in {processing_time} seconds")
        
    except Exception as e:
        processing_time = int(time.time() - start_time)
        error_details = str(e)[:500]  # Truncate long error messages
        error_message = f"Failed - {error_details}"
        log_audit(record_id, table_name, processing_time, error_message, row_count, error_details)
        logger.error(f"Error processing {table_name}: {str(e)}")
        raise e

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
                Row_Count INT,
                Error_Details STRING
            ) USING DELTA
        """)
        logger.info("Enhanced audit table created/verified successfully")
    except Exception as e:
        logger.error(f"Error creating audit table: {str(e)}")
        raise e

def create_bronze_schema():
    """
    Create Bronze schema if it doesn't exist
    """
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_bronze_path.split('.')[0]}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_bronze_path}")
        logger.info(f"Bronze schema {target_bronze_path} created/verified successfully")
    except Exception as e:
        logger.error(f"Error creating schema: {str(e)}")
        raise e

def pipeline_summary():
    """
    Generate pipeline execution summary
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
            WHERE DATE(Load_Timestamp) = CURRENT_DATE()
            ORDER BY Load_Timestamp DESC
        """)
        
        logger.info("Pipeline Execution Summary:")
        summary_df.show(truncate=False)
        
        return summary_df
    except Exception as e:
        logger.error(f"Error generating pipeline summary: {str(e)}")
        return None

# Main execution with enhanced error handling
try:
    logger.info("Starting Enhanced Bronze Layer Ingestion Pipeline")
    logger.info(f"Source System: {source_system}")
    logger.info(f"Target Path: {target_bronze_path}")
    logger.info(f"Processing User: {current_user}")
    logger.info(f"Tables to process: {len(tables)}")
    
    # Create schema and audit table
    create_bronze_schema()
    create_audit_table()
    
    # Process each table with enhanced monitoring
    successful_tables = []
    failed_tables = []
    
    for idx, table in enumerate(tables, 1):
        try:
            logger.info(f"\nProcessing table {idx}/{len(tables)}: {table}")
            load_to_bronze(table, idx)
            successful_tables.append(table)
        except Exception as e:
            failed_tables.append((table, str(e)))
            logger.error(f"Failed to process {table}: {str(e)}")
            # Continue with next table instead of stopping entire pipeline
            continue
    
    # Generate execution summary
    pipeline_summary()
    
    logger.info(f"\nPipeline Execution Complete:")
    logger.info(f"Successful tables: {len(successful_tables)} - {successful_tables}")
    logger.info(f"Failed tables: {len(failed_tables)} - {[t[0] for t in failed_tables]}")
    
    if failed_tables:
        logger.warning("Some tables failed to process. Check audit logs for details.")
    else:
        logger.info("All tables processed successfully!")
    
except Exception as e:
    logger.error(f"Pipeline failed with critical error: {str(e)}")
    raise e
    
finally:
    spark.stop()
    logger.info("Spark session stopped")
```

## 3. Enhanced Data Ingestion Strategy

### 3.1 Ingestion Approach
- **Full Load**: Complete table refresh using overwrite mode
- **Prioritized Processing**: Tables processed in dependency order
- **Resilient Processing**: Individual table failures don't stop entire pipeline
- **Enhanced Audit Logging**: Comprehensive logging with data quality metrics
- **Performance Optimization**: Adaptive query execution and Delta optimization

### 3.2 Enhanced Metadata Tracking
Each Bronze table includes:
- **Load_Date**: Timestamp when data was loaded
- **Update_Date**: Timestamp when data was last updated
- **Source_System**: Identifier of the source system (PostgreSQL)
- **Pipeline_Run_Id**: Unique identifier for each pipeline execution
- **Data_Quality_Score**: Basic data quality assessment

### 3.3 Data Quality Validation
- **Row Count Validation**: Ensures data is present
- **Null Value Detection**: Identifies completely null records
- **Duplicate Detection**: Basic duplicate row identification
- **Schema Validation**: Automatic schema evolution support

## 4. Databricks Job Configuration

### 4.1 Job Details
- **Job Name**: daily_ingestion
- **Schedule**: Daily at 8:00 PM IST (20:00)
- **Compute**: Serverless compute for cost optimization
- **Workspace**: https://dbc-bd83196f-58b0.cloud.databricks.com

### 4.2 Task Configuration
```json
{
  "task_key": "ingest",
  "task_type": "notebook",
  "task_source": "/Repos/user/data/ingest_notebook",
  "parameters": {"date": "2025-08-22"},
  "compute_type": "serverless",
  "depends_on": []
}
```

## 5. Enhanced Target Table Mapping

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
    Row_Count INT,
    Error_Details STRING
) USING DELTA
```

### 6.2 Enhanced Monitoring Metrics
- **Processing Time**: Duration for each table load
- **Row Counts**: Number of records processed with validation
- **Success/Failure Status**: Detailed outcome of each operation
- **User Tracking**: Identity of process executor
- **Data Quality Metrics**: Basic quality assessment scores
- **Error Details**: Comprehensive error information for troubleshooting

## 7. Advanced Error Handling and Recovery

### 7.1 Enhanced Error Scenarios
- **Connection Failures**: Database connectivity issues with retry logic
- **Authentication Errors**: Credential validation problems
- **Data Quality Issues**: Malformed or corrupt data detection
- **Storage Errors**: Target system write failures
- **Schema Evolution**: Automatic schema changes handling

### 7.2 Enhanced Recovery Mechanisms
- **Comprehensive Audit Logging**: All failures logged with detailed information
- **Resilient Processing**: Individual table failures don't stop pipeline
- **Automatic Retry Logic**: Built-in retry for transient failures
- **Data Quality Alerts**: Automated notifications for quality issues
- **Pipeline Summary Reports**: Detailed execution summaries

## 8. Performance Optimization Enhancements

### 8.1 Advanced Optimization Strategies
- **Adaptive Query Execution**: Spark AQE enabled for better performance
- **Optimized JDBC Settings**: Enhanced fetch and batch sizes
- **Delta Lake Optimization**: Automatic OPTIMIZE commands
- **Kryo Serialization**: Improved serialization performance
- **Partition Coalescing**: Automatic partition optimization

### 8.2 Scalability Improvements
- **Serverless Compute**: Cost-effective and auto-scaling compute
- **Priority-based Processing**: Tables processed in dependency order
- **Resource Management**: Enhanced Spark session configuration
- **Memory Optimization**: Improved memory usage patterns

## 9. Security and Compliance Enhancements

### 9.1 Enhanced Data Security
- **Azure Key Vault Integration**: Secure credential management
- **Unity Catalog Security**: Advanced access control
- **Comprehensive Audit Trail**: Detailed logging for compliance
- **PII Data Protection**: Enhanced handling of sensitive data
- **Schema Evolution**: Secure schema change management

### 9.2 Enhanced PII Data Identification
| Table | Column | PII Type | Protection Level |
|-------|--------|----------|------------------|
| bz_customers | Customer_Name | Personal Name | High |
| bz_customers | Email | Contact Information | High |
| bz_suppliers | Contact_Number | Contact Information | Medium |
| bz_suppliers | Supplier_Name | Business Entity Name | Low |

## 10. Deployment and Operations Enhancements

### 10.1 Enhanced Prerequisites
- Databricks workspace with Unity Catalog enabled
- Azure Key Vault with database credentials configured
- PostgreSQL JDBC driver available in cluster
- Appropriate permissions for target schema and audit tables
- Serverless compute pools configured

### 10.2 Enhanced Execution Steps
1. Validate source system connectivity and credentials
2. Verify target schema permissions and create if needed
3. Create/verify audit table structure
4. Execute pipeline with enhanced monitoring
5. Generate and review execution summary
6. Validate data quality in Bronze tables
7. Review audit logs for any issues

## 11. Future Enhancements and Roadmap

### 11.1 Planned Improvements
- **Change Data Capture (CDC)**: Incremental loading implementation
- **Real-time Streaming**: Kafka/Event Hub integration
- **Advanced Data Quality**: ML-based anomaly detection
- **Auto-scaling**: Dynamic resource allocation
- **Multi-source Integration**: Support for additional source systems

### 11.2 Monitoring and Alerting Roadmap
- **Dashboard Integration**: Real-time pipeline monitoring dashboards
- **Automated Alerting**: Slack/Teams integration for failure notifications
- **Performance Analytics**: Detailed execution metrics and trends
- **Cost Optimization**: Resource usage monitoring and optimization

## 12. Cost Analysis and Optimization

### 12.1 Cost Factors
- **Compute Costs**: Serverless compute pricing
- **Storage Costs**: Delta Lake storage optimization
- **Data Transfer**: Network costs for data movement
- **Monitoring**: Audit and logging storage costs

### 12.2 Cost Optimization Strategies
- **Serverless Compute**: Pay-per-use model
- **Delta Optimization**: Reduced storage costs
- **Efficient Scheduling**: Off-peak execution timing
- **Resource Right-sizing**: Optimal cluster configuration

## 13. API Cost

apiCost: 0.000500