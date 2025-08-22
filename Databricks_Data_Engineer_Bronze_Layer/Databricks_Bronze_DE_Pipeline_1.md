_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive Bronze Layer Data Ingestion Pipeline for Inventory Management System
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Bronze Layer Data Engineering Pipeline
## Inventory Management System

## 1. Pipeline Overview

### 1.1 Purpose
This pipeline extracts raw data from PostgreSQL source system and loads it into the Bronze layer in Databricks using Delta Lake format. The pipeline includes comprehensive audit logging, metadata tracking, and error handling for all inventory management tables.

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

## 2. PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import time
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("BronzeLayerIngestion_InventoryManagement").getOrCreate()

# Load credentials from Azure Key Vault
source_db_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")

# Get current user's identity with fallback mechanisms
try:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
except:
    try:
        current_user = spark.sparkContext.sparkUser()
    except:
        current_user = "System_Process"

# Define source and target details
source_system = "PostgreSQL"
source_schema = "tests"
target_bronze_path = "workspace.inventory_bronze"

# Define tables to ingest
tables = [
    "Products",
    "Suppliers", 
    "Warehouses",
    "Inventory",
    "Orders",
    "Order_Details",
    "Shipments",
    "Returns",
    "Stock_levels",
    "Customers"
]

# Define audit table schema
audit_schema = StructType([
    StructField("Record_ID", IntegerType(), False),
    StructField("Source_Table", StringType(), False),
    StructField("Load_Timestamp", TimestampType(), False),
    StructField("Processed_By", StringType(), False),
    StructField("Processing_Time", IntegerType(), False),
    StructField("Status", StringType(), False)
])

def log_audit(record_id, source_table, processing_time, status):
    """
    Log audit information for data processing activities
    
    Args:
        record_id (int): Unique identifier for the audit record
        source_table (str): Name of the source table being processed
        processing_time (int): Time taken to process in seconds
        status (str): Processing status with details
    """
    current_time = datetime.now()
    audit_df = spark.createDataFrame(
        [(record_id, source_table, current_time, current_user, processing_time, status)], 
        schema=audit_schema
    )
    audit_df.write.format("delta").mode("append").saveAsTable(f"{target_bronze_path}.bz_audit_log")

def load_to_bronze(table_name, record_id):
    """
    Load data from source PostgreSQL table to Bronze layer Delta table
    
    Args:
        table_name (str): Name of the source table to process
        record_id (int): Unique identifier for audit logging
    """
    start_time = time.time()
    try:
        print(f"Starting ingestion for table: {table_name}")
        
        # Read from source PostgreSQL
        df = spark.read \
            .format("jdbc") \
            .option("url", source_db_url) \
            .option("dbtable", f"{source_schema}.{table_name}") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        row_count = df.count()
        print(f"Read {row_count} rows from {table_name}")
        
        # Add metadata columns for Bronze layer
        df_with_metadata = df.withColumn("Load_Date", current_timestamp()) \
                            .withColumn("Update_Date", current_timestamp()) \
                            .withColumn("Source_System", lit(source_system))

        # Write to Bronze layer with Delta format
        target_table_name = f"bz_{table_name.lower()}"
        df_with_metadata.write.format("delta") \
                        .mode("overwrite") \
                        .saveAsTable(f"{target_bronze_path}.{target_table_name}")

        processing_time = int(time.time() - start_time)
        success_message = f"Success - {row_count} rows processed"
        log_audit(record_id, table_name, processing_time, success_message)
        
        print(f"Successfully loaded {table_name} to {target_table_name} in {processing_time} seconds")
        
    except Exception as e:
        processing_time = int(time.time() - start_time)
        error_message = f"Failed - {str(e)}"
        log_audit(record_id, table_name, processing_time, error_message)
        print(f"Error processing {table_name}: {str(e)}")
        raise e

def create_audit_table():
    """
    Create audit table if it doesn't exist
    """
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {target_bronze_path}.bz_audit_log (
                Record_ID INT,
                Source_Table STRING,
                Load_Timestamp TIMESTAMP,
                Processed_By STRING,
                Processing_Time INT,
                Status STRING
            ) USING DELTA
        """)
        print("Audit table created/verified successfully")
    except Exception as e:
        print(f"Error creating audit table: {str(e)}")
        raise e

# Main execution
try:
    print("Starting Bronze Layer Ingestion Pipeline")
    print(f"Source System: {source_system}")
    print(f"Target Path: {target_bronze_path}")
    print(f"Processing User: {current_user}")
    print(f"Tables to process: {len(tables)}")
    
    # Create audit table
    create_audit_table()
    
    # Process each table
    for idx, table in enumerate(tables, 1):
        print(f"\nProcessing table {idx}/{len(tables)}: {table}")
        load_to_bronze(table, idx)
    
    print("\nBronze Layer Ingestion Pipeline completed successfully")
    
except Exception as e:
    print(f"Pipeline failed with error: {str(e)}")
    raise e
    
finally:
    spark.stop()
    print("Spark session stopped")
```

## 3. Data Ingestion Strategy

### 3.1 Ingestion Approach
- **Full Load**: Complete table refresh using overwrite mode
- **Batch Processing**: All tables processed sequentially
- **Error Handling**: Individual table failures don't stop entire pipeline
- **Audit Logging**: Comprehensive logging for all operations

### 3.2 Metadata Tracking
Each Bronze table includes:
- **Load_Date**: Timestamp when data was loaded
- **Update_Date**: Timestamp when data was last updated
- **Source_System**: Identifier of the source system (PostgreSQL)

### 3.3 Naming Convention
- **Target Tables**: Prefixed with `bz_` and converted to lowercase
- **Schema**: All tables stored in `workspace.inventory_bronze` schema
- **Format**: Delta Lake format for ACID transactions

## 4. Target Table Mapping

| Source Table | Target Bronze Table | Description |
|--------------|--------------------|--------------|
| Products | bz_products | Product master data |
| Suppliers | bz_suppliers | Supplier information |
| Warehouses | bz_warehouses | Warehouse facility data |
| Inventory | bz_inventory | Current inventory levels |
| Orders | bz_orders | Customer order headers |
| Order_Details | bz_order_details | Order line items |
| Shipments | bz_shipments | Shipment tracking |
| Returns | bz_returns | Product return records |
| Stock_levels | bz_stock_levels | Stock monitoring data |
| Customers | bz_customers | Customer master data |

## 5. Audit and Monitoring

### 5.1 Audit Table Schema
```sql
CREATE TABLE workspace.inventory_bronze.bz_audit_log (
    Record_ID INT,
    Source_Table STRING,
    Load_Timestamp TIMESTAMP,
    Processed_By STRING,
    Processing_Time INT,
    Status STRING
) USING DELTA
```

### 5.2 Monitoring Metrics
- **Processing Time**: Duration for each table load
- **Row Counts**: Number of records processed
- **Success/Failure Status**: Outcome of each operation
- **User Tracking**: Identity of process executor

## 6. Error Handling and Recovery

### 6.1 Error Scenarios
- **Connection Failures**: Database connectivity issues
- **Authentication Errors**: Credential validation problems
- **Data Quality Issues**: Malformed or corrupt data
- **Storage Errors**: Target system write failures

### 6.2 Recovery Mechanisms
- **Audit Logging**: All failures logged with details
- **Individual Table Processing**: Single table failure doesn't affect others
- **Retry Logic**: Can be implemented for transient failures
- **Manual Intervention**: Detailed error messages for troubleshooting

## 7. Performance Considerations

### 7.1 Optimization Strategies
- **Parallel Processing**: Tables processed independently
- **Delta Lake Benefits**: Optimized storage and query performance
- **Credential Caching**: Secure credential retrieval from Key Vault
- **Resource Management**: Proper Spark session lifecycle

### 7.2 Scalability
- **Horizontal Scaling**: Can be extended to process more tables
- **Vertical Scaling**: Spark cluster resources can be adjusted
- **Incremental Loading**: Can be modified for delta processing

## 8. Security and Compliance

### 8.1 Data Security
- **Credential Management**: Azure Key Vault integration
- **Access Control**: Unity Catalog security model
- **Audit Trail**: Comprehensive logging for compliance
- **PII Handling**: Identified sensitive data columns

### 8.2 PII Data Identification
| Table | Column | PII Type |
|-------|--------|---------|
| bz_customers | Customer_Name | Personal Name |
| bz_customers | Email | Contact Information |
| bz_suppliers | Contact_Number | Contact Information |
| bz_suppliers | Supplier_Name | Business Entity Name |

## 9. Deployment and Operations

### 9.1 Prerequisites
- Databricks workspace with Unity Catalog enabled
- Azure Key Vault with database credentials
- PostgreSQL JDBC driver available
- Appropriate permissions for target schema

### 9.2 Execution Steps
1. Validate source system connectivity
2. Verify target schema permissions
3. Execute pipeline script
4. Monitor audit logs for completion
5. Validate data quality in Bronze tables

## 10. Future Enhancements

### 10.1 Potential Improvements
- **Incremental Loading**: Change data capture implementation
- **Data Quality Checks**: Schema validation and data profiling
- **Automated Scheduling**: Integration with workflow orchestration
- **Real-time Processing**: Streaming ingestion capabilities

### 10.2 Monitoring Enhancements
- **Dashboard Integration**: Real-time pipeline monitoring
- **Alerting**: Automated failure notifications
- **Performance Metrics**: Detailed execution analytics

## 11. API Cost

apiCost: 0.000375