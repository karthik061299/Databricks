# Databricks Bronze Layer Data Engineering Pipeline
## Inventory Management System - Source to Bronze Ingestion Strategy

## 1. Executive Summary

This document defines a comprehensive ingestion strategy for moving data from PostgreSQL source systems to the Bronze layer in Databricks using the Medallion architecture. The Bronze layer serves as a landing zone for raw, unprocessed data while ensuring data integrity, metadata tracking, and comprehensive audit logging.

### 1.1 Key Objectives
- Efficient data ingestion from PostgreSQL to Databricks Bronze layer
- Comprehensive metadata tracking for lineage and governance
- Robust audit logging for compliance and troubleshooting
- Data quality monitoring and validation
- PII data protection and encryption
- Scalable and fault-tolerant ingestion processes

## 2. Source System Analysis

### 2.1 Source System Configuration

| Component | Details |
|-----------|----------|
| **Source System** | PostgreSQL |
| **Database Name** | DE |
| **Schema Name** | tests |
| **Connection Method** | Azure Key Vault Secrets |
| **Authentication** | Username/Password via Key Vault |

### 2.2 Source Tables Overview

| Table Name | Primary Key | Record Count (Est.) | Update Frequency | Business Criticality |
|------------|-------------|---------------------|------------------|---------------------|
| Products | Product_ID | 10,000+ | Daily | High |
| Suppliers | Supplier_ID | 1,000+ | Weekly | Medium |
| Warehouses | Warehouse_ID | 50+ | Monthly | High |
| Inventory | Inventory_ID | 100,000+ | Real-time | Critical |
| Orders | Order_ID | 500,000+ | Real-time | Critical |
| Order_Details | Order_Detail_ID | 2,000,000+ | Real-time | Critical |
| Shipments | Shipment_ID | 400,000+ | Daily | High |
| Returns | Return_ID | 50,000+ | Daily | Medium |
| Stock_Levels | Stock_Level_ID | 100,000+ | Hourly | High |
| Customers | Customer_ID | 100,000+ | Daily | High |

## 3. Bronze Layer Architecture

### 3.1 Target Schema Design

| Component | Configuration |
|-----------|---------------|
| **Target System** | Databricks (Delta Lake) |
| **Bronze Schema** | workspace.inventory_bronze |
| **Storage Format** | Delta Lake |
| **Partitioning Strategy** | Date-based for time-series tables |
| **Optimization** | Z-ordering on frequently queried columns |

## 4. Data Ingestion Strategy

### 4.1 Ingestion Architecture

```
PostgreSQL Source → Azure Key Vault → Databricks Auto Loader → Bronze Delta Tables
                                    ↓
                              Metadata Tracking
                                    ↓
                               Audit Logging
```

### 4.2 Ingestion Patterns by Table Type

| Ingestion Pattern | Tables | Frequency | Method |
|-------------------|--------|-----------|--------|
| **Real-time Streaming** | Inventory, Orders, Order_Details | Continuous | Auto Loader + Change Data Capture |
| **Batch Processing** | Products, Suppliers, Warehouses | Daily | Scheduled batch jobs |
| **Micro-batch** | Shipments, Returns, Stock_Levels | Hourly | Auto Loader micro-batching |
| **Full Refresh** | Customers | Daily | Complete table refresh |

## 5. PySpark Implementation

### 5.1 Connection Configuration
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import uuid
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder.appName("BronzeLayerIngestion").getOrCreate()

# Connection Configuration
source_db_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")

# JDBC Properties
jdbc_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver",
    "fetchsize": "10000",
    "batchsize": "10000"
}
```

### 5.2 Bronze Layer Table Creation
```python
# Create Bronze Schema
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.inventory_bronze")

# Products Table
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.inventory_bronze.bz_products (
    Product_ID INT,
    Product_Name STRING,
    Category STRING,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT,
    batch_id STRING
) USING DELTA
LOCATION '/mnt/bronze/inventory/bz_products'
PARTITIONED BY (DATE(load_timestamp))
""")

# Inventory Table
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.inventory_bronze.bz_inventory (
    Inventory_ID INT,
    Product_ID INT,
    Quantity_Available INT,
    Warehouse_ID INT,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT,
    batch_id STRING
) USING DELTA
LOCATION '/mnt/bronze/inventory/bz_inventory'
PARTITIONED BY (DATE(load_timestamp))
CLUSTER BY (Product_ID, Warehouse_ID)
""")

# Create all other Bronze tables similarly...
```

### 5.3 Generic Ingestion Function
```python
def ingest_table_to_bronze(spark, source_table, target_table, ingestion_type="batch"):
    """
    Generic function to ingest data from PostgreSQL to Bronze layer
    """
    try:
        # Generate batch metadata
        batch_id = str(uuid.uuid4())
        current_timestamp = datetime.now()
        
        # Read from source
        source_df = spark.read \
            .format("jdbc") \
            .option("url", source_db_url) \
            .option("dbtable", f"tests.{source_table}") \
            .options(**jdbc_properties) \
            .load()
        
        # Add metadata columns
        enriched_df = source_df \
            .withColumn("load_timestamp", lit(current_timestamp)) \
            .withColumn("update_timestamp", lit(current_timestamp)) \
            .withColumn("source_system", lit("PostgreSQL_DE")) \
            .withColumn("record_status", lit("ACTIVE")) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("data_quality_score", lit(100))
        
        # Apply data quality checks
        quality_df = apply_data_quality_checks(enriched_df, source_table)
        
        # Write to Bronze layer
        write_to_bronze(quality_df, target_table)
        
        # Log successful ingestion
        log_ingestion_audit(spark, source_table, target_table, batch_id, "SUCCESS", quality_df.count())
        
        return True
        
    except Exception as e:
        # Log failed ingestion
        log_ingestion_audit(spark, source_table, target_table, batch_id, "FAILED", 0, str(e))
        raise e

def apply_data_quality_checks(df, table_name):
    """
    Apply data quality checks and calculate quality scores
    """
    total_records = df.count()
    
    # Basic quality checks
    null_count = df.filter(col(df.columns[0]).isNull()).count()
    quality_score = max(0, 100 - (null_count / total_records * 100))
    
    return df.withColumn("data_quality_score", lit(int(quality_score)))

def write_to_bronze(df, target_table):
    """
    Write data to Bronze layer using Delta Lake
    """
    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"workspace.inventory_bronze.{target_table}")
```

### 5.4 Main Ingestion Job
```python
def main_bronze_ingestion_job():
    """
    Main job to orchestrate all Bronze layer ingestions
    """
    # Define tables to ingest
    tables_to_ingest = [
        ("products", "bz_products"),
        ("suppliers", "bz_suppliers"),
        ("warehouses", "bz_warehouses"),
        ("inventory", "bz_inventory"),
        ("orders", "bz_orders"),
        ("order_details", "bz_order_details"),
        ("shipments", "bz_shipments"),
        ("returns", "bz_returns"),
        ("stock_levels", "bz_stock_levels"),
        ("customers", "bz_customers")
    ]
    
    # Execute ingestions
    for source_table, target_table in tables_to_ingest:
        try:
            success = ingest_table_to_bronze(spark, source_table, target_table)
            if success:
                print(f"Successfully ingested {source_table} to {target_table}")
        except Exception as e:
            print(f"Failed to ingest {source_table}: {str(e)}")
            continue

# Execute the main job
if __name__ == "__main__":
    main_bronze_ingestion_job()
```

## 6. Audit and Monitoring

### 6.1 Audit Log Implementation
```python
def log_ingestion_audit(spark, source_table, target_table, batch_id, status, record_count, error_msg=None):
    """
    Log ingestion audit information
    """
    audit_data = [{
        "audit_id": str(uuid.uuid4()),
        "source_table": source_table,
        "target_table": target_table,
        "batch_id": batch_id,
        "ingestion_timestamp": datetime.now(),
        "records_processed": record_count,
        "status": status,
        "error_message": error_msg,
        "processed_by": "Databricks_Bronze_Pipeline"
    }]
    
    audit_df = spark.createDataFrame(audit_data)
    audit_df.write.format("delta").mode("append").saveAsTable("workspace.inventory_bronze.bz_audit_log")
```

## 7. Performance Optimization

### 7.1 Table Optimization
```python
def optimize_bronze_tables():
    """
    Optimize Bronze layer tables for better performance
    """
    tables_to_optimize = [
        ("bz_products", ["Product_ID"]),
        ("bz_inventory", ["Product_ID", "Warehouse_ID"]),
        ("bz_orders", ["Customer_ID", "Order_Date"]),
        ("bz_customers", ["Customer_ID"])
    ]
    
    for table, z_order_cols in tables_to_optimize:
        try:
            spark.sql(f"OPTIMIZE workspace.inventory_bronze.{table} ZORDER BY ({', '.join(z_order_cols)})")
            spark.sql(f"VACUUM workspace.inventory_bronze.{table} RETAIN 168 HOURS")
            print(f"Optimized table: {table}")
        except Exception as e:
            print(f"Error optimizing {table}: {str(e)}")
```

## 8. Error Handling and Recovery

### 8.1 Retry Logic
```python
def robust_ingestion_with_retry(spark, source_table, target_table, max_retries=3):
    """
    Robust ingestion with retry logic
    """
    import time
    
    for retry_count in range(max_retries):
        try:
            success = ingest_table_to_bronze(spark, source_table, target_table)
            if success:
                return True
        except Exception as e:
            print(f"Attempt {retry_count + 1} failed for {source_table}: {str(e)}")
            if retry_count < max_retries - 1:
                time.sleep(2 ** retry_count)  # Exponential backoff
            else:
                raise e
    return False
```

## 9. Data Quality and Validation

### 9.1 Comprehensive Data Quality Checks
```python
def comprehensive_data_quality_check(df, table_name):
    """
    Perform comprehensive data quality checks
    """
    total_records = df.count()
    quality_issues = []
    
    # Check for null values in key columns
    key_columns = {
        "products": ["Product_ID", "Product_Name"],
        "customers": ["Customer_ID", "Customer_Name", "Email"],
        "orders": ["Order_ID", "Customer_ID"]
    }
    
    if table_name in key_columns:
        for col_name in key_columns[table_name]:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                quality_issues.append(f"{col_name}: {null_count} null values")
    
    # Calculate quality score
    quality_score = max(0, 100 - len(quality_issues) * 10)
    
    return df.withColumn("data_quality_score", lit(quality_score)), quality_issues
```

## 10. Deployment and Execution

### 10.1 Job Scheduling Configuration
```json
{
  "name": "Bronze_Layer_Ingestion_Pipeline",
  "job_clusters": [{
    "job_cluster_key": "bronze_cluster",
    "new_cluster": {
      "spark_version": "13.3.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 4
    }
  }],
  "tasks": [{
    "task_key": "bronze_ingestion",
    "job_cluster_key": "bronze_cluster",
    "notebook_task": {
      "notebook_path": "/bronze_ingestion_pipeline"
    }
  }],
  "schedule": {
    "quartz_cron_expression": "0 0 */1 * * ?",
    "timezone_id": "Asia/Kolkata"
  }
}
```

## Conclusion

This Bronze Layer Data Engineering Pipeline provides a comprehensive solution for ingesting data from PostgreSQL sources into Databricks Bronze layer with proper metadata tracking, audit logging, and data quality monitoring. The implementation ensures scalability, reliability, and compliance with data governance requirements.