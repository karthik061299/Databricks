_____________________________________________
## *Author*: AAVA Data Engineer
## *Created on*: 2024-12-19
## *Description*: Comprehensive Bronze Layer Data Engineering Pipeline for Inventory Management System
## *Version*: 1
## *Updated on*: 2024-12-19
_____________________________________________

# Databricks Bronze Layer Data Engineering Pipeline
## Inventory Management System - Source to Bronze Ingestion Strategy

## 1. Executive Summary

This document defines a comprehensive ingestion strategy for moving data from PostgreSQL source systems to the Bronze layer in Databricks using the Medallion architecture. The Bronze layer serves as a landing zone for raw, unprocessed data while ensuring data integrity, metadata tracking, and comprehensive audit logging.

## 2. Architecture Overview

### 2.1 Source System Configuration

| Component | Details |
|-----------|----------|
| **Source System** | PostgreSQL |
| **Database Name** | DE |
| **Schema Name** | tests |
| **Connection Method** | Azure Key Vault Secrets |
| **Authentication** | Username/Password via Key Vault |

### 2.2 Target System Configuration

| Component | Details |
|-----------|----------|
| **Target System** | Databricks (Delta Lake) |
| **Bronze Schema** | workspace.inventory_bronze |
| **Storage Format** | Delta Lake |
| **Catalog** | Unity Catalog |
| **Location** | /mnt/bronze/ |

## 3. Data Ingestion Strategy

### 3.1 Ingestion Patterns

| Table | Ingestion Pattern | Frequency | Volume Estimate | CDC Required |
|-------|------------------|-----------|-----------------|-------------|
| Products | Batch | Daily | Low (< 10K records) | No |
| Suppliers | Batch | Daily | Low (< 5K records) | No |
| Warehouses | Batch | Weekly | Very Low (< 100 records) | No |
| Inventory | Streaming/Batch | Real-time/Hourly | High (> 1M records) | Yes |
| Orders | Streaming | Real-time | High (> 500K records) | Yes |
| Order_Details | Streaming | Real-time | Very High (> 2M records) | Yes |
| Shipments | Batch | Hourly | Medium (< 100K records) | Yes |
| Returns | Batch | Daily | Low (< 50K records) | No |
| Stock_Levels | Batch | Hourly | Medium (< 200K records) | Yes |
| Customers | Batch | Daily | Medium (< 100K records) | No |

### 3.2 Ingestion Methods

#### 3.2.1 Auto Loader Configuration
```python
# Auto Loader for streaming ingestion
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/mnt/bronze/schemas/") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("/mnt/landing/inventory/")
```

#### 3.2.2 JDBC Connection for Batch Ingestion
```python
# JDBC connection configuration
jdbc_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")

connection_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver",
    "fetchsize": "10000",
    "batchsize": "10000"
}
```

## 4. Bronze Layer Table Definitions

### 4.1 Enhanced Bronze Schema with Metadata

#### 4.1.1 Products Bronze Table
```sql
CREATE TABLE IF NOT EXISTS workspace.inventory_bronze.bz_products (
    Product_ID INT,
    Product_Name STRING,
    Category STRING,
    -- Metadata columns
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT,
    batch_id STRING,
    file_name STRING,
    row_hash STRING
) USING DELTA
LOCATION '/mnt/bronze/bz_products'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

#### 4.1.2 Inventory Bronze Table (High Volume)
```sql
CREATE TABLE IF NOT EXISTS workspace.inventory_bronze.bz_inventory (
    Inventory_ID INT,
    Product_ID INT,
    Quantity_Available INT,
    Warehouse_ID INT,
    -- Metadata columns
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT,
    batch_id STRING,
    file_name STRING,
    row_hash STRING
) USING DELTA
LOCATION '/mnt/bronze/bz_inventory'
PARTITIONED BY (DATE(load_timestamp))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.dataSkippingNumIndexedCols' = '5'
);
```

## 5. Data Ingestion Implementation

### 5.1 Generic Ingestion Framework

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import hashlib
import uuid
from datetime import datetime

class BronzeIngestionFramework:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.current_timestamp = datetime.now()
        self.batch_id = str(uuid.uuid4())
    
    def add_metadata_columns(self, df, source_system, file_name=None):
        """
        Add standard metadata columns to the dataframe
        """
        return df.withColumn("load_timestamp", lit(self.current_timestamp)) \
                 .withColumn("update_timestamp", lit(self.current_timestamp)) \
                 .withColumn("source_system", lit(source_system)) \
                 .withColumn("record_status", lit("ACTIVE")) \
                 .withColumn("batch_id", lit(self.batch_id)) \
                 .withColumn("file_name", lit(file_name)) \
                 .withColumn("row_hash", sha2(concat_ws("|", *df.columns), 256))
    
    def calculate_data_quality_score(self, df):
        """
        Calculate data quality score based on null values and data validation
        """
        total_columns = len(df.columns)
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        
        # Calculate quality score (0-100)
        quality_score = 100 - (sum(null_counts.collect()[0]) / (df.count() * total_columns) * 100)
        
        return df.withColumn("data_quality_score", lit(int(quality_score)))
    
    def ingest_batch_data(self, source_table, target_table, primary_keys):
        """
        Generic batch ingestion method
        """
        try:
            # Read from source
            source_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"tests.{source_table}") \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            # Add metadata and quality score
            enriched_df = self.add_metadata_columns(source_df, "PostgreSQL_DE")
            final_df = self.calculate_data_quality_score(enriched_df)
            
            # Write to Bronze layer using MERGE
            if DeltaTable.isDeltaTable(self.spark, f"/mnt/bronze/{target_table}"):
                delta_table = DeltaTable.forPath(self.spark, f"/mnt/bronze/{target_table}")
                
                # Build merge condition
                merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
                
                delta_table.alias("target").merge(
                    final_df.alias("source"),
                    merge_condition
                ).whenMatchedUpdate(set={
                    col: f"source.{col}" for col in final_df.columns if col not in ["load_timestamp"]
                }).whenNotMatchedInsert(values={
                    col: f"source.{col}" for col in final_df.columns
                }).execute()
            else:
                final_df.write.format("delta").mode("overwrite").save(f"/mnt/bronze/{target_table}")
            
            # Log success
            self.log_ingestion_audit(source_table, target_table, final_df.count(), "SUCCESS")
            
        except Exception as e:
            self.log_ingestion_audit(source_table, target_table, 0, f"FAILED: {str(e)}")
            raise e
    
    def log_ingestion_audit(self, source_table, target_table, record_count, status):
        """
        Log ingestion audit information
        """
        audit_data = [{
            "record_id": str(uuid.uuid4()),
            "source_table": source_table,
            "target_table": target_table,
            "load_timestamp": self.current_timestamp,
            "processed_by": "BronzeIngestionFramework",
            "record_count": record_count,
            "batch_id": self.batch_id,
            "status": status
        }]
        
        audit_df = self.spark.createDataFrame(audit_data)
        audit_df.write.format("delta").mode("append").saveAsTable("workspace.inventory_bronze.bz_audit_log")
```

### 5.2 Streaming Ingestion for High-Volume Tables

```python
def setup_streaming_ingestion():
    """
    Setup streaming ingestion for high-volume tables
    """
    # Orders streaming ingestion
    orders_stream = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/mnt/bronze/schemas/orders") \
        .option("cloudFiles.maxFilesPerTrigger", "100") \
        .load("/mnt/landing/orders/")
    
    # Add metadata and write to Bronze
    orders_enriched = add_metadata_columns(orders_stream, "PostgreSQL_DE", "orders_stream")
    
    orders_query = orders_enriched.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/mnt/bronze/checkpoints/orders") \
        .trigger(processingTime="5 minutes") \
        .toTable("workspace.inventory_bronze.bz_orders")
    
    return orders_query
```

## 6. Data Quality and Validation

### 6.1 Data Quality Framework

```python
class DataQualityValidator:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.quality_rules = {
            "products": {
                "required_fields": ["Product_ID", "Product_Name"],
                "unique_fields": ["Product_ID"],
                "data_types": {"Product_ID": "integer", "Product_Name": "string"}
            },
            "customers": {
                "required_fields": ["Customer_ID", "Customer_Name", "Email"],
                "unique_fields": ["Customer_ID", "Email"],
                "pii_fields": ["Customer_Name", "Email"]
            }
        }
    
    def validate_data_quality(self, df, table_name):
        """
        Comprehensive data quality validation
        """
        quality_score = 100
        quality_issues = []
        
        if table_name in self.quality_rules:
            rules = self.quality_rules[table_name]
            
            # Check required fields
            for field in rules.get("required_fields", []):
                null_count = df.filter(col(field).isNull()).count()
                if null_count > 0:
                    quality_score -= 10
                    quality_issues.append(f"Null values in required field: {field}")
            
            # Check unique constraints
            for field in rules.get("unique_fields", []):
                duplicate_count = df.groupBy(field).count().filter(col("count") > 1).count()
                if duplicate_count > 0:
                    quality_score -= 15
                    quality_issues.append(f"Duplicate values in unique field: {field}")
        
        return max(0, quality_score), quality_issues
```

### 6.2 PII Data Handling

```python
def encrypt_pii_fields(df, pii_columns):
    """
    Encrypt PII fields for compliance
    """
    from pyspark.sql.functions import expr
    
    for col_name in pii_columns:
        if col_name in df.columns:
            df = df.withColumn(
                f"{col_name}_encrypted",
                expr(f"aes_encrypt({col_name}, 'encryption_key_from_keyvault')")
            ).drop(col_name)
    
    return df
```

## 7. Monitoring and Alerting

### 7.1 Data Pipeline Monitoring

```python
class PipelineMonitor:
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def check_data_freshness(self, table_name, max_age_hours=2):
        """
        Check if data is fresh within specified hours
        """
        latest_load = self.spark.sql(f"""
            SELECT MAX(load_timestamp) as latest_load
            FROM workspace.inventory_bronze.{table_name}
        """).collect()[0]["latest_load"]
        
        if latest_load:
            age_hours = (datetime.now() - latest_load).total_seconds() / 3600
            if age_hours > max_age_hours:
                self.send_alert(f"Data freshness alert: {table_name} is {age_hours:.1f} hours old")
                return False
        return True
    
    def check_data_quality_trends(self, table_name):
        """
        Monitor data quality score trends
        """
        avg_quality = self.spark.sql(f"""
            SELECT AVG(data_quality_score) as avg_quality
            FROM workspace.inventory_bronze.{table_name}
            WHERE DATE(load_timestamp) = CURRENT_DATE()
        """).collect()[0]["avg_quality"]
        
        if avg_quality and avg_quality < 80:
            self.send_alert(f"Data quality alert: {table_name} average quality score is {avg_quality:.1f}")
            return False
        return True
    
    def send_alert(self, message):
        """
        Send alert to monitoring system
        """
        print(f"ALERT: {message}")
        # Integrate with actual alerting system (email, Slack, etc.)
```

## 8. Job Orchestration and Scheduling

### 8.1 Databricks Job Configuration

```json
{
  "name": "Bronze_Layer_Ingestion_Pipeline",
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2,
    "spark_conf": {
      "spark.databricks.delta.preview.enabled": "true",
      "spark.sql.adaptive.enabled": "true",
      "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
  },
  "tasks": [
    {
      "task_key": "ingest_reference_data",
      "notebook_task": {
        "notebook_path": "/Repos/bronze_ingestion/reference_data_ingestion",
        "base_parameters": {
          "tables": "products,suppliers,warehouses,customers"
        }
      }
    },
    {
      "task_key": "ingest_transactional_data",
      "notebook_task": {
        "notebook_path": "/Repos/bronze_ingestion/transactional_data_ingestion",
        "base_parameters": {
          "tables": "orders,order_details,inventory,shipments,returns,stock_levels"
        }
      },
      "depends_on": [
        {
          "task_key": "ingest_reference_data"
        }
      ]
    },
    {
      "task_key": "data_quality_validation",
      "notebook_task": {
        "notebook_path": "/Repos/bronze_ingestion/data_quality_validation"
      },
      "depends_on": [
        {
          "task_key": "ingest_transactional_data"
        }
      ]
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 */2 * * ?",
    "timezone_id": "Asia/Kolkata"
  }
}
```

### 8.2 Error Handling and Recovery

```python
def robust_ingestion_with_retry(table_config, max_retries=3):
    """
    Robust ingestion with retry logic
    """
    for attempt in range(max_retries):
        try:
            ingestion_framework = BronzeIngestionFramework(spark)
            ingestion_framework.ingest_batch_data(
                table_config["source_table"],
                table_config["target_table"],
                table_config["primary_keys"]
            )
            print(f"Successfully ingested {table_config['source_table']}")
            break
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {table_config['source_table']}: {str(e)}")
            if attempt == max_retries - 1:
                # Log to dead letter queue or alert system
                print(f"All retry attempts failed for {table_config['source_table']}")
                raise e
            time.sleep(2 ** attempt)  # Exponential backoff
```

## 9. Performance Optimization

### 9.1 Partitioning Strategy

| Table | Partitioning Column | Z-Order Columns | Rationale |
|-------|-------------------|-----------------|----------|
| bz_orders | DATE(load_timestamp) | Order_ID, Customer_ID | Time-based queries, customer analysis |
| bz_inventory | DATE(load_timestamp) | Product_ID, Warehouse_ID | Product and location-based queries |
| bz_order_details | DATE(load_timestamp) | Order_ID, Product_ID | Order and product analysis |
| bz_shipments | DATE(load_timestamp) | Order_ID, Shipment_Date | Shipping analysis |

### 9.2 Auto-Optimization Configuration

```sql
-- Enable auto-optimization for all Bronze tables
ALTER TABLE workspace.inventory_bronze.bz_orders 
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '5'
);

-- Set up liquid clustering for high-volume tables
ALTER TABLE workspace.inventory_bronze.bz_inventory 
CLUSTER BY (Product_ID, Warehouse_ID);
```

## 10. Security and Compliance

### 10.1 Access Control

```sql
-- Grant permissions to Bronze layer
GRANT SELECT, MODIFY ON SCHEMA workspace.inventory_bronze TO `data_engineers`;
GRANT SELECT ON SCHEMA workspace.inventory_bronze TO `data_analysts`;
GRANT ALL PRIVILEGES ON SCHEMA workspace.inventory_bronze TO `data_admin`;

-- Row-level security for PII data
CREATE OR REPLACE FUNCTION mask_pii(user_role STRING, pii_value STRING)
RETURNS STRING
RETURN CASE 
  WHEN user_role IN ('admin', 'compliance') THEN pii_value
  ELSE '***MASKED***'
END;
```

### 10.2 Audit Logging

```python
def comprehensive_audit_logging(operation, table_name, user, record_count):
    """
    Comprehensive audit logging for compliance
    """
    audit_record = {
        "audit_id": str(uuid.uuid4()),
        "operation": operation,
        "table_name": table_name,
        "user_id": user,
        "timestamp": datetime.now(),
        "record_count": record_count,
        "ip_address": spark.sparkContext.getConf().get("spark.driver.host"),
        "session_id": spark.sparkContext.applicationId
    }
    
    audit_df = spark.createDataFrame([audit_record])
    audit_df.write.format("delta").mode("append").saveAsTable("workspace.inventory_bronze.security_audit_log")
```

## 11. Disaster Recovery and Backup

### 11.1 Backup Strategy

| Component | Backup Method | Frequency | Retention |
|-----------|---------------|-----------|----------|
| Delta Tables | Deep Clone | Daily | 30 days |
| Checkpoints | File System Backup | Hourly | 7 days |
| Schemas | Export Scripts | Weekly | 90 days |
| Job Configurations | Version Control | On Change | Indefinite |

### 11.2 Recovery Procedures

```python
def create_backup_clone(table_name, backup_location):
    """
    Create deep clone for backup
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {backup_location}.{table_name}_backup_{datetime.now().strftime('%Y%m%d')}
        DEEP CLONE workspace.inventory_bronze.{table_name}
    """)

def restore_from_backup(table_name, restore_point):
    """
    Restore table from backup or time travel
    """
    spark.sql(f"""
        CREATE OR REPLACE TABLE workspace.inventory_bronze.{table_name}
        AS SELECT * FROM workspace.inventory_bronze.{table_name} 
        VERSION AS OF {restore_point}
    """)
```

## 12. Cost Optimization

### 12.1 Storage Optimization

```python
# Regular maintenance job for cost optimization
def optimize_bronze_tables():
    """
    Regular optimization for cost and performance
    """
    tables = [
        "bz_products", "bz_suppliers", "bz_warehouses", "bz_inventory",
        "bz_orders", "bz_order_details", "bz_shipments", "bz_returns",
        "bz_stock_levels", "bz_customers"
    ]
    
    for table in tables:
        # Optimize table
        spark.sql(f"OPTIMIZE workspace.inventory_bronze.{table}")
        
        # Vacuum old files (retain 7 days)
        spark.sql(f"VACUUM workspace.inventory_bronze.{table} RETAIN 168 HOURS")
        
        # Update table statistics
        spark.sql(f"ANALYZE TABLE workspace.inventory_bronze.{table} COMPUTE STATISTICS")
```

### 12.2 Compute Optimization

| Workload Type | Cluster Configuration | Auto-scaling | Spot Instances |
|---------------|----------------------|--------------|----------------|
| Batch Ingestion | Standard_DS3_v2 | 2-8 workers | Yes (70% savings) |
| Streaming | Standard_DS4_v2 | Fixed 4 workers | No (reliability) |
| Maintenance | Standard_DS2_v2 | 1-4 workers | Yes (80% savings) |

## 13. Implementation Roadmap

### 13.1 Phase 1: Foundation (Week 1-2)
- [ ] Set up Bronze schema and basic tables
- [ ] Implement basic ingestion framework
- [ ] Configure JDBC connections and security
- [ ] Set up basic monitoring

### 13.2 Phase 2: Core Ingestion (Week 3-4)
- [ ] Implement batch ingestion for reference data
- [ ] Set up streaming ingestion for transactional data
- [ ] Implement data quality validation
- [ ] Configure job scheduling

### 13.3 Phase 3: Advanced Features (Week 5-6)
- [ ] Implement PII encryption and masking
- [ ] Set up comprehensive monitoring and alerting
- [ ] Configure backup and disaster recovery
- [ ] Performance optimization and tuning

### 13.4 Phase 4: Production Readiness (Week 7-8)
- [ ] Security hardening and access control
- [ ] Comprehensive testing and validation
- [ ] Documentation and training
- [ ] Go-live and production support

## 14. Success Metrics and KPIs

| Metric | Target | Measurement Method |
|--------|--------|-----------------|
| Data Freshness | < 2 hours | Monitor load_timestamp |
| Data Quality Score | > 95% | Average quality score across tables |
| Ingestion Success Rate | > 99.5% | Success/failure ratio in audit logs |
| Processing Time | < 30 minutes | End-to-end pipeline execution time |
| Cost per GB | < $0.10 | Monthly cost analysis |
| Recovery Time | < 4 hours | Disaster recovery testing |

## 15. Conclusion

This comprehensive Bronze Layer Data Engineering Pipeline provides:

- **Robust Data Ingestion**: Multiple ingestion patterns for different data volumes and frequencies
- **Data Quality Assurance**: Comprehensive validation and quality scoring
- **Metadata Management**: Complete lineage and audit capabilities
- **Security and Compliance**: PII handling, encryption, and access control
- **Performance Optimization**: Partitioning, clustering, and auto-optimization
- **Monitoring and Alerting**: Proactive monitoring for data freshness and quality
- **Disaster Recovery**: Backup strategies and recovery procedures
- **Cost Optimization**: Storage and compute optimization strategies

The implementation follows Databricks best practices and provides a solid foundation for the Silver and Gold layers in the Medallion architecture.

## 16. API Cost

**apiCost**: 0.001250

---

**Document Status**: Ready for Implementation  
**Next Steps**: Begin Phase 1 implementation with foundation setup  
**Review Date**: Weekly during implementation phases