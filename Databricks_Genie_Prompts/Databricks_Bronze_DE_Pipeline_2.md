# Databricks Bronze DE Pipeline - Enhanced Version

## Pipeline Overview

This enhanced Bronze layer data engineering pipeline implements a comprehensive ingestion strategy for moving data from PostgreSQL source systems to Databricks Bronze layer tables. The pipeline ensures efficient data ingestion, comprehensive metadata tracking, audit logging, and data governance compliance for the Inventory Management System.

## Architecture Components

### Source System Configuration
- **Source System**: PostgreSQL Database
- **Database Name**: DE
- **Schema Name**: tests
- **Connection Method**: Azure Key Vault secured credentials
- **Authentication**: Managed through Azure Key Vault secrets

### Target System Configuration
- **Target System**: Databricks Unity Catalog
- **Bronze Schema Path**: workspace.inventory_bronze
- **Storage Format**: Delta Lake
- **Location Pattern**: `/mnt/bronze/{table_name}`

## Data Sources and Target Mapping

### Source Tables (PostgreSQL)
| Source Table | Records Expected | Business Domain |
|--------------|------------------|------------------|
| Products | Variable | Product Catalog |
| Suppliers | Variable | Vendor Management |
| Warehouses | Variable | Location Management |
| Inventory | High Volume | Stock Management |
| Orders | High Volume | Order Processing |
| Order_Details | High Volume | Order Line Items |
| Shipments | Medium Volume | Logistics |
| Returns | Low Volume | Returns Processing |
| Stock_Levels | Medium Volume | Inventory Thresholds |
| Customers | Variable | Customer Management |

### Target Bronze Tables (Databricks)
| Target Table | Source Mapping | Partitioning Strategy |
|--------------|----------------|----------------------|
| bz_products | Products | None (Reference Data) |
| bz_suppliers | Suppliers | None (Reference Data) |
| bz_warehouses | Warehouses | None (Reference Data) |
| bz_inventory | Inventory | Partitioned by load_date |
| bz_orders | Orders | Partitioned by order_date |
| bz_order_details | Order_Details | Partitioned by load_date |
| bz_shipments | Shipments | Partitioned by shipment_date |
| bz_returns | Returns | Partitioned by load_date |
| bz_stock_levels | Stock_Levels | Partitioned by load_date |
| bz_customers | Customers | None (PII Encrypted) |
| bz_audit_log | System Generated | Partitioned by log_date |

## Ingestion Strategy

### 1. Data Extraction Methods

#### Full Load Strategy
- **Frequency**: Initial load and monthly refresh for reference tables
- **Tables**: Products, Suppliers, Warehouses, Customers
- **Method**: Complete table extraction with timestamp-based validation

#### Incremental Load Strategy
- **Frequency**: Daily incremental loads
- **Tables**: Inventory, Orders, Order_Details, Shipments, Returns, Stock_Levels
- **Method**: Change Data Capture (CDC) using timestamp columns
- **Watermark Column**: update_timestamp or created_timestamp

## Pipeline Implementation Code

### Main Ingestion Pipeline

```python
# Databricks Bronze Layer Ingestion Pipeline
# File: bronze_ingestion_pipeline.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime
import logging

# Initialize Spark Session
spark = SparkSession.builder \n    .appName("Bronze_Layer_Ingestion_Enhanced") \n    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \n    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \n    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeIngestionPipeline:
    def __init__(self):
        self.source_system = "PostgreSQL_DE"
        self.target_schema = "workspace.inventory_bronze"
        self.bronze_path = "/mnt/bronze"
        
    def get_postgres_connection(self):
        """Retrieve PostgreSQL connection details from Azure Key Vault"""
        try:
            source_db_url = mssparkutils.credentials.getSecret(
                "https://akv-poc-fabric.vault.azure.net/", 
                "KConnectionString"
            )
            user = mssparkutils.credentials.getSecret(
                "https://akv-poc-fabric.vault.azure.net/", 
                "KUser"
            )
            password = mssparkutils.credentials.getSecret(
                "https://akv-poc-fabric.vault.azure.net/", 
                "KPassword"
            )
            
            return {
                "url": source_db_url,
                "user": user,
                "password": password,
                "driver": "org.postgresql.Driver"
            }
        except Exception as e:
            logger.error(f"Failed to retrieve connection details: {str(e)}")
            raise
    
    def calculate_data_quality_score(self, df, table_name):
        """Calculate data quality score based on validation rules"""
        try:
            total_records = df.count()
            if total_records == 0:
                return 0
            
            # Check for null values
            null_counts = {}
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_counts[column] = null_count
            
            # Calculate overall null percentage
            total_nulls = sum(null_counts.values())
            total_cells = total_records * len(df.columns)
            null_percentage = (total_nulls / total_cells) * 100 if total_cells > 0 else 0
            
            # Quality score calculation (100 - null_percentage)
            quality_score = max(0, min(100, 100 - null_percentage))
            
            logger.info(f"Data quality score for {table_name}: {quality_score}")
            return int(quality_score)
            
        except Exception as e:
            logger.error(f"Error calculating data quality score: {str(e)}")
            return 50  # Default score on error
    
    def encrypt_pii_columns(self, df, pii_columns):
        """Encrypt PII columns for compliance"""
        try:
            for col_name in pii_columns:
                if col_name in df.columns:
                    # Simple encryption placeholder - implement proper encryption
                    df = df.withColumn(
                        col_name,
                        when(col(col_name).isNotNull(), 
                             concat(lit("ENC_"), col(col_name)))
                        .otherwise(col(col_name))
                    )
            return df
        except Exception as e:
            logger.error(f"Error encrypting PII columns: {str(e)}")
            return df
    
    def get_last_watermark(self, target_table):
        """Get the last watermark timestamp for incremental loads"""
        try:
            watermark_query = f"""
            SELECT COALESCE(MAX(update_timestamp), '1900-01-01 00:00:00') as last_watermark
            FROM {self.target_schema}.{target_table}
            """
            result = spark.sql(watermark_query).collect()
            return result[0]['last_watermark'] if result else '1900-01-01 00:00:00'
        except:
            return '1900-01-01 00:00:00'  # Default for new tables
    
    def get_merge_condition(self, target_table):
        """Get merge condition based on table primary keys"""
        merge_conditions = {
            "bz_products": "target.Product_ID = source.Product_ID",
            "bz_suppliers": "target.Supplier_ID = source.Supplier_ID",
            "bz_warehouses": "target.Warehouse_ID = source.Warehouse_ID",
            "bz_inventory": "target.Inventory_ID = source.Inventory_ID",
            "bz_orders": "target.Order_ID = source.Order_ID",
            "bz_order_details": "target.Order_Detail_ID = source.Order_Detail_ID",
            "bz_shipments": "target.Shipment_ID = source.Shipment_ID",
            "bz_returns": "target.Return_ID = source.Return_ID",
            "bz_stock_levels": "target.Stock_Level_ID = source.Stock_Level_ID",
            "bz_customers": "target.Customer_ID = source.Customer_ID"
        }
        return merge_conditions.get(target_table, "target.id = source.id")
    
    def ingest_table_to_bronze(self, source_table, target_table, ingestion_type="full"):
        """Main function to ingest data from source to Bronze layer"""
        audit_start = datetime.now()
        record_count = 0
        
        try:
            logger.info(f"Starting ingestion: {source_table} -> {target_table} ({ingestion_type})")
            
            # Get connection properties
            conn_props = self.get_postgres_connection()
            
            # Read from source
            if ingestion_type == "full":
                source_df = spark.read \n                    .format("jdbc") \n                    .options(**conn_props) \n                    .option("dbtable", f"tests.{source_table}") \n                    .load()
            else:
                # Incremental load
                watermark = self.get_last_watermark(target_table)
                query = f"""(
                    SELECT * FROM tests.{source_table} 
                    WHERE COALESCE(update_timestamp, created_timestamp, current_timestamp) > '{watermark}'
                ) as incremental"""
                source_df = spark.read \n                    .format("jdbc") \n                    .options(**conn_props) \n                    .option("dbtable", query) \n                    .load()
            
            record_count = source_df.count()
            logger.info(f"Retrieved {record_count} records from source")
            
            if record_count == 0:
                logger.info(f"No new records to process for {source_table}")
                self.log_audit_entry(source_table, target_table, "SUCCESS", 
                                   audit_start, datetime.now(), 0, "No new records")
                return True
            
            # Add metadata columns
            enriched_df = source_df \n                .withColumn("load_timestamp", current_timestamp()) \n                .withColumn("update_timestamp", current_timestamp()) \n                .withColumn("source_system", lit(self.source_system)) \n                .withColumn("record_status", lit("ACTIVE"))
            
            # Calculate data quality score
            quality_score = self.calculate_data_quality_score(enriched_df, source_table)
            enriched_df = enriched_df.withColumn("data_quality_score", lit(quality_score))
            
            # Apply PII encryption if needed
            pii_mapping = {
                "bz_customers": ["Customer_Name", "Email"],
                "bz_suppliers": ["Contact_Number"]
            }
            
            if target_table in pii_mapping:
                enriched_df = self.encrypt_pii_columns(enriched_df, pii_mapping[target_table])
            
            # Create target path
            target_path = f"{self.bronze_path}/{target_table}"
            
            # Write to Bronze layer
            if ingestion_type == "full":
                enriched_df.write \n                    .format("delta") \n                    .mode("overwrite") \n                    .option("path", target_path) \n                    .saveAsTable(f"{self.target_schema}.{target_table}")
            else:
                # Check if table exists
                try:
                    delta_table = DeltaTable.forPath(spark, target_path)
                    
                    # Perform merge
                    merge_condition = self.get_merge_condition(target_table)
                    
                    delta_table.alias("target").merge(
                        enriched_df.alias("source"),
                        merge_condition
                    ).whenMatchedUpdateAll() \n                     .whenNotMatchedInsertAll() \n                     .execute()
                     
                except Exception as merge_error:
                    # Table doesn't exist, create it
                    logger.info(f"Table {target_table} doesn't exist, creating new table")
                    enriched_df.write \n                        .format("delta") \n                        .mode("overwrite") \n                        .option("path", target_path) \n                        .saveAsTable(f"{self.target_schema}.{target_table}")
            
            # Log successful completion
            audit_end = datetime.now()
            self.log_audit_entry(source_table, target_table, "SUCCESS", 
                               audit_start, audit_end, record_count)
            
            logger.info(f"Successfully ingested {record_count} records to {target_table}")
            return True
            
        except Exception as e:
            # Log failure
            audit_end = datetime.now()
            error_msg = str(e)
            self.log_audit_entry(source_table, target_table, "FAILED", 
                               audit_start, audit_end, record_count, error_msg)
            logger.error(f"Failed to ingest {source_table}: {error_msg}")
            raise e
    
    def log_audit_entry(self, source_table, target_table, status, start_time, end_time, record_count, error_msg=None):
        """Log audit information to Bronze audit table"""
        try:
            audit_data = [{
                "record_id": f"{source_table