# Databricks notebook source
# MAGIC %md
# MAGIC # Enhanced Bronze Layer Data Engineering Pipeline
# MAGIC ## Inventory Management System - Raw Data Ingestion
# MAGIC ### Version: 3.0 - Databricks Optimized
# MAGIC ### Created: 2025-01-27

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import logging
from datetime import datetime, timedelta
import json
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Setup

# COMMAND ----------

class BronzeLayerConfig:
    """Configuration class for Bronze layer ingestion"""
    
    def __init__(self):
        # Storage paths
        self.bronze_base_path = "/mnt/datalake/bronze/inventory_management"
        self.checkpoint_path = "/mnt/datalake/checkpoints/bronze"
        self.audit_log_path = "/mnt/datalake/audit/bronze_ingestion"
        
        # Source system configurations
        self.source_systems = {
            "erp_system": {
                "connection_string": "jdbc:sqlserver://erp-db:1433;database=InventoryDB",
                "tables": ["Products", "Suppliers", "Warehouses", "Customers"]
            },
            "order_system": {
                "connection_string": "jdbc:mysql://order-db:3306/orders",
                "tables": ["Orders", "Order_Details"]
            },
            "warehouse_system": {
                "file_path": "/mnt/source/warehouse_files",
                "file_format": "csv",
                "tables": ["Inventory", "Stock_Levels", "Shipments", "Returns"]
            }
        }
        
        # Schema definitions
        self.table_schemas = self._define_schemas()
        
    def _define_schemas(self):
        """Define schemas for all source tables"""
        return {
            "products": StructType([
                StructField("Product_ID", IntegerType(), False),
                StructField("Product_Name", StringType(), False),
                StructField("Category", StringType(), False),
                StructField("Brand", StringType(), True),
                StructField("Unit_Price", DecimalType(10,2), True),
                StructField("Unit_Cost", DecimalType(10,2), True)
            ]),
            "suppliers": StructType([
                StructField("Supplier_ID", IntegerType(), False),
                StructField("Supplier_Name", StringType(), False),
                StructField("Contact_Number", StringType(), False),
                StructField("Email", StringType(), True),
                StructField("Address", StringType(), True)
            ]),
            "warehouses": StructType([
                StructField("Warehouse_ID", IntegerType(), False),
                StructField("Location", StringType(), False),
                StructField("Capacity", IntegerType(), False),
                StructField("Zone", StringType(), True)
            ]),
            "inventory": StructType([
                StructField("Inventory_ID", IntegerType(), False),
                StructField("Product_ID", IntegerType(), False),
                StructField("Warehouse_ID", IntegerType(), False),
                StructField("Quantity_Available", IntegerType(), False),
                StructField("Last_Updated", TimestampType(), True)
            ]),
            "orders": StructType([
                StructField("Order_ID", IntegerType(), False),
                StructField("Customer_ID", IntegerType(), False),
                StructField("Order_Date", DateType(), False),
                StructField("Order_Status", StringType(), True)
            ]),
            "order_details": StructType([
                StructField("Order_Detail_ID", IntegerType(), False),
                StructField("Order_ID", IntegerType(), False),
                StructField("Product_ID", IntegerType(), False),
                StructField("Quantity_Ordered", IntegerType(), False),
                StructField("Unit_Price", DecimalType(10,2), True)
            ]),
            "shipments": StructType([
                StructField("Shipment_ID", IntegerType(), False),
                StructField("Order_ID", IntegerType(), False),
                StructField("Shipment_Date", DateType(), False),
                StructField("Tracking_Number", StringType(), True)
            ]),
            "returns": StructType([
                StructField("Return_ID", IntegerType(), False),
                StructField("Order_ID", IntegerType(), False),
                StructField("Return_Reason", StringType(), False),
                StructField("Return_Date", DateType(), True)
            ]),
            "stock_levels": StructType([
                StructField("Stock_Level_ID", IntegerType(), False),
                StructField("Warehouse_ID", IntegerType(), False),
                StructField("Product_ID", IntegerType(), False),
                StructField("Reorder_Threshold", IntegerType(), False),
                StructField("Max_Stock_Level", IntegerType(), True)
            ]),
            "customers": StructType([
                StructField("Customer_ID", IntegerType(), False),
                StructField("Customer_Name", StringType(), False),
                StructField("Email", StringType(), False),
                StructField("Phone", StringType(), True)
            ])
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Ingestion Class

# COMMAND ----------

class BronzeLayerIngestion:
    """Main class for Bronze layer data ingestion"""
    
    def __init__(self, config):
        self.config = config
        self.ingestion_timestamp = datetime.now()
        self.batch_id = self.ingestion_timestamp.strftime("%Y%m%d_%H%M%S")
        
    def add_metadata_columns(self, df, source_system, table_name):
        """Add metadata columns for lineage and auditing"""
        return df.withColumn("_bronze_ingestion_timestamp", lit(self.ingestion_timestamp)) \
                 .withColumn("_bronze_batch_id", lit(self.batch_id)) \
                 .withColumn("_source_system", lit(source_system)) \
                 .withColumn("_source_table", lit(table_name)) \
                 .withColumn("_file_name", input_file_name()) \
                 .withColumn("_row_hash", sha2(concat_ws("|", *[col(c) for c in df.columns]), 256))
    
    def validate_data_quality(self, df, table_name):
        """Basic data quality validation"""
        validation_results = {
            "table_name": table_name,
            "total_records": df.count(),
            "null_records": {},
            "duplicate_records": 0,
            "validation_timestamp": self.ingestion_timestamp
        }
        
        # Check for null values in key columns
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                validation_results["null_records"][column] = null_count
        
        return validation_results
    
    def log_ingestion_audit(self, table_name, source_system, status, record_count, error_message=None):
        """Log ingestion audit information"""
        audit_record = {
            "batch_id": self.batch_id,
            "table_name": table_name,
            "source_system": source_system,
            "ingestion_timestamp": self.ingestion_timestamp.isoformat(),
            "status": status,
            "record_count": record_count,
            "error_message": error_message
        }
        
        # Convert to DataFrame and write to audit log
        try:
            audit_df = spark.createDataFrame([audit_record])
            audit_df.write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .format("delta") \
                .save(f"{self.config.audit_log_path}/ingestion_audit")
            
            print(f"Audit logged for {table_name}: {status}")
        except Exception as e:
            print(f"Failed to write audit log: {str(e)}")
    
    def create_sample_data(self, table_name):
        """Create sample data for demonstration purposes"""
        if table_name.lower() == "products":
            data = [
                (1, "Laptop", "Electronics", "Dell", 999.99, 750.00),
                (2, "Mouse", "Electronics", "Logitech", 29.99, 15.00),
                (3, "Keyboard", "Electronics", "Microsoft", 79.99, 45.00)
            ]
            schema = self.config.table_schemas["products"]
            return spark.createDataFrame(data, schema)
        
        elif table_name.lower() == "suppliers":
            data = [
                (1, "Tech Supplies Inc", "+1-555-0123", "tech@supplies.com", "123 Tech Street"),
                (2, "Global Electronics", "+1-555-0456", "sales@global.com", "456 Global Ave")
            ]
            schema = self.config.table_schemas["suppliers"]
            return spark.createDataFrame(data, schema)
        
        elif table_name.lower() == "warehouses":
            data = [
                (1, "New York Warehouse", 10000, "Zone A"),
                (2, "California Warehouse", 15000, "Zone B")
            ]
            schema = self.config.table_schemas["warehouses"]
            return spark.createDataFrame(data, schema)
        
        elif table_name.lower() == "customers":
            data = [
                (1, "John Doe", "john.doe@email.com", "+1-555-1234"),
                (2, "Jane Smith", "jane.smith@email.com", "+1-555-5678")
            ]
            schema = self.config.table_schemas["customers"]
            return spark.createDataFrame(data, schema)
        
        else:
            # Return empty DataFrame with correct schema
            schema = self.config.table_schemas.get(table_name.lower())
            if schema:
                return spark.createDataFrame([], schema)
            else:
                return None
    
    def ingest_table_data(self, table_name, source_system):
        """Ingest data for a specific table"""
        try:
            print(f"Starting ingestion for {table_name} from {source_system}")
            
            # For demonstration, create sample data
            df = self.create_sample_data(table_name)
            
            if df is None:
                print(f"No schema defined for {table_name}")
                return None
            
            # Add metadata columns
            df_with_metadata = self.add_metadata_columns(df, source_system, table_name)
            
            # Validate data quality
            validation_results = self.validate_data_quality(df, table_name)
            
            # Write to Bronze layer
            bronze_path = f"{self.config.bronze_base_path}/{table_name.lower()}"
            
            df_with_metadata.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(bronze_path)
            
            # Log successful ingestion
            self.log_ingestion_audit(table_name, source_system, "SUCCESS", df.count())
            
            print(f"Successfully ingested {df.count()} records for {table_name}")
            return validation_results
            
        except Exception as e:
            error_msg = str(e)
            print(f"Error ingesting {table_name}: {error_msg}")
            self.log_ingestion_audit(table_name, source_system, "FAILED", 0, error_msg)
            return None
    
    def run_full_ingestion(self):
        """Run full ingestion for all configured sources"""
        print(f"Starting Bronze layer ingestion - Batch ID: {self.batch_id}")
        
        ingestion_summary = {
            "batch_id": self.batch_id,
            "start_time": self.ingestion_timestamp,
            "tables_processed": [],
            "total_records": 0,
            "failed_tables": []
        }
        
        try:
            # Process ERP system tables
            erp_config = self.config.source_systems["erp_system"]
            for table in erp_config["tables"]:
                try:
                    validation_results = self.ingest_table_data(table, "erp_system")
                    if validation_results:
                        ingestion_summary["tables_processed"].append({
                            "table": table,
                            "source": "erp_system",
                            "records": validation_results["total_records"]
                        })
                        ingestion_summary["total_records"] += validation_results["total_records"]
                except Exception as e:
                    ingestion_summary["failed_tables"].append({"table": table, "error": str(e)})
            
            # Process Order system tables
            order_config = self.config.source_systems["order_system"]
            for table in order_config["tables"]:
                try:
                    validation_results = self.ingest_table_data(table, "order_system")
                    if validation_results:
                        ingestion_summary["tables_processed"].append({
                            "table": table,
                            "source": "order_system",
                            "records": validation_results["total_records"]
                        })
                        ingestion_summary["total_records"] += validation_results["total_records"]
                except Exception as e:
                    ingestion_summary["failed_tables"].append({"table": table, "error": str(e)})
            
            ingestion_summary["end_time"] = datetime.now()
            ingestion_summary["status"] = "COMPLETED" if not ingestion_summary["failed_tables"] else "COMPLETED_WITH_ERRORS"
            
            print(f"Bronze layer ingestion completed. Total records: {ingestion_summary['total_records']}")
            return ingestion_summary
            
        except Exception as e:
            ingestion_summary["end_time"] = datetime.now()
            ingestion_summary["status"] = "FAILED"
            ingestion_summary["error"] = str(e)
            print(f"Bronze layer ingestion failed: {str(e)}")
            return ingestion_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

# Initialize configuration
config = BronzeLayerConfig()

# Initialize ingestion engine
ingestion_engine = BronzeLayerIngestion(config)

# Run full ingestion
summary = ingestion_engine.run_full_ingestion()

# Display results
print("=" * 80)
print("BRONZE LAYER INGESTION SUMMARY")
print("=" * 80)
print(f"Batch ID: {summary['batch_id']}")
print(f"Status: {summary['status']}")
print(f"Total Records Processed: {summary['total_records']}")
print(f"Tables Processed: {len(summary['tables_processed'])}")

if summary.get('failed_tables'):
    print(f"Failed Tables: {len(summary['failed_tables'])}")
    for failed in summary['failed_tables']:
        print(f"  - {failed['table']}: {failed['error']}")

print("\nTable Details:")
for table_info in summary['tables_processed']:
    print(f"  - {table_info['table']} ({table_info['source']}): {table_info['records']} records")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Monitoring

# COMMAND ----------

class BronzeDataQualityMonitor:
    """Data quality monitoring for Bronze layer"""
    
    def __init__(self, config):
        self.config = config
    
    def generate_data_profile(self, table_name):
        """Generate data profile for a Bronze table"""
        try:
            bronze_path = f"{self.config.bronze_base_path}/{table_name.lower()}"
            df = spark.read.format("delta").load(bronze_path)
            
            profile = {
                "table_name": table_name,
                "total_records": df.count(),
                "total_columns": len(df.columns),
                "column_profiles": {},
                "generated_at": datetime.now().isoformat()
            }
            
            for column in df.columns:
                if not column.startswith("_bronze"):
                    col_profile = {
                        "data_type": str(df.schema[column].dataType),
                        "null_count": df.filter(col(column).isNull()).count(),
                        "distinct_count": df.select(column).distinct().count()
                    }
                    
                    # Add statistics for numeric columns
                    if isinstance(df.schema[column].dataType, (IntegerType, DecimalType, DoubleType)):
                        try:
                            stats = df.select(
                                min(col(column)).alias("min_val"),
                                max(col(column)).alias("max_val"),
                                avg(col(column)).alias("avg_val")
                            ).collect()[0]
                            
                            col_profile.update({
                                "min_value": stats["min_val"],
                                "max_value": stats["max_val"],
                                "avg_value": stats["avg_val"]
                            })
                        except:
                            pass
                    
                    profile["column_profiles"][column] = col_profile
            
            return profile
        except Exception as e:
            print(f"Error generating profile for {table_name}: {str(e)}")
            return None
    
    def check_data_freshness(self, table_name, expected_frequency_hours=24):
        """Check data freshness for a Bronze table"""
        try:
            bronze_path = f"{self.config.bronze_base_path}/{table_name.lower()}"
            df = spark.read.format("delta").load(bronze_path)
            
            latest_ingestion = df.select(max(col("_bronze_ingestion_timestamp"))).collect()[0][0]
            current_time = datetime.now()
            
            if latest_ingestion:
                hours_since_last_ingestion = (current_time - latest_ingestion).total_seconds() / 3600
                is_fresh = hours_since_last_ingestion <= expected_frequency_hours
            else:
                hours_since_last_ingestion = None
                is_fresh = False
            
            return {
                "table_name": table_name,
                "latest_ingestion": latest_ingestion.isoformat() if latest_ingestion else None,
                "hours_since_last_ingestion": hours_since_last_ingestion,
                "is_fresh": is_fresh,
                "expected_frequency_hours": expected_frequency_hours
            }
        except Exception as e:
            print(f"Error checking freshness for {table_name}: {str(e)}")
            return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Data Quality Reports

# COMMAND ----------

# Initialize quality monitor
quality_monitor = BronzeDataQualityMonitor(config)

# Generate profiles for processed tables
print("\n" + "=" * 80)
print("DATA QUALITY PROFILES")
print("=" * 80)

for table_info in summary['tables_processed']:
    table_name = table_info['table']
    profile = quality_monitor.generate_data_profile(table_name)
    
    if profile:
        print(f"\nTable: {profile['table_name']}")
        print(f"Records: {profile['total_records']}")
        print(f"Columns: {profile['total_columns']}")
        
        for col_name, col_stats in profile['column_profiles'].items():
            print(f"  - {col_name}: {col_stats['data_type']}, Nulls: {col_stats['null_count']}, Distinct: {col_stats['distinct_count']}")

print("\n" + "=" * 80)
print("Bronze Layer Ingestion Pipeline Completed Successfully!")
print("=" * 80)