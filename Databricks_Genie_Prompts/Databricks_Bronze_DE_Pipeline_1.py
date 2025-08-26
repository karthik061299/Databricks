# Databricks Bronze Layer Data Engineering Pipeline
# Inventory Management System - Raw Data Ingestion
# Version: 1.0
# Created: 2025-01-27

# ============================================================================
# BRONZE LAYER INGESTION STRATEGY
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import logging
from datetime import datetime
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("InventoryManagement_Bronze_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION AND METADATA SETUP
# ============================================================================

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
        
        # Schema definitions for each table
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

# ============================================================================
# BRONZE LAYER INGESTION CLASS
# ============================================================================

class BronzeLayerIngestion:
    """Main class for Bronze layer data ingestion"""
    
    def __init__(self, config: BronzeLayerConfig):
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
        
        # Check for duplicates (if primary key exists)
        if table_name.lower() in ["products", "suppliers", "warehouses", "customers"]:
            pk_column = f"{table_name[:-1]}_ID"  # Remove 's' and add '_ID'
            if pk_column in df.columns:
                total_records = df.count()
                distinct_records = df.select(pk_column).distinct().count()
                validation_results["duplicate_records"] = total_records - distinct_records
        
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
        audit_df = spark.createDataFrame([audit_record])
        audit_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .parquet(f"{self.config.audit_log_path}/ingestion_audit")
        
        logger.info(f"Audit logged for {table_name}: {status}")
    
    def ingest_from_jdbc(self, connection_string, table_name, source_system):
        """Ingest data from JDBC sources"""
        try:
            logger.info(f"Starting ingestion for {table_name} from {source_system}")
            
            # Read from JDBC source
            df = spark.read \
                .format("jdbc") \
                .option("url", connection_string) \
                .option("dbtable", table_name) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .load()
            
            # Add metadata columns
            df_with_metadata = self.add_metadata_columns(df, source_system, table_name)
            
            # Validate data quality
            validation_results = self.validate_data_quality(df, table_name)
            
            # Write to Bronze layer
            bronze_path = f"{self.config.bronze_base_path}/{table_name.lower()}"
            
            df_with_metadata.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(bronze_path)
            
            # Log successful ingestion
            self.log_ingestion_audit(table_name, source_system, "SUCCESS", df.count())
            
            logger.info(f"Successfully ingested {df.count()} records for {table_name}")
            return validation_results
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error ingesting {table_name}: {error_msg}")
            self.log_ingestion_audit(table_name, source_system, "FAILED", 0, error_msg)
            raise e
    
    def ingest_from_files(self, file_path, table_name, source_system, file_format="csv"):
        """Ingest data from file sources"""
        try:
            logger.info(f"Starting file ingestion for {table_name} from {source_system}")
            
            # Read from file source
            if file_format.lower() == "csv":
                df = spark.read \
                    .format("csv") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .load(f"{file_path}/{table_name.lower()}*.csv")
            elif file_format.lower() == "json":
                df = spark.read \
                    .format("json") \
                    .option("multiline", "true") \
                    .load(f"{file_path}/{table_name.lower()}*.json")
            elif file_format.lower() == "parquet":
                df = spark.read \
                    .format("parquet") \
                    .load(f"{file_path}/{table_name.lower()}*.parquet")
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Add metadata columns
            df_with_metadata = self.add_metadata_columns(df, source_system, table_name)
            
            # Validate data quality
            validation_results = self.validate_data_quality(df, table_name)
            
            # Write to Bronze layer
            bronze_path = f"{self.config.bronze_base_path}/{table_name.lower()}"
            
            df_with_metadata.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(bronze_path)
            
            # Log successful ingestion
            self.log_ingestion_audit(table_name, source_system, "SUCCESS", df.count())
            
            logger.info(f"Successfully ingested {df.count()} records for {table_name}")
            return validation_results
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error ingesting {table_name}: {error_msg}")
            self.log_ingestion_audit(table_name, source_system, "FAILED", 0, error_msg)
            raise e
    
    def ingest_streaming_data(self, source_path, table_name, source_system):
        """Ingest streaming data using structured streaming"""
        try:
            logger.info(f"Starting streaming ingestion for {table_name}")
            
            # Define schema for streaming
            schema = self.config.table_schemas.get(table_name.lower())
            
            # Read streaming data
            streaming_df = spark.readStream \
                .format("json") \
                .schema(schema) \
                .option("maxFilesPerTrigger", 1) \
                .load(source_path)
            
            # Add metadata columns
            streaming_df_with_metadata = self.add_metadata_columns(streaming_df, source_system, table_name)
            
            # Write streaming data to Bronze layer
            bronze_path = f"{self.config.bronze_base_path}/{table_name.lower()}"
            checkpoint_path = f"{self.config.checkpoint_path}/{table_name.lower()}"
            
            query = streaming_df_with_metadata.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_path) \
                .option("mergeSchema", "true") \
                .start(bronze_path)
            
            logger.info(f"Streaming query started for {table_name}")
            return query
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error starting streaming ingestion for {table_name}: {error_msg}")
            self.log_ingestion_audit(table_name, source_system, "STREAMING_FAILED", 0, error_msg)
            raise e
    
    def run_full_ingestion(self):
        """Run full ingestion for all configured sources"""
        logger.info(f"Starting full Bronze layer ingestion - Batch ID: {self.batch_id}")
        
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
                    validation_results = self.ingest_from_jdbc(
                        erp_config["connection_string"], 
                        table, 
                        "erp_system"
                    )
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
                    validation_results = self.ingest_from_jdbc(
                        order_config["connection_string"], 
                        table, 
                        "order_system"
                    )
                    ingestion_summary["tables_processed"].append({
                        "table": table,
                        "source": "order_system",
                        "records": validation_results["total_records"]
                    })
                    ingestion_summary["total_records"] += validation_results["total_records"]
                except Exception as e:
                    ingestion_summary["failed_tables"].append({"table": table, "error": str(e)})
            
            # Process Warehouse system files
            warehouse_config = self.config.source_systems["warehouse_system"]
            for table in warehouse_config["tables"]:
                try:
                    validation_results = self.ingest_from_files(
                        warehouse_config["file_path"], 
                        table, 
                        "warehouse_system",
                        warehouse_config["file_format"]
                    )
                    ingestion_summary["tables_processed"].append({
                        "table": table,
                        "source": "warehouse_system",
                        "records": validation_results["total_records"]
                    })
                    ingestion_summary["total_records"] += validation_results["total_records"]
                except Exception as e:
                    ingestion_summary["failed_tables"].append({"table": table, "error": str(e)})
            
            ingestion_summary["end_time"] = datetime.now()
            ingestion_summary["status"] = "COMPLETED" if not ingestion_summary["failed_tables"] else "COMPLETED_WITH_ERRORS"
            
            logger.info(f"Bronze layer ingestion completed. Total records: {ingestion_summary['total_records']}")
            return ingestion_summary
            
        except Exception as e:
            ingestion_summary["end_time"] = datetime.now()
            ingestion_summary["status"] = "FAILED"
            ingestion_summary["error"] = str(e)
            logger.error(f"Bronze layer ingestion failed: {str(e)}")
            raise e

# ============================================================================
# DATA QUALITY AND MONITORING
# ============================================================================

class BronzeDataQualityMonitor:
    """Data quality monitoring for Bronze layer"""
    
    def __init__(self, config: BronzeLayerConfig):
        self.config = config
    
    def generate_data_profile(self, table_name):
        """Generate data profile for a Bronze table"""
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
                
                profile["column_profiles"][column] = col_profile
        
        return profile
    
    def check_data_freshness(self, table_name, expected_frequency_hours=24):
        """Check data freshness for a Bronze table"""
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

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    try:
        # Initialize configuration
        config = BronzeLayerConfig()
        
        # Initialize ingestion engine
        ingestion_engine = BronzeLayerIngestion(config)
        
        # Run full ingestion
        summary = ingestion_engine.run_full_ingestion()
        
        # Generate data quality reports
        quality_monitor = BronzeDataQualityMonitor(config)
        
        print("=" * 80)
        print("BRONZE LAYER INGESTION SUMMARY")
        print("=" * 80)
        print(f"Batch ID: {summary['batch_id']}")
        print(f"Status: {summary['status']}")
        print(f"Total Records Processed: {summary['total_records']}")
        print(f"Tables Processed: {len(summary['tables_processed'])}")
        
        if summary['failed_tables']:
            print(f"Failed Tables: {len(summary['failed_tables'])}")
            for failed in summary['failed_tables']:
                print(f"  - {failed['table']}: {failed['error']}")
        
        print("\nTable Details:")
        for table_info in summary['tables_processed']:
            print(f"  - {table_info['table']} ({table_info['source']}): {table_info['records']} records")
        
        print("=" * 80)
        
        return summary
        
    except Exception as e:
        logger.error(f"Bronze layer ingestion failed: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()