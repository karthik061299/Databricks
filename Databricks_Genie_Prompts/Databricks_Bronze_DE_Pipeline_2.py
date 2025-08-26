# Databricks Bronze Layer Data Engineering Pipeline - Enhanced Version
# Inventory Management System - Source to Bronze Ingestion Strategy
# Author: Data Engineer - Version: 2.0
# Enhancements: Improved error handling, performance optimization, advanced monitoring

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import logging
from datetime import datetime, timedelta
import json
from typing import Dict, List, Any, Optional
import time

# ============================================================================
# ENHANCED BRONZE LAYER CONFIGURATION
# ============================================================================

class EnhancedBronzeConfig:
    def __init__(self):
        self.source_system = "PostgreSQL"
        self.source_database = "DE"
        self.source_schema = "tests"
        self.bronze_catalog = "workspace"
        self.bronze_schema = "inventory_bronze"
        self.key_vault_url = "https://akv-poc-fabric.vault.azure.net/"
        self.quality_threshold = 80
        self.batch_size = 50000
        self.max_retries = 3
        self.retry_delay_seconds = 30
        self.enable_cdc = True
        self.enable_schema_evolution = True
        self.checkpoint_location = "/mnt/bronze/checkpoints/"
        self.dead_letter_location = "/mnt/bronze/dead_letter/"
        
        # Enhanced table configuration with CDC and partitioning
        self.tables_config = {
            "products": {
                "source_table": "Products", 
                "bronze_table": "bz_products", 
                "primary_key": "Product_ID",
                "partition_columns": ["Category"],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Product_Name"],
                "data_retention_days": 2555,
                "quality_rules": {
                    "not_null_columns": ["Product_ID", "Product_Name"],
                    "unique_columns": ["Product_ID"],
                    "valid_values": {"Category": ["Electronics", "Apparel", "Furniture", "Books", "Sports"]}
                }
            },
            "suppliers": {
                "source_table": "Suppliers", 
                "bronze_table": "bz_suppliers", 
                "primary_key": "Supplier_ID",
                "partition_columns": [],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Supplier_Name"],
                "data_retention_days": 2555,
                "quality_rules": {
                    "not_null_columns": ["Supplier_ID", "Supplier_Name", "Contact_Number"],
                    "unique_columns": ["Supplier_ID"]
                }
            },
            "warehouses": {
                "source_table": "Warehouses", 
                "bronze_table": "bz_warehouses", 
                "primary_key": "Warehouse_ID",
                "partition_columns": [],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Location"],
                "data_retention_days": 2555,
                "quality_rules": {
                    "not_null_columns": ["Warehouse_ID", "Location", "Capacity"],
                    "unique_columns": ["Warehouse_ID"]
                }
            },
            "inventory": {
                "source_table": "Inventory", 
                "bronze_table": "bz_inventory", 
                "primary_key": "Inventory_ID",
                "partition_columns": ["Warehouse_ID"],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Product_ID", "Warehouse_ID"],
                "data_retention_days": 1095,
                "quality_rules": {
                    "not_null_columns": ["Inventory_ID", "Product_ID", "Warehouse_ID"],
                    "unique_columns": ["Inventory_ID"]
                }
            },
            "orders": {
                "source_table": "Orders", 
                "bronze_table": "bz_orders", 
                "primary_key": "Order_ID",
                "partition_columns": ["Order_Date"],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Order_ID"],
                "data_retention_days": 2555,
                "quality_rules": {
                    "not_null_columns": ["Order_ID", "Customer_ID", "Order_Date"],
                    "unique_columns": ["Order_ID"]
                }
            },
            "order_details": {
                "source_table": "Order_Details", 
                "bronze_table": "bz_order_details", 
                "primary_key": "Order_Detail_ID",
                "partition_columns": [],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Order_ID", "Product_ID"],
                "data_retention_days": 2555,
                "quality_rules": {
                    "not_null_columns": ["Order_Detail_ID", "Order_ID", "Product_ID", "Quantity_Ordered"],
                    "unique_columns": ["Order_Detail_ID"]
                }
            },
            "shipments": {
                "source_table": "Shipments", 
                "bronze_table": "bz_shipments", 
                "primary_key": "Shipment_ID",
                "partition_columns": ["Shipment_Date"],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Order_ID"],
                "data_retention_days": 2555,
                "quality_rules": {
                    "not_null_columns": ["Shipment_ID", "Order_ID", "Shipment_Date"],
                    "unique_columns": ["Shipment_ID"]
                }
            },
            "returns": {
                "source_table": "Returns", 
                "bronze_table": "bz_returns", 
                "primary_key": "Return_ID",
                "partition_columns": [],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Order_ID"],
                "data_retention_days": 2555,
                "quality_rules": {
                    "not_null_columns": ["Return_ID", "Order_ID", "Return_Reason"],
                    "unique_columns": ["Return_ID"]
                }
            },
            "stock_levels": {
                "source_table": "Stock_Levels", 
                "bronze_table": "bz_stock_levels", 
                "primary_key": "Stock_Level_ID",
                "partition_columns": ["Warehouse_ID"],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Warehouse_ID", "Product_ID"],
                "data_retention_days": 1095,
                "quality_rules": {
                    "not_null_columns": ["Stock_Level_ID", "Warehouse_ID", "Product_ID", "Reorder_Threshold"],
                    "unique_columns": ["Stock_Level_ID"]
                }
            },
            "customers": {
                "source_table": "Customers", 
                "bronze_table": "bz_customers", 
                "primary_key": "Customer_ID",
                "partition_columns": [],
                "cdc_column": "updated_timestamp",
                "business_keys": ["Email"],
                "data_retention_days": 2555,
                "quality_rules": {
                    "not_null_columns": ["Customer_ID", "Customer_Name", "Email"],
                    "unique_columns": ["Customer_ID", "Email"]
                }
            }
        }

# ============================================================================
# ENHANCED BRONZE LAYER INGESTION ENGINE
# ============================================================================

class EnhancedBronzeIngestionEngine:
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config
        self.logger = self._setup_logging()
        self.metrics = {}
        
    def _setup_logging(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        return logger
        
    def get_credentials(self):
        for attempt in range(self.config.max_retries):
            try:
                connection_string = mssparkutils.credentials.getSecret(self.config.key_vault_url, "KConnectionString")
                username = mssparkutils.credentials.getSecret(self.config.key_vault_url, "KUser")
                password = mssparkutils.credentials.getSecret(self.config.key_vault_url, "KPassword")
                return {"url": connection_string, "user": username, "password": password}
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay_seconds)
                else:
                    raise
    
    def create_bronze_schema(self):
        try:
            schema_sql = f"CREATE SCHEMA IF NOT EXISTS {self.config.bronze_catalog}.{self.config.bronze_schema}"
            self.spark.sql(schema_sql)
            self.logger.info(f"Enhanced Bronze schema {self.config.bronze_schema} created")
        except Exception as e:
            self.logger.error(f"Failed to create bronze schema: {str(e)}")
            raise
    
    def create_audit_table(self):
        audit_ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.bronze_catalog}.{self.config.bronze_schema}.bz_audit_log (
            record_id STRING, source_table STRING, target_table STRING, load_timestamp TIMESTAMP,
            processed_by STRING, processing_time_seconds DOUBLE, records_processed BIGINT,
            records_failed BIGINT, records_skipped BIGINT, data_quality_score DOUBLE, 
            status STRING, error_message STRING, batch_id STRING, source_system STRING,
            pipeline_version STRING, data_size_mb DOUBLE, partition_info STRING,
            cdc_watermark TIMESTAMP, retry_count INT, execution_id STRING
        ) USING DELTA LOCATION '/mnt/bronze/audit/bz_audit_log'
        """
        self.spark.sql(audit_ddl)
        self.logger.info("Enhanced audit table created")
    
    def create_bronze_table(self, table_name, table_config):
        bronze_table_name = f"{self.config.bronze_catalog}.{self.config.bronze_schema}.{table_config['bronze_table']}"
        
        metadata_cols = """
            load_timestamp TIMESTAMP, update_timestamp TIMESTAMP, source_system STRING,
            record_status STRING, data_quality_score DOUBLE, batch_id STRING,
            cdc_operation STRING, source_file_name STRING, record_hash STRING,
            is_current BOOLEAN, effective_start_date TIMESTAMP, effective_end_date TIMESTAMP,
            row_number BIGINT, partition_date DATE
        """
        
        table_ddls = {
            "products": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Product_ID INT, Product_Name STRING, Category STRING, Brand STRING, Unit_Price DECIMAL(10,2), Description STRING, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_products'",
            "suppliers": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Supplier_ID INT, Supplier_Name STRING, Contact_Number STRING, Product_ID INT, Email STRING, Address STRING, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_suppliers'",
            "warehouses": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Warehouse_ID INT, Location STRING, Capacity INT, Manager_Name STRING, Phone STRING, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_warehouses'",
            "inventory": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Inventory_ID INT, Product_ID INT, Quantity_Available INT, Warehouse_ID INT, Last_Updated TIMESTAMP, Reserved_Quantity INT, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_inventory'",
            "orders": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Order_ID INT, Customer_ID INT, Order_Date DATE, Order_Status STRING, Total_Amount DECIMAL(12,2), {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_orders'",
            "order_details": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Order_Detail_ID INT, Order_ID INT, Product_ID INT, Quantity_Ordered INT, Unit_Price DECIMAL(10,2), Line_Total DECIMAL(12,2), {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_order_details'",
            "shipments": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Shipment_ID INT, Order_ID INT, Shipment_Date DATE, Carrier STRING, Tracking_Number STRING, Shipment_Status STRING, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_shipments'",
            "returns": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Return_ID INT, Order_ID INT, Return_Reason STRING, Return_Date DATE, Return_Status STRING, Refund_Amount DECIMAL(12,2), {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_returns'",
            "stock_levels": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Stock_Level_ID INT, Warehouse_ID INT, Product_ID INT, Reorder_Threshold INT, Max_Stock_Level INT, Safety_Stock INT, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_stock_levels'",
            "customers": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Customer_ID INT, Customer_Name STRING, Email STRING, Phone STRING, Address STRING, Registration_Date DATE, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_customers'"
        }
        
        self.spark.sql(table_ddls[table_name])
        
        properties_sql = f"""
        ALTER TABLE {bronze_table_name} SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.enableChangeDataFeed' = 'true',
            'delta.deletedFileRetentionDuration' = 'interval {table_config['data_retention_days']} days'
        )
        """
        self.spark.sql(properties_sql)
        self.logger.info(f"Enhanced Bronze table {bronze_table_name} created")
    
    def read_source_data(self, table_config):
        credentials = self.get_credentials()
        jdbc_properties = {
            "user": credentials["user"],
            "password": credentials["password"],
            "driver": "org.postgresql.Driver",
            "fetchsize": str(self.config.batch_size)
        }
        
        source_table = f"{self.config.source_schema}.{table_config['source_table']}"
        
        try:
            df = self.spark.read.format("jdbc") \
                .option("url", credentials["url"]) \
                .option("dbtable", source_table) \
                .options(**jdbc_properties) \
                .load()
            
            record_count = df.count()
            self.logger.info(f"Read {record_count} records from {source_table}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read from {source_table}: {str(e)}")
            raise
    
    def add_metadata_columns(self, df, table_name, batch_id):
        current_ts = current_timestamp()
        
        hash_cols = [col for col in df.columns]
        hash_expr = sha2(concat_ws("|", *hash_cols), 256)
        
        df_with_metadata = df \
            .withColumn("load_timestamp", current_ts) \
            .withColumn("update_timestamp", current_ts) \
            .withColumn("source_system", lit(self.config.source_system)) \
            .withColumn("record_status", lit("ACTIVE")) \
            .withColumn("data_quality_score", lit(100.0)) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("cdc_operation", lit("INSERT")) \
            .withColumn("source_file_name", lit(f"{table_name}_extract")) \
            .withColumn("record_hash", hash_expr) \
            .withColumn("is_current", lit(True)) \
            .withColumn("effective_start_date", current_ts) \
            .withColumn("effective_end_date", lit(None).cast("timestamp")) \
            .withColumn("row_number", row_number().over(Window.orderBy(monotonically_increasing_id()))) \
            .withColumn("partition_date", current_date())
        
        return df_with_metadata
    
    def validate_data_quality(self, df, table_config):
        total_records = df.count()
        quality_score = 100.0
        quality_issues = []
        
        quality_rules = table_config.get('quality_rules', {})
        
        # Check not null constraints
        for col_name in quality_rules.get('not_null_columns', []):
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    quality_score -= min(20, (null_count / total_records) * 100)
                    quality_issues.append(f"Null values in {col_name}: {null_count}")
        
        # Check unique constraints
        for col_name in quality_rules.get('unique_columns', []):
            if col_name in df.columns:
                distinct_count = df.select(col_name).distinct().count()
                if distinct_count != total_records:
                    duplicates = total_records - distinct_count
                    quality_score -= min(15, (duplicates / total_records) * 100)
                    quality_issues.append(f"Duplicate values in {col_name}: {duplicates}")
        
        df_with_quality = df.withColumn("data_quality_score", lit(max(0, quality_score)))
        self.logger.info(f"Data quality score: {quality_score}")
        return df_with_quality, quality_score, quality_issues
    
    def write_to_bronze(self, df, table_config, batch_id):
        bronze_table_name = f"{self.config.bronze_catalog}.{self.config.bronze_schema}.{table_config['bronze_table']}"
        primary_key = table_config['primary_key']
        
        try:
            delta_table = DeltaTable.forName(self.spark, bronze_table_name)
            merge_condition = f"target.{primary_key} = source.{primary_key}"
            
            update_expr = {col: f"source.{col}" for col in df.columns if col != primary_key}
            update_expr["update_timestamp"] = "current_timestamp()"
            insert_expr = {col: f"source.{col}" for col in df.columns}
            
            delta_table.alias("target").merge(df.alias("source"), merge_condition) \
                .whenMatchedUpdate(set=update_expr) \
                .whenNotMatchedInsert(values=insert_expr) \
                .execute()
            
            records_written = df.count()
            self.logger.info(f"Wrote {records_written} records to {bronze_table_name}")
            return records_written
        except Exception as e:
            # Fallback to append mode if merge fails
            self.logger.warning(f"Merge failed, using append mode: {str(e)}")
            df.write.format("delta").mode("append").saveAsTable(bronze_table_name)
            return df.count()
    
    def log_audit_record(self, table_name, table_config, batch_id, records_processed, records_failed, quality_score, status, error_message=None, processing_time=0):
        audit_record = self.spark.createDataFrame([
            (f"{batch_id}_{table_name}", table_config['source_table'], table_config['bronze_table'], 
             datetime.now(), "EnhancedBronzeIngestionEngine", processing_time, records_processed, 
             records_failed, 0, quality_score, status, error_message, batch_id, 
             self.config.source_system, "2.0", 0.0, "", None, 0, batch_id)
        ], ["record_id", "source_table", "target_table", "load_timestamp", "processed_by", 
            "processing_time_seconds", "records_processed", "records_failed", "records_skipped", 
            "data_quality_score", "status", "error_message", "batch_id", "source_system", 
            "pipeline_version", "data_size_mb", "partition_info", "cdc_watermark", "retry_count", "execution_id"])
        
        audit_record.write.format("delta").mode("append").saveAsTable(
            f"{self.config.bronze_catalog}.{self.config.bronze_schema}.bz_audit_log")
    
    def process_table(self, table_name):
        start_time = datetime.now()
        table_config = self.config.tables_config[table_name]
        batch_id = f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        records_processed = 0
        records_failed = 0
        quality_score = 0
        
        try:
            self.logger.info(f"Processing table: {table_name}")
            self.create_bronze_table(table_name, table_config)
            
            source_df = self.read_source_data(table_config)
            records_processed = source_df.count()
            
            df_with_metadata = self.add_metadata_columns(source_df, table_name, batch_id)
            df_validated, quality_score, quality_issues = self.validate_data_quality(df_with_metadata, table_config)
            
            if quality_score >= self.config.quality_threshold:
                records_written = self.write_to_bronze(df_validated, table_config, batch_id)
                status = "SUCCESS"
                self.logger.info(f"Successfully processed {table_name}: {records_written} records")
            else:
                status = "QUALITY_FAILURE"
                records_failed = records_processed
                records_processed = 0
                self.logger.warning(f"Quality failure for {table_name}: {quality_issues}")
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self.log_audit_record(table_name, table_config, batch_id, records_processed, 
                                records_failed, quality_score, status, None, int(processing_time))
            
            return {
                "table_name": table_name, "status": status, "records_processed": records_processed,
                "records_failed": records_failed, "quality_score": quality_score,
                "processing_time": processing_time, "batch_id": batch_id
            }
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_message = str(e)
            status = "ERROR"
            records_failed = records_processed
            records_processed = 0
            
            self.log_audit_record(table_name, table_config, batch_id, records_processed, 
                                records_failed, quality_score, status, error_message, int(processing_time))
            self.logger.error(f"Failed to process table {table_name}: {error_message}")
            
            return {
                "table_name": table_name, "status": status, "records_processed": records_processed,
                "records_failed": records_failed, "quality_score": quality_score,
                "processing_time": processing_time, "error_message": error_message, "batch_id": batch_id
            }
    
    def process_all_tables(self):
        self.logger.info("Starting Enhanced Bronze layer ingestion")
        self.create_bronze_schema()
        self.create_audit_table()
        
        results = []
        for table_name in self.config.tables_config.keys():
            result = self.process_table(table_name)
            results.append(result)
        
        total_tables = len(results)
        successful_tables = len([r for r in results if r['status'] == 'SUCCESS'])
        failed_tables = total_tables - successful_tables
        total_records = sum([r['records_processed'] for r in results])
        total_processing_time = sum([r['processing_time'] for r in results])
        
        summary = {
            "total_tables": total_tables, "successful_tables": successful_tables,
            "failed_tables": failed_tables, "total_records_processed": total_records,
            "total_processing_time_seconds": total_processing_time, "results": results
        }
        
        self.logger.info(f"Enhanced Bronze layer ingestion completed: {summary}")
        return summary

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    spark = SparkSession.builder \
        .appName("EnhancedBronzeLayerIngestion") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        config = EnhancedBronzeConfig()
        ingestion_engine = EnhancedBronzeIngestionEngine(spark, config)
        results = ingestion_engine.process_all_tables()
        
        print("\n" + "="*80)
        print("ENHANCED BRONZE LAYER INGESTION RESULTS")
        print("="*80)
        print(f"Total Tables: {results['total_tables']}")
        print(f"Successful: {results['successful_tables']}")
        print(f"Failed: {results['failed_tables']}")
        print(f"Total Records: {results['total_records_processed']}")
        print(f"Processing Time: {results['total_processing_time_seconds']:.2f}s")
        
        for result in results['results']:
            print(f"\nTable: {result['table_name']} - Status: {result['status']} - Records: {result['records_processed']} - Quality: {result['quality_score']:.1f}% - Time: {result['processing_time']:.2f}s")
            if 'error_message' in result:
                print(f"  Error: {result['error_message']}")
        
        return results
        
    except Exception as e:
        print(f"Enhanced Bronze layer ingestion failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()