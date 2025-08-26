# Databricks Bronze Layer Data Engineering Pipeline
# Inventory Management System - Source to Bronze Ingestion Strategy
# Author: Data Engineer - Version: 1.0

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import logging
from datetime import datetime

# ============================================================================
# BRONZE LAYER CONFIGURATION
# ============================================================================

class BronzeConfig:
    def __init__(self):
        self.source_system = "PostgreSQL"
        self.source_database = "DE"
        self.source_schema = "tests"
        self.bronze_catalog = "workspace"
        self.bronze_schema = "inventory_bronze"
        self.key_vault_url = "https://akv-poc-fabric.vault.azure.net/"
        self.quality_threshold = 80
        
        self.tables_config = {
            "products": {"source_table": "Products", "bronze_table": "bz_products", "primary_key": "Product_ID"},
            "suppliers": {"source_table": "Suppliers", "bronze_table": "bz_suppliers", "primary_key": "Supplier_ID"},
            "warehouses": {"source_table": "Warehouses", "bronze_table": "bz_warehouses", "primary_key": "Warehouse_ID"},
            "inventory": {"source_table": "Inventory", "bronze_table": "bz_inventory", "primary_key": "Inventory_ID"},
            "orders": {"source_table": "Orders", "bronze_table": "bz_orders", "primary_key": "Order_ID"},
            "order_details": {"source_table": "Order_Details", "bronze_table": "bz_order_details", "primary_key": "Order_Detail_ID"},
            "shipments": {"source_table": "Shipments", "bronze_table": "bz_shipments", "primary_key": "Shipment_ID"},
            "returns": {"source_table": "Returns", "bronze_table": "bz_returns", "primary_key": "Return_ID"},
            "stock_levels": {"source_table": "Stock_Levels", "bronze_table": "bz_stock_levels", "primary_key": "Stock_Level_ID"},
            "customers": {"source_table": "Customers", "bronze_table": "bz_customers", "primary_key": "Customer_ID"}
        }

# ============================================================================
# BRONZE LAYER INGESTION ENGINE
# ============================================================================

class BronzeIngestionEngine:
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def get_credentials(self):
        """Retrieve database credentials from Azure Key Vault"""
        try:
            connection_string = mssparkutils.credentials.getSecret(self.config.key_vault_url, "KConnectionString")
            username = mssparkutils.credentials.getSecret(self.config.key_vault_url, "KUser")
            password = mssparkutils.credentials.getSecret(self.config.key_vault_url, "KPassword")
            return {"url": connection_string, "user": username, "password": password}
        except Exception as e:
            self.logger.error(f"Failed to retrieve credentials: {str(e)}")
            raise
    
    def create_bronze_schema(self):
        """Create Bronze layer schema"""
        schema_sql = f"CREATE SCHEMA IF NOT EXISTS {self.config.bronze_catalog}.{self.config.bronze_schema}"
        self.spark.sql(schema_sql)
        self.logger.info(f"Bronze schema {self.config.bronze_schema} created")
    
    def create_audit_table(self):
        """Create audit table for tracking"""
        audit_ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.config.bronze_catalog}.{self.config.bronze_schema}.bz_audit_log (
            record_id STRING, source_table STRING, target_table STRING, load_timestamp TIMESTAMP,
            processed_by STRING, processing_time_seconds INT, records_processed BIGINT,
            records_failed BIGINT, data_quality_score INT, status STRING, error_message STRING, batch_id STRING
        ) USING DELTA LOCATION '/mnt/bronze/audit/bz_audit_log'
        """
        self.spark.sql(audit_ddl)
        self.logger.info("Audit table created")
    
    def create_bronze_table(self, table_name, table_config):
        """Create Bronze layer table"""
        bronze_table_name = f"{self.config.bronze_catalog}.{self.config.bronze_schema}.{table_config['bronze_table']}"
        metadata_cols = "load_timestamp TIMESTAMP, update_timestamp TIMESTAMP, source_system STRING, record_status STRING, data_quality_score INT, batch_id STRING"
        
        table_ddls = {
            "products": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Product_ID INT, Product_Name STRING, Category STRING, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_products'",
            "suppliers": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Supplier_ID INT, Supplier_Name STRING, Contact_Number STRING, Product_ID INT, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_suppliers'",
            "warehouses": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Warehouse_ID INT, Location STRING, Capacity INT, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_warehouses'",
            "inventory": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Inventory_ID INT, Product_ID INT, Quantity_Available INT, Warehouse_ID INT, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_inventory'",
            "orders": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Order_ID INT, Customer_ID INT, Order_Date DATE, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_orders' PARTITIONED BY (Order_Date)",
            "order_details": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Order_Detail_ID INT, Order_ID INT, Product_ID INT, Quantity_Ordered INT, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_order_details'",
            "shipments": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Shipment_ID INT, Order_ID INT, Shipment_Date DATE, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_shipments' PARTITIONED BY (Shipment_Date)",
            "returns": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Return_ID INT, Order_ID INT, Return_Reason STRING, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_returns'",
            "stock_levels": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Stock_Level_ID INT, Warehouse_ID INT, Product_ID INT, Reorder_Threshold INT, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_stock_levels'",
            "customers": f"CREATE TABLE IF NOT EXISTS {bronze_table_name} (Customer_ID INT, Customer_Name STRING, Email STRING, {metadata_cols}) USING DELTA LOCATION '/mnt/bronze/bz_customers'"
        }
        
        self.spark.sql(table_ddls[table_name])
        
        # Apply optimization properties
        properties_sql = f"ALTER TABLE {bronze_table_name} SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'delta.enableChangeDataFeed' = 'true')"
        self.spark.sql(properties_sql)
        self.logger.info(f"Bronze table {bronze_table_name} created")
    
    def read_source_data(self, table_config):
        """Read data from source PostgreSQL"""
        credentials = self.get_credentials()
        jdbc_properties = {"user": credentials["user"], "password": credentials["password"], "driver": "org.postgresql.Driver", "fetchsize": "10000"}
        source_table = f"{self.config.source_schema}.{table_config['source_table']}"
        
        df = self.spark.read.format("jdbc").option("url", credentials["url"]).option("dbtable", source_table).options(**jdbc_properties).load()
        self.logger.info(f"Read {df.count()} records from {source_table}")
        return df
    
    def add_metadata_columns(self, df, table_name):
        """Add metadata columns"""
        current_ts = current_timestamp()
        batch_id = f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        df_with_metadata = df.withColumn("load_timestamp", current_ts).withColumn("update_timestamp", current_ts).withColumn("source_system", lit(self.config.source_system)).withColumn("record_status", lit("ACTIVE")).withColumn("data_quality_score", lit(100)).withColumn("batch_id", lit(batch_id))
        return df_with_metadata, batch_id
    
    def validate_data_quality(self, df, table_config):
        """Validate data quality"""
        total_records = df.count()
        quality_score = 100
        quality_issues = []
        
        primary_key = table_config['primary_key']
        null_pk_count = df.filter(col(primary_key).isNull()).count()
        
        if null_pk_count > 0:
            quality_score -= 30
            quality_issues.append(f"Null primary key values: {null_pk_count}")
        
        distinct_pk_count = df.select(primary_key).distinct().count()
        if distinct_pk_count != total_records:
            quality_score -= 25
            quality_issues.append(f"Duplicate primary keys: {total_records - distinct_pk_count}")
        
        df_with_quality = df.withColumn("data_quality_score", lit(max(0, quality_score)))
        self.logger.info(f"Data quality score: {quality_score}")
        return df_with_quality, quality_score, quality_issues
    
    def write_to_bronze(self, df, table_config, batch_id):
        """Write data to Bronze layer using MERGE"""
        bronze_table_name = f"{self.config.bronze_catalog}.{self.config.bronze_schema}.{table_config['bronze_table']}"
        primary_key = table_config['primary_key']
        
        delta_table = DeltaTable.forName(self.spark, bronze_table_name)
        merge_condition = f"target.{primary_key} = source.{primary_key}"
        
        update_expr = {col: f"source.{col}" for col in df.columns if col != primary_key}
        update_expr["update_timestamp"] = "current_timestamp()"
        insert_expr = {col: f"source.{col}" for col in df.columns}
        
        delta_table.alias("target").merge(df.alias("source"), merge_condition).whenMatchedUpdate(set=update_expr).whenNotMatchedInsert(values=insert_expr).execute()
        
        records_written = df.count()
        self.logger.info(f"Wrote {records_written} records to {bronze_table_name}")
        return records_written
    
    def log_audit_record(self, table_name, table_config, batch_id, records_processed, records_failed, quality_score, status, error_message=None, processing_time=0):
        """Log audit record"""
        audit_record = self.spark.createDataFrame([(f"{batch_id}_{table_name}", table_config['source_table'], table_config['bronze_table'], datetime.now(), "BronzeIngestionEngine", processing_time, records_processed, records_failed, quality_score, status, error_message, batch_id)], ["record_id", "source_table", "target_table", "load_timestamp", "processed_by", "processing_time_seconds", "records_processed", "records_failed", "data_quality_score", "status", "error_message", "batch_id"])
        audit_record.write.format("delta").mode("append").saveAsTable(f"{self.config.bronze_catalog}.{self.config.bronze_schema}.bz_audit_log")
    
    def process_table(self, table_name):
        """Process single table from source to bronze"""
        start_time = datetime.now()
        table_config = self.config.tables_config[table_name]
        batch_id = None
        records_processed = 0
        records_failed = 0
        quality_score = 0
        
        try:
            self.logger.info(f"Processing table: {table_name}")
            self.create_bronze_table(table_name, table_config)
            
            source_df = self.read_source_data(table_config)
            records_processed = source_df.count()
            
            df_with_metadata, batch_id = self.add_metadata_columns(source_df, table_name)
            df_validated, quality_score, quality_issues = self.validate_data_quality(df_with_metadata, table_config)
            
            if quality_score >= self.config.quality_threshold:
                records_written = self.write_to_bronze(df_validated, table_config, batch_id)
                status = "SUCCESS"
                self.logger.info(f"Successfully processed {table_name}: {records_written} records")
            else:
                status = "QUALITY_FAILURE"
                records_failed = records_processed
                records_processed = 0
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self.log_audit_record(table_name, table_config, batch_id, records_processed, records_failed, quality_score, status, None, int(processing_time))
            
            return {"table_name": table_name, "status": status, "records_processed": records_processed, "records_failed": records_failed, "quality_score": quality_score, "processing_time": processing_time, "batch_id": batch_id}
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_message = str(e)
            status = "ERROR"
            records_failed = records_processed
            records_processed = 0
            
            self.log_audit_record(table_name, table_config, batch_id or f"error_{table_name}", records_processed, records_failed, quality_score, status, error_message, int(processing_time))
            self.logger.error(f"Failed to process table {table_name}: {error_message}")
            
            return {"table_name": table_name, "status": status, "records_processed": records_processed, "records_failed": records_failed, "quality_score": quality_score, "processing_time": processing_time, "error_message": error_message, "batch_id": batch_id}
    
    def process_all_tables(self):
        """Process all tables"""
        self.logger.info("Starting Bronze layer ingestion")
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
        
        summary = {"total_tables": total_tables, "successful_tables": successful_tables, "failed_tables": failed_tables, "total_records_processed": total_records, "total_processing_time_seconds": total_processing_time, "results": results}
        
        self.logger.info(f"Bronze layer ingestion completed: {summary}")
        return summary

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    spark = SparkSession.builder.appName("BronzeLayerIngestion").config("spark.sql.adaptive.enabled", "true").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
    
    try:
        config = BronzeConfig()
        ingestion_engine = BronzeIngestionEngine(spark, config)
        results = ingestion_engine.process_all_tables()
        
        print("\n" + "="*80)
        print("BRONZE LAYER INGESTION RESULTS")
        print("="*80)
        print(f"Total Tables: {results['total_tables']}")
        print(f"Successful: {results['successful_tables']}")
        print(f"Failed: {results['failed_tables']}")
        print(f"Total Records: {results['total_records_processed']}")
        print(f"Processing Time: {results['total_processing_time_seconds']:.2f}s")
        
        for result in results['results']:
            print(f"\nTable: {result['table_name']} - Status: {result['status']} - Records: {result['records_processed']} - Quality: {result['quality_score']} - Time: {result['processing_time']:.2f}s")
            if 'error_message' in result:
                print(f"  Error: {result['error_message']}")
        
        return results
        
    except Exception as e:
        print(f"Bronze layer ingestion failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()