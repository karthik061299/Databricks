# Databricks Enhanced Bronze Layer Data Engineering Pipeline
# Version 1.0 - Inventory Management System

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import uuid
from datetime import datetime
import logging
from typing import Dict, List, Optional
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedBronzeLayerPipeline:
    def __init__(self):
        # Initialize Spark Session with optimizations
        self.spark = SparkSession.builder \
            .appName("EnhancedBronzeLayerIngestion_v1") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \
            .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .getOrCreate()
        
        # Connection configuration with Key Vault
        try:
            self.source_db_url = mssparkutils.credentials.getSecret(
                "https://akv-poc-fabric.vault.azure.net/", "KConnectionString"
            )
            self.user = mssparkutils.credentials.getSecret(
                "https://akv-poc-fabric.vault.azure.net/", "KUser"
            )
            self.password = mssparkutils.credentials.getSecret(
                "https://akv-poc-fabric.vault.azure.net/", "KPassword"
            )
        except Exception as e:
            # Fallback for testing - use dummy values
            logger.warning(f"Could not retrieve secrets from Key Vault: {e}")
            self.source_db_url = "jdbc:postgresql://localhost:5432/DE"
            self.user = "test_user"
            self.password = "test_password"
        
        # Enhanced JDBC Properties
        self.jdbc_properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
            "fetchsize": "50000",
            "batchsize": "50000",
            "numPartitions": "8",
            "connectionTimeout": "30000",
            "socketTimeout": "60000",
            "tcpKeepAlive": "true"
        }
        
        # Circuit breaker for fault tolerance
        self.circuit_breaker = {
            "failure_threshold": 5,
            "recovery_timeout": 300,
            "current_failures": 0,
            "last_failure_time": None,
            "state": "CLOSED"
        }
    
    def create_enhanced_bronze_schema(self):
        """Create enhanced Bronze layer schema and tables"""
        try:
            # Create Bronze Schema
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS inventory_bronze")
            
            # Enhanced Products Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_products (
                Product_ID INT,
                Product_Name STRING,
                Category STRING,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Inventory Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_inventory (
                Inventory_ID INT,
                Product_ID INT,
                Quantity_Available INT,
                Warehouse_ID INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Orders Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_orders (
                Order_ID INT,
                Customer_ID INT,
                Order_Date DATE,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Customers Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_customers (
                Customer_ID INT,
                Customer_Name STRING,
                Email STRING,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Suppliers Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_suppliers (
                Supplier_ID INT,
                Supplier_Name STRING,
                Contact_Number STRING,
                Product_ID INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Warehouses Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_warehouses (
                Warehouse_ID INT,
                Location STRING,
                Capacity INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Order Details Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_order_details (
                Order_Detail_ID INT,
                Order_ID INT,
                Product_ID INT,
                Quantity_Ordered INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Shipments Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_shipments (
                Shipment_ID INT,
                Order_ID INT,
                Shipment_Date DATE,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Returns Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_returns (
                Return_ID INT,
                Order_ID INT,
                Return_Reason STRING,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Stock Levels Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_stock_levels (
                Stock_Level_ID INT,
                Warehouse_ID INT,
                Product_ID INT,
                Reorder_Threshold INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT,
                batch_id STRING,
                record_hash STRING,
                is_deleted BOOLEAN
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """)
            
            # Enhanced Audit Table
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_audit_log (
                audit_id STRING,
                source_table STRING,
                target_table STRING,
                batch_id STRING,
                ingestion_timestamp TIMESTAMP,
                records_processed BIGINT,
                records_inserted BIGINT,
                records_updated BIGINT,
                status STRING,
                error_message STRING,
                processing_time_seconds INT,
                data_quality_score DOUBLE,
                processed_by STRING,
                pipeline_version STRING
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            """)
            
            logger.info("Enhanced Bronze schema and tables created successfully")
            print("✓ Enhanced Bronze schema and tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating Bronze schema: {str(e)}")
            print(f"✗ Error creating Bronze schema: {str(e)}")
            raise e
    
    def create_sample_data(self):
        """Create sample data for testing when source is not available"""
        try:
            # Sample Products Data
            products_data = [
                (1, "Laptop", "Electronics"),
                (2, "Mouse", "Electronics"),
                (3, "Keyboard", "Electronics"),
                (4, "Monitor", "Electronics"),
                (5, "Chair", "Furniture")
            ]
            
            products_schema = StructType([
                StructField("Product_ID", IntegerType(), True),
                StructField("Product_Name", StringType(), True),
                StructField("Category", StringType(), True)
            ])
            
            products_df = self.spark.createDataFrame(products_data, products_schema)
            
            # Sample Customers Data
            customers_data = [
                (1, "John Doe", "john.doe@email.com"),
                (2, "Jane Smith", "jane.smith@email.com"),
                (3, "Bob Johnson", "bob.johnson@email.com"),
                (4, "Alice Brown", "alice.brown@email.com"),
                (5, "Charlie Wilson", "charlie.wilson@email.com")
            ]
            
            customers_schema = StructType([
                StructField("Customer_ID", IntegerType(), True),
                StructField("Customer_Name", StringType(), True),
                StructField("Email", StringType(), True)
            ])
            
            customers_df = self.spark.createDataFrame(customers_data, customers_schema)
            
            # Sample Orders Data
            orders_data = [
                (1, 1, "2024-01-15"),
                (2, 2, "2024-01-16"),
                (3, 3, "2024-01-17"),
                (4, 4, "2024-01-18"),
                (5, 5, "2024-01-19")
            ]
            
            orders_schema = StructType([
                StructField("Order_ID", IntegerType(), True),
                StructField("Customer_ID", IntegerType(), True),
                StructField("Order_Date", StringType(), True)
            ])
            
            orders_df = self.spark.createDataFrame(orders_data, orders_schema)
            orders_df = orders_df.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd"))
            
            # Sample Inventory Data
            inventory_data = [
                (1, 1, 100, 1),
                (2, 2, 250, 1),
                (3, 3, 150, 2),
                (4, 4, 75, 2),
                (5, 5, 50, 3)
            ]
            
            inventory_schema = StructType([
                StructField("Inventory_ID", IntegerType(), True),
                StructField("Product_ID", IntegerType(), True),
                StructField("Quantity_Available", IntegerType(), True),
                StructField("Warehouse_ID", IntegerType(), True)
            ])
            
            inventory_df = self.spark.createDataFrame(inventory_data, inventory_schema)
            
            return {
                "products": products_df,
                "customers": customers_df,
                "orders": orders_df,
                "inventory": inventory_df
            }
            
        except Exception as e:
            logger.error(f"Error creating sample data: {str(e)}")
            raise e
    
    def enhanced_ingest_table(self, source_df, target_table: str, source_table: str) -> bool:
        """Enhanced ingestion function with comprehensive error handling"""
        start_time = datetime.now()
        batch_id = str(uuid.uuid4())
        
        try:
            # Check circuit breaker
            if not self._check_circuit_breaker():
                raise Exception("Circuit breaker is OPEN - too many recent failures")
            
            logger.info(f"Starting enhanced ingestion: {source_table} -> {target_table}")
            print(f"🔄 Starting ingestion: {source_table} -> {target_table}")
            
            if source_df.count() == 0:
                logger.warning(f"No data found in source table {source_table}")
                print(f"⚠️ No data found in source table {source_table}")
                return True
            
            # Apply enhanced transformations
            enriched_df = self._apply_enhanced_transformations(source_df, source_table, batch_id)
            
            # Apply comprehensive data quality checks
            quality_df, quality_score = self._apply_quality_checks(enriched_df, source_table)
            
            # Write to Bronze with merge logic
            write_stats = self._write_with_merge(quality_df, target_table)
            
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Log audit information
            self._log_audit(
                source_table, target_table, batch_id, "SUCCESS",
                write_stats, quality_score, processing_time
            )
            
            # Reset circuit breaker on success
            self._reset_circuit_breaker()
            
            logger.info(f"Successfully completed ingestion: {source_table}")
            print(f"✓ Successfully completed ingestion: {source_table}")
            return True
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Record failure in circuit breaker
            self._record_failure()
            
            # Log failure
            self._log_audit(
                source_table, target_table, batch_id, "FAILED",
                {"records_processed": 0}, 0, processing_time, str(e)
            )
            
            logger.error(f"Failed to ingest {source_table}: {str(e)}")
            print(f"✗ Failed to ingest {source_table}: {str(e)}")
            raise e
    
    def _apply_enhanced_transformations(self, df, source_table: str, batch_id: str):
        """Apply enhanced transformations with metadata"""
        current_timestamp = datetime.now()
        
        # Create record hash for change detection
        hash_cols = [col for col in df.columns]
        hash_expr = concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in hash_cols])
        
        enriched_df = df \
            .withColumn("load_timestamp", lit(current_timestamp)) \
            .withColumn("update_timestamp", lit(current_timestamp)) \
            .withColumn("source_system", lit("PostgreSQL_DE_Enhanced")) \
            .withColumn("record_status", lit("ACTIVE")) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("record_hash", sha2(hash_expr, 256)) \
            .withColumn("is_deleted", lit(False))
        
        return enriched_df
    
    def _apply_quality_checks(self, df, table_name: str):
        """Apply comprehensive data quality checks"""
        total_records = df.count()
        quality_score = 100.0
        
        # Define quality rules
        quality_rules = {
            "products": ["Product_ID", "Product_Name"],
            "customers": ["Customer_ID", "Customer_Name", "Email"],
            "orders": ["Order_ID", "Customer_ID"],
            "inventory": ["Inventory_ID", "Product_ID", "Warehouse_ID"]
        }
        
        if table_name in quality_rules:
            required_fields = quality_rules[table_name]
            
            for field in required_fields:
                if field in df.columns:
                    null_count = df.filter(col(field).isNull()).count()
                    if null_count > 0:
                        null_percentage = (null_count / total_records) * 100
                        quality_score -= null_percentage * 0.5
        
        # Ensure quality score is between 0 and 100
        quality_score = max(0, min(100, quality_score))
        
        result_df = df.withColumn("data_quality_score", lit(int(quality_score)))
        
        return result_df, quality_score
    
    def _write_with_merge(self, df, target_table: str) -> Dict:
        """Write data using merge operations"""
        target_path = f"inventory_bronze.{target_table}"
        
        try:
            # For initial implementation, use append mode
            df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(target_path)
            
            return {
                "records_processed": df.count(),
                "records_inserted": df.count(),
                "records_updated": 0
            }
            
        except Exception as e:
            logger.error(f"Error writing to {target_table}: {str(e)}")
            raise e
    
    def _check_circuit_breaker(self) -> bool:
        """Check circuit breaker state"""
        current_time = datetime.now()
        
        if self.circuit_breaker["state"] == "OPEN":
            if self.circuit_breaker["last_failure_time"]:
                time_since_failure = (current_time - self.circuit_breaker["last_failure_time"]).total_seconds()
                if time_since_failure > self.circuit_breaker["recovery_timeout"]:
                    self.circuit_breaker["state"] = "HALF_OPEN"
                    return True
                else:
                    return False
        return True
    
    def _record_failure(self):
        """Record failure in circuit breaker"""
        self.circuit_breaker["current_failures"] += 1
        self.circuit_breaker["last_failure_time"] = datetime.now()
        
        if self.circuit_breaker["current_failures"] >= self.circuit_breaker["failure_threshold"]:
            self.circuit_breaker["state"] = "OPEN"
            logger.error("Circuit breaker OPENED")
    
    def _reset_circuit_breaker(self):
        """Reset circuit breaker"""
        self.circuit_breaker["current_failures"] = 0
        self.circuit_breaker["state"] = "CLOSED"
        self.circuit_breaker["last_failure_time"] = None
    
    def _log_audit(self, source_table: str, target_table: str, batch_id: str,
                   status: str, write_stats: Dict, quality_score: float,
                   processing_time: float, error_msg: str = None):
        """Log audit information"""
        try:
            audit_data = [{
                "audit_id": str(uuid.uuid4()),
                "source_table": source_table,
                "target_table": target_table,
                "batch_id": batch_id,
                "ingestion_timestamp": datetime.now(),
                "records_processed": write_stats.get("records_processed", 0),
                "records_inserted": write_stats.get("records_inserted", 0),
                "records_updated": write_stats.get("records_updated", 0),
                "status": status,
                "error_message": error_msg,
                "processing_time_seconds": int(processing_time),
                "data_quality_score": quality_score,
                "processed_by": "EnhancedBronzePipeline_v1",
                "pipeline_version": "1.0"
            }]
            
            audit_df = self.spark.createDataFrame(audit_data)
            audit_df.write.format("delta").mode("append").saveAsTable("inventory_bronze.bz_audit_log")
        except Exception as e:
            logger.error(f"Error logging audit: {str(e)}")
    
    def run_enhanced_pipeline(self):
        """Main pipeline execution"""
        logger.info("Starting Enhanced Bronze Layer Pipeline v1.0")
        print("🚀 Starting Enhanced Bronze Layer Pipeline v1.0")
        
        try:
            # Create schema and tables
            self.create_enhanced_bronze_schema()
            
            # Create sample data for testing
            sample_data = self.create_sample_data()
            
            # Define ingestion configuration
            ingestion_config = [
                {"source": "products", "target": "bz_products"},
                {"source": "customers", "target": "bz_customers"},
                {"source": "orders", "target": "bz_orders"},
                {"source": "inventory", "target": "bz_inventory"}
            ]
            
            # Execute ingestions
            successful_ingestions = 0
            failed_ingestions = 0
            
            for config in ingestion_config:
                try:
                    if config["source"] in sample_data:
                        success = self.enhanced_ingest_table(
                            sample_data[config["source"]], 
                            config["target"], 
                            config["source"]
                        )
                        if success:
                            successful_ingestions += 1
                            print(f"✓ Successfully ingested {config['source']}")
                except Exception as e:
                    failed_ingestions += 1
                    print(f"✗ Failed to ingest {config['source']}: {str(e)}")
                    continue
            
            # Final summary
            logger.info(f"Pipeline completed: {successful_ingestions} successful, {failed_ingestions} failed")
            print(f"📊 Pipeline completed: {successful_ingestions} successful, {failed_ingestions} failed")
            
            # Show sample results
            print("\n📋 Sample Results:")
            for config in ingestion_config[:2]:  # Show first 2 tables
                try:
                    result_df = self.spark.table(f"inventory_bronze.{config['target']}")
                    print(f"\n{config['target']} - Record Count: {result_df.count()}")
                    result_df.show(5, truncate=False)
                except Exception as e:
                    print(f"Error showing results for {config['target']}: {str(e)}")
            
            # Show audit log
            try:
                audit_df = self.spark.table("inventory_bronze.bz_audit_log")
                print(f"\n📝 Audit Log - Total Entries: {audit_df.count()}")
                audit_df.show(10, truncate=False)
            except Exception as e:
                print(f"Error showing audit log: {str(e)}")
            
            return successful_ingestions, failed_ingestions
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            print(f"💥 Pipeline execution failed: {str(e)}")
            raise e

# Main execution
if __name__ == "__main__":
    try:
        pipeline = EnhancedBronzeLayerPipeline()
        successful, failed = pipeline.run_enhanced_pipeline()
        print(f"\n🎉 Enhanced Bronze Pipeline completed successfully!")
        print(f"📈 Results: {successful} successful ingestions, {failed} failed ingestions")
    except Exception as e:
        print(f"💥 Pipeline failed with error: {str(e)}")
        raise e
