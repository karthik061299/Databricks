# Databricks Bronze Layer Data Engineering Pipeline
# Inventory Management System - Comprehensive Ingestion Strategy
# Author: Data Engineering Team
# Version: 1.0
# Created: 2024

# ============================================================================
# IMPORTS AND CONFIGURATION
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import logging
from datetime import datetime, timedelta
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion_Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION AND CREDENTIALS
# ============================================================================

class BronzeConfig:
    """Configuration class for Bronze layer ingestion"""
    
    # Source Database Configuration
    SOURCE_DB_URL = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
    SOURCE_USER = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
    SOURCE_PASSWORD = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")
    SOURCE_DATABASE = "DE"
    SOURCE_SCHEMA = "tests"
    
    # Target Bronze Layer Configuration
    BRONZE_CATALOG = "workspace"
    BRONZE_SCHEMA = "inventory_bronze"
    BRONZE_LOCATION = "/mnt/bronze/"
    
    # Metadata Configuration
    SOURCE_SYSTEM = "PostgreSQL_DE_Tests"
    BATCH_SIZE = 10000
    MAX_RETRIES = 3
    RETRY_DELAY = 30  # seconds
    
    # Data Quality Thresholds
    MIN_QUALITY_SCORE = 70
    MAX_NULL_PERCENTAGE = 10
    
    # Table Definitions
    SOURCE_TABLES = [
        "Products", "Suppliers", "Warehouses", "Inventory", 
        "Orders", "Order_Details", "Shipments", "Returns", 
        "Stock_Levels", "Customers"
    ]

# ============================================================================
# BRONZE LAYER INGESTION ENGINE
# ============================================================================

class BronzeIngestionEngine:
    """Main ingestion engine for Bronze layer"""
    
    def __init__(self):
        self.config = BronzeConfig()
        self.current_timestamp = current_timestamp()
        
    def create_bronze_tables(self):
        """Create Bronze layer tables if they don't exist"""
        logger.info("Creating Bronze layer tables...")
        
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}")
        
        # Products Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_products (
                Product_ID INT,
                Product_Name STRING,
                Category STRING,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_products'
        """)
        
        # Suppliers Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_suppliers (
                Supplier_ID INT,
                Supplier_Name STRING,
                Contact_Number STRING,
                Product_ID INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_suppliers'
        """)
        
        # Warehouses Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_warehouses (
                Warehouse_ID INT,
                Location STRING,
                Capacity INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_warehouses'
        """)
        
        # Inventory Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_inventory (
                Inventory_ID INT,
                Product_ID INT,
                Quantity_Available INT,
                Warehouse_ID INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_inventory'
        """)
        
        # Orders Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_orders (
                Order_ID INT,
                Customer_ID INT,
                Order_Date DATE,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_orders'
        """)
        
        # Order Details Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_order_details (
                Order_Detail_ID INT,
                Order_ID INT,
                Product_ID INT,
                Quantity_Ordered INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_order_details'
        """)
        
        # Shipments Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_shipments (
                Shipment_ID INT,
                Order_ID INT,
                Shipment_Date DATE,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_shipments'
        """)
        
        # Returns Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_returns (
                Return_ID INT,
                Order_ID INT,
                Return_Reason STRING,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_returns'
        """)
        
        # Stock Levels Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_stock_levels (
                Stock_Level_ID INT,
                Warehouse_ID INT,
                Product_ID INT,
                Reorder_Threshold INT,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_stock_levels'
        """)
        
        # Customers Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_customers (
                Customer_ID INT,
                Customer_Name STRING,
                Email STRING,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                data_quality_score INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_customers'
        """)
        
        # Audit Log Table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_audit_log (
                record_id STRING,
                source_table STRING,
                load_timestamp TIMESTAMP,
                processed_by STRING,
                processing_time INT,
                status STRING,
                error_message STRING,
                records_processed INT
            ) USING DELTA
            LOCATION '{self.config.BRONZE_LOCATION}bz_audit_log'
        """)
        
        logger.info("Bronze layer tables created successfully")
    
    def extract_from_source(self, table_name):
        """Extract data from source PostgreSQL database"""
        try:
            logger.info(f"Extracting data from source table: {table_name}")
            
            # Build JDBC URL
            jdbc_url = f"jdbc:postgresql://{self.config.SOURCE_DB_URL}/{self.config.SOURCE_DATABASE}"
            
            # Connection properties
            connection_properties = {
                "user": self.config.SOURCE_USER,
                "password": self.config.SOURCE_PASSWORD,
                "driver": "org.postgresql.Driver"
            }
            
            # Read from source
            source_df = spark.read \
                .jdbc(
                    url=jdbc_url,
                    table=f"{self.config.SOURCE_SCHEMA}.{table_name}",
                    properties=connection_properties
                )
            
            logger.info(f"Extracted {source_df.count()} records from {table_name}")
            return source_df
            
        except Exception as e:
            logger.error(f"Error extracting from {table_name}: {str(e)}")
            raise
    
    def calculate_quality_score(self, df, table_name):
        """Calculate data quality score based on validation rules"""
        total_records = df.count()
        if total_records == 0:
            return 0
        
        quality_score = 100
        
        # Get key columns for validation
        key_columns_map = {
            "Products": ["Product_ID"],
            "Suppliers": ["Supplier_ID"],
            "Warehouses": ["Warehouse_ID"],
            "Inventory": ["Inventory_ID"],
            "Orders": ["Order_ID"],
            "Order_Details": ["Order_Detail_ID"],
            "Shipments": ["Shipment_ID"],
            "Returns": ["Return_ID"],
            "Stock_Levels": ["Stock_Level_ID"],
            "Customers": ["Customer_ID"]
        }
        
        key_columns = key_columns_map.get(table_name, [])
        
        # Check for null values in key columns
        for col in key_columns:
            if col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                null_percentage = (null_count / total_records) * 100
                if null_percentage > self.config.MAX_NULL_PERCENTAGE:
                    quality_score -= 20
                elif null_percentage > 5:
                    quality_score -= 10
        
        # Check for duplicate records
        if len(key_columns) > 0 and all(col in df.columns for col in key_columns):
            distinct_count = df.select(*key_columns).distinct().count()
            duplicate_percentage = ((total_records - distinct_count) / total_records) * 100
            if duplicate_percentage > 5:
                quality_score -= 15
            elif duplicate_percentage > 1:
                quality_score -= 5
        
        return max(0, min(100, quality_score))
    
    def transform_for_bronze(self, source_df, table_name):
        """Transform source data for Bronze layer"""
        try:
            logger.info(f"Transforming data for Bronze layer: {table_name}")
            
            # Add metadata columns
            transformed_df = source_df \
                .withColumn("load_timestamp", self.current_timestamp) \
                .withColumn("update_timestamp", self.current_timestamp) \
                .withColumn("source_system", lit(self.config.SOURCE_SYSTEM)) \
                .withColumn("record_status", lit("ACTIVE"))
            
            # Calculate data quality score
            quality_score = self.calculate_quality_score(transformed_df, table_name)
            transformed_df = transformed_df.withColumn("data_quality_score", lit(quality_score))
            
            logger.info(f"Transformation completed for {table_name} with quality score: {quality_score}")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error transforming {table_name}: {str(e)}")
            raise
    
    def load_to_bronze(self, transformed_df, table_name):
        """Load transformed data to Bronze layer"""
        try:
            logger.info(f"Loading data to Bronze layer: {table_name}")
            
            bronze_table_name = f"bz_{table_name.lower()}"
            full_table_name = f"{self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.{bronze_table_name}"
            
            # Use append mode for Bronze layer (raw data preservation)
            transformed_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(full_table_name)
            
            record_count = transformed_df.count()
            logger.info(f"Successfully loaded {record_count} records to {full_table_name}")
            
            return record_count
            
        except Exception as e:
            logger.error(f"Error loading to Bronze layer {table_name}: {str(e)}")
            raise
    
    def log_audit_entry(self, table_name, status, records_processed=0, error_message=None, processing_time=0):
        """Log audit entry for tracking"""
        try:
            audit_data = [{
                "record_id": f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "source_table": table_name,
                "load_timestamp": datetime.now(),
                "processed_by": "Bronze_Ingestion_Pipeline",
                "processing_time": processing_time,
                "status": status,
                "error_message": error_message,
                "records_processed": records_processed
            }]
            
            audit_df = spark.createDataFrame(audit_data)
            
            audit_table = f"{self.config.BRONZE_CATALOG}.{self.config.BRONZE_SCHEMA}.bz_audit_log"
            audit_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(audit_table)
            
        except Exception as e:
            logger.error(f"Error logging audit entry: {str(e)}")
    
    def process_table(self, table_name):
        """Process a single table through the Bronze pipeline"""
        start_time = datetime.now()
        
        try:
            logger.info(f"Starting Bronze ingestion for table: {table_name}")
            
            # Extract from source
            source_df = self.extract_from_source(table_name)
            
            # Transform for Bronze
            transformed_df = self.transform_for_bronze(source_df, table_name)
            
            # Load to Bronze
            record_count = self.load_to_bronze(transformed_df, table_name)
            
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Log success
            self.log_audit_entry(
                table_name=table_name,
                status="SUCCESS",
                records_processed=record_count,
                processing_time=int(processing_time)
            )
            
            logger.info(f"Successfully processed {table_name}: {record_count} records in {processing_time:.2f} seconds")
            return True
            
        except Exception as e:
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Log failure
            self.log_audit_entry(
                table_name=table_name,
                status="FAILED",
                error_message=str(e),
                processing_time=int(processing_time)
            )
            
            logger.error(f"Failed to process {table_name}: {str(e)}")
            return False
    
    def run_full_ingestion(self):
        """Run full ingestion for all tables"""
        logger.info("Starting full Bronze layer ingestion...")
        
        # Create tables if they don't exist
        self.create_bronze_tables()
        
        # Process each table
        results = {}
        for table_name in self.config.SOURCE_TABLES:
            results[table_name] = self.process_table(table_name)
        
        # Summary
        successful_tables = [table for table, success in results.items() if success]
        failed_tables = [table for table, success in results.items() if not success]
        
        logger.info(f"Ingestion completed. Successful: {len(successful_tables)}, Failed: {len(failed_tables)}")
        
        if failed_tables:
            logger.warning(f"Failed tables: {failed_tables}")
        
        return results

# ============================================================================
# MONITORING AND UTILITIES
# ============================================================================

class BronzeMonitoring:
    """Monitoring and alerting for Bronze layer"""
    
    @staticmethod
    def check_data_freshness():
        """Check data freshness across all Bronze tables"""
        freshness_report = []
        
        for table in BronzeConfig.SOURCE_TABLES:
            table_name = f"{BronzeConfig.BRONZE_CATALOG}.{BronzeConfig.BRONZE_SCHEMA}.bz_{table.lower()}"
            
            try:
                latest_load = spark.sql(f"""
                    SELECT MAX(load_timestamp) as latest_load
                    FROM {table_name}
                """).collect()[0]['latest_load']
                
                if latest_load:
                    hours_since_load = (datetime.now() - latest_load).total_seconds() / 3600
                    freshness_report.append({
                        "table": table,
                        "latest_load": latest_load,
                        "hours_since_load": hours_since_load,
                        "status": "STALE" if hours_since_load > 2 else "FRESH"
                    })
                
            except Exception as e:
                freshness_report.append({
                    "table": table,
                    "latest_load": None,
                    "hours_since_load": None,
                    "status": "ERROR",
                    "error": str(e)
                })
        
        return freshness_report
    
    @staticmethod
    def get_ingestion_summary():
        """Get ingestion summary from audit log"""
        try:
            audit_table = f"{BronzeConfig.BRONZE_CATALOG}.{BronzeConfig.BRONZE_SCHEMA}.bz_audit_log"
            
            summary = spark.sql(f"""
                SELECT 
                    source_table,
                    status,
                    COUNT(*) as execution_count,
                    AVG(processing_time) as avg_processing_time,
                    SUM(records_processed) as total_records_processed,
                    MAX(load_timestamp) as last_execution
                FROM {audit_table}
                WHERE load_timestamp >= current_timestamp() - INTERVAL 24 HOURS
                GROUP BY source_table, status
                ORDER BY source_table, status
            """).collect()
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting ingestion summary: {str(e)}")
            return []

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    try:
        # Initialize Bronze Ingestion Engine
        bronze_engine = BronzeIngestionEngine()
        
        # Run full ingestion
        results = bronze_engine.run_full_ingestion()
        
        # Print results
        print("\n" + "="*80)
        print("BRONZE LAYER INGESTION RESULTS")
        print("="*80)
        
        for table_name, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            print(f"{table_name:<20}: {status}")
        
        # Check data freshness
        print("\n" + "="*80)
        print("DATA FRESHNESS REPORT")
        print("="*80)
        
        freshness_report = BronzeMonitoring.check_data_freshness()
        for report in freshness_report:
            print(f"{report['table']:<20}: {report['status']} ({report.get('hours_since_load', 'N/A')} hours)")
        
        # Get ingestion summary
        print("\n" + "="*80)
        print("INGESTION SUMMARY (Last 24 Hours)")
        print("="*80)
        
        summary = BronzeMonitoring.get_ingestion_summary()
        for row in summary:
            print(f"{row['source_table']:<20}: {row['status']} - {row['execution_count']} executions, {row['total_records_processed']} records")
        
        print("\nBronze layer ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()

# ============================================================================
# ADDITIONAL UTILITY FUNCTIONS
# ============================================================================

def run_single_table_ingestion(table_name):
    """Run ingestion for a single table"""
    bronze_engine = BronzeIngestionEngine()
    bronze_engine.create_bronze_tables()
    return bronze_engine.process_table(table_name)

def get_table_statistics(table_name):
    """Get statistics for a specific Bronze table"""
    bronze_table_name = f"bz_{table_name.lower()}"
    full_table_name = f"{BronzeConfig.BRONZE_CATALOG}.{BronzeConfig.BRONZE_SCHEMA}.{bronze_table_name}"
    
    try:
        stats = spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT source_system) as source_systems,
                MIN(load_timestamp) as first_load,
                MAX(load_timestamp) as last_load,
                AVG(data_quality_score) as avg_quality_score
            FROM {full_table_name}
        """).collect()[0]
        
        return {
            "table": table_name,
            "total_records": stats['total_records'],
            "source_systems": stats['source_systems'],
            "first_load": stats['first_load'],
            "last_load": stats['last_load'],
            "avg_quality_score": stats['avg_quality_score']
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics for {table_name}: {str(e)}")
        return None

def cleanup_old_data(days_to_retain=30):
    """Cleanup old data from Bronze tables based on retention policy"""
    cutoff_date = datetime.now() - timedelta(days=days_to_retain)
    
    for table in BronzeConfig.SOURCE_TABLES:
        bronze_table_name = f"bz_{table.lower()}"
        full_table_name = f"{BronzeConfig.BRONZE_CATALOG}.{BronzeConfig.BRONZE_SCHEMA}.{bronze_table_name}"
        
        try:
            # Delete old records
            spark.sql(f"""
                DELETE FROM {full_table_name}
                WHERE load_timestamp < '{cutoff_date}'
            """)
            
            logger.info(f"Cleaned up old data from {full_table_name}")
            
        except Exception as e:
            logger.error(f"Error cleaning up {full_table_name}: {str(e)}")

# End of Bronze Layer Data Engineering Pipeline