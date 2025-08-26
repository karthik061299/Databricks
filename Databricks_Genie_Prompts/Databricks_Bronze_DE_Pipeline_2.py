# Databricks Bronze Layer Data Engineering Pipeline - Enhanced Version
# Inventory Management System - Raw Data Ingestion
# Version: 2.0
# Created: 2025-01-27
# Enhanced with better error handling and simplified execution

# ============================================================================
# BRONZE LAYER INGESTION STRATEGY - ENHANCED
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import logging
from datetime import datetime
import json
import sys

# Initialize Spark Session with enhanced configuration
spark = SparkSession.builder \
    .appName("InventoryManagement_Bronze_Ingestion_v2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("=" * 80)
print("DATABRICKS BRONZE LAYER INGESTION PIPELINE v2.0")
print("Inventory Management System")
print("=" * 80)

# ============================================================================
# ENHANCED CONFIGURATION CLASS
# ============================================================================

class EnhancedBronzeConfig:
    """Enhanced configuration class for Bronze layer ingestion"""
    
    def __init__(self):
        # Storage paths - using DBFS for Databricks
        self.bronze_base_path = "/dbfs/mnt/bronze/inventory_management"
        self.checkpoint_path = "/dbfs/mnt/checkpoints/bronze"
        self.audit_log_path = "/dbfs/mnt/audit/bronze_ingestion"
        
        # Create directories if they don't exist
        self._create_directories()
        
        # Enhanced source system configurations
        self.source_systems = {
            "sample_data": {
                "description": "Sample inventory data for demonstration",
                "tables": ["products", "suppliers", "warehouses", "inventory", "orders"]
            }
        }
        
        # Enhanced schema definitions
        self.table_schemas = self._define_enhanced_schemas()
        
        print(f"✓ Configuration initialized")
        print(f"✓ Bronze path: {self.bronze_base_path}")
        print(f"✓ Checkpoint path: {self.checkpoint_path}")
        print(f"✓ Audit log path: {self.audit_log_path}")
    
    def _create_directories(self):
        """Create necessary directories"""
        import os
        directories = [self.bronze_base_path, self.checkpoint_path, self.audit_log_path]
        for directory in directories:
            try:
                os.makedirs(directory, exist_ok=True)
            except Exception as e:
                logger.warning(f"Could not create directory {directory}: {e}")
    
    def _define_enhanced_schemas(self):
        """Define enhanced schemas for all source tables"""
        return {
            "products": StructType([
                StructField("product_id", StringType(), False),
                StructField("product_name", StringType(), False),
                StructField("category", StringType(), False),
                StructField("brand", StringType(), True),
                StructField("unit_price", DecimalType(10,2), True),
                StructField("unit_cost", DecimalType(10,2), True),
                StructField("status", StringType(), True)
            ]),
            "suppliers": StructType([
                StructField("supplier_id", StringType(), False),
                StructField("supplier_name", StringType(), False),
                StructField("contact_number", StringType(), False),
                StructField("email", StringType(), True),
                StructField("address", StringType(), True),
                StructField("rating", DecimalType(3,2), True)
            ]),
            "warehouses": StructType([
                StructField("warehouse_id", StringType(), False),
                StructField("warehouse_name", StringType(), False),
                StructField("location", StringType(), False),
                StructField("capacity", IntegerType(), False),
                StructField("zone", StringType(), True),
                StructField("manager", StringType(), True)
            ]),
            "inventory": StructType([
                StructField("inventory_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("warehouse_id", StringType(), False),
                StructField("quantity_available", IntegerType(), False),
                StructField("reorder_point", IntegerType(), True),
                StructField("last_updated", TimestampType(), True)
            ]),
            "orders": StructType([
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("quantity_ordered", IntegerType(), False),
                StructField("order_date", DateType(), False),
                StructField("order_status", StringType(), True),
                StructField("total_amount", DecimalType(12,2), True)
            ])
        }

# ============================================================================
# ENHANCED BRONZE LAYER INGESTION ENGINE
# ============================================================================

class EnhancedBronzeIngestion:
    """Enhanced Bronze layer data ingestion engine"""
    
    def __init__(self, config: EnhancedBronzeConfig):
        self.config = config
        self.ingestion_timestamp = datetime.now()
        self.batch_id = self.ingestion_timestamp.strftime("%Y%m%d_%H%M%S")
        self.processed_tables = []
        self.failed_tables = []
        
        print(f"✓ Ingestion engine initialized")
        print(f"✓ Batch ID: {self.batch_id}")
        print(f"✓ Ingestion timestamp: {self.ingestion_timestamp}")
    
    def add_bronze_metadata(self, df, source_system, table_name):
        """Add comprehensive metadata columns for Bronze layer"""
        return df.withColumn("_bronze_ingestion_timestamp", lit(self.ingestion_timestamp)) \
                 .withColumn("_bronze_batch_id", lit(self.batch_id)) \
                 .withColumn("_source_system", lit(source_system)) \
                 .withColumn("_source_table", lit(table_name)) \
                 .withColumn("_ingestion_date", lit(self.ingestion_timestamp.date())) \
                 .withColumn("_record_hash", sha2(concat_ws("|", *[col(c) for c in df.columns]), 256)) \
                 .withColumn("_is_current", lit(True)) \
                 .withColumn("_created_by", lit("bronze_ingestion_pipeline"))
    
    def create_sample_data(self, table_name):
        """Create sample data for demonstration purposes"""
        logger.info(f"Creating sample data for {table_name}")
        
        if table_name == "products":
            data = [
                ("P001", "Laptop Dell XPS", "Electronics", "Dell", 1200.00, 800.00, "Active"),
                ("P002", "Office Chair", "Furniture", "Steelcase", 350.00, 200.00, "Active"),
                ("P003", "Wireless Mouse", "Electronics", "Logitech", 25.00, 15.00, "Active"),
                ("P004", "Standing Desk", "Furniture", "IKEA", 299.00, 180.00, "Active"),
                ("P005", "Monitor 24inch", "Electronics", "Samsung", 180.00, 120.00, "Active")
            ]
            schema = self.config.table_schemas["products"]
            
        elif table_name == "suppliers":
            data = [
                ("S001", "Tech Solutions Inc", "+1-555-0101", "contact@techsolutions.com", "123 Tech St, Silicon Valley", 4.5),
                ("S002", "Furniture World", "+1-555-0102", "sales@furnitureworld.com", "456 Furniture Ave, Chicago", 4.2),
                ("S003", "Electronics Hub", "+1-555-0103", "info@electronicshub.com", "789 Electronics Blvd, Austin", 4.7),
                ("S004", "Office Supplies Co", "+1-555-0104", "orders@officesupplies.com", "321 Office Way, Denver", 4.0),
                ("S005", "Global Components", "+1-555-0105", "support@globalcomponents.com", "654 Component Dr, Seattle", 4.3)
            ]
            schema = self.config.table_schemas["suppliers"]
            
        elif table_name == "warehouses":
            data = [
                ("W001", "Main Warehouse", "New York, NY", 10000, "Zone A", "John Smith"),
                ("W002", "West Coast Hub", "Los Angeles, CA", 8000, "Zone B", "Jane Doe"),
                ("W003", "Central Distribution", "Chicago, IL", 12000, "Zone C", "Mike Johnson"),
                ("W004", "East Coast Facility", "Atlanta, GA", 9000, "Zone D", "Sarah Wilson"),
                ("W005", "Northwest Center", "Seattle, WA", 7500, "Zone E", "David Brown")
            ]
            schema = self.config.table_schemas["warehouses"]
            
        elif table_name == "inventory":
            data = [
                ("I001", "P001", "W001", 50, 10, datetime.now()),
                ("I002", "P002", "W001", 25, 5, datetime.now()),
                ("I003", "P003", "W002", 100, 20, datetime.now()),
                ("I004", "P004", "W002", 15, 3, datetime.now()),
                ("I005", "P005", "W003", 75, 15, datetime.now())
            ]
            schema = self.config.table_schemas["inventory"]
            
        elif table_name == "orders":
            from datetime import date
            data = [
                ("O001", "C001", "P001", 2, date(2025, 1, 25), "Completed", 2400.00),
                ("O002", "C002", "P002", 1, date(2025, 1, 26), "Processing", 350.00),
                ("O003", "C003", "P003", 5, date(2025, 1, 26), "Shipped", 125.00),
                ("O004", "C001", "P004", 1, date(2025, 1, 27), "Pending", 299.00),
                ("O005", "C004", "P005", 3, date(2025, 1, 27), "Processing", 540.00)
            ]
            schema = self.config.table_schemas["orders"]
        
        else:
            raise ValueError(f"Sample data not defined for table: {table_name}")
        
        return spark.createDataFrame(data, schema)
    
    def validate_and_profile_data(self, df, table_name):
        """Enhanced data validation and profiling"""
        logger.info(f"Validating and profiling data for {table_name}")
        
        validation_results = {
            "table_name": table_name,
            "validation_timestamp": self.ingestion_timestamp.isoformat(),
            "total_records": df.count(),
            "total_columns": len(df.columns),
            "data_quality_score": 0.0,
            "issues": []
        }
        
        try:
            # Basic record count validation
            if validation_results["total_records"] == 0:
                validation_results["issues"].append("No records found in source data")
            
            # Column-level validation
            null_checks = {}
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                if null_count > 0:
                    null_percentage = (null_count / validation_results["total_records"]) * 100
                    null_checks[column] = {
                        "null_count": null_count,
                        "null_percentage": round(null_percentage, 2)
                    }
                    
                    if null_percentage > 50:
                        validation_results["issues"].append(f"High null percentage in {column}: {null_percentage}%")
            
            validation_results["null_analysis"] = null_checks
            
            # Calculate data quality score
            base_score = 100.0
            if validation_results["total_records"] == 0:
                base_score = 0.0
            else:
                # Deduct points for issues
                base_score -= len(validation_results["issues"]) * 10
                base_score = max(0.0, base_score)
            
            validation_results["data_quality_score"] = base_score
            
            logger.info(f"Data validation completed for {table_name}. Quality score: {base_score}")
            return validation_results
            
        except Exception as e:
            validation_results["issues"].append(f"Validation error: {str(e)}")
            validation_results["data_quality_score"] = 0.0
            logger.error(f"Error during validation of {table_name}: {str(e)}")
            return validation_results
    
    def write_to_bronze_layer(self, df, table_name, source_system):
        """Write data to Bronze layer with enhanced error handling"""
        try:
            logger.info(f"Writing {df.count()} records to Bronze layer for {table_name}")
            
            # Add Bronze metadata
            df_with_metadata = self.add_bronze_metadata(df, source_system, table_name)
            
            # Define Bronze table path
            bronze_table_path = f"{self.config.bronze_base_path}/{table_name.lower()}"
            
            # Write to Delta table
            df_with_metadata.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .option("overwriteSchema", "false") \
                .save(bronze_table_path)
            
            # Create or replace table in catalog
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS bronze_{table_name.lower()}
                USING DELTA
                LOCATION '{bronze_table_path}'
            """)
            
            logger.info(f"✓ Successfully wrote data to Bronze layer: bronze_{table_name.lower()}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error writing to Bronze layer for {table_name}: {str(e)}")
            return False
    
    def log_ingestion_audit(self, table_name, source_system, status, record_count, validation_results=None, error_message=None):
        """Enhanced audit logging"""
        audit_record = {
            "batch_id": self.batch_id,
            "table_name": table_name,
            "source_system": source_system,
            "ingestion_timestamp": self.ingestion_timestamp.isoformat(),
            "status": status,
            "record_count": record_count,
            "data_quality_score": validation_results.get("data_quality_score", 0.0) if validation_results else 0.0,
            "issues_count": len(validation_results.get("issues", [])) if validation_results else 0,
            "error_message": error_message
        }
        
        try:
            # Create audit DataFrame
            audit_df = spark.createDataFrame([audit_record])
            
            # Write to audit log
            audit_path = f"{self.config.audit_log_path}/ingestion_audit"
            audit_df.write \
                .mode("append") \
                .option("mergeSchema", "true") \
                .parquet(audit_path)
            
            logger.info(f"✓ Audit logged for {table_name}: {status}")
            
        except Exception as e:
            logger.warning(f"Could not write audit log: {str(e)}")
    
    def process_table(self, table_name, source_system="sample_data"):
        """Process a single table through the Bronze ingestion pipeline"""
        logger.info(f"Processing table: {table_name}")
        
        try:
            # Create or read source data
            source_df = self.create_sample_data(table_name)
            
            # Validate and profile data
            validation_results = self.validate_and_profile_data(source_df, table_name)
            
            # Write to Bronze layer
            write_success = self.write_to_bronze_layer(source_df, table_name, source_system)
            
            if write_success:
                # Log successful ingestion
                self.log_ingestion_audit(
                    table_name, source_system, "SUCCESS", 
                    validation_results["total_records"], validation_results
                )
                
                self.processed_tables.append({
                    "table_name": table_name,
                    "record_count": validation_results["total_records"],
                    "quality_score": validation_results["data_quality_score"],
                    "status": "SUCCESS"
                })
                
                logger.info(f"✓ Successfully processed {table_name}")
                return True
            else:
                raise Exception("Failed to write to Bronze layer")
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"✗ Error processing {table_name}: {error_msg}")
            
            # Log failed ingestion
            self.log_ingestion_audit(
                table_name, source_system, "FAILED", 0, None, error_msg
            )
            
            self.failed_tables.append({
                "table_name": table_name,
                "error": error_msg,
                "status": "FAILED"
            })
            
            return False
    
    def run_full_ingestion_pipeline(self):
        """Execute the complete Bronze layer ingestion pipeline"""
        logger.info("Starting Bronze Layer Ingestion Pipeline")
        print("\n" + "=" * 60)
        print("EXECUTING BRONZE LAYER INGESTION PIPELINE")
        print("=" * 60)
        
        start_time = datetime.now()
        
        # Process all configured tables
        tables_to_process = self.config.source_systems["sample_data"]["tables"]
        
        print(f"\nTables to process: {', '.join(tables_to_process)}")
        print(f"Batch ID: {self.batch_id}")
        print(f"Start time: {start_time}")
        print("-" * 60)
        
        for table_name in tables_to_process:
            print(f"\nProcessing: {table_name.upper()}")
            success = self.process_table(table_name)
            if success:
                print(f"✓ {table_name} - COMPLETED")
            else:
                print(f"✗ {table_name} - FAILED")
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Generate summary
        summary = {
            "batch_id": self.batch_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration.total_seconds(),
            "total_tables": len(tables_to_process),
            "successful_tables": len(self.processed_tables),
            "failed_tables": len(self.failed_tables),
            "total_records_processed": sum([t["record_count"] for t in self.processed_tables]),
            "average_quality_score": sum([t["quality_score"] for t in self.processed_tables]) / len(self.processed_tables) if self.processed_tables else 0,
            "processed_tables": self.processed_tables,
            "failed_tables": self.failed_tables,
            "status": "COMPLETED" if not self.failed_tables else "COMPLETED_WITH_ERRORS"
        }
        
        self._print_summary(summary)
        return summary
    
    def _print_summary(self, summary):
        """Print detailed execution summary"""
        print("\n" + "=" * 80)
        print("BRONZE LAYER INGESTION PIPELINE - EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Batch ID: {summary['batch_id']}")
        print(f"Status: {summary['status']}")
        print(f"Duration: {summary['duration_seconds']:.2f} seconds")
        print(f"Total Tables: {summary['total_tables']}")
        print(f"Successful: {summary['successful_tables']}")
        print(f"Failed: {summary['failed_tables']}")
        print(f"Total Records: {summary['total_records_processed']}")
        print(f"Average Quality Score: {summary['average_quality_score']:.2f}")
        
        if summary['processed_tables']:
            print("\nSUCCESSFUL TABLES:")
            print("-" * 50)
            for table in summary['processed_tables']:
                print(f"  ✓ {table['table_name']:<15} | Records: {table['record_count']:<8} | Quality: {table['quality_score']:.1f}")
        
        if summary['failed_tables']:
            print("\nFAILED TABLES:")
            print("-" * 50)
            for table in summary['failed_tables']:
                print(f"  ✗ {table['table_name']:<15} | Error: {table['error']}")
        
        print("\nBRONZE LAYER TABLES CREATED:")
        print("-" * 50)
        for table in summary['processed_tables']:
            print(f"  📊 bronze_{table['table_name'].lower()}")
        
        print("=" * 80)

# ============================================================================
# DATA QUALITY AND MONITORING ENHANCEMENTS
# ============================================================================

class BronzeDataQualityDashboard:
    """Enhanced data quality monitoring and dashboard"""
    
    def __init__(self, config: EnhancedBronzeConfig):
        self.config = config
    
    def generate_quality_report(self):
        """Generate comprehensive data quality report"""
        print("\n" + "=" * 80)
        print("BRONZE LAYER DATA QUALITY REPORT")
        print("=" * 80)
        
        try:
            # List all Bronze tables
            bronze_tables = spark.sql("SHOW TABLES").filter(col("tableName").startswith("bronze_")).collect()
            
            if not bronze_tables:
                print("No Bronze tables found.")
                return
            
            for table_row in bronze_tables:
                table_name = table_row['tableName']
                print(f"\nTable: {table_name.upper()}")
                print("-" * 40)
                
                try:
                    # Get table statistics
                    df = spark.table(table_name)
                    record_count = df.count()
                    column_count = len(df.columns)
                    
                    print(f"Records: {record_count:,}")
                    print(f"Columns: {column_count}")
                    
                    # Get latest ingestion info
                    if "_bronze_ingestion_timestamp" in df.columns:
                        latest_ingestion = df.select(max(col("_bronze_ingestion_timestamp"))).collect()[0][0]
                        print(f"Latest Ingestion: {latest_ingestion}")
                    
                    # Show sample data
                    print("\nSample Data:")
                    df.select([c for c in df.columns if not c.startswith("_")]).show(3, truncate=False)
                    
                except Exception as e:
                    print(f"Error analyzing {table_name}: {str(e)}")
            
        except Exception as e:
            print(f"Error generating quality report: {str(e)}")

# ============================================================================
# MAIN EXECUTION FUNCTION
# ============================================================================

def main():
    """Main execution function for Bronze layer ingestion"""
    try:
        print("Initializing Bronze Layer Ingestion Pipeline...")
        
        # Initialize configuration
        config = EnhancedBronzeConfig()
        
        # Initialize ingestion engine
        ingestion_engine = EnhancedBronzeIngestion(config)
        
        # Run the complete ingestion pipeline
        execution_summary = ingestion_engine.run_full_ingestion_pipeline()
        
        # Generate data quality report
        quality_dashboard = BronzeDataQualityDashboard(config)
        quality_dashboard.generate_quality_report()
        
        # Final status
        if execution_summary["status"] == "COMPLETED":
            print("\n🎉 Bronze Layer Ingestion Pipeline completed successfully!")
        else:
            print("\n⚠️  Bronze Layer Ingestion Pipeline completed with some errors.")
        
        return execution_summary
        
    except Exception as e:
        logger.error(f"Critical error in Bronze layer ingestion: {str(e)}")
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        raise e
    
    finally:
        # Cleanup
        try:
            spark.stop()
            print("\n✓ Spark session stopped.")
        except:
            pass

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    print("Starting Databricks Bronze Layer Ingestion Pipeline...")
    result = main()
    print("Pipeline execution completed.")
else:
    print("Bronze Layer Ingestion Pipeline module loaded successfully.")
    # If running in Databricks notebook, execute main automatically
    try:
        result = main()
    except Exception as e:
        print(f"Error executing pipeline: {e}")