# Databricks Bronze Layer Data Engineering Pipeline
# Inventory Management System - PySpark Implementation
# Author: AAVA Data Engineer
# Version: 1
# Created: 2024-12-19

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import hashlib
import uuid
from datetime import datetime
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion_Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.preview.enabled", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("Spark Session initialized successfully")
print(f"Spark Version: {spark.version}")

# Database and connection configuration
try:
    # In a real Databricks environment, these would come from Key Vault
    # For demo purposes, using placeholder values
    jdbc_url = "jdbc:postgresql://localhost:5432/DE"
    user = "demo_user"
    password = "demo_password"
    
    connection_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
        "batchsize": "10000"
    }
    
    print("Database connection configuration loaded")
except Exception as e:
    print(f"Warning: Could not load database credentials: {e}")
    print("Proceeding with demo data creation")

class BronzeIngestionFramework:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.current_timestamp = datetime.now()
        self.batch_id = str(uuid.uuid4())
        print(f"Initialized BronzeIngestionFramework with batch_id: {self.batch_id}")
    
    def create_demo_data(self):
        """
        Create demo data for testing the pipeline
        """
        print("Creating demo data for testing...")
        
        # Demo Products data
        products_data = [
            (1, "Laptop", "Electronics"),
            (2, "T-Shirt", "Apparel"),
            (3, "Office Chair", "Furniture"),
            (4, "Smartphone", "Electronics"),
            (5, "Jeans", "Apparel")
        ]
        
        products_schema = StructType([
            StructField("Product_ID", IntegerType(), True),
            StructField("Product_Name", StringType(), True),
            StructField("Category", StringType(), True)
        ])
        
        products_df = self.spark.createDataFrame(products_data, products_schema)
        
        # Demo Customers data
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
        
        # Demo Inventory data
        inventory_data = [
            (1, 1, 100, 1),
            (2, 2, 250, 1),
            (3, 3, 50, 2),
            (4, 4, 75, 1),
            (5, 5, 200, 2)
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
            "inventory": inventory_df
        }
    
    def add_metadata_columns(self, df, source_system, file_name=None):
        """
        Add standard metadata columns to the dataframe
        """
        print(f"Adding metadata columns for source system: {source_system}")
        
        # Create row hash for change detection
        columns_to_hash = [col for col in df.columns]
        row_hash_expr = sha2(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in columns_to_hash]), 256)
        
        enriched_df = df.withColumn("load_timestamp", lit(self.current_timestamp)) \
                       .withColumn("update_timestamp", lit(self.current_timestamp)) \
                       .withColumn("source_system", lit(source_system)) \
                       .withColumn("record_status", lit("ACTIVE")) \
                       .withColumn("batch_id", lit(self.batch_id)) \
                       .withColumn("file_name", lit(file_name if file_name else "demo_data")) \
                       .withColumn("row_hash", row_hash_expr)
        
        print(f"Added metadata columns. Total columns now: {len(enriched_df.columns)}")
        return enriched_df
    
    def calculate_data_quality_score(self, df):
        """
        Calculate data quality score based on null values and data validation
        """
        print("Calculating data quality score...")
        
        total_columns = len([col for col in df.columns if not col.startswith("load_") and not col.startswith("update_")])
        total_records = df.count()
        
        if total_records == 0:
            return df.withColumn("data_quality_score", lit(0))
        
        # Count null values across all original columns (excluding metadata)
        original_columns = [col for col in df.columns if not col.startswith(("load_", "update_", "source_", "record_", "batch_", "file_", "row_"))]
        
        null_count = 0
        for col_name in original_columns:
            null_count += df.filter(col(col_name).isNull()).count()
        
        # Calculate quality score (0-100)
        if total_columns > 0 and total_records > 0:
            null_percentage = (null_count / (total_records * total_columns)) * 100
            quality_score = max(0, 100 - int(null_percentage))
        else:
            quality_score = 100
        
        print(f"Data quality score calculated: {quality_score}")
        return df.withColumn("data_quality_score", lit(quality_score))
    
    def create_bronze_tables(self):
        """
        Create Bronze layer tables with proper schema
        """
        print("Creating Bronze layer tables...")
        
        # Create database if not exists
        self.spark.sql("CREATE DATABASE IF NOT EXISTS inventory_bronze")
        print("Created inventory_bronze database")
        
        # Products table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_products (
                Product_ID INT,
                Product_Name STRING,
                Category STRING,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                batch_id STRING,
                file_name STRING,
                row_hash STRING,
                data_quality_score INT
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """)
        
        # Customers table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_customers (
                Customer_ID INT,
                Customer_Name STRING,
                Email STRING,
                load_timestamp TIMESTAMP,
                update_timestamp TIMESTAMP,
                source_system STRING,
                record_status STRING,
                batch_id STRING,
                file_name STRING,
                row_hash STRING,
                data_quality_score INT
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """)
        
        # Inventory table
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
                batch_id STRING,
                file_name STRING,
                row_hash STRING,
                data_quality_score INT
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """)
        
        # Audit log table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS inventory_bronze.bz_audit_log (
                record_id STRING,
                source_table STRING,
                target_table STRING,
                load_timestamp TIMESTAMP,
                processed_by STRING,
                record_count BIGINT,
                batch_id STRING,
                status STRING
            ) USING DELTA
        """)
        
        print("Bronze layer tables created successfully")
    
    def ingest_data(self, source_df, source_table, target_table, primary_keys):
        """
        Generic data ingestion method
        """
        try:
            print(f"Starting ingestion for {source_table} -> {target_table}")
            
            # Add metadata and quality score
            enriched_df = self.add_metadata_columns(source_df, "Demo_PostgreSQL")
            final_df = self.calculate_data_quality_score(enriched_df)
            
            record_count = final_df.count()
            print(f"Processing {record_count} records")
            
            # Write to Bronze layer
            final_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(f"inventory_bronze.{target_table}")
            
            print(f"Successfully wrote {record_count} records to {target_table}")
            
            # Log success
            self.log_ingestion_audit(source_table, target_table, record_count, "SUCCESS")
            
            return True
            
        except Exception as e:
            print(f"Error ingesting {source_table}: {str(e)}")
            self.log_ingestion_audit(source_table, target_table, 0, f"FAILED: {str(e)}")
            return False
    
    def log_ingestion_audit(self, source_table, target_table, record_count, status):
        """
        Log ingestion audit information
        """
        try:
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
            audit_df.write.format("delta").mode("append").saveAsTable("inventory_bronze.bz_audit_log")
            
            print(f"Audit log entry created: {status} for {source_table}")
        except Exception as e:
            print(f"Warning: Could not write audit log: {e}")

class DataQualityValidator:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.quality_rules = {
            "products": {
                "required_fields": ["Product_ID", "Product_Name"],
                "unique_fields": ["Product_ID"]
            },
            "customers": {
                "required_fields": ["Customer_ID", "Customer_Name", "Email"],
                "unique_fields": ["Customer_ID", "Email"],
                "pii_fields": ["Customer_Name", "Email"]
            },
            "inventory": {
                "required_fields": ["Inventory_ID", "Product_ID", "Warehouse_ID"],
                "unique_fields": ["Inventory_ID"]
            }
        }
    
    def validate_data_quality(self, df, table_name):
        """
        Comprehensive data quality validation
        """
        print(f"Validating data quality for {table_name}")
        
        quality_score = 100
        quality_issues = []
        
        if table_name in self.quality_rules:
            rules = self.quality_rules[table_name]
            
            # Check required fields
            for field in rules.get("required_fields", []):
                if field in df.columns:
                    null_count = df.filter(col(field).isNull()).count()
                    if null_count > 0:
                        quality_score -= 10
                        quality_issues.append(f"Null values in required field: {field}")
            
            # Check unique constraints
            for field in rules.get("unique_fields", []):
                if field in df.columns:
                    total_count = df.count()
                    unique_count = df.select(field).distinct().count()
                    if unique_count < total_count:
                        quality_score -= 15
                        quality_issues.append(f"Duplicate values in unique field: {field}")
        
        quality_score = max(0, quality_score)
        print(f"Data quality validation completed. Score: {quality_score}")
        if quality_issues:
            print(f"Quality issues found: {quality_issues}")
        
        return quality_score, quality_issues

def main():
    """
    Main execution function
    """
    print("=" * 80)
    print("DATABRICKS BRONZE LAYER DATA ENGINEERING PIPELINE")
    print("Inventory Management System")
    print("=" * 80)
    
    try:
        # Initialize framework
        ingestion_framework = BronzeIngestionFramework(spark)
        validator = DataQualityValidator(spark)
        
        # Create Bronze layer tables
        ingestion_framework.create_bronze_tables()
        
        # Create demo data (in real scenario, this would read from source systems)
        demo_data = ingestion_framework.create_demo_data()
        
        # Table configurations
        table_configs = [
            {
                "source_table": "products",
                "target_table": "bz_products",
                "primary_keys": ["Product_ID"],
                "data": demo_data["products"]
            },
            {
                "source_table": "customers",
                "target_table": "bz_customers",
                "primary_keys": ["Customer_ID"],
                "data": demo_data["customers"]
            },
            {
                "source_table": "inventory",
                "target_table": "bz_inventory",
                "primary_keys": ["Inventory_ID"],
                "data": demo_data["inventory"]
            }
        ]
        
        # Process each table
        successful_ingestions = 0
        total_records_processed = 0
        
        for config in table_configs:
            print(f"\n{'-' * 60}")
            print(f"Processing: {config['source_table']}")
            print(f"{'-' * 60}")
            
            # Validate data quality
            quality_score, issues = validator.validate_data_quality(
                config["data"], 
                config["source_table"]
            )
            
            # Ingest data
            success = ingestion_framework.ingest_data(
                config["data"],
                config["source_table"],
                config["target_table"],
                config["primary_keys"]
            )
            
            if success:
                successful_ingestions += 1
                total_records_processed += config["data"].count()
        
        # Summary report
        print(f"\n{'=' * 80}")
        print("INGESTION SUMMARY REPORT")
        print(f"{'=' * 80}")
        print(f"Total tables processed: {len(table_configs)}")
        print(f"Successful ingestions: {successful_ingestions}")
        print(f"Failed ingestions: {len(table_configs) - successful_ingestions}")
        print(f"Total records processed: {total_records_processed}")
        print(f"Batch ID: {ingestion_framework.batch_id}")
        print(f"Processing time: {datetime.now() - ingestion_framework.current_timestamp}")
        
        # Display sample data from Bronze tables
        print(f"\n{'-' * 60}")
        print("SAMPLE DATA FROM BRONZE TABLES")
        print(f"{'-' * 60}")
        
        for config in table_configs:
            if successful_ingestions > 0:
                print(f"\nSample from {config['target_table']}:")
                sample_df = spark.table(f"inventory_bronze.{config['target_table']}")
                sample_df.select("*").limit(3).show(truncate=False)
        
        # Display audit log
        print(f"\n{'-' * 60}")
        print("AUDIT LOG ENTRIES")
        print(f"{'-' * 60}")
        audit_df = spark.table("inventory_bronze.bz_audit_log")
        audit_df.show(truncate=False)
        
        print(f"\n{'=' * 80}")
        print("BRONZE LAYER PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        print(f"{'=' * 80}")
        
        return True
        
    except Exception as e:
        print(f"\nERROR: Pipeline execution failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up
        print("\nCleaning up resources...")
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Bronze Layer Data Engineering Pipeline completed successfully!")
    else:
        print("\n❌ Bronze Layer Data Engineering Pipeline failed!")
        exit(1)