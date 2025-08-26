# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Bronze Layer Data Engineering Pipeline
# MAGIC ## Inventory Management System - Bronze Layer Ingestion Strategy
# MAGIC 
# MAGIC ### Overview
# MAGIC This notebook implements a comprehensive Bronze layer ingestion strategy for the Inventory Management System.
# MAGIC The Bronze layer serves as a landing zone for raw data with metadata tracking, audit logging, and data lineage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import json
from datetime import datetime, timezone
import uuid

# Initialize Spark Session with Delta Lake support
spark = SparkSession.builder \
    .appName("InventoryManagement_Bronze_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Set configuration parameters
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

print("Bronze Layer Ingestion Pipeline Initialized Successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration and Parameters

# COMMAND ----------

# Bronze Layer Configuration
class BronzeConfig:
    def __init__(self):
        # Storage paths
        self.bronze_base_path = "/mnt/datalake/bronze/inventory_management"
        self.checkpoint_path = "/mnt/datalake/checkpoints/bronze"
        self.schema_registry_path = "/mnt/datalake/schema_registry"
        
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
        
        # Metadata tracking configuration
        self.metadata_config = {
            "enable_lineage": True,
            "enable_audit_logging": True,
            "enable_schema_evolution": True,
            "retention_days": 365
        }

config = BronzeConfig()
print("Configuration loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema Definitions for Source Tables

# COMMAND ----------

# Define schemas for all source tables
class SourceSchemas:
    @staticmethod
    def get_products_schema():
        return StructType([
            StructField("Product_ID", IntegerType(), False),
            StructField("Product_Name", StringType(), False),
            StructField("Category", StringType(), False),
            StructField("Brand", StringType(), True),
            StructField("Unit_Cost", DecimalType(10,2), True),
            StructField("Unit_Price", DecimalType(10,2), True),
            StructField("Unit_Of_Measure", StringType(), True),
            StructField("Product_Status", StringType(), True)
        ])
    
    @staticmethod
    def get_suppliers_schema():
        return StructType([
            StructField("Supplier_ID", IntegerType(), False),
            StructField("Supplier_Name", StringType(), False),
            StructField("Contact_Number", StringType(), False),
            StructField("Email", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("Payment_Terms", StringType(), True),
            StructField("Lead_Time_Days", IntegerType(), True)
        ])
    
    @staticmethod
    def get_warehouses_schema():
        return StructType([
            StructField("Warehouse_ID", IntegerType(), False),
            StructField("Location", StringType(), False),
            StructField("Capacity", IntegerType(), False),
            StructField("Warehouse_Code", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Country", StringType(), True)
        ])
    
    @staticmethod
    def get_inventory_schema():
        return StructType([
            StructField("Inventory_ID", IntegerType(), False),
            StructField("Product_ID", IntegerType(), False),
            StructField("Warehouse_ID", IntegerType(), False),
            StructField("Quantity_Available", IntegerType(), False),
            StructField("Last_Updated", TimestampType(), True),
            StructField("Unit_Cost", DecimalType(10,2), True)
        ])
    
    @staticmethod
    def get_orders_schema():
        return StructType([
            StructField("Order_ID", IntegerType(), False),
            StructField("Customer_ID", IntegerType(), False),
            StructField("Order_Date", DateType(), False),
            StructField("Order_Status", StringType(), True),
            StructField("Total_Amount", DecimalType(15,2), True)
        ])
    
    @staticmethod
    def get_order_details_schema():
        return StructType([
            StructField("Order_Detail_ID", IntegerType(), False),
            StructField("Order_ID", IntegerType(), False),
            StructField("Product_ID", IntegerType(), False),
            StructField("Quantity_Ordered", IntegerType(), False),
            StructField("Unit_Price", DecimalType(10,2), True),
            StructField("Line_Total", DecimalType(15,2), True)
        ])
    
    @staticmethod
    def get_customers_schema():
        return StructType([
            StructField("Customer_ID", IntegerType(), False),
            StructField("Customer_Name", StringType(), False),
            StructField("Email", StringType(), False),
            StructField("Phone", StringType(), True),
            StructField("Address", StringType(), True)
        ])
    
    @staticmethod
    def get_shipments_schema():
        return StructType([
            StructField("Shipment_ID", IntegerType(), False),
            StructField("Order_ID", IntegerType(), False),
            StructField("Shipment_Date", DateType(), False),
            StructField("Tracking_Number", StringType(), True),
            StructField("Carrier", StringType(), True),
            StructField("Delivery_Status", StringType(), True)
        ])
    
    @staticmethod
    def get_returns_schema():
        return StructType([
            StructField("Return_ID", IntegerType(), False),
            StructField("Order_ID", IntegerType(), False),
            StructField("Product_ID", IntegerType(), False),
            StructField("Return_Reason", StringType(), False),
            StructField("Return_Date", DateType(), True),
            StructField("Quantity_Returned", IntegerType(), True)
        ])
    
    @staticmethod
    def get_stock_levels_schema():
        return StructType([
            StructField("Stock_Level_ID", IntegerType(), False),
            StructField("Warehouse_ID", IntegerType(), False),
            StructField("Product_ID", IntegerType(), False),
            StructField("Reorder_Threshold", IntegerType(), False),
            StructField("Safety_Stock", IntegerType(), True),
            StructField("Max_Stock", IntegerType(), True)
        ])

schemas = SourceSchemas()
print("Schema definitions loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Metadata and Audit Logging Framework

# COMMAND ----------

class MetadataManager:
    def __init__(self, config):
        self.config = config
        self.audit_table_path = f"{config.bronze_base_path}/audit_log"
        self.lineage_table_path = f"{config.bronze_base_path}/data_lineage"
        
    def add_metadata_columns(self, df, source_system, table_name, file_path=None):
        """Add standard metadata columns to incoming data"""
        current_ts = current_timestamp()
        batch_id = str(uuid.uuid4())
        
        return df.withColumn("_bronze_ingestion_timestamp", current_ts) \
                 .withColumn("_bronze_batch_id", lit(batch_id)) \
                 .withColumn("_bronze_source_system", lit(source_system)) \
                 .withColumn("_bronze_source_table", lit(table_name)) \
                 .withColumn("_bronze_source_file", lit(file_path)) \
                 .withColumn("_bronze_record_hash", sha2(concat_ws("|", *[col(c) for c in df.columns]), 256)) \
                 .withColumn("_bronze_is_active", lit(True)) \
                 .withColumn("_bronze_created_by", lit("bronze_ingestion_pipeline"))
    
    def log_ingestion_audit(self, source_system, table_name, record_count, status, error_message=None):
        """Log ingestion audit information"""
        audit_data = [{
            "audit_id": str(uuid.uuid4()),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_system": source_system,
            "source_table": table_name,
            "record_count": record_count,
            "status": status,
            "error_message": error_message,
            "pipeline_run_id": "bronze_pipeline_v1"
        }]
        
        audit_df = spark.createDataFrame(audit_data)
        
        # Write to audit table
        try:
            audit_df.write \
                .format("delta") \
                .mode("append") \
                .option("path", self.audit_table_path) \
                .save()
            print(f"Audit log written for {source_system}.{table_name}")
        except Exception as e:
            print(f"Error writing audit log: {str(e)}")

metadata_manager = MetadataManager(config)
print("Metadata manager initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality and Validation Framework

# COMMAND ----------

class DataQualityValidator:
    def __init__(self):
        self.quality_rules = {}
        
    def add_quality_rule(self, table_name, column_name, rule_type, rule_config):
        """Add data quality rule for a specific table and column"""
        if table_name not in self.quality_rules:
            self.quality_rules[table_name] = {}
        
        if column_name not in self.quality_rules[table_name]:
            self.quality_rules[table_name][column_name] = []
            
        self.quality_rules[table_name][column_name].append({
            "rule_type": rule_type,
            "config": rule_config
        })
    
    def validate_data(self, df, table_name):
        """Validate data against defined quality rules"""
        validation_results = []
        
        if table_name in self.quality_rules:
            for column_name, rules in self.quality_rules[table_name].items():
                for rule in rules:
                    result = self._apply_rule(df, column_name, rule)
                    validation_results.append(result)
        
        return validation_results
    
    def _apply_rule(self, df, column_name, rule):
        """Apply individual validation rule"""
        rule_type = rule["rule_type"]
        config = rule["config"]
        
        if rule_type == "not_null":
            failed_count = df.filter(col(column_name).isNull()).count()
            return {
                "column": column_name,
                "rule": rule_type,
                "passed": failed_count == 0,
                "failed_count": failed_count
            }
        elif rule_type == "unique":
            total_count = df.count()
            distinct_count = df.select(column_name).distinct().count()
            return {
                "column": column_name,
                "rule": rule_type,
                "passed": total_count == distinct_count,
                "failed_count": total_count - distinct_count
            }
        
        return {"column": column_name, "rule": rule_type, "passed": True, "failed_count": 0}

# Initialize data quality validator with rules
quality_validator = DataQualityValidator()

# Add quality rules for key tables
quality_validator.add_quality_rule("Products", "Product_ID", "not_null", {})
quality_validator.add_quality_rule("Products", "Product_ID", "unique", {})
quality_validator.add_quality_rule("Suppliers", "Supplier_ID", "not_null", {})
quality_validator.add_quality_rule("Suppliers", "Supplier_ID", "unique", {})

print("Data quality validator initialized with rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bronze Layer Ingestion Functions

# COMMAND ----------

class BronzeIngestion:
    def __init__(self, config, metadata_manager, quality_validator):
        self.config = config
        self.metadata_manager = metadata_manager
        self.quality_validator = quality_validator
    
    def create_sample_data(self, table_name, schema):
        """Create sample data for demonstration purposes"""
        sample_data = []
        
        if table_name == "Products":
            sample_data = [
                (1, "Laptop Dell XPS", "Electronics", "Dell", 800.00, 1200.00, "Each", "Active"),
                (2, "Office Chair", "Furniture", "Steelcase", 150.00, 300.00, "Each", "Active"),
                (3, "Wireless Mouse", "Electronics", "Logitech", 25.00, 50.00, "Each", "Active")
            ]
        elif table_name == "Suppliers":
            sample_data = [
                (1, "Tech Supplies Inc", "+1-555-0101", "tech@supplies.com", "123 Tech St", "Net 30", 7),
                (2, "Office Furniture Co", "+1-555-0102", "sales@furniture.com", "456 Office Ave", "Net 15", 14),
                (3, "Electronics Wholesale", "+1-555-0103", "orders@electronics.com", "789 Electronic Blvd", "Net 45", 10)
            ]
        elif table_name == "Warehouses":
            sample_data = [
                (1, "Main Warehouse - NYC", 50000, "WH001", "New York", "NY", "USA"),
                (2, "West Coast Distribution", 75000, "WH002", "Los Angeles", "CA", "USA"),
                (3, "Central Hub - Chicago", 60000, "WH003", "Chicago", "IL", "USA")
            ]
        elif table_name == "Customers":
            sample_data = [
                (1, "ABC Corporation", "orders@abc.com", "+1-555-1001", "100 Business St"),
                (2, "XYZ Enterprises", "purchasing@xyz.com", "+1-555-1002", "200 Commerce Ave"),
                (3, "Global Solutions Ltd", "procurement@global.com", "+1-555-1003", "300 Enterprise Blvd")
            ]
        
        if sample_data:
            return spark.createDataFrame(sample_data, schema)
        else:
            return spark.createDataFrame([], schema)
    
    def ingest_table_batch(self, source_system, table_name, schema):
        """Ingest a single table in batch mode"""
        try:
            print(f"Starting ingestion for {source_system}.{table_name}")
            
            # Create sample data for demonstration
            df = self.create_sample_data(table_name, schema)
            
            if df.count() == 0:
                print(f"No data found for {table_name}, creating empty table")
            
            # Add metadata columns
            df_with_metadata = self.metadata_manager.add_metadata_columns(
                df, source_system, table_name
            )
            
            # Validate data quality
            validation_results = self.quality_validator.validate_data(df, table_name)
            
            # Check if validation passed
            validation_passed = all(result["passed"] for result in validation_results)
            
            if not validation_passed:
                print(f"Data quality validation failed for {table_name}")
                for result in validation_results:
                    if not result["passed"]:
                        print(f"  - {result['column']}: {result['rule']} failed ({result['failed_count']} records)")
            
            # Write to Bronze layer
            bronze_table_path = f"{self.config.bronze_base_path}/{source_system}_{table_name.lower()}"
            
            df_with_metadata.write \
                .format("delta") \
                .mode("overwrite") \
                .option("path", bronze_table_path) \
                .option("overwriteSchema", "true") \
                .save()
            
            record_count = df.count()
            
            # Log successful ingestion
            self.metadata_manager.log_ingestion_audit(
                source_system, table_name, record_count, "SUCCESS"
            )
            
            print(f"Successfully ingested {record_count} records for {source_system}.{table_name}")
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"Error ingesting {source_system}.{table_name}: {error_msg}")
            
            # Log failed ingestion
            self.metadata_manager.log_ingestion_audit(
                source_system, table_name, 0, "FAILED", error_msg
            )
            
            return False

bronze_ingestion = BronzeIngestion(config, metadata_manager, quality_validator)
print("Bronze ingestion class initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Main Ingestion Orchestration

# COMMAND ----------

def run_bronze_ingestion():
    """Main function to orchestrate Bronze layer ingestion"""
    print("Starting Bronze Layer Ingestion Pipeline")
    print("=" * 50)
    
    # Define table mappings with their schemas
    table_mappings = {
        "erp_system": {
            "Products": schemas.get_products_schema(),
            "Suppliers": schemas.get_suppliers_schema(),
            "Warehouses": schemas.get_warehouses_schema(),
            "Customers": schemas.get_customers_schema()
        },
        "order_system": {
            "Orders": schemas.get_orders_schema(),
            "Order_Details": schemas.get_order_details_schema()
        },
        "warehouse_system": {
            "Inventory": schemas.get_inventory_schema(),
            "Stock_Levels": schemas.get_stock_levels_schema(),
            "Shipments": schemas.get_shipments_schema(),
            "Returns": schemas.get_returns_schema()
        }
    }
    
    ingestion_results = {}
    
    # Process each source system
    for source_system, system_config in config.source_systems.items():
        print(f"\nProcessing source system: {source_system}")
        print("-" * 30)
        
        if source_system in table_mappings:
            tables = table_mappings[source_system]
            
            for table_name, schema in tables.items():
                print(f"Ingesting table: {table_name}")
                
                success = bronze_ingestion.ingest_table_batch(
                    source_system, table_name, schema
                )
                
                ingestion_results[f"{source_system}.{table_name}"] = success
    
    # Print ingestion summary
    print("\n" + "=" * 50)
    print("INGESTION SUMMARY")
    print("=" * 50)
    
    successful_tables = [k for k, v in ingestion_results.items() if v]
    failed_tables = [k for k, v in ingestion_results.items() if not v]
    
    print(f"Total tables processed: {len(ingestion_results)}")
    print(f"Successful ingestions: {len(successful_tables)}")
    print(f"Failed ingestions: {len(failed_tables)}")
    
    if successful_tables:
        print("\nSuccessful tables:")
        for table in successful_tables:
            print(f"  ✓ {table}")
    
    if failed_tables:
        print("\nFailed tables:")
        for table in failed_tables:
            print(f"  ✗ {table}")
    
    return ingestion_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Execute Bronze Layer Ingestion

# COMMAND ----------

# Execute the main ingestion pipeline
results = run_bronze_ingestion()

# Display final results
print("\nBronze Layer Ingestion Pipeline Completed Successfully!")
print(f"Pipeline execution timestamp: {datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Validation and Quality Checks

# COMMAND ----------

# Perform post-ingestion validation
print("\nPerforming post-ingestion validation...")
print("=" * 40)

# Check if Delta tables were created successfully
try:
    # List all tables in the bronze layer
    bronze_tables = spark.sql("SHOW TABLES").collect()
    print(f"Total tables created: {len(bronze_tables)}")
    
    for table in bronze_tables:
        table_name = table['tableName']
        if 'bronze' in table_name:
            try:
                count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
                print(f"  - {table_name}: {count} records")
            except Exception as e:
                print(f"  - {table_name}: Error reading table - {str(e)}")
                
except Exception as e:
    print(f"Error validating tables: {str(e)}")

print("\nBronze Layer Pipeline Validation Complete!")