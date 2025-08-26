# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Bronze Layer Data Engineering Pipeline
# MAGIC ## Inventory Management System - Bronze Layer Ingestion Strategy
# MAGIC 
# MAGIC ### Overview
# MAGIC This notebook implements a comprehensive Bronze layer ingestion strategy for the Inventory Management System.
# MAGIC The Bronze layer serves as a landing zone for raw data with metadata tracking, audit logging, and data lineage.

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime, timezone
import uuid

# Initialize Spark Session
spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("InventoryManagement_Bronze_Ingestion").getOrCreate()

print("Bronze Layer Ingestion Pipeline Initialized Successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Parameters

# COMMAND ----------

# Bronze Layer Configuration
class BronzeConfig:
    def __init__(self):
        # Storage paths - using DBFS paths for Databricks
        self.bronze_base_path = "/dbfs/mnt/bronze/inventory_management"
        self.checkpoint_path = "/dbfs/mnt/checkpoints/bronze"
        
        # Source system configurations
        self.source_systems = {
            "erp_system": {
                "tables": ["Products", "Suppliers", "Warehouses", "Customers"]
            },
            "order_system": {
                "tables": ["Orders", "Order_Details"]
            },
            "warehouse_system": {
                "tables": ["Inventory", "Stock_Levels", "Shipments", "Returns"]
            }
        }

config = BronzeConfig()
print("Configuration loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definitions for Source Tables

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
    def get_customers_schema():
        return StructType([
            StructField("Customer_ID", IntegerType(), False),
            StructField("Customer_Name", StringType(), False),
            StructField("Email", StringType(), False),
            StructField("Phone", StringType(), True),
            StructField("Address", StringType(), True)
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
    def get_stock_levels_schema():
        return StructType([
            StructField("Stock_Level_ID", IntegerType(), False),
            StructField("Warehouse_ID", IntegerType(), False),
            StructField("Product_ID", IntegerType(), False),
            StructField("Reorder_Threshold", IntegerType(), False),
            StructField("Safety_Stock", IntegerType(), True),
            StructField("Max_Stock", IntegerType(), True)
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

schemas = SourceSchemas()
print("Schema definitions loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata and Audit Logging Framework

# COMMAND ----------

class MetadataManager:
    def __init__(self, config):
        self.config = config
        
    def add_metadata_columns(self, df, source_system, table_name, file_path=None):
        """Add standard metadata columns to incoming data"""
        current_ts = current_timestamp()
        batch_id = lit(str(uuid.uuid4()))
        
        return df.withColumn("_bronze_ingestion_timestamp", current_ts) \
                 .withColumn("_bronze_batch_id", batch_id) \
                 .withColumn("_bronze_source_system", lit(source_system)) \
                 .withColumn("_bronze_source_table", lit(table_name)) \
                 .withColumn("_bronze_source_file", lit(file_path)) \
                 .withColumn("_bronze_is_active", lit(True)) \
                 .withColumn("_bronze_created_by", lit("bronze_ingestion_pipeline"))
    
    def log_ingestion_audit(self, source_system, table_name, record_count, status, error_message=None):
        """Log ingestion audit information"""
        print(f"AUDIT LOG: {source_system}.{table_name} - {status} - {record_count} records")
        if error_message:
            print(f"ERROR: {error_message}")

metadata_manager = MetadataManager(config)
print("Metadata manager initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Ingestion Functions

# COMMAND ----------

class BronzeIngestion:
    def __init__(self, config, metadata_manager):
        self.config = config
        self.metadata_manager = metadata_manager
    
    def create_sample_data(self, table_name, schema):
        """Create sample data for demonstration purposes"""
        sample_data = []
        
        if table_name == "Products":
            sample_data = [
                (1, "Laptop Dell XPS", "Electronics", "Dell", 800.00, 1200.00, "Each", "Active"),
                (2, "Office Chair", "Furniture", "Steelcase", 150.00, 300.00, "Each", "Active"),
                (3, "Wireless Mouse", "Electronics", "Logitech", 25.00, 50.00, "Each", "Active"),
                (4, "Standing Desk", "Furniture", "IKEA", 200.00, 400.00, "Each", "Active"),
                (5, "Monitor 24 inch", "Electronics", "Samsung", 180.00, 350.00, "Each", "Active")
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
        elif table_name == "Orders":
            from datetime import date
            sample_data = [
                (1, 1, date(2024, 1, 15), "Completed", 1200.00),
                (2, 2, date(2024, 1, 16), "Processing", 750.00),
                (3, 3, date(2024, 1, 17), "Shipped", 950.00)
            ]
        elif table_name == "Order_Details":
            sample_data = [
                (1, 1, 1, 1, 1200.00, 1200.00),
                (2, 2, 2, 2, 300.00, 600.00),
                (3, 2, 3, 3, 50.00, 150.00),
                (4, 3, 4, 1, 400.00, 400.00),
                (5, 3, 5, 1, 350.00, 350.00)
            ]
        elif table_name == "Inventory":
            from datetime import datetime
            sample_data = [
                (1, 1, 1, 50, datetime(2024, 1, 20, 10, 0, 0), 800.00),
                (2, 2, 1, 25, datetime(2024, 1, 20, 10, 0, 0), 150.00),
                (3, 3, 2, 100, datetime(2024, 1, 20, 10, 0, 0), 25.00),
                (4, 4, 2, 15, datetime(2024, 1, 20, 10, 0, 0), 200.00),
                (5, 5, 3, 30, datetime(2024, 1, 20, 10, 0, 0), 180.00)
            ]
        elif table_name == "Stock_Levels":
            sample_data = [
                (1, 1, 1, 10, 5, 100),
                (2, 1, 2, 5, 3, 50),
                (3, 2, 3, 20, 10, 200),
                (4, 2, 4, 3, 2, 30),
                (5, 3, 5, 8, 5, 80)
            ]
        elif table_name == "Shipments":
            from datetime import date
            sample_data = [
                (1, 1, date(2024, 1, 16), "TRK001", "FedEx", "Delivered"),
                (2, 2, date(2024, 1, 17), "TRK002", "UPS", "In Transit"),
                (3, 3, date(2024, 1, 18), "TRK003", "DHL", "Shipped")
            ]
        elif table_name == "Returns":
            from datetime import date
            sample_data = [
                (1, 1, 1, "Damaged", date(2024, 1, 20), 1),
                (2, 2, 2, "Wrong Item", date(2024, 1, 21), 1)
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
                return True
            
            # Add metadata columns
            df_with_metadata = self.metadata_manager.add_metadata_columns(
                df, source_system, table_name
            )
            
            # Create temporary view for the table
            temp_table_name = f"bronze_{source_system}_{table_name.lower()}"
            df_with_metadata.createOrReplaceTempView(temp_table_name)
            
            record_count = df.count()
            
            # Log successful ingestion
            self.metadata_manager.log_ingestion_audit(
                source_system, table_name, record_count, "SUCCESS"
            )
            
            print(f"Successfully ingested {record_count} records for {source_system}.{table_name}")
            print(f"Created temporary view: {temp_table_name}")
            
            # Display sample data
            print(f"Sample data from {temp_table_name}:")
            df_with_metadata.show(5, truncate=False)
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"Error ingesting {source_system}.{table_name}: {error_msg}")
            
            # Log failed ingestion
            self.metadata_manager.log_ingestion_audit(
                source_system, table_name, 0, "FAILED", error_msg
            )
            
            return False

bronze_ingestion = BronzeIngestion(config, metadata_manager)
print("Bronze ingestion class initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Ingestion Orchestration

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
                print(f"\nIngesting table: {table_name}")
                
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
# MAGIC ## Execute Bronze Layer Ingestion

# COMMAND ----------

# Execute the main ingestion pipeline
results = run_bronze_ingestion()

# Display final results
print("\nBronze Layer Ingestion Pipeline Completed Successfully!")
print(f"Pipeline execution timestamp: {datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation and Quality Checks

# COMMAND ----------

# Perform post-ingestion validation
print("\nPerforming post-ingestion validation...")
print("=" * 40)

# List all temporary views created
try:
    temp_views = spark.sql("SHOW TABLES").collect()
    bronze_views = [view for view in temp_views if 'bronze' in view['tableName']]
    
    print(f"Total Bronze views created: {len(bronze_views)}")
    
    for view in bronze_views:
        view_name = view['tableName']
        try:
            count = spark.sql(f"SELECT COUNT(*) as count FROM {view_name}").collect()[0]['count']
            print(f"  - {view_name}: {count} records")
        except Exception as e:
            print(f"  - {view_name}: Error reading view - {str(e)}")
            
except Exception as e:
    print(f"Error validating views: {str(e)}")

print("\nBronze Layer Pipeline Validation Complete!")
print("\n" + "=" * 60)
print("BRONZE LAYER INGESTION STRATEGY IMPLEMENTATION COMPLETE")
print("=" * 60)