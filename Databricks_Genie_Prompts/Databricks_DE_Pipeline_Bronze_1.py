# Databricks Bronze Layer Data Engineering Pipeline
# Inventory Management System - Bronze Layer Implementation

import uuid
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bronze_Layer_Ingestion_Pipeline") \
    .getOrCreate()

print("=== Databricks Bronze Layer Pipeline Started ===")
print(f"Pipeline Start Time: {datetime.now()}")

# Configuration
class BronzePipelineConfig:
    def __init__(self):
        self.source_system = "PostgreSQL_DE"
        self.bronze_schema = "default"
        self.batch_id = str(uuid.uuid4())
        self.processing_timestamp = datetime.now()
        
        # Table configurations
        self.table_configs = {
            "products": {"primary_key": "Product_ID"},
            "suppliers": {"primary_key": "Supplier_ID"},
            "warehouses": {"primary_key": "Warehouse_ID"},
            "inventory": {"primary_key": "Inventory_ID"},
            "customers": {"primary_key": "Customer_ID"}
        }

config = BronzePipelineConfig()

print(f"Using schema: {config.bronze_schema}")
print(f"Batch ID: {config.batch_id}")

# Utility Functions
def calculate_data_quality_score(df, table_name):
    """Calculate data quality score for a DataFrame"""
    total_records = df.count()
    if total_records == 0:
        return 0
    
    # Count null values across all columns
    null_counts = {}
    for column in df.columns:
        if column not in ['load_timestamp', 'update_timestamp', 'source_system', 'batch_id']:
            null_count = df.filter(col(column).isNull()).count()
            null_counts[column] = null_count
    
    total_nulls = sum(null_counts.values())
    total_cells = total_records * len([c for c in df.columns if c not in ['load_timestamp', 'update_timestamp', 'source_system', 'batch_id']])
    
    if total_cells == 0:
        return 100
    
    null_percentage = (total_nulls / total_cells) * 100
    quality_score = max(0, 100 - (null_percentage * 2))  # Penalize nulls
    
    return round(quality_score, 2)

def add_metadata_columns(df, table_name):
    """Add standard metadata columns to DataFrame"""
    current_timestamp = datetime.now()
    
    # Create a hash for each record
    columns_for_hash = [c for c in df.columns if c not in ['load_timestamp', 'update_timestamp']]
    hash_expr = concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in columns_for_hash])
    
    df_with_metadata = df \
        .withColumn("load_timestamp", lit(current_timestamp)) \
        .withColumn("update_timestamp", lit(current_timestamp)) \
        .withColumn("source_system", lit(config.source_system)) \
        .withColumn("record_status", lit("ACTIVE")) \
        .withColumn("batch_id", lit(config.batch_id)) \
        .withColumn("file_name", lit(f"{table_name}_batch_{config.batch_id[:8]}")) \
        .withColumn("record_hash", md5(hash_expr))
    
    # Calculate and add data quality score
    quality_score = calculate_data_quality_score(df, table_name)
    df_with_metadata = df_with_metadata.withColumn("data_quality_score", lit(quality_score))
    
    return df_with_metadata

def create_sample_data(table_name):
    """Create sample data for demonstration purposes"""
    print(f"Creating sample data for {table_name}...")
    
    if table_name == "products":
        data = [
            (1, "Laptop", "Electronics"),
            (2, "Chair", "Furniture"),
            (3, "T-Shirt", "Apparel"),
            (4, "Phone", "Electronics"),
            (5, "Desk", "Furniture")
        ]
        schema = StructType([
            StructField("Product_ID", IntegerType(), True),
            StructField("Product_Name", StringType(), True),
            StructField("Category", StringType(), True)
        ])
        
    elif table_name == "suppliers":
        data = [
            (1, "Tech Supplier Inc", "+1-555-0101", 1),
            (2, "Furniture World", "+1-555-0102", 2),
            (3, "Apparel Co", "+1-555-0103", 3),
            (4, "Electronics Hub", "+1-555-0104", 4),
            (5, "Office Solutions", "+1-555-0105", 5)
        ]
        schema = StructType([
            StructField("Supplier_ID", IntegerType(), True),
            StructField("Supplier_Name", StringType(), True),
            StructField("Contact_Number", StringType(), True),
            StructField("Product_ID", IntegerType(), True)
        ])
        
    elif table_name == "warehouses":
        data = [
            (1, "New York Warehouse", 10000),
            (2, "Los Angeles Warehouse", 15000),
            (3, "Chicago Warehouse", 12000)
        ]
        schema = StructType([
            StructField("Warehouse_ID", IntegerType(), True),
            StructField("Location", StringType(), True),
            StructField("Capacity", IntegerType(), True)
        ])
        
    elif table_name == "customers":
        data = [
            (1, "John Doe", "john.doe@email.com"),
            (2, "Jane Smith", "jane.smith@email.com"),
            (3, "Bob Johnson", "bob.johnson@email.com"),
            (4, "Alice Brown", "alice.brown@email.com"),
            (5, "Charlie Wilson", "charlie.wilson@email.com")
        ]
        schema = StructType([
            StructField("Customer_ID", IntegerType(), True),
            StructField("Customer_Name", StringType(), True),
            StructField("Email", StringType(), True)
        ])
        
    elif table_name == "inventory":
        data = [
            (1, 1, 100, 1),
            (2, 2, 50, 1),
            (3, 3, 200, 2),
            (4, 4, 75, 2),
            (5, 5, 30, 3)
        ]
        schema = StructType([
            StructField("Inventory_ID", IntegerType(), True),
            StructField("Product_ID", IntegerType(), True),
            StructField("Quantity_Available", IntegerType(), True),
            StructField("Warehouse_ID", IntegerType(), True)
        ])
        
    else:
        # Default empty DataFrame for other tables
        data = []
        schema = StructType([StructField("id", IntegerType(), True)])
    
    return spark.createDataFrame(data, schema)

def create_bronze_table(table_name):
    """Create Bronze layer table with proper schema"""
    bronze_table_name = f"bz_{table_name}"
    full_table_name = f"{config.bronze_schema}.{bronze_table_name}"
    
    print(f"Creating Bronze table: {full_table_name}")
    
    try:
        # Get sample data to infer schema
        sample_df = create_sample_data(table_name)
        
        # Add metadata columns
        df_with_metadata = add_metadata_columns(sample_df, table_name)
        
        # Write data to create the table
        df_with_metadata.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(full_table_name)
        
        record_count = df_with_metadata.count()
        print(f"✅ Successfully created table {full_table_name} with {record_count} records")
        return True, record_count
        
    except Exception as e:
        print(f"❌ Error creating table {full_table_name}: {str(e)}")
        return False, 0

# Main Pipeline Execution
print("\n=== Starting Bronze Layer Table Creation ===")

# Create all Bronze layer tables
tables_to_create = ["products", "suppliers", "warehouses", "customers", "inventory"]
success_count = 0
failed_count = 0
total_records = 0

for table_name in tables_to_create:
    print(f"\n--- Processing table: {table_name} ---")
    
    success, record_count = create_bronze_table(table_name)
    if success:
        success_count += 1
        total_records += record_count
    else:
        failed_count += 1

# Display pipeline summary
print("\n=== Pipeline Execution Summary ===")
print(f"Total tables processed: {len(tables_to_create)}")
print(f"Successfully created: {success_count}")
print(f"Failed: {failed_count}")
print(f"Total records created: {total_records}")
print(f"Batch ID: {config.batch_id}")
print(f"Processing completed at: {datetime.now()}")

# Verify created tables
print("\n=== Verifying Created Tables ===")
try:
    tables_in_schema = spark.sql(f"SHOW TABLES IN {config.bronze_schema}").collect()
    print(f"Tables in {config.bronze_schema}:")
    for table in tables_in_schema:
        table_name = table['tableName']
        if table_name.startswith('bz_'):
            record_count = spark.sql(f"SELECT COUNT(*) as count FROM {config.bronze_schema}.{table_name}").collect()[0]['count']
            print(f"  - {table_name}: {record_count} records")
except Exception as e:
    print(f"Error verifying tables: {str(e)}")

# Data Quality Report
print("\n=== Data Quality Report ===")
for table_name in tables_to_create:
    try:
        bronze_table_name = f"bz_{table_name}"
        full_table_name = f"{config.bronze_schema}.{bronze_table_name}"
        
        quality_df = spark.sql(f"""
            SELECT 
                '{table_name}' as table_name,
                COUNT(*) as total_records,
                AVG(data_quality_score) as avg_quality_score,
                MIN(data_quality_score) as min_quality_score,
                MAX(data_quality_score) as max_quality_score
            FROM {full_table_name}
        """)
        
        quality_result = quality_df.collect()[0]
        print(f"  {table_name}:")
        print(f"    - Total Records: {quality_result['total_records']}")
        print(f"    - Avg Quality Score: {quality_result['avg_quality_score']:.2f}")
        print(f"    - Quality Range: {quality_result['min_quality_score']:.2f} - {quality_result['max_quality_score']:.2f}")
        
    except Exception as e:
        print(f"  {table_name}: Error generating quality report - {str(e)}")

print("\n=== Bronze Layer Pipeline Completed Successfully ===")
print(f"Pipeline End Time: {datetime.now()}")
print(f"Total Processing Time: {datetime.now() - config.processing_timestamp}")

# Final validation
if success_count == len(tables_to_create):
    print("\n🎉 All tables created successfully! Bronze layer is ready for Silver layer processing.")
else:
    print(f"\n⚠️ Warning: {failed_count} tables failed to create. Please review the logs.")

# Create audit summary
audit_summary = {
    "pipeline_run_id": config.batch_id,
    "execution_timestamp": datetime.now().isoformat(),
    "total_tables_processed": len(tables_to_create),
    "successful_tables": success_count,
    "failed_tables": failed_count,
    "total_records_created": total_records,
    "processing_duration_seconds": int((datetime.now() - config.processing_timestamp).total_seconds()),
    "status": "SUCCESS" if failed_count == 0 else "PARTIAL_SUCCESS"
}

print(f"\n=== Audit Summary ===")
for key, value in audit_summary.items():
    print(f"{key}: {value}")

print("\n=== Pipeline execution completed ===")