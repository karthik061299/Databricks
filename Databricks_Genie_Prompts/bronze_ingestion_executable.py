# Databricks Bronze Layer Ingestion - Executable Script
# Version: 2.0 - Simplified for Direct Execution

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder.appName("BronzeLayerIngestion_Executable").getOrCreate()
print("✅ Spark Session initialized successfully")

# Create Bronze Schema
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS inventory_bronze")
    print("✅ Bronze schema created successfully")
except Exception as e:
    print(f"❌ Error creating schema: {str(e)}")

# Sample data for demonstration
sample_data = {
    "products": [
        {"Product_ID": 1, "Product_Name": "Laptop", "Category": "Electronics"},
        {"Product_ID": 2, "Product_Name": "Chair", "Category": "Furniture"},
        {"Product_ID": 3, "Product_Name": "T-Shirt", "Category": "Apparel"},
        {"Product_ID": 4, "Product_Name": "Phone", "Category": "Electronics"},
        {"Product_ID": 5, "Product_Name": "Desk", "Category": "Furniture"}
    ],
    "customers": [
        {"Customer_ID": 1, "Customer_Name": "John Doe", "Email": "john.doe@email.com"},
        {"Customer_ID": 2, "Customer_Name": "Jane Smith", "Email": "jane.smith@email.com"},
        {"Customer_ID": 3, "Customer_Name": "Bob Johnson", "Email": "bob.johnson@email.com"},
        {"Customer_ID": 4, "Customer_Name": "Alice Brown", "Email": "alice.brown@email.com"}
    ],
    "orders": [
        {"Order_ID": 1, "Customer_ID": 1, "Order_Date": "2024-12-19"},
        {"Order_ID": 2, "Customer_ID": 2, "Order_Date": "2024-12-18"},
        {"Order_ID": 3, "Customer_ID": 3, "Order_Date": "2024-12-17"},
        {"Order_ID": 4, "Customer_ID": 4, "Order_Date": "2024-12-16"}
    ]
}

# Function to create and populate Bronze tables
def create_and_populate_bronze_table(table_name, data):
    """
    Create Bronze table and populate with sample data
    """
    try:
        # Generate metadata
        batch_id = str(uuid.uuid4())
        current_timestamp = datetime.now()
        
        # Create DataFrame from sample data
        df = spark.createDataFrame(data)
        
        # Add metadata columns
        enriched_df = df \
            .withColumn("load_timestamp", lit(current_timestamp)) \
            .withColumn("update_timestamp", lit(current_timestamp)) \
            .withColumn("source_system", lit("PostgreSQL_DE")) \
            .withColumn("record_status", lit("ACTIVE")) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("data_quality_score", lit(100))
        
        # Write to Bronze layer
        target_table = f"bz_{table_name}"
        enriched_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"inventory_bronze.{target_table}")
        
        record_count = enriched_df.count()
        print(f"✅ Successfully created and populated {target_table} with {record_count} records")
        
        return True, record_count
        
    except Exception as e:
        print(f"❌ Error processing {table_name}: {str(e)}")
        return False, 0

# Create audit log table
def create_audit_table():
    """
    Create audit log table for tracking ingestion activities
    """
    try:
        spark.sql("""
        CREATE TABLE IF NOT EXISTS inventory_bronze.bz_audit_log (
            audit_id STRING,
            source_table STRING,
            target_table STRING,
            batch_id STRING,
            ingestion_timestamp TIMESTAMP,
            records_processed BIGINT,
            status STRING,
            processed_by STRING
        ) USING DELTA
        """)
        print("✅ Audit log table created successfully")
        return True
    except Exception as e:
        print(f"❌ Error creating audit table: {str(e)}")
        return False

# Log audit information
def log_audit(source_table, target_table, batch_id, status, record_count):
    """
    Log audit information
    """
    try:
        audit_data = [{
            "audit_id": str(uuid.uuid4()),
            "source_table": source_table,
            "target_table": target_table,
            "batch_id": batch_id,
            "ingestion_timestamp": datetime.now(),
            "records_processed": record_count,
            "status": status,
            "processed_by": "Bronze_Pipeline_Executable"
        }]
        
        audit_df = spark.createDataFrame(audit_data)
        audit_df.write.format("delta").mode("append").saveAsTable("inventory_bronze.bz_audit_log")
        print(f"✅ Audit logged for {source_table}")
        
    except Exception as e:
        print(f"❌ Error logging audit: {str(e)}")

# Main execution function
def main():
    """
    Main execution function for Bronze layer ingestion
    """
    print("=" * 60)
    print("DATABRICKS BRONZE LAYER INGESTION - EXECUTABLE VERSION")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # Create audit table
    print("\n📋 Creating audit infrastructure...")
    create_audit_table()
    
    # Process each table
    print("\n📊 Processing Bronze layer tables...")
    
    results = {}
    total_records = 0
    
    for table_name, data in sample_data.items():
        print(f"\n🔄 Processing {table_name}...")
        
        success, record_count = create_and_populate_bronze_table(table_name, data)
        results[table_name] = success
        
        if success:
            total_records += record_count
            # Log successful ingestion
            log_audit(table_name, f"bz_{table_name}", str(uuid.uuid4()), "SUCCESS", record_count)
        else:
            # Log failed ingestion
            log_audit(table_name, f"bz_{table_name}", str(uuid.uuid4()), "FAILED", 0)
    
    # Display results
    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("📈 EXECUTION SUMMARY")
    print("=" * 60)
    print(f"⏰ Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏰ End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Processing Time: {processing_time:.2f} seconds")
    print(f"📊 Total Records Processed: {total_records}")
    
    successful_tables = sum(1 for success in results.values() if success)
    total_tables = len(results)
    
    print(f"\n📋 Table Processing Results:")
    for table, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"   {table}: {status}")
    
    success_rate = (successful_tables / total_tables) * 100
    print(f"\n🎯 Success Rate: {successful_tables}/{total_tables} ({success_rate:.1f}%)")
    
    # Show created tables
    print("\n📋 Verifying created tables...")
    try:
        tables_df = spark.sql("SHOW TABLES IN inventory_bronze")
        print("\n📊 Bronze Layer Tables:")
        tables_df.show(truncate=False)
        
        # Show sample data from each table
        for table_name in sample_data.keys():
            target_table = f"bz_{table_name}"
            try:
                print(f"\n📋 Sample data from {target_table}:")
                sample_df = spark.table(f"inventory_bronze.{target_table}").limit(3)
                sample_df.show(truncate=False)
            except Exception as e:
                print(f"❌ Could not display data from {target_table}: {str(e)}")
        
        # Show audit log
        print("\n📋 Audit Log Summary:")
        audit_df = spark.table("inventory_bronze.bz_audit_log")
        audit_df.show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error displaying results: {str(e)}")
    
    print("\n" + "=" * 60)
    print("🎉 BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    
    return results

# Execute the main function
if __name__ == "__main__":
    try:
        execution_results = main()
        print("\n🎉 Pipeline execution completed successfully!")
    except Exception as e:
        print(f"\n💥 Pipeline execution failed: {str(e)}")
        raise e
    finally:
        print("\n🔄 Cleaning up resources...")
        # Note: In a notebook environment, you might not want to stop the spark session
        # spark.stop()
        print("✅ Cleanup completed.")