# Databricks Bronze Layer Data Engineering Pipeline
# Version: 1.0
# Purpose: Comprehensive ingestion strategy for moving data from Source to Bronze layer
# Author: Data Engineer
# Date: 2025-01-27

# ============================================================================
# BRONZE LAYER INGESTION STRATEGY - DATABRICKS
# ============================================================================

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import uuid

# ============================================================================
# 1. CONFIGURATION AND INITIALIZATION
# ============================================================================

class BronzeLayerConfig:
    """Configuration class for Bronze layer ingestion"""
    
    def __init__(self):
        self.bronze_path = "/mnt/datalake/bronze/"
        self.checkpoint_path = "/mnt/datalake/checkpoints/bronze/"
        self.metadata_path = "/mnt/datalake/metadata/bronze/"
        self.audit_log_path = "/mnt/datalake/audit/bronze/"
        self.source_systems = {
            "inventory_management": {
                "path": "/mnt/sources/inventory/",
                "format": "json",
                "schema_evolution": True
            },
            "sales_data": {
                "path": "/mnt/sources/sales/",
                "format": "parquet",
                "schema_evolution": True
            },
            "customer_data": {
                "path": "/mnt/sources/customers/",
                "format": "csv",
                "schema_evolution": False
            }
        }

# ============================================================================
# 2. METADATA TRACKING AND LINEAGE
# ============================================================================

class MetadataTracker:
    """Handles metadata tracking, lineage, and audit logging"""
    
    def __init__(self, spark: SparkSession, config: BronzeLayerConfig):
        self.spark = spark
        self.config = config
        self.batch_id = str(uuid.uuid4())
        self.ingestion_timestamp = datetime.now(timezone.utc)
        
    def create_metadata_record(self, source_system: str, source_path: str, 
                             target_path: str, record_count: int, 
                             file_size_mb: float, status: str) -> Dict[str, Any]:
        """Create metadata record for ingestion tracking"""
        return {
            "batch_id": self.batch_id,
            "ingestion_timestamp": self.ingestion_timestamp.isoformat(),
            "source_system": source_system,
            "source_path": source_path,
            "target_path": target_path,
            "record_count": record_count,
            "file_size_mb": file_size_mb,
            "status": status,
            "schema_version": "1.0",
            "data_quality_score": None,
            "processing_duration_seconds": None
        }
    
    def log_audit_event(self, event_type: str, source_system: str, 
                       message: str, status: str = "SUCCESS"):
        """Log audit events for compliance and debugging"""
        audit_record = {
            "audit_id": str(uuid.uuid4()),
            "batch_id": self.batch_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "source_system": source_system,
            "message": message,
            "status": status,
            "user": "databricks_service_principal"
        }
        
        # Convert to DataFrame and append to audit log
        audit_df = self.spark.createDataFrame([audit_record])
        audit_df.write.mode("append").format("delta").save(self.config.audit_log_path)

# ============================================================================
# 3. DATA INGESTION ENGINE
# ============================================================================

class BronzeIngestionEngine:
    """Core ingestion engine for Bronze layer"""
    
    def __init__(self, spark: SparkSession, config: BronzeLayerConfig, 
                 metadata_tracker: MetadataTracker):
        self.spark = spark
        self.config = config
        self.metadata_tracker = metadata_tracker
        
    def add_bronze_metadata_columns(self, df: DataFrame, source_system: str, 
                                  source_path: str) -> DataFrame:
        """Add standard Bronze layer metadata columns"""
        return df.withColumn("_bronze_ingestion_timestamp", 
                           lit(self.metadata_tracker.ingestion_timestamp)) \
                .withColumn("_bronze_batch_id", 
                           lit(self.metadata_tracker.batch_id)) \
                .withColumn("_bronze_source_system", lit(source_system)) \
                .withColumn("_bronze_source_path", lit(source_path)) \
                .withColumn("_bronze_record_id", 
                           expr("uuid()")) \
                .withColumn("_bronze_is_deleted", lit(False)) \
                .withColumn("_bronze_created_at", current_timestamp()) \
                .withColumn("_bronze_updated_at", current_timestamp())
    
    def ingest_json_data(self, source_system: str, source_path: str, 
                        target_path: str) -> bool:
        """Ingest JSON data with schema evolution support"""
        try:
            self.metadata_tracker.log_audit_event(
                "INGESTION_START", source_system, 
                f"Starting JSON ingestion from {source_path}")
            
            # Read JSON with schema inference
            df = self.spark.read.option("multiline", "true") \
                          .option("inferSchema", "true") \
                          .json(source_path)
            
            # Add Bronze metadata columns
            df_with_metadata = self.add_bronze_metadata_columns(
                df, source_system, source_path)
            
            # Write to Bronze layer with Delta format
            df_with_metadata.write.mode("append") \
                           .format("delta") \
                           .option("mergeSchema", "true") \
                           .save(target_path)
            
            record_count = df.count()
            self.metadata_tracker.log_audit_event(
                "INGESTION_SUCCESS", source_system, 
                f"Successfully ingested {record_count} records")
            
            return True
            
        except Exception as e:
            self.metadata_tracker.log_audit_event(
                "INGESTION_ERROR", source_system, 
                f"Error ingesting JSON data: {str(e)}", "ERROR")
            return False
    
    def ingest_parquet_data(self, source_system: str, source_path: str, 
                           target_path: str) -> bool:
        """Ingest Parquet data with optimized performance"""
        try:
            self.metadata_tracker.log_audit_event(
                "INGESTION_START", source_system, 
                f"Starting Parquet ingestion from {source_path}")
            
            # Read Parquet files
            df = self.spark.read.parquet(source_path)
            
            # Add Bronze metadata columns
            df_with_metadata = self.add_bronze_metadata_columns(
                df, source_system, source_path)
            
            # Write to Bronze layer with Delta format
            df_with_metadata.write.mode("append") \
                           .format("delta") \
                           .option("mergeSchema", "true") \
                           .save(target_path)
            
            record_count = df.count()
            self.metadata_tracker.log_audit_event(
                "INGESTION_SUCCESS", source_system, 
                f"Successfully ingested {record_count} records")
            
            return True
            
        except Exception as e:
            self.metadata_tracker.log_audit_event(
                "INGESTION_ERROR", source_system, 
                f"Error ingesting Parquet data: {str(e)}", "ERROR")
            return False
    
    def ingest_csv_data(self, source_system: str, source_path: str, 
                       target_path: str, schema: StructType = None) -> bool:
        """Ingest CSV data with optional schema enforcement"""
        try:
            self.metadata_tracker.log_audit_event(
                "INGESTION_START", source_system, 
                f"Starting CSV ingestion from {source_path}")
            
            # Read CSV with options
            reader = self.spark.read.option("header", "true") \
                                  .option("inferSchema", "true" if schema is None else "false")
            
            if schema:
                reader = reader.schema(schema)
            
            df = reader.csv(source_path)
            
            # Add Bronze metadata columns
            df_with_metadata = self.add_bronze_metadata_columns(
                df, source_system, source_path)
            
            # Write to Bronze layer with Delta format
            df_with_metadata.write.mode("append") \
                           .format("delta") \
                           .option("mergeSchema", "false" if schema else "true") \
                           .save(target_path)
            
            record_count = df.count()
            self.metadata_tracker.log_audit_event(
                "INGESTION_SUCCESS", source_system, 
                f"Successfully ingested {record_count} records")
            
            return True
            
        except Exception as e:
            self.metadata_tracker.log_audit_event(
                "INGESTION_ERROR", source_system, 
                f"Error ingesting CSV data: {str(e)}", "ERROR")
            return False

# ============================================================================
# 4. STREAMING INGESTION SUPPORT
# ============================================================================

class StreamingIngestionEngine:
    """Handles real-time streaming ingestion to Bronze layer"""
    
    def __init__(self, spark: SparkSession, config: BronzeLayerConfig):
        self.spark = spark
        self.config = config
    
    def setup_kafka_stream(self, kafka_servers: str, topic: str, 
                          target_path: str, checkpoint_path: str):
        """Setup Kafka streaming ingestion"""
        stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse Kafka messages
        parsed_stream = stream.select(
            col("key").cast("string"),
            col("value").cast("string"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        ).withColumn("_bronze_ingestion_timestamp", current_timestamp()) \
         .withColumn("_bronze_batch_id", lit(str(uuid.uuid4())))
        
        # Write stream to Bronze layer
        query = parsed_stream.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .start(target_path)
        
        return query
    
    def setup_file_stream(self, source_path: str, file_format: str, 
                         target_path: str, checkpoint_path: str):
        """Setup file-based streaming ingestion"""
        stream = self.spark.readStream \
            .format(file_format) \
            .option("path", source_path) \
            .load()
        
        # Add Bronze metadata
        stream_with_metadata = stream.withColumn(
            "_bronze_ingestion_timestamp", current_timestamp()) \
            .withColumn("_bronze_batch_id", lit(str(uuid.uuid4()))) \
            .withColumn("_bronze_source_path", lit(source_path))
        
        # Write stream to Bronze layer
        query = stream_with_metadata.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .start(target_path)
        
        return query

# ============================================================================
# 5. DATA QUALITY AND VALIDATION
# ============================================================================

class DataQualityValidator:
    """Performs data quality checks on Bronze layer data"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def validate_data_freshness(self, df: DataFrame, 
                               max_age_hours: int = 24) -> Dict[str, Any]:
        """Check if data is within acceptable freshness window"""
        current_time = datetime.now(timezone.utc)
        max_timestamp = df.agg(max("_bronze_ingestion_timestamp")).collect()[0][0]
        
        if max_timestamp:
            age_hours = (current_time - max_timestamp).total_seconds() / 3600
            is_fresh = age_hours <= max_age_hours
        else:
            age_hours = None
            is_fresh = False
        
        return {
            "check_name": "data_freshness",
            "is_valid": is_fresh,
            "age_hours": age_hours,
            "max_age_hours": max_age_hours
        }
    
    def validate_record_count(self, df: DataFrame, 
                             min_records: int = 1) -> Dict[str, Any]:
        """Validate minimum record count"""
        record_count = df.count()
        is_valid = record_count >= min_records
        
        return {
            "check_name": "record_count",
            "is_valid": is_valid,
            "record_count": record_count,
            "min_records": min_records
        }
    
    def validate_schema_compliance(self, df: DataFrame, 
                                  required_columns: List[str]) -> Dict[str, Any]:
        """Validate schema compliance"""
        df_columns = set(df.columns)
        required_columns_set = set(required_columns)
        missing_columns = required_columns_set - df_columns
        
        is_valid = len(missing_columns) == 0
        
        return {
            "check_name": "schema_compliance",
            "is_valid": is_valid,
            "missing_columns": list(missing_columns),
            "required_columns": required_columns
        }

# ============================================================================
# 6. MAIN ORCHESTRATION CLASS
# ============================================================================

class BronzeLayerOrchestrator:
    """Main orchestration class for Bronze layer operations"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("BronzeLayerIngestion") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        self.config = BronzeLayerConfig()
        self.metadata_tracker = MetadataTracker(self.spark, self.config)
        self.ingestion_engine = BronzeIngestionEngine(
            self.spark, self.config, self.metadata_tracker)
        self.streaming_engine = StreamingIngestionEngine(self.spark, self.config)
        self.quality_validator = DataQualityValidator(self.spark)
    
    def run_batch_ingestion(self):
        """Execute batch ingestion for all configured source systems"""
        print("Starting Bronze Layer Batch Ingestion...")
        
        for source_system, config in self.config.source_systems.items():
            print(f"Processing source system: {source_system}")
            
            source_path = config["path"]
            target_path = f"{self.config.bronze_path}{source_system}/"
            file_format = config["format"]
            
            success = False
            
            if file_format == "json":
                success = self.ingestion_engine.ingest_json_data(
                    source_system, source_path, target_path)
            elif file_format == "parquet":
                success = self.ingestion_engine.ingest_parquet_data(
                    source_system, source_path, target_path)
            elif file_format == "csv":
                success = self.ingestion_engine.ingest_csv_data(
                    source_system, source_path, target_path)
            
            if success:
                print(f"✅ Successfully ingested {source_system}")
                # Run data quality checks
                self.run_quality_checks(target_path, source_system)
            else:
                print(f"❌ Failed to ingest {source_system}")
    
    def run_quality_checks(self, table_path: str, source_system: str):
        """Run data quality validation checks"""
        try:
            df = self.spark.read.format("delta").load(table_path)
            
            # Run quality checks
            freshness_check = self.quality_validator.validate_data_freshness(df)
            count_check = self.quality_validator.validate_record_count(df)
            
            print(f"Data Quality Results for {source_system}:")
            print(f"  - Freshness: {'✅' if freshness_check['is_valid'] else '❌'}")
            print(f"  - Record Count: {'✅' if count_check['is_valid'] else '❌'} ({count_check['record_count']} records)")
            
        except Exception as e:
            print(f"❌ Quality check failed for {source_system}: {str(e)}")
    
    def setup_streaming_ingestion(self, kafka_servers: str = None):
        """Setup streaming ingestion pipelines"""
        print("Setting up streaming ingestion...")
        
        if kafka_servers:
            # Setup Kafka streams for real-time data
            kafka_query = self.streaming_engine.setup_kafka_stream(
                kafka_servers, "inventory_events", 
                f"{self.config.bronze_path}inventory_stream/",
                f"{self.config.checkpoint_path}inventory_stream/")
            print("✅ Kafka streaming setup complete")
        
        # Setup file-based streaming
        file_query = self.streaming_engine.setup_file_stream(
            "/mnt/sources/streaming/", "json",
            f"{self.config.bronze_path}file_stream/",
            f"{self.config.checkpoint_path}file_stream/")
        
        print("✅ File streaming setup complete")
        return [kafka_query] if kafka_servers else [], [file_query]

# ============================================================================
# 7. EXECUTION ENTRY POINT
# ============================================================================

def main():
    """Main execution function"""
    print("="*80)
    print("DATABRICKS BRONZE LAYER INGESTION PIPELINE")
    print("="*80)
    
    # Initialize orchestrator
    orchestrator = BronzeLayerOrchestrator()
    
    try:
        # Run batch ingestion
        orchestrator.run_batch_ingestion()
        
        # Optionally setup streaming (uncomment if needed)
        # kafka_queries, file_queries = orchestrator.setup_streaming_ingestion()
        
        print("\n✅ Bronze Layer Ingestion Pipeline completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        raise
    
    finally:
        # Stop Spark session
        orchestrator.spark.stop()

# ============================================================================
# 8. UTILITY FUNCTIONS
# ============================================================================

def create_bronze_table_ddl(table_name: str, source_schema: StructType) -> str:
    """Generate DDL for creating Bronze layer Delta tables"""
    
    columns = []
    for field in source_schema.fields:
        columns.append(f"{field.name} {field.dataType.simpleString()}")
    
    # Add Bronze metadata columns
    bronze_columns = [
        "_bronze_ingestion_timestamp TIMESTAMP",
        "_bronze_batch_id STRING",
        "_bronze_source_system STRING",
        "_bronze_source_path STRING",
        "_bronze_record_id STRING",
        "_bronze_is_deleted BOOLEAN",
        "_bronze_created_at TIMESTAMP",
        "_bronze_updated_at TIMESTAMP"
    ]
    
    all_columns = columns + bronze_columns
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {',\n        '.join(all_columns)}
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    return ddl

def optimize_bronze_tables(spark: SparkSession, bronze_path: str):
    """Optimize Bronze layer Delta tables"""
    print("Optimizing Bronze layer tables...")
    
    # Get list of Delta tables in Bronze layer
    tables = spark.sql(f"SHOW TABLES IN delta.`{bronze_path}`").collect()
    
    for table in tables:
        table_name = table['tableName']
        try:
            # Run OPTIMIZE command
            spark.sql(f"OPTIMIZE delta.`{bronze_path}{table_name}`")
            
            # Run VACUUM to clean up old files (retain 7 days)
            spark.sql(f"VACUUM delta.`{bronze_path}{table_name}` RETAIN 168 HOURS")
            
            print(f"✅ Optimized table: {table_name}")
            
        except Exception as e:
            print(f"❌ Failed to optimize table {table_name}: {str(e)}")

# ============================================================================
# EXECUTION
# ============================================================================

if __name__ == "__main__":
    main()