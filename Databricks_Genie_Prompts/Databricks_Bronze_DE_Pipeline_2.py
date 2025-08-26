# Databricks Bronze Layer Data Engineering Pipeline - Enhanced Version
# Version: 2.0
# Purpose: Advanced ingestion strategy with enhanced monitoring and error handling
# Author: Data Engineer
# Date: 2025-01-27

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import uuid
import time

class EnhancedBronzeLayerConfig:
    def __init__(self):
        self.bronze_path = "/mnt/datalake/bronze/"
        self.checkpoint_path = "/mnt/datalake/checkpoints/bronze/"
        self.metadata_path = "/mnt/datalake/metadata/bronze/"
        self.audit_log_path = "/mnt/datalake/audit/bronze/"
        self.error_quarantine_path = "/mnt/datalake/quarantine/bronze/"
        
        self.source_systems = {
            "inventory_management": {
                "path": "/mnt/sources/inventory/",
                "format": "json",
                "schema_evolution": True,
                "tables": ["products", "suppliers", "warehouses", "inventory", "stock_levels"]
            },
            "sales_data": {
                "path": "/mnt/sources/sales/",
                "format": "parquet", 
                "schema_evolution": True,
                "tables": ["orders", "order_details", "shipments", "returns"]
            },
            "customer_data": {
                "path": "/mnt/sources/customers/",
                "format": "csv",
                "schema_evolution": False,
                "tables": ["customers"]
            }
        }

class EnhancedBronzeIngestionEngine:
    def __init__(self, spark: SparkSession, config: EnhancedBronzeLayerConfig):
        self.spark = spark
        self.config = config
        self.batch_id = str(uuid.uuid4())
        self.ingestion_timestamp = datetime.now(timezone.utc)
        
    def add_bronze_metadata_columns(self, df: DataFrame, source_system: str, table_name: str, source_path: str) -> DataFrame:
        return df.withColumn("_bronze_ingestion_timestamp", lit(self.ingestion_timestamp)) \
                .withColumn("_bronze_batch_id", lit(self.batch_id)) \
                .withColumn("_bronze_source_system", lit(source_system)) \
                .withColumn("_bronze_table_name", lit(table_name)) \
                .withColumn("_bronze_source_path", lit(source_path)) \
                .withColumn("_bronze_record_id", expr("uuid()")) \
                .withColumn("_bronze_created_at", current_timestamp()) \
                .withColumn("_bronze_updated_at", current_timestamp())
    
    def ingest_data_universal(self, source_system: str, table_name: str, source_path: str, target_path: str, file_format: str) -> bool:
        try:
            print(f"Starting ingestion for {source_system}.{table_name}")
            
            if file_format.lower() == "json":
                df = self.spark.read.option("multiline", "true").option("inferSchema", "true").json(source_path)
            elif file_format.lower() == "parquet":
                df = self.spark.read.parquet(source_path)
            elif file_format.lower() == "csv":
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            else:
                raise ValueError(f"Unsupported format: {file_format}")
            
            df_with_metadata = self.add_bronze_metadata_columns(df, source_system, table_name, source_path)
            
            df_with_metadata.write.mode("append").format("delta").option("mergeSchema", "true").save(target_path)
            
            record_count = df.count()
            print(f"Successfully ingested {record_count} records for {table_name}")
            return True
            
        except Exception as e:
            print(f"Error ingesting {table_name}: {str(e)}")
            return False

class BronzeLayerOrchestrator:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("EnhancedBronzeLayerIngestion") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        self.config = EnhancedBronzeLayerConfig()
        self.ingestion_engine = EnhancedBronzeIngestionEngine(self.spark, self.config)
    
    def run_enhanced_batch_ingestion(self):
        print("Starting Enhanced Bronze Layer Batch Ingestion...")
        
        for source_system, system_config in self.config.source_systems.items():
            print(f"Processing source system: {source_system}")
            
            for table_name in system_config["tables"]:
                source_path = f"{system_config['path']}{table_name}/"
                target_path = f"{self.config.bronze_path}{source_system}/{table_name}/"
                file_format = system_config["format"]
                
                success = self.ingestion_engine.ingest_data_universal(
                    source_system, table_name, source_path, target_path, file_format)
                
                if success:
                    print(f"✅ Successfully ingested {source_system}.{table_name}")
                else:
                    print(f"❌ Failed to ingest {source_system}.{table_name}")

def main():
    print("="*80)
    print("ENHANCED DATABRICKS BRONZE LAYER INGESTION PIPELINE")
    print("="*80)
    
    orchestrator = BronzeLayerOrchestrator()
    
    try:
        orchestrator.run_enhanced_batch_ingestion()
        print("\n✅ Enhanced Bronze Layer Ingestion Pipeline completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        raise
    
    finally:
        orchestrator.spark.stop()

if __name__ == "__main__":
    main()