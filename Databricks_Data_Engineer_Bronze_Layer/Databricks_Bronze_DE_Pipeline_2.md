_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Enhanced Bronze Layer Data Ingestion Pipeline with Advanced Monitoring and Error Handling
## *Version*: 2
## *Updated on*: 
_____________________________________________

# Databricks Bronze Layer Data Engineering Pipeline - Enhanced Version
## Inventory Management System

## 1. Pipeline Overview

### 1.1 Purpose
This enhanced pipeline extracts raw data from PostgreSQL source system and loads it into the Bronze layer in Databricks using Delta Lake format. The pipeline includes advanced audit logging, comprehensive metadata tracking, enhanced error handling, data quality checks, and performance monitoring for all inventory management tables.

### 1.2 Source System Details
- **Source System**: PostgreSQL
- **Database**: DE
- **Schema**: tests
- **Tables**: Products, Suppliers, Warehouses, Inventory, Orders, Order_Details, Shipments, Returns, Stock_levels, Customers

### 1.3 Target System Details
- **Target System**: Databricks (Delta Tables in Unity Catalog)
- **Bronze Schema Path**: workspace.inventory_bronze
- **Storage Format**: Delta Lake
- **Write Mode**: Overwrite with validation

### 1.4 Enhanced Features
- **Advanced Error Handling**: Retry mechanisms and detailed error classification
- **Data Quality Validation**: Schema validation and data profiling
- **Performance Monitoring**: Detailed metrics and execution analytics
- **Cost Tracking**: Resource utilization monitoring
- **Parallel Processing**: Optimized table processing with configurable parallelism

## 2. Enhanced PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, count, sum as spark_sum, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
import time
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \n    .appName("BronzeLayerIngestion_InventoryManagement_Enhanced") \n    .config("spark.sql.adaptive.enabled", "true") \n    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n    .config("spark.sql.adaptive.skewJoin.enabled", "true") \n    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \n    .config("spark.databricks.delta.autoCompact.enabled", "true") \n    .getOrCreate()

# Load credentials from Azure Key Vault
source_db_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")

# Get current user's identity with enhanced fallback mechanisms
try:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
except:
    try:
        current_user = spark.sparkContext.sparkUser()
    except:
        try:
            current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        except:
            current_user = "System_Process"

# Configuration parameters
CONFIG = {
    "source_system": "PostgreSQL",
    "source_schema": "tests",
    "target_bronze_path": "workspace.inventory_bronze",
    "max_retries": 3,
    "retry_delay": 30,
    "parallel_processing": True,
    "max_workers": 4,
    "enable_data_quality_checks": True,
    "enable_performance_monitoring": True
}

# Define tables to ingest with priority levels
tables_config = [
    {"name": "Products", "priority": 1