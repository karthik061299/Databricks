_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Enhanced Bronze Layer Data Ingestion Pipeline with Advanced Features for Inventory Management System
## *Version*: 2
## *Updated on*: 
_____________________________________________

# Databricks Bronze Layer Data Engineering Pipeline - Enhanced Version
## Inventory Management System

## 1. Pipeline Overview

### 1.1 Purpose
This enhanced pipeline extracts raw data from PostgreSQL source system and loads it into the Bronze layer in Databricks using Delta Lake format. The pipeline includes comprehensive audit logging, metadata tracking, data quality validation, performance optimization, and advanced error handling for all inventory management tables.

### 1.2 Source System Details
- **Source System**: PostgreSQL
- **Database**: DE
- **Schema**: tests
- **Tables**: Products, Suppliers, Warehouses, Inventory, Orders, Order_Details, Shipments, Returns, Stock_levels, Customers

### 1.3 Target System Details
- **Target System**: Databricks (Delta Tables in Unity Catalog)
- **Bronze Schema Path**: workspace.inventory_bronze
- **Storage Format**: Delta Lake
- **Write Mode**: Overwrite with optimization

### 1.4 New Features in Version 2
- **Data Quality Validation**: Schema validation and null checks
- **Performance Optimization**: Parallel processing and caching
- **Enhanced Error Handling**: Retry logic and detailed error reporting
- **Monitoring Dashboard**: Real-time processing metrics
- **Cost Optimization**: Resource management and query optimization

## 2. Enhanced PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, count, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
import time
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

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
            current_user = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId", "System_Process")
        except:
            current_user = "System_Process"

# Define source and target details
source_system = "PostgreSQL"
source_schema = "tests"
target_bronze_path = "workspace.inventory_bronze"

# Define tables to ingest with priority levels
tables_config = {
    "Products": {"priority": 1