_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Advanced Bronze Layer Data Ingestion Pipeline with Databricks Job Scheduling and Enhanced Monitoring
## *Version*: 3
## *Updated on*: 
_____________________________________________

# Databricks Bronze Layer Data Engineering Pipeline - Advanced Version
## Inventory Management System with Job Scheduling

## 1. Pipeline Overview

### 1.1 Purpose
This advanced pipeline extracts raw data from PostgreSQL source system and loads it into the Bronze layer in Databricks using Delta Lake format. The pipeline includes comprehensive audit logging, metadata tracking, error handling, data quality checks, performance optimizations, and automated job scheduling for all inventory management tables.

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
- **Job Scheduling**: Daily at 8:00 PM IST via Databricks Jobs

### 1.4 Advanced Features
- **Automated Job Scheduling**: Databricks Jobs with serverless compute
- **Real-time Monitoring**: Enhanced metrics and alerting
- **Cost Optimization**: Resource-aware processing
- **Advanced Error Recovery**: Multi-level retry with circuit breaker
- **Data Lineage Tracking**: Complete end-to-end lineage

## 2. Advanced PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, count, when, isnan, isnull, max as spark_max
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType, BooleanType
import time
from datetime import datetime, timedelta
import logging
import json
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure advanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Spark session with advanced configurations
spark = SparkSession.builder \n    .appName("BronzeLayerIngestion_InventoryManagement_Advanced") \n    .config("spark.sql.adaptive.enabled", "true") \n    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n    .config("spark.sql.adaptive.skewJoin.enabled", "true") \n    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \n    .config("spark.databricks.delta.autoCompact.enabled", "true") \n    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \n    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \n    .getOrCreate()

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
            import getpass
            current_user = getpass.getuser()
        except:
            current_user = "System_Process"

# Generate unique pipeline run ID
pipeline_run_id = str(uuid.uuid4())
logger.info(f"Pipeline Run ID: {pipeline_run_id}")

# Define source and target details
source_system = "PostgreSQL"
source_schema = "tests"
target_bronze_path = "workspace.inventory_bronze"
pipeline_version = "3.0"

# Advanced table configuration with SLA and dependencies
tables_config = {
    "Products": {
        "priority": 1