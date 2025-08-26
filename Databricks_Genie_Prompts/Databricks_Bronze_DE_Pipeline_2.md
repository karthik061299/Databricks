# Databricks Bronze DE Pipeline - Enhanced Version

## Pipeline Overview

This enhanced Bronze layer data engineering pipeline implements a comprehensive ingestion strategy for moving data from PostgreSQL source systems to Databricks Bronze layer tables. The pipeline ensures efficient data ingestion, comprehensive metadata tracking, audit logging, and data governance compliance for the Inventory Management System.

## Architecture Components

### Source System Configuration
- **Source System**: PostgreSQL Database
- **Database Name**: DE
- **Schema Name**: tests
- **Connection Method**: Azure Key Vault secured credentials
- **Authentication**: Managed through Azure Key Vault secrets

### Target System Configuration
- **Target System**: Databricks Unity Catalog
- **Bronze Schema Path**: workspace.inventory_bronze
- **Storage Format**: Delta Lake
- **Location Pattern**: `/mnt/bronze/{table_name}`

## Data Sources and Target Mapping

### Source Tables (PostgreSQL)
| Source Table | Records Expected | Business Domain |
|--------------|------------------|------------------|
| Products | Variable | Product Catalog |
| Suppliers | Variable | Vendor Management |
| Warehouses | Variable | Location Management |
| Inventory | High Volume | Stock Management |
| Orders | High Volume | Order Processing |
| Order_Details | High Volume | Order Line Items |
| Shipments | Medium Volume | Logistics |
| Returns | Low Volume | Returns Processing |
| Stock_Levels | Medium Volume | Inventory Thresholds |
| Customers | Variable | Customer Management |

### Target Bronze Tables (Databricks)
| Target Table | Source Mapping | Partitioning Strategy |
|--------------|----------------|----------------------|
| bz_products | Products | None (Reference Data) |
| bz_suppliers | Suppliers | None (Reference Data) |
| bz_warehouses | Warehouses | None (Reference Data) |
| bz_inventory | Inventory | Partitioned by load_date |
| bz_orders | Orders | Partitioned by order_date |
| bz_order_details | Order_Details | Partitioned by load_date |
| bz_shipments | Shipments | Partitioned by shipment_date |
| bz_returns | Returns | Partitioned by load_date |
| bz_stock_levels | Stock_Levels | Partitioned by load_date |
| bz_customers | Customers | None (PII Encrypted) |
| bz_audit_log | System Generated | Partitioned by log_date |

## Ingestion Strategy

### 1. Data Extraction Methods

#### Full Load Strategy
- **Frequency**: Initial load and monthly refresh for reference tables
- **Tables**: Products, Suppliers, Warehouses, Customers
- **Method**: Complete table extraction with timestamp-based validation

#### Incremental Load Strategy
- **Frequency**: Daily incremental loads
- **Tables**: Inventory, Orders, Order_Details, Shipments, Returns, Stock_Levels
- **Method**: Change Data Capture (CDC) using timestamp columns
- **Watermark Column**: update_timestamp or created_timestamp

#### Real-time Streaming (Future Enhancement)
- **Method**: Databricks Auto Loader with cloud file notifications
- **Format**: JSON/Parquet files from source system exports
- **Trigger**: File arrival in cloud storage

### 2. Data Ingestion Pipeline Architecture

```python
# PySpark Pipeline Structure
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# Initialize Spark Session with Delta Lake
spark = SparkSession.builder \n    .appName("Bronze_Layer_Ingestion") \n    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \n    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \n    .getOrCreate()
```

### 3. Connection Management

```python
# Secure Connection to PostgreSQL
def get_postgres_connection():
    source_db_url = mssparkutils.credentials.getSecret(
        "https://akv-poc-fabric.vault.azure.net/", 
        "KConnectionString"
    )
    user = mssparkutils.credentials.getSecret(
        "https://akv-poc-fabric.vault.azure.net/", 
        "KUser"
    )
    password = mssparkutils.credentials.getSecret(
        "https://akv-poc-fabric.vault.azure.net/", 
        "KPassword"
    )
    
    return {
        "url": source_db_url,
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
```

## Detailed Transformation Rules

### 1. Schema Transformation

#### Source to Bronze Data Type Mapping
| PostgreSQL Type | Databricks Type | Transformation Rule |
|-----------------|-----------------|--------------------|
| INTEGER | INT | Direct mapping |
| VARCHAR(n) | STRING | Direct mapping with length preservation |
| DATE | DATE | Direct mapping |
| TIMESTAMP | TIMESTAMP | Direct mapping |
| BOOLEAN | BOOLEAN | Direct mapping |
| DECIMAL(p,s) | DECIMAL(p,s) | Precision preserved |

#### Metadata Column Addition
```python
# Standard metadata columns added to all Bronze tables
metadata_columns = [
    col("load_timestamp").cast("timestamp"),
    col("update_timestamp").cast("timestamp"),
    col("source_system").cast("string"),
    col("record_status").cast("string"),
    col("data_quality_score").cast("int")
]
```

### 2. Data Quality Validation

#### Validation Rules Implementation
```python
def calculate_data_quality_score(df, table_name):
    """
    Calculate data quality score based on validation rules
    Score range: 0-100
    """
    total_records = df.count()
    
    # Null value check
    null_count = df.select([count(when(col(c).isNull(), c)).alias(c) 
                           for c in df.columns]).collect()[0]
    
    # Calculate quality score
    null_percentage = sum(null_count.asDict().values()) / (total_records * len(df.columns))
    quality_score = max(0, 100 - (null_percentage * 100))
    
    return quality_score
```

### 3. PII Data Handling

#### PII Encryption Strategy
```python
def encrypt_pii_columns(df, pii_columns):
    """
    Encrypt PII columns using AES encryption
    """
    from pyspark.sql.functions import expr
    
    for col_name in pii_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                expr(f"aes_encrypt({col_name}, 'encryption_key', 'GCM')")
            )
    return df

# PII columns by table
pii_mapping = {
    "bz_customers": ["Customer_Name", "Email"],
    "bz_suppliers": ["Contact_Number", "Supplier_Name"]
}
```

## Pipeline Implementation

### 1. Main Ingestion Function

```python
def ingest_table_to_bronze(source_table, target_table, ingestion_type="full"):
    """
    Main function to ingest data from source to Bronze layer
    """
    try:
        # Start audit logging
        audit_start = datetime.now()
        
        # Get connection properties
        conn_props = get_postgres_connection()
        
        # Read from source
        if ingestion_type == "full":
            source_df = spark.read \n                .format("jdbc") \n                .options(**conn_props) \n                .option("dbtable", f"tests.{source_table}") \n                .load()
        else:
            # Incremental load logic
            watermark = get_last_watermark(target_table)
            query = f"(SELECT * FROM tests.{source_table} WHERE update_timestamp > '{watermark}') as incremental"
            source_df = spark.read \n                .format("jdbc") \n                .options(**conn_props) \n                .option("dbtable", query) \n                .load()
        
        # Add metadata columns
        enriched_df = source_df \n            .withColumn("load_timestamp", current_timestamp()) \n            .withColumn("update_timestamp", current_timestamp()) \n            .withColumn("source_system", lit("PostgreSQL_DE")) \n            .withColumn("record_status", lit("ACTIVE"))
        
        # Calculate data quality score
        quality_score = calculate_data_quality_score(enriched_df, source_table)
        enriched_df = enriched_df.withColumn("data_quality_score", lit(quality_score))
        
        # Apply PII encryption if needed
        if target_table in pii_mapping:
            enriched_df = encrypt_pii_columns(enriched_df