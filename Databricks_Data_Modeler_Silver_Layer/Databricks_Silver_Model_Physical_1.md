_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive Physical Data Model for Silver Layer of Inventory Management System
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Silver Layer Physical Data Model
## Inventory Management System

## 1. Silver Layer DDL Scripts

### 1.1 Products Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_products (
    Product_ID INT,
    Product_Name STRING,
    Category STRING,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
PARTITIONED BY (Category)
LOCATION '/mnt/silver/si_products';
```

### 1.2 Suppliers Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_suppliers (
    Supplier_ID INT,
    Supplier_Name STRING,
    Contact_Number STRING,
    Product_ID INT,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
LOCATION '/mnt/silver/si_suppliers';
```

### 1.3 Warehouses Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_warehouses (
    Warehouse_ID INT,
    Location STRING,
    Capacity INT,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
LOCATION '/mnt/silver/si_warehouses';
```

### 1.4 Inventory Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_inventory (
    Inventory_ID INT,
    Product_ID INT,
    Quantity_Available INT,
    Warehouse_ID INT,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
PARTITIONED BY (Warehouse_ID)
LOCATION '/mnt/silver/si_inventory';
```

### 1.5 Orders Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_orders (
    Order_ID INT,
    Customer_ID INT,
    Order_Date DATE,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
PARTITIONED BY (Order_Date)
LOCATION '/mnt/silver/si_orders';
```

### 1.6 Order Details Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_order_details (
    Order_Detail_ID INT,
    Order_ID INT,
    Product_ID INT,
    Quantity_Ordered INT,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
LOCATION '/mnt/silver/si_order_details';
```

### 1.7 Shipments Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_shipments (
    Shipment_ID INT,
    Order_ID INT,
    Shipment_Date DATE,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
PARTITIONED BY (Shipment_Date)
LOCATION '/mnt/silver/si_shipments';
```

### 1.8 Returns Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_returns (
    Return_ID INT,
    Order_ID INT,
    Return_Reason STRING,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
LOCATION '/mnt/silver/si_returns';
```

### 1.9 Stock Levels Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_stock_levels (
    Stock_Level_ID INT,
    Warehouse_ID INT,
    Product_ID INT,
    Reorder_Threshold INT,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
PARTITIONED BY (Warehouse_ID)
LOCATION '/mnt/silver/si_stock_levels';
```

### 1.10 Customers Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_customers (
    Customer_ID INT,
    Customer_Name STRING,
    Email STRING,
    load_date TIMESTAMP,
    update_date TIMESTAMP,
    source_system STRING
) USING DELTA
LOCATION '/mnt/silver/si_customers';
```

## 2. Error Data Table DDL Script

### 2.1 Data Quality Errors Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_data_quality_errors (
    error_id STRING,
    error_timestamp TIMESTAMP,
    source_table STRING,
    error_type STRING,
    error_description STRING,
    error_column STRING,
    error_value STRING,
    record_identifier STRING,
    severity_level STRING,
    validation_rule STRING,
    error_count INT,
    processed_by STRING,
    resolution_status STRING,
    resolution_timestamp TIMESTAMP,
    load_date TIMESTAMP,
    source_system STRING
) USING DELTA
PARTITIONED BY (error_timestamp)
LOCATION '/mnt/silver/si_data_quality_errors';
```

## 3. Audit Table DDL Script

### 3.1 Pipeline Audit Table
```sql
CREATE TABLE IF NOT EXISTS silver.si_pipeline_audit (
    audit_id STRING,
    audit_timestamp TIMESTAMP,
    pipeline_name STRING,
    pipeline_run_id STRING,
    source_table STRING,
    target_table STRING,
    records_read INT,
    records_processed INT,
    records_inserted INT,
    records_updated INT,
    records_deleted INT,
    records_rejected INT,
    processing_start_time TIMESTAMP,
    processing_end_time TIMESTAMP,
    processing_duration INT,
    pipeline_status STRING,
    error_message STRING,
    data_quality_score FLOAT,
    processed_by STRING,
    environment STRING,
    pipeline_version STRING,
    load_date TIMESTAMP,
    source_system STRING
) USING DELTA
PARTITIONED BY (audit_timestamp)
LOCATION '/mnt/silver/si_pipeline_audit';
```

## 4. Update DDL Scripts

### 4.1 Add Column Script Template
```sql
-- Template for adding new columns to existing Silver layer tables
-- Example: Adding new column to products table
ALTER TABLE silver.si_products ADD COLUMN (
    new_column_name STRING COMMENT 'Description of new column'
);
```

### 4.2 Modify Column Script Template
```sql
-- Template for modifying existing columns
-- Note: Delta Lake supports limited column modifications
-- For major changes, consider creating new table and migrating data
ALTER TABLE silver.si_products ALTER COLUMN Category COMMENT 'Updated description';
```

### 4.3 Table Evolution Script
```sql
-- Enable automatic schema evolution for Silver layer tables
SET spark.databricks.delta.schema.autoMerge.enabled = true;
SET spark.databricks.delta.autoOptimize.optimizeWrite = true;
SET spark.databricks.delta.autoOptimize.autoCompact = true;
```

## 5. Data Retention Policies

### 5.1 Retention Periods for Silver Layer

| Table Name | Retention Period | Rationale |
|------------|------------------|----------|
| si_products | 7 years | Product lifecycle and regulatory compliance |
| si_suppliers | 7 years | Vendor relationship and audit requirements |
| si_warehouses | 7 years | Facility management and compliance |
| si_inventory | 3 years | Operational data with moderate retention needs |
| si_orders | 7 years | Financial and regulatory compliance |
| si_order_details | 7 years | Transaction details for audit purposes |
| si_shipments | 5 years | Logistics and customer service requirements |
| si_returns | 5 years | Quality analysis and customer service |
| si_stock_levels | 2 years | Operational data with short-term value |
| si_customers | 7 years | Customer relationship and privacy compliance |
| si_data_quality_errors | 2 years | Data quality monitoring and improvement |
| si_pipeline_audit | 3 years | Pipeline monitoring and troubleshooting |

### 5.2 Archiving Strategies

#### 5.2.1 Hot Storage (0-1 year)
- **Location**: Premium SSD storage
- **Access Pattern**: Frequent read/write operations
- **Optimization**: Z-ordering and bloom filters enabled
- **Backup**: Daily incremental backups

#### 5.2.2 Warm Storage (1-3 years)
- **Location**: Standard storage tier
- **Access Pattern**: Occasional read operations
- **Optimization**: Compaction and vacuum operations
- **Backup**: Weekly full backups

#### 5.2.3 Cold Storage (3+ years)
- **Location**: Archive storage tier
- **Access Pattern**: Rare access for compliance
- **Optimization**: Compressed and partitioned
- **Backup**: Monthly archive backups

#### 5.2.4 Data Lifecycle Management
```sql
-- Automated archiving script template
-- Move data older than 1 year to warm storage
CREATE OR REPLACE TABLE silver.si_orders_archive
USING DELTA
LOCATION '/mnt/archive/silver/si_orders_archive'
AS SELECT * FROM silver.si_orders 
WHERE Order_Date < date_sub(current_date(), 365);

-- Remove archived data from hot storage
DELETE FROM silver.si_orders 
WHERE Order_Date < date_sub(current_date(), 365);

-- Optimize remaining data
OPTIMIZE silver.si_orders ZORDER BY (Order_Date, Customer_ID);
```

## 6. Conceptual Data Model Diagram in Tabular Form

| Primary Table | Related Table | Relationship Key Field | Relationship Type |
|---------------|---------------|------------------------|-----------------|
| si_products | si_inventory | Product_ID | One-to-Many |
| si_products | si_order_details | Product_ID | One-to-Many |
| si_products | si_stock_levels | Product_ID | One-to-Many |
| si_products | si_suppliers | Product_ID | Many-to-Many |
| si_warehouses | si_inventory | Warehouse_ID | One-to-Many |
| si_warehouses | si_stock_levels | Warehouse_ID | One-to-Many |
| si_customers | si_orders | Customer_ID | One-to-Many |
| si_orders | si_order_details | Order_ID | One-to-Many |
| si_orders | si_shipments | Order_ID | One-to-One |
| si_orders | si_returns | Order_ID | One-to-Many |
| si_data_quality_errors | si_products | source_table | Many-to-One |
| si_data_quality_errors | si_suppliers | source_table | Many-to-One |
| si_data_quality_errors | si_warehouses | source_table | Many-to-One |
| si_data_quality_errors | si_inventory | source_table | Many-to-One |
| si_data_quality_errors | si_orders | source_table | Many-to-One |
| si_data_quality_errors | si_order_details | source_table | Many-to-One |
| si_data_quality_errors | si_shipments | source_table | Many-to-One |
| si_data_quality_errors | si_returns | source_table | Many-to-One |
| si_data_quality_errors | si_stock_levels | source_table | Many-to-One |
| si_data_quality_errors | si_customers | source_table | Many-to-One |
| si_pipeline_audit | si_products | target_table | Many-to-One |
| si_pipeline_audit | si_suppliers | target_table | Many-to-One |
| si_pipeline_audit | si_warehouses | target_table | Many-to-One |
| si_pipeline_audit | si_inventory | target_table | Many-to-One |
| si_pipeline_audit | si_orders | target_table | Many-to-One |
| si_pipeline_audit | si_order_details | target_table | Many-to-One |
| si_pipeline_audit | si_shipments | target_table | Many-to-One |
| si_pipeline_audit | si_returns | target_table | Many-to-One |
| si_pipeline_audit | si_stock_levels | target_table | Many-to-One |
| si_pipeline_audit | si_customers | target_table | Many-to-One |

## 7. Design Rationale and Key Decisions

### 7.1 Naming Convention
- **Si_ Prefix**: All Silver layer tables use "si_" prefix to clearly identify the data layer
- **Schema**: All tables are created in the `silver` schema
- **Lowercase**: Table names are lowercase with underscores for consistency

### 7.2 Data Structure Decisions
- **ID Fields**: All tables include appropriate ID fields for referential integrity
- **Metadata Columns**: Standard metadata columns (load_date, update_date, source_system) maintained
- **No Constraints**: No primary key or foreign key constraints as per Spark SQL limitations
- **Delta Lake**: All tables use Delta Lake format for ACID transactions and time travel

### 7.3 Partitioning Strategy
- **Date-based Partitioning**: Tables with date columns partitioned for query performance
- **Category Partitioning**: Products table partitioned by category for analytical queries
- **Warehouse Partitioning**: Inventory and stock levels partitioned by warehouse for operational efficiency

### 7.4 Performance Optimization
- **Z-ordering**: Recommended on frequently queried columns
- **Auto-optimization**: Enabled for write optimization and auto-compaction
- **Bloom Filters**: Can be added for high-cardinality columns

### 7.5 Data Quality and Governance
- **Error Tracking**: Comprehensive error data structure for data quality monitoring
- **Audit Trail**: Detailed pipeline audit for governance and compliance
- **Data Lineage**: Source system tracking for complete data lineage

### 7.6 Storage and Cost Optimization
- **Tiered Storage**: Hot, warm, and cold storage strategies
- **Data Lifecycle**: Automated archiving and retention policies
- **Compression**: Delta Lake automatic compression for storage efficiency

## 8. Implementation Guidelines

### 8.1 Data Processing
- **Incremental Loading**: Use MERGE operations for efficient data updates
- **Data Validation**: Implement comprehensive data quality checks
- **Error Handling**: Capture and log all data quality issues
- **Transformation**: Apply business rules and data standardization

### 8.2 Performance Tuning
- **Partitioning**: Implement appropriate partitioning strategies
- **Indexing**: Use Z-ordering for frequently queried columns
- **Caching**: Cache frequently accessed tables
- **Optimization**: Regular OPTIMIZE and VACUUM operations

### 8.3 Monitoring and Alerting
- **Pipeline Monitoring**: Track all ETL operations in audit table
- **Data Quality Monitoring**: Monitor error rates and data quality scores
- **Performance Monitoring**: Track processing times and resource usage
- **Alerting**: Implement alerts for pipeline failures and data quality issues

### 8.4 Security and Compliance
- **Access Control**: Implement role-based access control
- **Data Masking**: Apply data masking for PII fields
- **Encryption**: Enable encryption at rest and in transit
- **Audit Logging**: Maintain comprehensive audit logs

## 9. Key Assumptions

### 9.1 Data Sources
- Bronze layer provides clean, validated data
- Source systems maintain referential integrity
- Data arrives in expected formats and structures
- Incremental data loading is supported

### 9.2 Business Requirements
- Data freshness requirements are within SLA
- Business rules are well-defined and stable
- Data quality thresholds are established
- Retention policies align with regulatory requirements

### 9.3 Technical Environment
- Databricks runtime supports all specified features
- Storage infrastructure supports tiered storage
- Network connectivity is reliable and secure
- Backup and disaster recovery processes are in place

## 10. API Cost

apiCost: 0.002875