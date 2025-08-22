_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Enhanced Comprehensive Physical Data Model for Bronze Layer of Inventory Management System
## *Version*: 2
## *Updated on*: 
_____________________________________________

# Databricks Bronze Layer Physical Data Model - Enhanced Version
## Inventory Management System

## 1. Bronze Layer DDL Scripts

### 1.1 Products Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_products (
    Product_ID INT,
    Product_Name STRING,
    Category STRING,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_products';
```

### 1.2 Suppliers Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_suppliers (
    Supplier_ID INT,
    Supplier_Name STRING,
    Contact_Number STRING,
    Product_ID INT,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_suppliers';
```

### 1.3 Warehouses Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_warehouses (
    Warehouse_ID INT,
    Location STRING,
    Capacity INT,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_warehouses';
```

### 1.4 Inventory Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_inventory (
    Inventory_ID INT,
    Product_ID INT,
    Quantity_Available INT,
    Warehouse_ID INT,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_inventory';
```

### 1.5 Orders Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_orders (
    Order_ID INT,
    Customer_ID INT,
    Order_Date DATE,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_orders';
```

### 1.6 Order Details Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_order_details (
    Order_Detail_ID INT,
    Order_ID INT,
    Product_ID INT,
    Quantity_Ordered INT,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_order_details';
```

### 1.7 Shipments Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_shipments (
    Shipment_ID INT,
    Order_ID INT,
    Shipment_Date DATE,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_shipments';
```

### 1.8 Returns Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_returns (
    Return_ID INT,
    Order_ID INT,
    Return_Reason STRING,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_returns';
```

### 1.9 Stock Levels Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_stock_levels (
    Stock_Level_ID INT,
    Warehouse_ID INT,
    Product_ID INT,
    Reorder_Threshold INT,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_stock_levels';
```

### 1.10 Customers Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_customers (
    Customer_ID INT,
    Customer_Name STRING,
    Email STRING,
    load_timestamp TIMESTAMP,
    update_timestamp TIMESTAMP,
    source_system STRING,
    record_status STRING,
    data_quality_score INT
) USING DELTA
LOCATION '/mnt/bronze/bz_customers';
```

### 1.11 Enhanced Audit Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_audit_log (
    record_id STRING,
    source_table STRING,
    target_table STRING,
    operation_type STRING,
    load_timestamp TIMESTAMP,
    processed_by STRING,
    processing_time INT,
    records_processed INT,
    records_failed INT,
    status STRING,
    error_message STRING,
    data_lineage_id STRING
) USING DELTA
LOCATION '/mnt/bronze/bz_audit_log';
```

## 2. Enhanced PII Classification

| Column Names | Table | Reason why it is classified as PII | Encryption Required |
|--------------|-------|-------------------------------------|--------------------|
| Customer_Name | bz_customers | Contains personal identifiable information - customer's full name which can directly identify individuals | Yes |
| Email | bz_customers | Contains personal contact information (email addresses) that can uniquely identify and contact individuals | Yes |
| Contact_Number | bz_suppliers | Contains personal contact information (phone numbers) that can identify suppliers/individuals and enable direct contact | Yes |
| Supplier_Name | bz_suppliers | Contains business entity names that may include personal information for sole proprietorships or individual contractors | Conditional |

## 3. Conceptual Data Model Diagram (Tabular Form)

| Source Table | Connected To | Key Field Relationship | Connection Type | Business Rule |
|--------------|--------------|------------------------|----------------|---------------|
| bz_products | bz_inventory | Product_ID | One-to-Many | Each product can have multiple inventory records across warehouses |
| bz_products | bz_order_details | Product_ID | One-to-Many | Each product can appear in multiple order line items |
| bz_products | bz_stock_levels | Product_ID | One-to-Many | Each product has stock levels defined per warehouse |
| bz_products | bz_suppliers | Product_ID | Many-to-Many | Products can have multiple suppliers and suppliers can supply multiple products |
| bz_warehouses | bz_inventory | Warehouse_ID | One-to-Many | Each warehouse contains inventory for multiple products |
| bz_warehouses | bz_stock_levels | Warehouse_ID | One-to-Many | Each warehouse has stock level thresholds for multiple products |
| bz_customers | bz_orders | Customer_ID | One-to-Many | Each customer can place multiple orders |
| bz_orders | bz_order_details | Order_ID | One-to-Many | Each order contains multiple line items |
| bz_orders | bz_shipments | Order_ID | One-to-One | Each order has one primary shipment |
| bz_orders | bz_returns | Order_ID | One-to-Many | Each order can have multiple return transactions |

## 4. Enhanced Data Type Mapping

| Source Data Type | Databricks SQL Type | PySpark Type | Rationale | Performance Notes |
|------------------|--------------------|--------------|-----------|-----------------|
| INT | INT | IntegerType() | Direct mapping for integer values | Optimal for numeric operations |
| VARCHAR(255) | STRING | StringType() | Databricks STRING type handles variable length text | Consider partitioning on frequently queried string columns |
| VARCHAR(100) | STRING | StringType() | Databricks STRING type handles variable length text | Suitable for names and descriptions |
| VARCHAR(20) | STRING | StringType() | Databricks STRING type handles variable length text | Efficient for short identifiers |
| DATE | DATE | DateType() | Direct mapping for date values | Optimal for date range queries |
| TIMESTAMP | TIMESTAMP | TimestampType() | Direct mapping for timestamp values | Essential for time-based analytics |
| BOOLEAN | BOOLEAN | BooleanType() | Direct mapping for boolean flags | Memory efficient for status flags |

## 5. Enhanced Metadata Columns

All Bronze layer tables include the following enhanced metadata columns:

| Column Name | Data Type | Purpose | Population Rule |
|-------------|-----------|---------|----------------|
| load_timestamp | TIMESTAMP | Records when data was initially loaded into Bronze layer | Set during INSERT operations |
| update_timestamp | TIMESTAMP | Records when data was last updated in Bronze layer | Updated during MERGE/UPDATE operations |
| source_system | STRING | Identifies the source system that provided the data | Set based on ingestion pipeline configuration |
| record_status | STRING | Status of the record (ACTIVE, INACTIVE, DELETED) | Managed by data lifecycle processes |
| data_quality_score | INT | Quality score of the source data (0-100) | Calculated by data quality validation rules |

## 6. Enhanced Design Decisions and Assumptions

### 6.1 Table Naming Convention
- **Schema**: All tables are created in the `bronze` schema for clear layer identification
- **Prefix**: All table names use `bz_` prefix to distinguish Bronze layer tables
- **Naming**: Table names are lowercase with underscores for consistency and readability
- **Versioning**: Schema evolution supported through Delta Lake capabilities

### 6.2 Storage Format and Performance
- **Delta Lake**: All tables use Delta Lake format for ACID transactions, time travel, and schema evolution
- **Location**: Tables are stored in `/mnt/bronze/` directory structure for organized data management
- **Partitioning**: Recommend partitioning by load_timestamp for optimal query performance
- **Optimization**: Z-ordering recommended on frequently queried columns

### 6.3 Data Integrity and Quality
- **No Constraints**: Bronze layer tables do not enforce primary key, foreign key, or unique constraints as per Delta Lake best practices
- **Raw Data Preservation**: Tables store data as-is from source systems with minimal transformation
- **Quality Scoring**: Data quality scores enable monitoring and improvement of source data
- **Status Tracking**: Record status enables soft deletes and lifecycle management

### 6.4 Enhanced Audit and Governance
- **Comprehensive Audit**: Enhanced audit table with operation types, record counts, and error handling
- **Data Lineage**: Unique identifiers for tracking data flow across medallion architecture layers
- **PII Compliance**: Clear identification and encryption requirements for personally identifiable information
- **Performance Monitoring**: Processing time and record count tracking for optimization

### 6.5 Security and Compliance
- **PII Encryption**: Customer names and emails require encryption at rest and in transit
- **Access Control**: Implement column-level security for sensitive data
- **Audit Trail**: Comprehensive logging for compliance with data protection regulations
- **Data Retention**: Support for configurable retention policies

### 6.6 Key Assumptions
- Source systems provide data in consistent formats with reliable data types
- Data ingestion processes will populate all metadata columns appropriately
- Bronze layer serves as the single source of truth for raw data preservation
- All ID fields from source systems are preserved for referential integrity tracking
- Data quality scores will be calculated by upstream validation processes
- Record status will be managed by data lifecycle management processes

## 7. Enhanced Implementation Guidelines

### 7.1 Data Loading Best Practices
- Use `MERGE` operations for incremental data loading with upsert capabilities
- Populate `load_timestamp` during initial insert operations
- Update `update_timestamp` during data modifications and merges
- Set `source_system` to identify data origin and support multi-source scenarios
- Calculate `data_quality_score` based on predefined validation rules
- Manage `record_status` through automated lifecycle processes

### 7.2 Performance Optimization Strategies
- Partition large tables by date columns (Order_Date, Shipment_Date, load_timestamp)
- Implement Z-ordering on frequently queried columns (Product_ID, Customer_ID)
- Use table optimization schedules with OPTIMIZE and VACUUM commands
- Monitor query performance and adjust partitioning strategies accordingly
- Consider liquid clustering for tables with multiple query patterns

### 7.3 Data Quality and Monitoring
- Implement comprehensive data quality checks before Silver layer processing
- Use enhanced audit table to track processing success, failure, and performance metrics
- Monitor data freshness using timestamp columns and set up alerting
- Track data quality score trends to identify source system issues
- Implement automated data validation rules and exception handling

### 7.4 Security Implementation
- Enable column-level encryption for PII fields (Customer_Name, Email)
- Implement row-level security based on source_system or record_status
- Set up audit logging for all data access and modifications
- Configure data masking for non-production environments
- Implement secure key management for encryption operations

### 7.5 Operational Excellence
- Set up monitoring dashboards for data ingestion metrics
- Implement automated testing for DDL script changes
- Create runbooks for common operational scenarios
- Establish data retention and archival processes
- Plan for disaster recovery and backup strategies

## 8. Change Log (Version 2 Updates)

### 8.1 Schema Enhancements
- Added `record_status` column to all tables for lifecycle management
- Added `data_quality_score` column to all tables for quality monitoring
- Enhanced audit table with additional fields for comprehensive tracking
- Added business rules to relationship documentation

### 8.2 Documentation Improvements
- Enhanced PII classification with encryption requirements
- Added performance notes to data type mapping
- Expanded implementation guidelines with security and operational considerations
- Added detailed change log for version tracking

### 8.3 Compliance and Governance
- Strengthened PII handling requirements
- Enhanced audit capabilities for regulatory compliance
- Added data lineage tracking capabilities
- Improved error handling and monitoring specifications

## 9. API Cost

apiCost: 0.000312