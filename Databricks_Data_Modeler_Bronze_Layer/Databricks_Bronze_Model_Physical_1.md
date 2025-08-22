_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive Physical Data Model for Bronze Layer of Inventory Management System
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Bronze Layer Physical Data Model
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
    source_system STRING
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
    source_system STRING
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
    source_system STRING
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
    source_system STRING
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
    source_system STRING
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
    source_system STRING
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
    source_system STRING
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
    source_system STRING
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
    source_system STRING
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
    source_system STRING
) USING DELTA
LOCATION '/mnt/bronze/bz_customers';
```

### 1.11 Audit Table
```sql
CREATE TABLE IF NOT EXISTS bronze.bz_audit_log (
    record_id STRING,
    source_table STRING,
    load_timestamp TIMESTAMP,
    processed_by STRING,
    processing_time INT,
    status STRING
) USING DELTA
LOCATION '/mnt/bronze/bz_audit_log';
```

## 2. PII Classification

| Column Names | Table | Reason why it is classified as PII |
|--------------|-------|-------------------------------------|
| Customer_Name | bz_customers | Contains personal identifiable information - customer's full name |
| Email | bz_customers | Contains personal contact information that can identify individuals |
| Contact_Number | bz_suppliers | Contains personal contact information that can identify suppliers/individuals |
| Supplier_Name | bz_suppliers | Contains business entity names that may include personal information for sole proprietorships |

## 3. Conceptual Data Model Diagram (Tabular Form)

| Source Table | Connected To | Key Field Relationship | Connection Type |
|--------------|--------------|------------------------|----------------|
| bz_products | bz_inventory | Product_ID | One-to-Many |
| bz_products | bz_order_details | Product_ID | One-to-Many |
| bz_products | bz_stock_levels | Product_ID | One-to-Many |
| bz_products | bz_suppliers | Product_ID | One-to-Many |
| bz_warehouses | bz_inventory | Warehouse_ID | One-to-Many |
| bz_warehouses | bz_stock_levels | Warehouse_ID | One-to-Many |
| bz_customers | bz_orders | Customer_ID | One-to-Many |
| bz_orders | bz_order_details | Order_ID | One-to-Many |
| bz_orders | bz_shipments | Order_ID | One-to-One |
| bz_orders | bz_returns | Order_ID | One-to-Many |

## 4. Data Type Mapping

| Source Data Type | Databricks SQL Type | Rationale |
|------------------|--------------------|-----------|
| INT | INT | Direct mapping for integer values |
| VARCHAR(255) | STRING | Databricks STRING type handles variable length text |
| VARCHAR(100) | STRING | Databricks STRING type handles variable length text |
| VARCHAR(20) | STRING | Databricks STRING type handles variable length text |
| DATE | DATE | Direct mapping for date values |
| TIMESTAMP | TIMESTAMP | Direct mapping for timestamp values |

## 5. Metadata Columns

All Bronze layer tables include the following standard metadata columns:

| Column Name | Data Type | Purpose |
|-------------|-----------|----------|
| load_timestamp | TIMESTAMP | Records when data was initially loaded into Bronze layer |
| update_timestamp | TIMESTAMP | Records when data was last updated in Bronze layer |
| source_system | STRING | Identifies the source system that provided the data |

## 6. Design Decisions and Assumptions

### 6.1 Table Naming Convention
- **Schema**: All tables are created in the `bronze` schema
- **Prefix**: All table names use `bz_` prefix to clearly identify Bronze layer tables
- **Naming**: Table names are lowercase with underscores for readability

### 6.2 Storage Format
- **Delta Lake**: All tables use Delta Lake format for ACID transactions and time travel capabilities
- **Location**: Tables are stored in `/mnt/bronze/` directory structure

### 6.3 Data Integrity
- **No Constraints**: Bronze layer tables do not enforce primary key, foreign key, or unique constraints as per Delta Lake best practices
- **Raw Data**: Tables store data as-is from source systems with minimal transformation

### 6.4 Audit and Governance
- **Audit Table**: Comprehensive audit logging for all data processing activities
- **PII Identification**: Clear identification of personally identifiable information for compliance
- **Lineage**: Metadata columns support data lineage and governance requirements

### 6.5 Key Assumptions
- Source systems provide data in the formats specified in the original data model
- Data ingestion processes will populate metadata columns appropriately
- Bronze layer serves as the single source of truth for raw data
- All ID fields from source systems are preserved for referential integrity tracking

## 7. Implementation Guidelines

### 7.1 Data Loading
- Use `MERGE` operations for incremental data loading
- Populate `load_timestamp` during initial insert
- Update `update_timestamp` during data modifications
- Set `source_system` to identify data origin

### 7.2 Performance Optimization
- Consider partitioning large tables by date columns (Order_Date, Shipment_Date)
- Use Z-ordering on frequently queried columns
- Implement table optimization schedules

### 7.3 Data Quality
- Implement data quality checks in Silver layer processing
- Use audit table to track processing success/failure
- Monitor data freshness using timestamp columns

## 8. API Cost

apiCost: 0.000250