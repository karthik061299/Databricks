_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Databricks Bronze Model Data Mapping for Inventory Management System
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Bronze Model Data Mapping
## Inventory Management System

## 1. Data Mapping for Bronze Layer

The following tables present the comprehensive data mapping between source systems and the Bronze layer in the Medallion architecture. This mapping ensures raw data preservation with minimal transformation while maintaining data lineage and metadata.

### 1.1 Products Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_products | Product_ID | Source | Products | Product_ID | 1-1 Mapping |
| Bronze | bz_products | Product_Name | Source | Products | Product_Name | 1-1 Mapping |
| Bronze | bz_products | Category | Source | Products | Category | 1-1 Mapping |
| Bronze | bz_products | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_products | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_products | source_system | Source | System | Source_System_ID | System Generated |

### 1.2 Suppliers Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_suppliers | Supplier_ID | Source | Suppliers | Supplier_ID | 1-1 Mapping |
| Bronze | bz_suppliers | Supplier_Name | Source | Suppliers | Supplier_Name | 1-1 Mapping |
| Bronze | bz_suppliers | Contact_Number | Source | Suppliers | Contact_Number | 1-1 Mapping |
| Bronze | bz_suppliers | Product_ID | Source | Suppliers | Product_ID | 1-1 Mapping |
| Bronze | bz_suppliers | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_suppliers | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_suppliers | source_system | Source | System | Source_System_ID | System Generated |

### 1.3 Warehouses Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_warehouses | Warehouse_ID | Source | Warehouses | Warehouse_ID | 1-1 Mapping |
| Bronze | bz_warehouses | Location | Source | Warehouses | Location | 1-1 Mapping |
| Bronze | bz_warehouses | Capacity | Source | Warehouses | Capacity | 1-1 Mapping |
| Bronze | bz_warehouses | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_warehouses | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_warehouses | source_system | Source | System | Source_System_ID | System Generated |

### 1.4 Inventory Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_inventory | Inventory_ID | Source | Inventory | Inventory_ID | 1-1 Mapping |
| Bronze | bz_inventory | Product_ID | Source | Inventory | Product_ID | 1-1 Mapping |
| Bronze | bz_inventory | Quantity_Available | Source | Inventory | Quantity_Available | 1-1 Mapping |
| Bronze | bz_inventory | Warehouse_ID | Source | Inventory | Warehouse_ID | 1-1 Mapping |
| Bronze | bz_inventory | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_inventory | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_inventory | source_system | Source | System | Source_System_ID | System Generated |

### 1.5 Orders Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_orders | Order_ID | Source | Orders | Order_ID | 1-1 Mapping |
| Bronze | bz_orders | Customer_ID | Source | Orders | Customer_ID | 1-1 Mapping |
| Bronze | bz_orders | Order_Date | Source | Orders | Order_Date | 1-1 Mapping |
| Bronze | bz_orders | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_orders | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_orders | source_system | Source | System | Source_System_ID | System Generated |

### 1.6 Order Details Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_order_details | Order_Detail_ID | Source | Order_Details | Order_Detail_ID | 1-1 Mapping |
| Bronze | bz_order_details | Order_ID | Source | Order_Details | Order_ID | 1-1 Mapping |
| Bronze | bz_order_details | Product_ID | Source | Order_Details | Product_ID | 1-1 Mapping |
| Bronze | bz_order_details | Quantity_Ordered | Source | Order_Details | Quantity_Ordered | 1-1 Mapping |
| Bronze | bz_order_details | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_order_details | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_order_details | source_system | Source | System | Source_System_ID | System Generated |

### 1.7 Shipments Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_shipments | Shipment_ID | Source | Shipments | Shipment_ID | 1-1 Mapping |
| Bronze | bz_shipments | Order_ID | Source | Shipments | Order_ID | 1-1 Mapping |
| Bronze | bz_shipments | Shipment_Date | Source | Shipments | Shipment_Date | 1-1 Mapping |
| Bronze | bz_shipments | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_shipments | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_shipments | source_system | Source | System | Source_System_ID | System Generated |

### 1.8 Returns Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_returns | Return_ID | Source | Returns | Return_ID | 1-1 Mapping |
| Bronze | bz_returns | Order_ID | Source | Returns | Order_ID | 1-1 Mapping |
| Bronze | bz_returns | Return_Reason | Source | Returns | Return_Reason | 1-1 Mapping |
| Bronze | bz_returns | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_returns | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_returns | source_system | Source | System | Source_System_ID | System Generated |

### 1.9 Stock Levels Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_stock_levels | Stock_Level_ID | Source | Stock_Levels | Stock_Level_ID | 1-1 Mapping |
| Bronze | bz_stock_levels | Warehouse_ID | Source | Stock_Levels | Warehouse_ID | 1-1 Mapping |
| Bronze | bz_stock_levels | Product_ID | Source | Stock_Levels | Product_ID | 1-1 Mapping |
| Bronze | bz_stock_levels | Reorder_Threshold | Source | Stock_Levels | Reorder_Threshold | 1-1 Mapping |
| Bronze | bz_stock_levels | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_stock_levels | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_stock_levels | source_system | Source | System | Source_System_ID | System Generated |

### 1.10 Customers Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | bz_customers | Customer_ID | Source | Customers | Customer_ID | 1-1 Mapping |
| Bronze | bz_customers | Customer_Name | Source | Customers | Customer_Name | 1-1 Mapping |
| Bronze | bz_customers | Email | Source | Customers | Email | 1-1 Mapping |
| Bronze | bz_customers | load_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_customers | update_timestamp | Source | System | Current_Timestamp | System Generated |
| Bronze | bz_customers | source_system | Source | System | Source_System_ID | System Generated |

## 2. Data Type Mapping

| Source Data Type | Bronze Layer Data Type | Databricks SQL Type | Rationale |
|------------------|------------------------|--------------------|-----------|
| INT | INT | INT | Direct mapping for integer values, compatible with Databricks Delta Lake |
| VARCHAR(255) | STRING | STRING | Databricks STRING type handles variable length text efficiently |
| VARCHAR(100) | STRING | STRING | Databricks STRING type handles variable length text efficiently |
| VARCHAR(20) | STRING | STRING | Databricks STRING type handles variable length text efficiently |
| DATE | DATE | DATE | Direct mapping for date values, compatible with PySpark |
| TIMESTAMP | TIMESTAMP | TIMESTAMP | System-generated timestamps for metadata tracking |

## 3. PII Classification

| Column Names | Table | Reason why it is classified as PII |
|--------------|-------|-------------------------------------|
| Customer_Name | bz_customers | Contains personal identifiable information - customer's full name |
| Email | bz_customers | Contains personal contact information that can identify individuals |
| Contact_Number | bz_suppliers | Contains personal contact information that can identify suppliers/individuals |
| Supplier_Name | bz_suppliers | Contains business entity names that may include personal information for sole proprietorships |

## 4. Metadata Management

### 4.1 Standard Metadata Columns
All Bronze layer tables include the following standard metadata columns for data lineage and governance:

| Column Name | Data Type | Purpose | Population Method |
|-------------|-----------|---------|-------------------|
| load_timestamp | TIMESTAMP | Records when data was initially loaded into Bronze layer | System generated during ingestion |
| update_timestamp | TIMESTAMP | Records when data was last updated in Bronze layer | System generated during updates |
| source_system | STRING | Identifies the source system that provided the data | Configured per ingestion pipeline |

### 4.2 Data Ingestion Rules

| Rule Category | Rule Description | Implementation |
|---------------|------------------|----------------|
| Data Preservation | Maintain original data structure with no business transformations | Raw data copied as-is from source |
| Metadata Enrichment | Add standard metadata columns to all tables | System-generated columns added during ingestion |
| Data Lineage | Track source system and ingestion timestamps | Populated automatically by ingestion framework |
| Schema Evolution | Support schema changes without breaking downstream processes | Delta Lake schema evolution enabled |

## 5. Initial Data Validation Rules

### 5.1 Basic Data Quality Checks

| Validation Type | Rule | Target Tables | Action on Failure |
|-----------------|------|---------------|-------------------|
| Null Check | Primary key fields cannot be null | All tables | Log error, continue processing |
| Data Type Check | Verify data types match expected schema | All tables | Log error, attempt type conversion |
| Duplicate Check | Identify duplicate records based on primary key | All tables | Log warning, keep all records |
| Referential Integrity | Log orphaned records (informational only) | All tables with foreign keys | Log warning, keep all records |

### 5.2 Data Freshness Monitoring

| Metric | Measurement | Threshold | Action |
|--------|-------------|-----------|--------|
| Data Latency | Time between source update and Bronze ingestion | < 1 hour | Alert if exceeded |
| Record Count | Compare source vs Bronze record counts | 100% match expected | Alert on significant variance |
| Schema Changes | Detect new columns or data type changes | Any change | Notify data engineering team |

## 6. Bronze Layer Architecture Details

### 6.1 Storage Configuration

| Configuration | Value | Rationale |
|---------------|-------|----------|
| Storage Format | Delta Lake | ACID transactions, time travel, schema evolution |
| Compression | Snappy | Good balance of compression ratio and query performance |
| Partitioning | Date-based for large tables (Orders, Shipments) | Improve query performance and data management |
| Location | /mnt/bronze/{table_name} | Organized storage structure |

### 6.2 Ingestion Patterns

| Pattern | Use Case | Implementation |
|---------|----------|----------------|
| Full Load | Initial data load and small tables | Complete table replacement |
| Incremental Load | Large tables with frequent updates | CDC-based or timestamp-based incremental processing |
| Real-time Streaming | High-frequency data sources | Structured Streaming with Delta Lake |

## 7. Data Governance and Compliance

### 7.1 Data Classification

| Classification Level | Tables | Access Controls |
|---------------------|--------|----------------|
| Public | bz_products, bz_warehouses | Standard access |
| Internal | bz_inventory, bz_orders, bz_shipments, bz_returns, bz_stock_levels | Role-based access |
| Confidential | bz_customers, bz_suppliers | Restricted access, PII handling |

### 7.2 Audit and Monitoring

| Audit Type | Frequency | Storage Location |
|------------|-----------|------------------|
| Data Lineage | Real-time | Unity Catalog lineage |
| Access Logs | Real-time | Databricks audit logs |
| Data Quality Metrics | Daily | Bronze audit tables |
| Schema Changes | Real-time | Delta Lake transaction log |

## 8. Implementation Guidelines

### 8.1 Naming Conventions
- **Schema**: All tables created in `bronze` schema
- **Table Prefix**: All table names use `bz_` prefix
- **Column Names**: Use source system column names with minimal changes
- **File Naming**: Follow pattern `{source_table_name}_YYYYMMDD_HHMMSS`

### 8.2 Performance Optimization
- **Z-Ordering**: Apply on frequently filtered columns
- **Table Optimization**: Schedule OPTIMIZE operations weekly
- **Vacuum**: Schedule VACUUM operations monthly
- **Statistics**: Auto-update table statistics enabled

### 8.3 Error Handling
- **Bad Records**: Store in separate error tables for investigation
- **Schema Mismatches**: Log and attempt automatic resolution
- **Connection Failures**: Implement retry logic with exponential backoff
- **Data Quality Issues**: Log but do not block ingestion

## 9. Monitoring and Alerting

### 9.1 Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|----------------|
| Ingestion Latency | Time from source to Bronze | > 2 hours |
| Error Rate | Percentage of failed records | > 5% |
| Data Volume | Daily record count variance | > 20% change |
| Schema Drift | Unexpected schema changes | Any occurrence |

### 9.2 Health Checks

| Check Type | Frequency | Success Criteria |
|------------|-----------|------------------|
| Connectivity | Every 15 minutes | Successful connection to all source systems |
| Data Freshness | Hourly | Data updated within SLA |
| Storage Health | Daily | Delta Lake tables accessible and optimized |
| Pipeline Status | Real-time | All ingestion jobs running successfully |

## 10. API Cost Reporting

apiCost: 0.000375

---

**Note**: This Bronze Model Data Mapping serves as the foundation for the Medallion architecture implementation in Databricks. It ensures raw data preservation while establishing the necessary metadata and governance framework for downstream Silver and Gold layer processing.