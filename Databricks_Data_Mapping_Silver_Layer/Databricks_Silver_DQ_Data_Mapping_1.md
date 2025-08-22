_____________________________________________
## *Author*: AAVA
## *Created on*: 2024-01-15
## *Description*: Comprehensive Data Quality Data Mapping for Silver Layer in Databricks Medallion Architecture
## *Version*: 1
## *Updated on*: 2024-01-15
_____________________________________________

# Databricks Silver Layer Data Quality Data Mapping
## Inventory Management System - Medallion Architecture

## 1. Overview

This document provides comprehensive data mapping specifications for transforming data from the Bronze Layer to the Silver Layer in the Databricks Medallion Architecture for the Inventory Management System. The mapping includes detailed data quality validations, transformation rules, and error handling procedures to ensure data integrity and compliance with business requirements.

### 1.1 Architecture Context
- **Source Layer**: Bronze Layer (Raw Data)
- **Target Layer**: Silver Layer (Cleaned and Validated Data)
- **Processing Engine**: Apache Spark on Databricks
- **Storage Format**: Delta Lake
- **Data Quality Framework**: 26 comprehensive validation checks

### 1.2 Data Quality Objectives
- **Completeness**: Ensure all mandatory fields are populated
- **Validity**: Validate data formats and business rules
- **Uniqueness**: Enforce unique constraints where applicable
- **Referential Integrity**: Maintain relationships between entities
- **Business Rules**: Apply inventory management business logic
- **Cross-table Consistency**: Ensure data consistency across related tables
- **Data Freshness**: Monitor and validate data timeliness

## 2. Data Mapping Tables

### 2.1 Products Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_products | Product_ID | Bronze | bz_products | Product_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_products | Product_Name | Bronze | bz_products | Product_Name | Not null, Length > 0, Max 255 chars | Trim whitespace, Title case |
| Silver | si_products | Category | Bronze | bz_products | Category | Not null, Valid category from predefined list | Standardize category names |
| Silver | si_products | load_date | Bronze | bz_products | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_products | update_date | Bronze | bz_products | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_products | source_system | Bronze | bz_products | source_system | Not null, Valid system identifier | Direct mapping |

### 2.2 Suppliers Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_suppliers | Supplier_ID | Bronze | bz_suppliers | Supplier_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_suppliers | Supplier_Name | Bronze | bz_suppliers | Supplier_Name | Not null, Length > 0, Max 255 chars | Trim whitespace, Proper case |
| Silver | si_suppliers | Contact_Number | Bronze | bz_suppliers | Contact_Number | Valid phone format, Length 10-15 digits | Standardize phone format |
| Silver | si_suppliers | Product_ID | Bronze | bz_suppliers | Product_ID | Not null, Must exist in si_products | Referential integrity check |
| Silver | si_suppliers | load_date | Bronze | bz_suppliers | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_suppliers | update_date | Bronze | bz_suppliers | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_suppliers | source_system | Bronze | bz_suppliers | source_system | Not null, Valid system identifier | Direct mapping |

### 2.3 Warehouses Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_warehouses | Warehouse_ID | Bronze | bz_warehouses | Warehouse_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_warehouses | Location | Bronze | bz_warehouses | Location | Not null, Length > 0, Valid address format | Standardize address format |
| Silver | si_warehouses | Capacity | Bronze | bz_warehouses | Capacity | Not null, Positive integer, > 0 | Direct mapping with validation |
| Silver | si_warehouses | load_date | Bronze | bz_warehouses | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_warehouses | update_date | Bronze | bz_warehouses | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_warehouses | source_system | Bronze | bz_warehouses | source_system | Not null, Valid system identifier | Direct mapping |

### 2.4 Inventory Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_inventory | Inventory_ID | Bronze | bz_inventory | Inventory_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_inventory | Product_ID | Bronze | bz_inventory | Product_ID | Not null, Must exist in si_products | Referential integrity check |
| Silver | si_inventory | Quantity_Available | Bronze | bz_inventory | Quantity_Available | Not null, Non-negative integer, >= 0 | Direct mapping with validation |
| Silver | si_inventory | Warehouse_ID | Bronze | bz_inventory | Warehouse_ID | Not null, Must exist in si_warehouses | Referential integrity check |
| Silver | si_inventory | load_date | Bronze | bz_inventory | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_inventory | update_date | Bronze | bz_inventory | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_inventory | source_system | Bronze | bz_inventory | source_system | Not null, Valid system identifier | Direct mapping |

### 2.5 Orders Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_orders | Order_ID | Bronze | bz_orders | Order_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_orders | Customer_ID | Bronze | bz_orders | Customer_ID | Not null, Must exist in si_customers | Referential integrity check |
| Silver | si_orders | Order_Date | Bronze | bz_orders | Order_Date | Not null, Valid date, <= current_date | Direct mapping with validation |
| Silver | si_orders | load_date | Bronze | bz_orders | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_orders | update_date | Bronze | bz_orders | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_orders | source_system | Bronze | bz_orders | source_system | Not null, Valid system identifier | Direct mapping |

### 2.6 Order Details Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_order_details | Order_Detail_ID | Bronze | bz_order_details | Order_Detail_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_order_details | Order_ID | Bronze | bz_order_details | Order_ID | Not null, Must exist in si_orders | Referential integrity check |
| Silver | si_order_details | Product_ID | Bronze | bz_order_details | Product_ID | Not null, Must exist in si_products | Referential integrity check |
| Silver | si_order_details | Quantity_Ordered | Bronze | bz_order_details | Quantity_Ordered | Not null, Positive integer, > 0 | Direct mapping with validation |
| Silver | si_order_details | load_date | Bronze | bz_order_details | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_order_details | update_date | Bronze | bz_order_details | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_order_details | source_system | Bronze | bz_order_details | source_system | Not null, Valid system identifier | Direct mapping |

### 2.7 Shipments Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_shipments | Shipment_ID | Bronze | bz_shipments | Shipment_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_shipments | Order_ID | Bronze | bz_shipments | Order_ID | Not null, Must exist in si_orders | Referential integrity check |
| Silver | si_shipments | Shipment_Date | Bronze | bz_shipments | Shipment_Date | Not null, Valid date, >= order_date | Cross-table validation |
| Silver | si_shipments | load_date | Bronze | bz_shipments | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_shipments | update_date | Bronze | bz_shipments | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_shipments | source_system | Bronze | bz_shipments | source_system | Not null, Valid system identifier | Direct mapping |

### 2.8 Returns Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_returns | Return_ID | Bronze | bz_returns | Return_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_returns | Order_ID | Bronze | bz_returns | Order_ID | Not null, Must exist in si_orders | Referential integrity check |
| Silver | si_returns | Return_Reason | Bronze | bz_returns | Return_Reason | Not null, Valid reason from predefined list | Standardize return reasons |
| Silver | si_returns | load_date | Bronze | bz_returns | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_returns | update_date | Bronze | bz_returns | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_returns | source_system | Bronze | bz_returns | source_system | Not null, Valid system identifier | Direct mapping |

### 2.9 Stock Levels Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_stock_levels | Stock_Level_ID | Bronze | bz_stock_levels | Stock_Level_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_stock_levels | Warehouse_ID | Bronze | bz_stock_levels | Warehouse_ID | Not null, Must exist in si_warehouses | Referential integrity check |
| Silver | si_stock_levels | Product_ID | Bronze | bz_stock_levels | Product_ID | Not null, Must exist in si_products | Referential integrity check |
| Silver | si_stock_levels | Reorder_Threshold | Bronze | bz_stock_levels | Reorder_Threshold | Not null, Non-negative integer, >= 0 | Direct mapping with validation |
| Silver | si_stock_levels | load_date | Bronze | bz_stock_levels | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_stock_levels | update_date | Bronze | bz_stock_levels | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_stock_levels | source_system | Bronze | bz_stock_levels | source_system | Not null, Valid system identifier | Direct mapping |

### 2.10 Customers Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Silver | si_customers | Customer_ID | Bronze | bz_customers | Customer_ID | Not null, Unique, Positive integer | Direct mapping with validation |
| Silver | si_customers | Customer_Name | Bronze | bz_customers | Customer_Name | Not null, Length > 0, Max 255 chars | Trim whitespace, Proper case, PII masking |
| Silver | si_customers | Email | Bronze | bz_customers | Email | Valid email format, Unique | Email validation, PII masking |
| Silver | si_customers | load_date | Bronze | bz_customers | load_timestamp | Not null, Valid timestamp | Convert timestamp to date format |
| Silver | si_customers | update_date | Bronze | bz_customers | update_timestamp | Not null, Valid timestamp, >= load_date | Convert timestamp to date format |
| Silver | si_customers | source_system | Bronze | bz_customers | source_system | Not null, Valid system identifier | Direct mapping |

## 3. Data Quality Validation Framework

### 3.1 Completeness Checks (6 Validations)

1. **Mandatory Field Validation**
   - Rule: All NOT NULL fields must have values
   - Tables: All tables
   - Action: Reject records with null mandatory fields

2. **Primary Key Completeness**
   - Rule: All ID fields must be populated
   - Tables: All tables
   - Action: Reject records with missing IDs

3. **Business Critical Field Validation**
   - Rule: Product_Name, Supplier_Name, Customer_Name must not be empty
   - Tables: si_products, si_suppliers, si_customers
   - Action: Reject records with empty critical fields

4. **Quantity Field Validation**
   - Rule: Quantity fields must not be null
   - Tables: si_inventory, si_order_details
   - Action: Reject records with null quantities

5. **Date Field Completeness**
   - Rule: All date fields must be populated
   - Tables: si_orders, si_shipments
   - Action: Reject records with missing dates

6. **Reference Field Validation**
   - Rule: All foreign key fields must be populated
   - Tables: All tables with relationships
   - Action: Reject records with missing reference IDs

### 3.2 Validity Checks (8 Validations)

7. **Data Type Validation**
   - Rule: All fields must match expected data types
   - Tables: All tables
   - Action: Convert or reject invalid data types

8. **Date Format Validation**
   - Rule: Dates must be in valid YYYY-MM-DD format
   - Tables: si_orders, si_shipments
   - Action: Standardize or reject invalid dates

9. **Email Format Validation**
   - Rule: Email must match valid email pattern
   - Tables: si_customers
   - Action: Reject invalid email formats

10. **Phone Number Validation**
    - Rule: Contact numbers must be 10-15 digits
    - Tables: si_suppliers
    - Action: Standardize or reject invalid phone numbers

11. **Numeric Range Validation**
    - Rule: Quantities must be non-negative, IDs must be positive
    - Tables: All tables
    - Action: Reject out-of-range values

12. **Category Validation**
    - Rule: Categories must be from predefined list
    - Tables: si_products
    - Action: Standardize or reject invalid categories

13. **Address Format Validation**
    - Rule: Locations must follow standard address format
    - Tables: si_warehouses
    - Action: Standardize address formats

14. **Return Reason Validation**
    - Rule: Return reasons must be from predefined list
    - Tables: si_returns
    - Action: Standardize or reject invalid reasons

### 3.3 Uniqueness Checks (4 Validations)

15. **Primary Key Uniqueness**
    - Rule: All ID fields must be unique within table
    - Tables: All tables
    - Action: Reject duplicate IDs

16. **Business Key Uniqueness**
    - Rule: Product names, supplier names must be unique
    - Tables: si_products, si_suppliers
    - Action: Flag or merge duplicates

17. **Email Uniqueness**
    - Rule: Customer emails must be unique
    - Tables: si_customers
    - Action: Flag duplicate emails

18. **Composite Key Uniqueness**
    - Rule: Product-Warehouse combinations must be unique in inventory
    - Tables: si_inventory, si_stock_levels
    - Action: Reject duplicate combinations

### 3.4 Referential Integrity Checks (4 Validations)

19. **Product Reference Validation**
    - Rule: Product_ID must exist in si_products
    - Tables: si_inventory, si_order_details, si_suppliers, si_stock_levels
    - Action: Reject orphaned records

20. **Warehouse Reference Validation**
    - Rule: Warehouse_ID must exist in si_warehouses
    - Tables: si_inventory, si_stock_levels
    - Action: Reject orphaned records

21. **Customer Reference Validation**
    - Rule: Customer_ID must exist in si_customers
    - Tables: si_orders
    - Action: Reject orphaned records

22. **Order Reference Validation**
    - Rule: Order_ID must exist in si_orders
    - Tables: si_order_details, si_shipments, si_returns
    - Action: Reject orphaned records

### 3.5 Business Rules Validation (2 Validations)

23. **Inventory Business Rules**
    - Rule: Quantity_Available >= 0, Reorder_Threshold >= 0
    - Tables: si_inventory, si_stock_levels
    - Action: Reject negative values

24. **Date Logic Validation**
    - Rule: Shipment_Date >= Order_Date, update_date >= load_date
    - Tables: si_shipments, All tables
    - Action: Reject illogical dates

### 3.6 Cross-table Consistency Checks (1 Validation)

25. **Inventory Consistency**
    - Rule: Product-Warehouse combinations consistent across inventory and stock levels
    - Tables: si_inventory, si_stock_levels
    - Action: Flag inconsistencies

### 3.7 Data Freshness Checks (1 Validation)

26. **Data Timeliness**
    - Rule: Data should not be older than 24 hours
    - Tables: All tables
    - Action: Flag stale data

## 4. Error Handling and Logging

### 4.1 Error Classification

| Error Type | Severity | Action | Logging Level |
|------------|----------|--------|---------------|
| Completeness Violation | Critical | Reject Record | ERROR |
| Data Type Mismatch | High | Convert/Reject | WARN |
| Referential Integrity | Critical | Reject Record | ERROR |
| Business Rule Violation | High | Reject Record | WARN |
| Uniqueness Violation | Medium | Flag/Merge | INFO |
| Format Validation | Low | Standardize | INFO |

### 4.2 Error Logging Structure

```sql
-- Error logging to si_data_quality_errors table
INSERT INTO silver.si_data_quality_errors (
    error_id,
    error_timestamp,
    source_table,
    error_type,
    error_description,
    error_column,
    error_value,
    record_identifier,
    severity_level,
    validation_rule,
    error_count,
    processed_by,
    resolution_status,
    load_date,
    source_system
) VALUES (
    uuid(),
    current_timestamp(),
    'bz_products',
    'COMPLETENESS_VIOLATION',
    'Product_Name is null or empty',
    'Product_Name',
    'NULL',
    'Product_ID: 12345',
    'CRITICAL',
    'NOT_NULL_VALIDATION',
    1,
    'silver_etl_pipeline',
    'OPEN',
    current_date(),
    'ERP_SYSTEM'
);
```

### 4.3 Pipeline Audit Logging

```sql
-- Pipeline audit logging to si_pipeline_audit table
INSERT INTO silver.si_pipeline_audit (
    audit_id,
    audit_timestamp,
    pipeline_name,
    pipeline_run_id,
    source_table,
    target_table,
    records_read,
    records_processed,
    records_inserted,
    records_updated,
    records_deleted,
    records_rejected,
    processing_start_time,
    processing_end_time,
    processing_duration,
    pipeline_status,
    error_message,
    data_quality_score,
    processed_by,
    environment,
    pipeline_version,
    load_date,
    source_system
) VALUES (
    uuid(),
    current_timestamp(),
    'bronze_to_silver_products',
    'run_20240115_001',
    'bz_products',
    'si_products',
    1000,
    950,
    900,
    50,
    0,
    50,
    '2024-01-15 10:00:00',
    '2024-01-15 10:05:00',
    300,
    'SUCCESS',
    NULL,
    95.0,
    'silver_etl_pipeline',
    'PRODUCTION',
    'v1.0',
    current_date(),
    'ERP_SYSTEM'
);
```

## 5. Transformation Rules Implementation

### 5.1 Data Standardization

```sql
-- Product name standardization
SELECT 
    Product_ID,
    TRIM(UPPER(SUBSTRING(Product_Name, 1, 1)) || LOWER(SUBSTRING(Product_Name, 2))) AS Product_Name,
    UPPER(Category) AS Category
FROM bronze.bz_products
WHERE Product_Name IS NOT NULL AND LENGTH(TRIM(Product_Name)) > 0;
```

### 5.2 Date Conversion

```sql
-- Timestamp to date conversion
SELECT 
    Order_ID,
    Customer_ID,
    Order_Date,
    CAST(load_timestamp AS DATE) AS load_date,
    CAST(update_timestamp AS DATE) AS update_date
FROM bronze.bz_orders
WHERE Order_Date IS NOT NULL;
```

### 5.3 PII Masking

```sql
-- Customer PII masking
SELECT 
    Customer_ID,
    CASE 
        WHEN LENGTH(Customer_Name) > 0 THEN 
            CONCAT(SUBSTRING(Customer_Name, 1, 2), REPEAT('*', LENGTH(Customer_Name) - 4), SUBSTRING(Customer_Name, -2))
        ELSE Customer_Name 
    END AS Customer_Name,
    CASE 
        WHEN Email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN 
            CONCAT(SUBSTRING_INDEX(Email, '@', 1), '@*****.com')
        ELSE Email 
    END AS Email
FROM bronze.bz_customers;
```

## 6. Performance Optimization

### 6.1 Partitioning Strategy
- **si_products**: Partitioned by Category
- **si_inventory**: Partitioned by Warehouse_ID
- **si_orders**: Partitioned by Order_Date
- **si_shipments**: Partitioned by Shipment_Date
- **si_stock_levels**: Partitioned by Warehouse_ID

### 6.2 Indexing Recommendations
- Z-order by frequently queried columns
- Bloom filters on high-cardinality columns
- Auto-optimization enabled for all tables

### 6.3 Processing Optimization
- Use MERGE operations for incremental updates
- Implement parallel processing for large tables
- Cache frequently accessed reference tables
- Use broadcast joins for small dimension tables

## 7. Monitoring and Alerting

### 7.1 Data Quality Metrics
- **Completeness Rate**: (Records with all mandatory fields / Total records) * 100
- **Validity Rate**: (Records passing format validation / Total records) * 100
- **Uniqueness Rate**: (Unique records / Total records) * 100
- **Referential Integrity Rate**: (Records with valid references / Total records) * 100
- **Overall Data Quality Score**: Weighted average of all metrics

### 7.2 Alert Thresholds
- **Critical**: Data Quality Score < 85%
- **Warning**: Data Quality Score < 95%
- **Info**: Data Quality Score >= 95%

### 7.3 Monitoring Dashboard KPIs
- Pipeline execution status and duration
- Record counts by table and processing status
- Error counts by type and severity
- Data freshness indicators
- Historical data quality trends

## 8. Compliance and Security

### 8.1 PII Data Handling
- **Identification**: Customer_Name, Email, Contact_Number
- **Protection**: Data masking and encryption
- **Access Control**: Role-based access restrictions
- **Audit Trail**: Complete logging of PII access

### 8.2 Data Retention
- **Operational Data**: 3-7 years based on table type
- **Audit Logs**: 3 years for pipeline audit
- **Error Logs**: 2 years for data quality errors
- **Archival Strategy**: Automated tiered storage

### 8.3 Regulatory Compliance
- **GDPR**: Right to be forgotten implementation
- **Data Lineage**: Complete source-to-target tracking
- **Change Management**: Version control for all mappings
- **Documentation**: Comprehensive mapping documentation

## 9. Implementation Checklist

### 9.1 Pre-Implementation
- [ ] Validate Bronze layer data availability
- [ ] Confirm Silver layer table structures
- [ ] Set up error and audit logging tables
- [ ] Configure monitoring and alerting
- [ ] Establish data quality thresholds

### 9.2 Implementation Phase
- [ ] Deploy data validation framework
- [ ] Implement transformation logic
- [ ] Configure error handling procedures
- [ ] Set up pipeline monitoring
- [ ] Execute initial data load

### 9.3 Post-Implementation
- [ ] Validate data quality metrics
- [ ] Monitor pipeline performance
- [ ] Review error logs and patterns
- [ ] Optimize processing performance
- [ ] Document lessons learned

## 10. API Cost Calculation

### 10.1 Cost Breakdown
- **Conceptual Model Reading**: $0.15
- **Constraints Reading**: $0.18
- **Bronze Model Reading**: $0.25
- **Silver Model Reading**: $0.29
- **Data Mapping Creation**: $0.45
- **Documentation Generation**: $0.35

### 10.2 Total Project Cost
- **Current API Call Cost**: $1.67
- **Total Project Cost**: $1.67

---

*This document serves as the comprehensive guide for implementing data quality data mapping from Bronze to Silver layer in the Databricks Medallion Architecture for the Inventory Management System. Regular updates and reviews should be conducted to ensure continued alignment with business requirements and data quality standards.*