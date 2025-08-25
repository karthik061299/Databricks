_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive transformation rules for Fact tables in Gold layer of Inventory Management System
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Fact Transformation Recommender
## Inventory Management System - Fact Tables

## 1. Executive Summary

This document provides comprehensive transformation rules specifically for Fact tables in the Gold layer of the Inventory Management System. The transformation rules ensure data accuracy, consistency, and completeness for analytical reporting while maintaining proper relationships with dimension tables and implementing business logic requirements.

## 2. Fact Tables Identified

Based on the Gold Layer Physical DDL analysis, the following Fact tables have been identified:

| Fact Table Name | Primary Purpose | Key Metrics |
|-----------------|-----------------|-------------|
| Go_Inventory_Movement_Fact | Track all inventory movements | quantity_moved, unit_cost, total_value |
| Go_Sales_Fact | Record sales transactions | quantity_sold, unit_price, total_sales_amount, net_sales_amount |
| Go_Daily_Inventory_Summary | Daily inventory aggregations | opening_balance, closing_balance, total_value |
| Go_Monthly_Sales_Summary | Monthly sales aggregations | total_quantity_sold, net_sales_amount, average_transaction_value |

## 3. Transformation Rules for Fact Tables

### 3.1 Go_Inventory_Movement_Fact Transformation Rules

#### Rule 1: Surrogate Key Generation
**Description**: Generate unique surrogate keys for inventory movement records
- **Rationale**: Ensure unique identification and support for slowly changing dimensions
- **SQL Example**:
```sql
SELECT 
    ROW_NUMBER() OVER (ORDER BY si.Inventory_ID, si.load_date) as inventory_movement_id,
    CONCAT(si.Inventory_ID, '_', DATE_FORMAT(si.load_date, 'yyyyMMdd')) as inventory_movement_key,
    -- other columns
FROM silver.si_inventory si
```

#### Rule 2: Foreign Key Mapping to Dimensions
**Description**: Map natural keys to dimension surrogate keys
- **Rationale**: Establish proper star schema relationships and support dimension changes
- **SQL Example**:
```sql
SELECT 
    im.*,
    pd.product_key,
    wd.warehouse_key,
    sd.supplier_key,
    dd.date_key
FROM inventory_movement_staging im
LEFT JOIN Go_Product_Dimension pd ON im.Product_ID = pd.product_id AND pd.is_current = true
LEFT JOIN Go_Warehouse_Dimension wd ON im.Warehouse_ID = wd.warehouse_id AND wd.is_current = true
LEFT JOIN Go_Supplier_Dimension sd ON im.Supplier_ID = sd.supplier_id AND sd.is_current = true
LEFT JOIN Go_Date_Dimension dd ON DATE(im.movement_date) = dd.full_date
```

#### Rule 3: Movement Type Standardization
**Description**: Standardize movement types using predefined codes
- **Rationale**: Ensure consistency in movement type classification for accurate reporting
- **SQL Example**:
```sql
SELECT 
    CASE 
        WHEN movement_type IN ('IN', 'RECEIPT', 'PURCHASE') THEN 'INBOUND'
        WHEN movement_type IN ('OUT', 'ISSUE', 'SALE') THEN 'OUTBOUND'
        WHEN movement_type IN ('ADJ', 'ADJUSTMENT') THEN 'ADJUSTMENT'
        WHEN movement_type IN ('TRANSFER') THEN 'TRANSFER'
        ELSE 'OTHER'
    END as movement_type
FROM silver.si_inventory
```

#### Rule 4: Quantity and Value Calculations
**Description**: Calculate accurate quantity movements and monetary values
- **Rationale**: Ensure financial accuracy and proper inventory valuation
- **SQL Example**:
```sql
SELECT 
    COALESCE(quantity_moved, 0) as quantity_moved,
    COALESCE(unit_cost, 0.00) as unit_cost,
    ROUND(COALESCE(quantity_moved, 0) * COALESCE(unit_cost, 0.00), 2) as total_value
FROM silver.si_inventory
WHERE quantity_moved IS NOT NULL AND quantity_moved >= 0
```

#### Rule 5: Data Quality Validation
**Description**: Implement data quality checks for inventory movements
- **Rationale**: Prevent invalid data from entering the Gold layer
- **SQL Example**:
```sql
SELECT *
FROM inventory_movement_staging
WHERE quantity_moved > 0 
  AND unit_cost >= 0
  AND movement_date IS NOT NULL
  AND product_key IS NOT NULL
  AND warehouse_key IS NOT NULL
```

### 3.2 Go_Sales_Fact Transformation Rules

#### Rule 6: Sales Transaction Key Generation
**Description**: Generate unique keys for sales transactions
- **Rationale**: Support analytical queries and maintain referential integrity
- **SQL Example**:
```sql
SELECT 
    ROW_NUMBER() OVER (ORDER BY so.Order_ID, sod.Order_Detail_ID) as sales_id,
    CONCAT(so.Order_ID, '_', sod.Order_Detail_ID) as sales_key
FROM silver.si_orders so
JOIN silver.si_order_details sod ON so.Order_ID = sod.Order_ID
```

#### Rule 7: Sales Amount Calculations
**Description**: Calculate accurate sales amounts with proper rounding
- **Rationale**: Ensure financial accuracy and consistency in monetary calculations
- **SQL Example**:
```sql
SELECT 
    quantity_sold,
    unit_price,
    ROUND(quantity_sold * unit_price, 2) as total_sales_amount,
    COALESCE(discount_amount, 0.00) as discount_amount,
    COALESCE(tax_amount, 0.00) as tax_amount,
    ROUND((quantity_sold * unit_price) - COALESCE(discount_amount, 0) + COALESCE(tax_amount, 0), 2) as net_sales_amount
FROM sales_staging
```

#### Rule 8: Customer and Product Dimension Mapping
**Description**: Map sales transactions to appropriate dimension keys
- **Rationale**: Enable proper dimensional analysis and reporting
- **SQL Example**:
```sql
SELECT 
    sf.*,
    cd.customer_key,
    pd.product_key,
    wd.warehouse_key,
    dd.date_key
FROM sales_fact_staging sf
LEFT JOIN Go_Customer_Dimension cd ON sf.Customer_ID = cd.customer_id AND cd.is_current = true
LEFT JOIN Go_Product_Dimension pd ON sf.Product_ID = pd.product_id AND pd.is_current = true
LEFT JOIN Go_Warehouse_Dimension wd ON sf.Warehouse_ID = wd.warehouse_id AND wd.is_current = true
LEFT JOIN Go_Date_Dimension dd ON DATE(sf.order_date) = dd.full_date
```

#### Rule 9: Sales Data Validation
**Description**: Validate sales transaction data for completeness and accuracy
- **Rationale**: Ensure data quality and prevent analytical errors
- **SQL Example**:
```sql
SELECT *
FROM sales_staging
WHERE quantity_sold > 0
  AND unit_price > 0
  AND total_sales_amount > 0
  AND customer_key IS NOT NULL
  AND product_key IS NOT NULL
  AND date_key IS NOT NULL
```

### 3.3 Go_Daily_Inventory_Summary Transformation Rules

#### Rule 10: Daily Aggregation Logic
**Description**: Aggregate inventory movements by day, product, and warehouse
- **Rationale**: Provide daily inventory snapshots for operational reporting
- **SQL Example**:
```sql
SELECT 
    ROW_NUMBER() OVER (ORDER BY date_key, product_key, warehouse_key) as summary_id,
    CONCAT(date_key, '_', product_key, '_', warehouse_key) as summary_key,
    date_key,
    product_key,
    warehouse_key,
    LAG(closing_balance, 1, 0) OVER (PARTITION BY product_key, warehouse_key ORDER BY date_key) as opening_balance,
    SUM(CASE WHEN movement_type = 'INBOUND' THEN quantity_moved ELSE 0 END) as total_receipts,
    SUM(CASE WHEN movement_type = 'OUTBOUND' THEN quantity_moved ELSE 0 END) as total_issues,
    SUM(CASE WHEN movement_type = 'ADJUSTMENT' THEN quantity_moved ELSE 0 END) as total_adjustments
FROM Go_Inventory_Movement_Fact
GROUP BY date_key, product_key, warehouse_key
```

#### Rule 11: Closing Balance Calculation
**Description**: Calculate accurate closing balances for inventory
- **Rationale**: Maintain inventory accuracy and support stock level monitoring
- **SQL Example**:
```sql
SELECT 
    *,
    (opening_balance + total_receipts - total_issues + total_adjustments) as closing_balance,
    ROUND(closing_balance * average_cost, 2) as total_value
FROM daily_inventory_base
```

### 3.4 Go_Monthly_Sales_Summary Transformation Rules

#### Rule 12: Monthly Sales Aggregation
**Description**: Aggregate sales data by month, product, warehouse, and customer
- **Rationale**: Support monthly reporting and trend analysis
- **SQL Example**:
```sql
SELECT 
    ROW_NUMBER() OVER (ORDER BY year_month, product_key, warehouse_key, customer_key) as summary_id,
    CONCAT(year_month, '_', product_key, '_', warehouse_key, '_', customer_key) as summary_key,
    DATE_FORMAT(dd.full_date, 'yyyyMM') as year_month,
    sf.product_key,
    sf.warehouse_key,
    sf.customer_key,
    SUM(sf.quantity_sold) as total_quantity_sold,
    SUM(sf.total_sales_amount) as total_sales_amount,
    SUM(sf.discount_amount) as total_discount_amount,
    SUM(sf.net_sales_amount) as net_sales_amount,
    COUNT(*) as number_of_transactions,
    ROUND(AVG(sf.net_sales_amount), 2) as average_transaction_value
FROM Go_Sales_Fact sf
JOIN Go_Date_Dimension dd ON sf.date_key = dd.date_key
GROUP BY DATE_FORMAT(dd.full_date, 'yyyyMM'), sf.product_key, sf.warehouse_key, sf.customer_key
```

## 4. Data Quality and Validation Rules

### 4.1 Mandatory Field Validation
**Description**: Ensure all mandatory fields are populated
- **Rationale**: Prevent incomplete records from affecting analytical accuracy
- **SQL Example**:
```sql
-- Validation for Go_Inventory_Movement_Fact
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN product_key IS NULL THEN 1 END) as missing_product_key,
    COUNT(CASE WHEN warehouse_key IS NULL THEN 1 END) as missing_warehouse_key,
    COUNT(CASE WHEN quantity_moved IS NULL THEN 1 END) as missing_quantity
FROM Go_Inventory_Movement_Fact
```

### 4.2 Business Rule Validation
**Description**: Implement business logic validation
- **Rationale**: Ensure data conforms to business requirements and constraints
- **SQL Example**:
```sql
-- Validate inventory movements don't result in negative stock
WITH stock_validation AS (
    SELECT 
        product_key,
        warehouse_key,
        SUM(CASE WHEN movement_type = 'INBOUND' THEN quantity_moved 
                 WHEN movement_type = 'OUTBOUND' THEN -quantity_moved 
                 ELSE quantity_moved END) as net_movement
    FROM Go_Inventory_Movement_Fact
    GROUP BY product_key, warehouse_key
)
SELECT * FROM stock_validation WHERE net_movement < 0
```

### 4.3 Referential Integrity Validation
**Description**: Validate foreign key relationships
- **Rationale**: Ensure all fact records have valid dimension references
- **SQL Example**:
```sql
-- Check for orphaned fact records
SELECT 
    'Go_Inventory_Movement_Fact' as table_name,
    COUNT(*) as orphaned_records
FROM Go_Inventory_Movement_Fact imf
LEFT JOIN Go_Product_Dimension pd ON imf.product_key = pd.product_key
WHERE pd.product_key IS NULL
```

## 5. Performance Optimization Rules

### 5.1 Partitioning Strategy
**Description**: Implement optimal partitioning for fact tables
- **Rationale**: Improve query performance and data management
- **SQL Example**:
```sql
-- Partition fact tables by date for optimal performance
CREATE TABLE Go_Inventory_Movement_Fact_Optimized
USING DELTA
PARTITIONED BY (date_key)
AS SELECT * FROM Go_Inventory_Movement_Fact
```

### 5.2 Indexing and Clustering
**Description**: Apply Z-ORDER optimization for frequently queried columns
- **Rationale**: Accelerate analytical queries and reporting
- **SQL Example**:
```sql
-- Optimize fact tables with Z-ORDER
OPTIMIZE Go_Inventory_Movement_Fact ZORDER BY (product_key, warehouse_key, date_key);
OPTIMIZE Go_Sales_Fact ZORDER BY (product_key, customer_key, date_key);
```

## 6. Error Handling and Logging

### 6.1 Error Capture Rules
**Description**: Capture and log transformation errors
- **Rationale**: Enable data quality monitoring and issue resolution
- **SQL Example**:
```sql
-- Insert error records for failed transformations
INSERT INTO Go_Data_Validation_Error (
    error_key,
    table_name,
    error_type,
    error_description,
    record_identifier,
    error_timestamp
)
SELECT 
    CONCAT('ERR_', UUID()) as error_key,
    'Go_Inventory_Movement_Fact' as table_name,
    'MISSING_DIMENSION_KEY' as error_type,
    'Product key not found in dimension' as error_description,
    CAST(inventory_movement_id AS STRING) as record_identifier,
    CURRENT_TIMESTAMP() as error_timestamp
FROM inventory_movement_staging
WHERE product_key IS NULL
```

## 7. Incremental Loading Strategy

### 7.1 Delta Processing Rules
**Description**: Implement efficient incremental loading
- **Rationale**: Minimize processing time and resource usage
- **SQL Example**:
```sql
-- Incremental load using MERGE operation
MERGE INTO Go_Inventory_Movement_Fact as target
USING (
    SELECT * FROM inventory_movement_staging 
    WHERE load_date > (SELECT MAX(load_date) FROM Go_Inventory_Movement_Fact)
) as source
ON target.inventory_movement_key = source.inventory_movement_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## 8. Data Lineage and Audit Trail

### 8.1 Source System Tracking
**Description**: Maintain complete data lineage for fact records
- **Rationale**: Support data governance and troubleshooting
- **SQL Example**:
```sql
SELECT 
    *,
    'silver.si_inventory' as source_table,
    'ETL_BATCH_001' as pipeline_run_id,
    CURRENT_TIMESTAMP() as load_date,
    'INVENTORY_SYSTEM' as source_system
FROM transformed_inventory_data
```

## 9. Business KPI Calculations

### 9.1 Inventory Turnover Metrics
**Description**: Calculate key inventory performance indicators
- **Rationale**: Support business decision making and performance monitoring
- **SQL Example**:
```sql
-- Calculate inventory turnover ratio
WITH inventory_metrics AS (
    SELECT 
        product_key,
        warehouse_key,
        SUM(CASE WHEN movement_type = 'OUTBOUND' THEN total_value ELSE 0 END) as cogs,
        AVG(closing_balance * average_cost) as avg_inventory_value
    FROM Go_Daily_Inventory_Summary
    WHERE date_key >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -12), 'yyyyMMdd')
    GROUP BY product_key, warehouse_key
)
SELECT 
    *,
    CASE WHEN avg_inventory_value > 0 THEN ROUND(cogs / avg_inventory_value, 2) ELSE 0 END as inventory_turnover_ratio
FROM inventory_metrics
```

## 10. Implementation Checklist

### 10.1 Pre-Implementation Validation
- [ ] Verify all source tables exist in Silver layer
- [ ] Validate dimension tables are populated
- [ ] Confirm date dimension covers required date range
- [ ] Test transformation logic with sample data

### 10.2 Implementation Steps
1. **Phase 1**: Implement basic fact table transformations
2. **Phase 2**: Add data quality validation rules
3. **Phase 3**: Implement aggregated fact tables
4. **Phase 4**: Add performance optimizations
5. **Phase 5**: Implement error handling and monitoring

### 10.3 Post-Implementation Validation
- [ ] Verify record counts match expectations
- [ ] Validate calculated fields accuracy
- [ ] Confirm dimension key mappings
- [ ] Test query performance
- [ ] Validate business KPI calculations

## 11. Monitoring and Maintenance

### 11.1 Data Quality Monitoring
**Description**: Continuous monitoring of fact table data quality
- **Rationale**: Ensure ongoing data accuracy and completeness
- **SQL Example**:
```sql
-- Daily data quality check
SELECT 
    DATE(load_date) as load_date,
    COUNT(*) as total_records,
    COUNT(CASE WHEN product_key IS NULL THEN 1 END) as missing_product_keys,
    COUNT(CASE WHEN quantity_moved <= 0 THEN 1 END) as invalid_quantities,
    ROUND(100.0 * COUNT(CASE WHEN product_key IS NOT NULL AND quantity_moved > 0 THEN 1 END) / COUNT(*), 2) as data_quality_score
FROM Go_Inventory_Movement_Fact
WHERE DATE(load_date) = CURRENT_DATE()
GROUP BY DATE(load_date)
```

### 11.2 Performance Monitoring
**Description**: Monitor transformation performance and optimize as needed
- **Rationale**: Maintain efficient processing and meet SLA requirements
- **SQL Example**:
```sql
-- Monitor transformation performance
SELECT 
    process_name,
    AVG(process_duration_seconds) as avg_duration,
    MAX(process_duration_seconds) as max_duration,
    COUNT(*) as execution_count
FROM Go_Process_Audit
WHERE process_start_time >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY process_name
ORDER BY avg_duration DESC
```

## 12. API Cost Calculation

**apiCost**: 0.0425

---

**Document Version**: 1.0  
**Last Updated**: Generated automatically  
**Next Review Date**: 30 days from creation  
**Approval Status**: Pending Review