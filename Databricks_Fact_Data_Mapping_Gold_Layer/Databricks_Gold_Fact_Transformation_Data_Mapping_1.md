_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive data mapping for Fact tables in Gold Layer with transformations, validations, and aggregation rules
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Fact Transformation Data Mapping
## Inventory Management System - Comprehensive Data Mapping

## 1. Overview

This document provides a comprehensive data mapping for Fact tables in the Gold Layer of the Inventory Management System. The mapping incorporates necessary transformations, validations, and aggregation rules at the attribute level, ensuring high data quality and consistency for business intelligence applications.

### Key Considerations:
- **Fact-Dimension Relationships**: Proper foreign key mappings to dimension tables
- **Data Quality**: Comprehensive validation rules and cleansing logic
- **Performance**: Optimized transformations compatible with PySpark and Databricks
- **Business Logic**: Accurate metric calculations and aggregations
- **Scalability**: Incremental loading and efficient processing strategies

## 2. Data Mapping for Fact Tables

### 2.1 Go_Inventory_Movement_Fact

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Gold | Go_Inventory_Movement_Fact | inventory_movement_id | Silver | si_inventory | - | NOT NULL, UNIQUE | ROW_NUMBER() OVER (ORDER BY Inventory_ID, load_date) |
| Gold | Go_Inventory_Movement_Fact | inventory_movement_key | Silver | si_inventory | Inventory_ID, load_date | NOT NULL, UNIQUE | CONCAT(Inventory_ID, '_', DATE_FORMAT(load_date, 'yyyyMMdd')) |
| Gold | Go_Inventory_Movement_Fact | product_key | Silver | si_inventory | Product_ID | NOT NULL, FK to Go_Product_Dimension | Map Product_ID to product_key via dimension lookup |
| Gold | Go_Inventory_Movement_Fact | warehouse_key | Silver | si_inventory | Warehouse_ID | NOT NULL, FK to Go_Warehouse_Dimension | Map Warehouse_ID to warehouse_key via dimension lookup |
| Gold | Go_Inventory_Movement_Fact | supplier_key | Silver | si_inventory | Supplier_ID | FK to Go_Supplier_Dimension | Map Supplier_ID to supplier_key via dimension lookup |
| Gold | Go_Inventory_Movement_Fact | date_key | Silver | si_inventory | movement_date | NOT NULL, FK to Go_Date_Dimension | Map DATE(movement_date) to date_key via dimension lookup |
| Gold | Go_Inventory_Movement_Fact | movement_type | Silver | si_inventory | movement_type | NOT NULL, Valid values | Standardize: IN/RECEIPT/PURCHASE→INBOUND, OUT/ISSUE/SALE→OUTBOUND, ADJ/ADJUSTMENT→ADJUSTMENT, TRANSFER→TRANSFER |
| Gold | Go_Inventory_Movement_Fact | quantity_moved | Silver | si_inventory | quantity_moved | NOT NULL, >= 0 | COALESCE(quantity_moved, 0), validate > 0 |
| Gold | Go_Inventory_Movement_Fact | unit_cost | Silver | si_inventory | unit_cost | >= 0.00 | COALESCE(unit_cost, 0.00), ROUND(unit_cost, 2) |
| Gold | Go_Inventory_Movement_Fact | total_value | Silver | si_inventory | quantity_moved, unit_cost | >= 0.00 | ROUND(quantity_moved * unit_cost, 2) |
| Gold | Go_Inventory_Movement_Fact | load_date | Silver | si_inventory | load_date | NOT NULL | CURRENT_TIMESTAMP() |
| Gold | Go_Inventory_Movement_Fact | source_system | Silver | si_inventory | - | NOT NULL | 'INVENTORY_SYSTEM' |

### 2.2 Go_Sales_Fact

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Gold | Go_Sales_Fact | sales_id | Silver | si_orders, si_order_details | Order_ID, Order_Detail_ID | NOT NULL, UNIQUE | ROW_NUMBER() OVER (ORDER BY Order_ID, Order_Detail_ID) |
| Gold | Go_Sales_Fact | sales_key | Silver | si_orders, si_order_details | Order_ID, Order_Detail_ID | NOT NULL, UNIQUE | CONCAT(Order_ID, '_', Order_Detail_ID) |
| Gold | Go_Sales_Fact | customer_key | Silver | si_orders | Customer_ID | NOT NULL, FK to Go_Customer_Dimension | Map Customer_ID to customer_key via dimension lookup |
| Gold | Go_Sales_Fact | product_key | Silver | si_order_details | Product_ID | NOT NULL, FK to Go_Product_Dimension | Map Product_ID to product_key via dimension lookup |
| Gold | Go_Sales_Fact | warehouse_key | Silver | si_orders | Warehouse_ID | NOT NULL, FK to Go_Warehouse_Dimension | Map Warehouse_ID to warehouse_key via dimension lookup |
| Gold | Go_Sales_Fact | date_key | Silver | si_orders | order_date | NOT NULL, FK to Go_Date_Dimension | Map DATE(order_date) to date_key via dimension lookup |
| Gold | Go_Sales_Fact | quantity_sold | Silver | si_order_details | quantity_sold | NOT NULL, > 0 | Validate quantity_sold > 0 |
| Gold | Go_Sales_Fact | unit_price | Silver | si_order_details | unit_price | NOT NULL, > 0 | Validate unit_price > 0, ROUND(unit_price, 2) |
| Gold | Go_Sales_Fact | total_sales_amount | Silver | si_order_details | quantity_sold, unit_price | NOT NULL, > 0 | ROUND(quantity_sold * unit_price, 2) |
| Gold | Go_Sales_Fact | discount_amount | Silver | si_order_details | discount_amount | >= 0.00 | COALESCE(discount_amount, 0.00), ROUND(discount_amount, 2) |
| Gold | Go_Sales_Fact | tax_amount | Silver | si_order_details | tax_amount | >= 0.00 | COALESCE(tax_amount, 0.00), ROUND(tax_amount, 2) |
| Gold | Go_Sales_Fact | net_sales_amount | Silver | si_order_details | total_sales_amount, discount_amount, tax_amount | NOT NULL, >= 0 | ROUND(total_sales_amount - COALESCE(discount_amount, 0) + COALESCE(tax_amount, 0), 2) |
| Gold | Go_Sales_Fact | load_date | Silver | si_orders | load_date | NOT NULL | CURRENT_TIMESTAMP() |
| Gold | Go_Sales_Fact | source_system | Silver | si_orders | - | NOT NULL | 'SALES_SYSTEM' |

### 2.3 Go_Daily_Inventory_Summary

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Gold | Go_Daily_Inventory_Summary | summary_id | Gold | Go_Inventory_Movement_Fact | date_key, product_key, warehouse_key | NOT NULL, UNIQUE | ROW_NUMBER() OVER (ORDER BY date_key, product_key, warehouse_key) |
| Gold | Go_Daily_Inventory_Summary | summary_key | Gold | Go_Inventory_Movement_Fact | date_key, product_key, warehouse_key | NOT NULL, UNIQUE | CONCAT(date_key, '_', product_key, '_', warehouse_key) |
| Gold | Go_Daily_Inventory_Summary | date_key | Gold | Go_Inventory_Movement_Fact | date_key | NOT NULL, FK to Go_Date_Dimension | GROUP BY date_key |
| Gold | Go_Daily_Inventory_Summary | product_key | Gold | Go_Inventory_Movement_Fact | product_key | NOT NULL, FK to Go_Product_Dimension | GROUP BY product_key |
| Gold | Go_Daily_Inventory_Summary | warehouse_key | Gold | Go_Inventory_Movement_Fact | warehouse_key | NOT NULL, FK to Go_Warehouse_Dimension | GROUP BY warehouse_key |
| Gold | Go_Daily_Inventory_Summary | opening_balance | Gold | Go_Daily_Inventory_Summary | closing_balance | >= 0 | LAG(closing_balance, 1, 0) OVER (PARTITION BY product_key, warehouse_key ORDER BY date_key) |
| Gold | Go_Daily_Inventory_Summary | total_receipts | Gold | Go_Inventory_Movement_Fact | quantity_moved | >= 0 | SUM(CASE WHEN movement_type = 'INBOUND' THEN quantity_moved ELSE 0 END) |
| Gold | Go_Daily_Inventory_Summary | total_issues | Gold | Go_Inventory_Movement_Fact | quantity_moved | >= 0 | SUM(CASE WHEN movement_type = 'OUTBOUND' THEN quantity_moved ELSE 0 END) |
| Gold | Go_Daily_Inventory_Summary | total_adjustments | Gold | Go_Inventory_Movement_Fact | quantity_moved | Can be negative | SUM(CASE WHEN movement_type = 'ADJUSTMENT' THEN quantity_moved ELSE 0 END) |
| Gold | Go_Daily_Inventory_Summary | closing_balance | Gold | Go_Daily_Inventory_Summary | opening_balance, total_receipts, total_issues, total_adjustments | >= 0 | opening_balance + total_receipts - total_issues + total_adjustments |
| Gold | Go_Daily_Inventory_Summary | average_cost | Gold | Go_Inventory_Movement_Fact | unit_cost | >= 0.00 | AVG(unit_cost) WHERE movement_type = 'INBOUND' |
| Gold | Go_Daily_Inventory_Summary | total_value | Gold | Go_Daily_Inventory_Summary | closing_balance, average_cost | >= 0.00 | ROUND(closing_balance * average_cost, 2) |

### 2.4 Go_Monthly_Sales_Summary

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Gold | Go_Monthly_Sales_Summary | summary_id | Gold | Go_Sales_Fact | year_month, product_key, warehouse_key, customer_key | NOT NULL, UNIQUE | ROW_NUMBER() OVER (ORDER BY year_month, product_key, warehouse_key, customer_key) |
| Gold | Go_Monthly_Sales_Summary | summary_key | Gold | Go_Sales_Fact | year_month, product_key, warehouse_key, customer_key | NOT NULL, UNIQUE | CONCAT(year_month, '_', product_key, '_', warehouse_key, '_', customer_key) |
| Gold | Go_Monthly_Sales_Summary | year_month | Gold | Go_Sales_Fact, Go_Date_Dimension | date_key | NOT NULL | DATE_FORMAT(full_date, 'yyyyMM') GROUP BY |
| Gold | Go_Monthly_Sales_Summary | product_key | Gold | Go_Sales_Fact | product_key | NOT NULL, FK to Go_Product_Dimension | GROUP BY product_key |
| Gold | Go_Monthly_Sales_Summary | warehouse_key | Gold | Go_Sales_Fact | warehouse_key | NOT NULL, FK to Go_Warehouse_Dimension | GROUP BY warehouse_key |
| Gold | Go_Monthly_Sales_Summary | customer_key | Gold | Go_Sales_Fact | customer_key | NOT NULL, FK to Go_Customer_Dimension | GROUP BY customer_key |
| Gold | Go_Monthly_Sales_Summary | total_quantity_sold | Gold | Go_Sales_Fact | quantity_sold | > 0 | SUM(quantity_sold) |
| Gold | Go_Monthly_Sales_Summary | total_sales_amount | Gold | Go_Sales_Fact | total_sales_amount | > 0.00 | SUM(total_sales_amount) |
| Gold | Go_Monthly_Sales_Summary | total_discount_amount | Gold | Go_Sales_Fact | discount_amount | >= 0.00 | SUM(discount_amount) |
| Gold | Go_Monthly_Sales_Summary | net_sales_amount | Gold | Go_Sales_Fact | net_sales_amount | >= 0.00 | SUM(net_sales_amount) |
| Gold | Go_Monthly_Sales_Summary | number_of_transactions | Gold | Go_Sales_Fact | sales_id | > 0 | COUNT(*) |
| Gold | Go_Monthly_Sales_Summary | average_transaction_value | Gold | Go_Sales_Fact | net_sales_amount | >= 0.00 | ROUND(AVG(net_sales_amount), 2) |

## 3. Data Quality and Validation Framework

### 3.1 Mandatory Field Validation
- **Primary Keys**: All fact tables must have unique, non-null primary keys
- **Foreign Keys**: All dimension references must exist in corresponding dimension tables
- **Measures**: All numeric measures must be validated for appropriate ranges
- **Dates**: All date fields must be valid and within acceptable business ranges

### 3.2 Business Rule Validation
- **Inventory Balance**: Closing balance cannot be negative after movements
- **Sales Amounts**: Net sales amount must be positive and logical
- **Unit Costs**: Unit costs must be positive and within reasonable ranges
- **Quantities**: All quantities must be positive for sales and movements

### 3.3 Data Cleansing Rules
- **Missing Values**: Handle NULL values with appropriate defaults (0 for amounts, 'UNKNOWN' for categories)
- **Duplicates**: Remove duplicate records based on business keys
- **Currency**: Standardize all monetary values to 2 decimal places
- **Units**: Ensure consistent unit of measure across all quantity fields

## 4. Transformation Implementation

### 4.1 PySpark Implementation Example

```python
# Example transformation for Go_Inventory_Movement_Fact
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read Silver layer data
si_inventory = spark.table("silver.si_inventory")

# Apply transformations
inventory_movement_fact = si_inventory.select(
    F.row_number().over(Window.orderBy("Inventory_ID", "load_date")).alias("inventory_movement_id"),
    F.concat(F.col("Inventory_ID"), F.lit("_"), F.date_format("load_date", "yyyyMMdd")).alias("inventory_movement_key"),
    F.col("Product_ID").alias("product_id_source"),
    F.col("Warehouse_ID").alias("warehouse_id_source"),
    F.col("Supplier_ID").alias("supplier_id_source"),
    F.col("movement_date"),
    F.when(F.col("movement_type").isin(["IN", "RECEIPT", "PURCHASE"]), "INBOUND")
     .when(F.col("movement_type").isin(["OUT", "ISSUE", "SALE"]), "OUTBOUND")
     .when(F.col("movement_type").isin(["ADJ", "ADJUSTMENT"]), "ADJUSTMENT")
     .when(F.col("movement_type") == "TRANSFER", "TRANSFER")
     .otherwise("OTHER").alias("movement_type"),
    F.coalesce(F.col("quantity_moved"), F.lit(0)).alias("quantity_moved"),
    F.round(F.coalesce(F.col("unit_cost"), F.lit(0.00)), 2).alias("unit_cost"),
    F.round(F.coalesce(F.col("quantity_moved"), F.lit(0)) * F.coalesce(F.col("unit_cost"), F.lit(0.00)), 2).alias("total_value"),
    F.current_timestamp().alias("load_date"),
    F.lit("INVENTORY_SYSTEM").alias("source_system")
).filter(
    (F.col("quantity_moved") > 0) & 
    (F.col("unit_cost") >= 0) & 
    (F.col("movement_date").isNotNull())
)
```

## 5. Performance Optimization

### 5.1 Partitioning Strategy
- **Go_Inventory_Movement_Fact**: Partition by `date_key`
- **Go_Sales_Fact**: Partition by `date_key`
- **Go_Daily_Inventory_Summary**: Partition by `date_key`
- **Go_Monthly_Sales_Summary**: Partition by `year_month`

### 5.2 Z-ORDER Optimization
```sql
-- Optimize fact tables for query performance
OPTIMIZE gold.Go_Inventory_Movement_Fact ZORDER BY (product_key, warehouse_key, date_key);
OPTIMIZE gold.Go_Sales_Fact ZORDER BY (product_key, customer_key, date_key);
```

## 6. Data Lineage and Audit

### 6.1 Source System Tracking
All fact records include:
- `load_date`: Timestamp of when the record was processed
- `source_system`: Identifier of the source system
- `pipeline_run_id`: Unique identifier for the ETL batch

## 7. Business KPI Calculations

### 7.1 Inventory Turnover Metrics
```sql
-- Calculate inventory turnover ratio
WITH inventory_metrics AS (
    SELECT 
        product_key,
        warehouse_key,
        SUM(CASE WHEN movement_type = 'OUTBOUND' THEN total_value ELSE 0 END) as cogs,
        AVG(closing_balance * average_cost) as avg_inventory_value
    FROM gold.Go_Daily_Inventory_Summary
    WHERE date_key >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -12), 'yyyyMMdd')
    GROUP BY product_key, warehouse_key
)
SELECT 
    *,
    CASE WHEN avg_inventory_value > 0 
         THEN ROUND(cogs / avg_inventory_value, 2) 
         ELSE 0 END as inventory_turnover_ratio
FROM inventory_metrics;
```

---

**API Cost**: 0.0425

**Document Status**: Active  
**Version**: 1.0  
**Last Updated**: Auto-generated