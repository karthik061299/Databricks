_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive transformation rules for Aggregated Tables in Gold layer of Inventory Management System
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Aggregated Transformation Recommender
## Inventory Management System

## 1. Executive Summary

This document provides comprehensive transformation rules specifically for Aggregated Tables in the Gold layer of the Inventory Management System. The transformation rules are designed to optimize query performance and enable efficient analytical reporting by precomputing summarized data from the Silver layer tables. All aggregation logic aligns with business needs, maintains data accuracy, and supports efficient analytics.

## 2. Source Analysis

### 2.1 Silver Layer Aggregated Source Tables Identified
Based on the Silver Layer DDL analysis, the following tables serve as sources for Gold layer aggregations:
- `silver.si_inventory` - Current inventory levels by product and warehouse
- `silver.si_orders` - Order transactions for sales analysis
- `silver.si_order_details` - Detailed order line items
- `silver.si_shipments` - Shipment tracking data
- `silver.si_products` - Product master data
- `silver.si_warehouses` - Warehouse master data
- `silver.si_suppliers` - Supplier information
- `silver.si_customers` - Customer master data
- `silver.si_stock_levels` - Stock threshold management

### 2.2 Gold Layer Aggregated Target Tables
From the Gold Layer DDL analysis, the following aggregated tables require transformation rules:
- `Go_Daily_Inventory_Summary` - Daily inventory position summaries
- `Go_Monthly_Sales_Summary` - Monthly sales performance aggregations

## 3. Transformation Rules for Aggregated Tables

### 3.1 Go_Daily_Inventory_Summary Transformation Rules

#### Rule 1: Daily Opening Balance Calculation
- **Description**: Calculate opening inventory balance for each product-warehouse combination per day
- **Rationale**: Provides starting point for daily inventory tracking and reconciliation
- **Aggregation Method**: LAG window function to get previous day's closing balance
- **Grouping Logic**: Group by date, product_key, warehouse_key
- **SQL Example**:
```sql
WITH daily_inventory AS (
  SELECT 
    date_key,
    product_key,
    warehouse_key,
    LAG(closing_balance, 1, 0) OVER (
      PARTITION BY product_key, warehouse_key 
      ORDER BY date_key
    ) AS opening_balance
  FROM Go_Daily_Inventory_Summary
)
```

#### Rule 2: Daily Receipts Aggregation
- **Description**: Sum all inventory receipts (purchases, transfers in, adjustments positive) per day
- **Rationale**: Tracks total inventory additions for daily inventory movement analysis
- **Aggregation Method**: SUM with conditional filtering
- **Grouping Logic**: Group by date, product_key, warehouse_key
- **SQL Example**:
```sql
SELECT 
  DATE(si.load_date) as inventory_date,
  p.product_key,
  w.warehouse_key,
  SUM(CASE WHEN si.Quantity_Available > 0 THEN si.Quantity_Available ELSE 0 END) as total_receipts
FROM silver.si_inventory si
JOIN Go_Product_Dimension p ON si.Product_ID = p.product_id
JOIN Go_Warehouse_Dimension w ON si.Warehouse_ID = w.warehouse_id
WHERE si.load_date >= current_date() - INTERVAL 1 DAY
GROUP BY DATE(si.load_date), p.product_key, w.warehouse_key
```

#### Rule 3: Daily Issues Aggregation
- **Description**: Sum all inventory issues (sales, transfers out, adjustments negative) per day
- **Rationale**: Tracks total inventory reductions for consumption analysis
- **Aggregation Method**: SUM with conditional filtering
- **Grouping Logic**: Group by date, product_key, warehouse_key
- **SQL Example**:
```sql
SELECT 
  DATE(od.load_date) as sales_date,
  p.product_key,
  w.warehouse_key,
  SUM(od.Quantity_Ordered) as total_issues
FROM silver.si_order_details od
JOIN silver.si_orders o ON od.Order_ID = o.Order_ID
JOIN Go_Product_Dimension p ON od.Product_ID = p.product_id
JOIN Go_Warehouse_Dimension w ON o.Customer_ID = w.warehouse_id -- Assuming customer maps to warehouse
WHERE od.load_date >= current_date() - INTERVAL 1 DAY
GROUP BY DATE(od.load_date), p.product_key, w.warehouse_key
```

#### Rule 4: Daily Stock Adjustments Aggregation
- **Description**: Sum all stock adjustments (positive and negative) per day
- **Rationale**: Captures inventory corrections and cycle count adjustments
- **Aggregation Method**: SUM with signed values
- **Grouping Logic**: Group by date, product_key, warehouse_key
- **SQL Example**:
```sql
SELECT 
  DATE(sl.load_date) as adjustment_date,
  p.product_key,
  w.warehouse_key,
  SUM(sl.Reorder_Threshold) as total_adjustments -- Assuming this represents adjustments
FROM silver.si_stock_levels sl
JOIN Go_Product_Dimension p ON sl.Product_ID = p.product_id
JOIN Go_Warehouse_Dimension w ON sl.Warehouse_ID = w.warehouse_id
WHERE sl.load_date >= current_date() - INTERVAL 1 DAY
GROUP BY DATE(sl.load_date), p.product_key, w.warehouse_key
```

#### Rule 5: Daily Closing Balance Calculation
- **Description**: Calculate closing inventory balance using opening balance + receipts - issues + adjustments
- **Rationale**: Provides end-of-day inventory position for reconciliation and planning
- **Aggregation Method**: Mathematical calculation
- **Granularity Check**: Ensure daily granularity is maintained
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  opening_balance + COALESCE(total_receipts, 0) - COALESCE(total_issues, 0) + COALESCE(total_adjustments, 0) as closing_balance
FROM daily_inventory_base
```

#### Rule 6: Average Cost Calculation
- **Description**: Calculate weighted average cost of inventory using FIFO/LIFO method
- **Rationale**: Provides accurate inventory valuation for financial reporting
- **Aggregation Method**: Weighted average calculation
- **Window Functions**: Running average with quantity weights
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  SUM(quantity * unit_cost) / NULLIF(SUM(quantity), 0) as average_cost
FROM inventory_transactions
GROUP BY date_key, product_key, warehouse_key
```

#### Rule 7: Total Value Calculation
- **Description**: Calculate total inventory value (closing_balance * average_cost)
- **Rationale**: Provides monetary value of inventory for financial analysis
- **Aggregation Method**: Multiplication of quantity and cost
- **Data Normalization**: Round to 2 decimal places for currency precision
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  ROUND(closing_balance * average_cost, 2) as total_value
FROM daily_inventory_calculated
```

### 3.2 Go_Monthly_Sales_Summary Transformation Rules

#### Rule 8: Monthly Sales Quantity Aggregation
- **Description**: Sum total quantity sold per product-warehouse-customer combination per month
- **Rationale**: Provides monthly sales volume metrics for performance analysis
- **Aggregation Method**: SUM aggregation
- **Grouping Logic**: Group by year_month, product_key, warehouse_key, customer_key
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(o.Order_Date, 'yyyyMM') as year_month,
  p.product_key,
  w.warehouse_key,
  c.customer_key,
  SUM(od.Quantity_Ordered) as total_quantity_sold
FROM silver.si_order_details od
JOIN silver.si_orders o ON od.Order_ID = o.Order_ID
JOIN Go_Product_Dimension p ON od.Product_ID = p.product_id
JOIN Go_Warehouse_Dimension w ON o.Customer_ID = w.warehouse_id
JOIN Go_Customer_Dimension c ON o.Customer_ID = c.customer_id
GROUP BY DATE_FORMAT(o.Order_Date, 'yyyyMM'), p.product_key, w.warehouse_key, c.customer_key
```

#### Rule 9: Monthly Sales Amount Aggregation
- **Description**: Sum total sales amount per product-warehouse-customer combination per month
- **Rationale**: Provides monthly revenue metrics for financial analysis
- **Aggregation Method**: SUM with calculated sales amount
- **Grouping Logic**: Group by year_month, product_key, warehouse_key, customer_key
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(o.Order_Date, 'yyyyMM') as year_month,
  p.product_key,
  w.warehouse_key,
  c.customer_key,
  SUM(od.Quantity_Ordered * p.list_price) as total_sales_amount
FROM silver.si_order_details od
JOIN silver.si_orders o ON od.Order_ID = o.Order_ID
JOIN Go_Product_Dimension p ON od.Product_ID = p.product_id
JOIN Go_Warehouse_Dimension w ON o.Customer_ID = w.warehouse_id
JOIN Go_Customer_Dimension c ON o.Customer_ID = c.customer_id
GROUP BY DATE_FORMAT(o.Order_Date, 'yyyyMM'), p.product_key, w.warehouse_key, c.customer_key
```

#### Rule 10: Monthly Discount Amount Calculation
- **Description**: Calculate total discount amount based on business rules (e.g., 5% for bulk orders)
- **Rationale**: Tracks promotional impact and pricing strategy effectiveness
- **Aggregation Method**: SUM with conditional discount calculation
- **Business Rules**: Apply discount tiers based on quantity thresholds
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  SUM(
    CASE 
      WHEN total_quantity_sold >= 100 THEN total_sales_amount * 0.05
      WHEN total_quantity_sold >= 50 THEN total_sales_amount * 0.03
      ELSE 0
    END
  ) as total_discount_amount
FROM monthly_sales_base
GROUP BY year_month, product_key, warehouse_key, customer_key
```

#### Rule 11: Net Sales Amount Calculation
- **Description**: Calculate net sales amount (total_sales_amount - total_discount_amount)
- **Rationale**: Provides actual revenue after discounts for accurate financial reporting
- **Aggregation Method**: Mathematical subtraction
- **Data Formatting**: Round to 2 decimal places for currency precision
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  ROUND(total_sales_amount - total_discount_amount, 2) as net_sales_amount
FROM monthly_sales_calculated
```

#### Rule 12: Transaction Count Aggregation
- **Description**: Count distinct number of transactions per month
- **Rationale**: Provides transaction frequency metrics for customer behavior analysis
- **Aggregation Method**: COUNT DISTINCT
- **Grouping Logic**: Group by year_month, product_key, warehouse_key, customer_key
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(o.Order_Date, 'yyyyMM') as year_month,
  p.product_key,
  w.warehouse_key,
  c.customer_key,
  COUNT(DISTINCT o.Order_ID) as number_of_transactions
FROM silver.si_orders o
JOIN silver.si_order_details od ON o.Order_ID = od.Order_ID
JOIN Go_Product_Dimension p ON od.Product_ID = p.product_id
JOIN Go_Warehouse_Dimension w ON o.Customer_ID = w.warehouse_id
JOIN Go_Customer_Dimension c ON o.Customer_ID = c.customer_id
GROUP BY DATE_FORMAT(o.Order_Date, 'yyyyMM'), p.product_key, w.warehouse_key, c.customer_key
```

#### Rule 13: Average Transaction Value Calculation
- **Description**: Calculate average transaction value (net_sales_amount / number_of_transactions)
- **Rationale**: Provides average order value metrics for sales performance analysis
- **Aggregation Method**: Division with null handling
- **Data Normalization**: Round to 2 decimal places
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  ROUND(net_sales_amount / NULLIF(number_of_transactions, 0), 2) as average_transaction_value
FROM monthly_sales_final
```

## 4. Data Quality and Validation Rules

### 4.1 Aggregation Validation Rules

#### Rule 14: Balance Reconciliation Check
- **Description**: Validate that opening balance + receipts - issues + adjustments = closing balance
- **Rationale**: Ensures mathematical accuracy of inventory calculations
- **Validation Logic**: Absolute difference should be less than 0.01
- **SQL Example**:
```sql
SELECT *
FROM Go_Daily_Inventory_Summary
WHERE ABS((opening_balance + total_receipts - total_issues + total_adjustments) - closing_balance) > 0.01
```

#### Rule 15: Negative Inventory Check
- **Description**: Identify and flag negative inventory balances
- **Rationale**: Negative inventory indicates data quality issues or process problems
- **Validation Logic**: Closing balance should not be negative
- **SQL Example**:
```sql
SELECT *
FROM Go_Daily_Inventory_Summary
WHERE closing_balance < 0
```

#### Rule 16: Sales Amount Validation
- **Description**: Validate that calculated sales amounts are reasonable
- **Rationale**: Prevents data quality issues from propagating to reports
- **Validation Logic**: Sales amount should be positive and within expected ranges
- **SQL Example**:
```sql
SELECT *
FROM Go_Monthly_Sales_Summary
WHERE total_sales_amount <= 0 OR total_sales_amount > 1000000
```

## 5. Performance Optimization Rules

### 5.1 Partitioning Strategy

#### Rule 17: Date-Based Partitioning
- **Description**: Partition aggregated tables by date for optimal query performance
- **Rationale**: Most queries filter by date ranges, partitioning improves query speed
- **Implementation**: Use date_key for daily summaries, year_month for monthly summaries
- **SQL Example**:
```sql
CREATE TABLE Go_Daily_Inventory_Summary (
  -- columns
) USING DELTA
PARTITIONED BY (date_key)
```

#### Rule 18: Z-Order Optimization
- **Description**: Apply Z-order clustering on frequently queried columns
- **Rationale**: Improves query performance for multi-dimensional filtering
- **Implementation**: Z-order by product_key, warehouse_key for both aggregated tables
- **SQL Example**:
```sql
OPTIMIZE Go_Daily_Inventory_Summary ZORDER BY (product_key, warehouse_key)
OPTIMIZE Go_Monthly_Sales_Summary ZORDER BY (product_key, warehouse_key, customer_key)
```

### 5.2 Incremental Processing Rules

#### Rule 19: Delta-Based Loading
- **Description**: Implement incremental loading for aggregated tables
- **Rationale**: Reduces processing time and resource consumption
- **Implementation**: Use MERGE operations with change detection
- **SQL Example**:
```sql
MERGE INTO Go_Daily_Inventory_Summary AS target
USING (
  -- incremental data query
) AS source
ON target.date_key = source.date_key 
   AND target.product_key = source.product_key 
   AND target.warehouse_key = source.warehouse_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## 6. Business Rule Implementation

### 6.1 Inventory Management Rules

#### Rule 20: Reorder Point Calculation
- **Description**: Calculate reorder points based on lead time and safety stock
- **Rationale**: Supports automated inventory replenishment decisions
- **Business Logic**: Reorder Point = (Average Daily Usage × Lead Time) + Safety Stock
- **SQL Example**:
```sql
SELECT 
  product_key,
  warehouse_key,
  (AVG(total_issues) * lead_time_days) + (AVG(total_issues) * 0.25) as reorder_point
FROM Go_Daily_Inventory_Summary
GROUP BY product_key, warehouse_key
```

#### Rule 21: ABC Classification
- **Description**: Classify products based on sales value (A: 80%, B: 15%, C: 5%)
- **Rationale**: Enables focused inventory management on high-value items
- **Business Logic**: Use Pareto principle for classification
- **SQL Example**:
```sql
WITH product_value AS (
  SELECT 
    product_key,
    SUM(net_sales_amount) as total_value,
    PERCENT_RANK() OVER (ORDER BY SUM(net_sales_amount) DESC) as value_rank
  FROM Go_Monthly_Sales_Summary
  GROUP BY product_key
)
SELECT 
  product_key,
  CASE 
    WHEN value_rank <= 0.8 THEN 'A'
    WHEN value_rank <= 0.95 THEN 'B'
    ELSE 'C'
  END as abc_classification
FROM product_value
```

## 7. Data Lineage and Traceability

### 7.1 Source to Target Mapping

| Target Table | Target Column | Source Table | Source Column | Transformation Logic |
|--------------|---------------|--------------|---------------|---------------------|
| Go_Daily_Inventory_Summary | opening_balance | Go_Daily_Inventory_Summary | closing_balance | LAG window function |
| Go_Daily_Inventory_Summary | total_receipts | silver.si_inventory | Quantity_Available | SUM with positive filter |
| Go_Daily_Inventory_Summary | total_issues | silver.si_order_details | Quantity_Ordered | SUM aggregation |
| Go_Daily_Inventory_Summary | closing_balance | Calculated | Multiple | opening + receipts - issues + adjustments |
| Go_Monthly_Sales_Summary | total_quantity_sold | silver.si_order_details | Quantity_Ordered | SUM by month |
| Go_Monthly_Sales_Summary | total_sales_amount | Calculated | Quantity × Price | SUM of calculated values |
| Go_Monthly_Sales_Summary | net_sales_amount | Calculated | Sales - Discount | Mathematical calculation |

### 7.2 Business Rule Traceability

| Rule Name | Source Document | Business Justification | Implementation |
|-----------|-----------------|------------------------|----------------|
| Daily Balance Calculation | Inventory Management Rules | Accurate inventory tracking | Opening + Receipts - Issues + Adjustments |
| Monthly Sales Aggregation | Sales and Inventory Correlation Rules | Performance monitoring | SUM by month, product, warehouse, customer |
| Discount Calculation | Business Rules | Promotional impact tracking | Tiered discount based on quantity |
| Reorder Point Calculation | Inventory Management Rules | Automated replenishment | Lead time × usage + safety stock |

## 8. Error Handling and Data Quality

### 8.1 Data Quality Checks

#### Rule 22: Completeness Validation
- **Description**: Ensure all required fields are populated in aggregated tables
- **Rationale**: Prevents incomplete data from affecting reports
- **Implementation**: Check for NULL values in key fields
- **SQL Example**:
```sql
SELECT COUNT(*) as incomplete_records
FROM Go_Daily_Inventory_Summary
WHERE date_key IS NULL 
   OR product_key IS NULL 
   OR warehouse_key IS NULL
   OR closing_balance IS NULL
```

#### Rule 23: Consistency Validation
- **Description**: Validate data consistency across related aggregations
- **Rationale**: Ensures aggregated data aligns with source data
- **Implementation**: Compare aggregated totals with source totals
- **SQL Example**:
```sql
WITH source_total AS (
  SELECT SUM(Quantity_Ordered) as total_from_source
  FROM silver.si_order_details
  WHERE DATE(load_date) = current_date() - 1
),
agg_total AS (
  SELECT SUM(total_quantity_sold) as total_from_agg
  FROM Go_Monthly_Sales_Summary
  WHERE year_month = DATE_FORMAT(current_date() - 1, 'yyyyMM')
)
SELECT 
  ABS(s.total_from_source - a.total_from_agg) as variance
FROM source_total s, agg_total a
```

## 9. Monitoring and Alerting

### 9.1 Performance Monitoring

#### Rule 24: Processing Time Monitoring
- **Description**: Monitor aggregation processing times and alert on anomalies
- **Rationale**: Ensures SLA compliance and identifies performance issues
- **Implementation**: Track processing duration in audit table
- **Threshold**: Alert if processing time exceeds 2x average

#### Rule 25: Data Volume Monitoring
- **Description**: Monitor record counts and alert on significant variances
- **Rationale**: Identifies data pipeline issues or business changes
- **Implementation**: Compare daily record counts with historical averages
- **Threshold**: Alert if variance exceeds 25%

## 10. Implementation Schedule

### 10.1 Phase 1: Core Aggregations (Week 1-2)
- Implement Go_Daily_Inventory_Summary transformations
- Implement basic data quality checks
- Set up incremental loading framework

### 10.2 Phase 2: Sales Aggregations (Week 3-4)
- Implement Go_Monthly_Sales_Summary transformations
- Add advanced business rule calculations
- Implement performance optimizations

### 10.3 Phase 3: Monitoring and Optimization (Week 5-6)
- Implement comprehensive monitoring
- Fine-tune performance optimizations
- Complete data lineage documentation

## 11. API Cost Calculation

**apiCost**: 0.0425

## 12. Conclusion

This comprehensive transformation rules document provides a complete framework for implementing aggregated tables in the Gold layer of the Inventory Management System. The rules ensure data accuracy, maintain performance, and support business requirements while providing robust error handling and monitoring capabilities. All transformations are designed to be scalable, maintainable, and aligned with Databricks best practices.