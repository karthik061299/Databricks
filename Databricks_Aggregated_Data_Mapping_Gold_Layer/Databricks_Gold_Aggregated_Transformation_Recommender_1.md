_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive transformation rules for Aggregated Tables in Gold Layer of Inventory Management System
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Aggregated Transformation Recommender
## Inventory Management System

## 1. Executive Summary

This document provides comprehensive transformation rules specifically for Aggregated Tables in the Gold layer of the Inventory Management System. The transformation rules ensure optimal performance for analytical reporting by precomputing summarized data while maintaining data accuracy and business rule compliance.

## 2. Aggregated Tables Analysis

### 2.1 Identified Aggregated Tables from Gold Layer DDL

| Table Name | Purpose | Aggregation Level | Key Dimensions |
|------------|---------|-------------------|----------------|
| Go_Daily_Inventory_Summary | Daily inventory position tracking | Daily | Date, Product, Warehouse |
| Go_Monthly_Sales_Summary | Monthly sales performance analysis | Monthly | Year-Month, Product, Warehouse, Customer |

### 2.2 Source Tables from Silver Layer

| Silver Table | Usage in Aggregation | Key Fields |
|--------------|---------------------|------------|
| si_inventory | Current stock levels | Product_ID, Warehouse_ID, Quantity_Available |
| si_orders | Sales transaction data | Order_ID, Customer_ID, Order_Date |
| si_order_details | Sales quantities and amounts | Order_ID, Product_ID, Quantity_Ordered |
| si_products | Product master data | Product_ID, Product_Name, Category |
| si_warehouses | Warehouse master data | Warehouse_ID, Location, Capacity |
| si_customers | Customer master data | Customer_ID, Customer_Name |

## 3. Transformation Rules for Aggregated Tables

### 3.1 Go_Daily_Inventory_Summary Transformation Rules

#### Rule 1: Daily Opening Balance Calculation
- **Description**: Calculate opening balance for each product-warehouse combination per day
- **Rationale**: Provides starting point for daily inventory movement tracking
- **Business Rule Reference**: Inventory Management Rules - Stock Replenishment Rule
- **SQL Example**:
```sql
WITH opening_balance AS (
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
- **Rationale**: Tracks total inventory increases for accurate daily position
- **Business Rule Reference**: Inventory Management Rules - Lead Time Buffer Rule
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(inv.load_date, 'yyyyMMdd') AS date_key,
  p.product_key,
  w.warehouse_key,
  SUM(CASE WHEN inv.Quantity_Available > 0 THEN inv.Quantity_Available ELSE 0 END) AS total_receipts
FROM silver.si_inventory inv
JOIN Go_Product_Dimension p ON inv.Product_ID = p.product_id AND p.is_current = true
JOIN Go_Warehouse_Dimension w ON inv.Warehouse_ID = w.warehouse_id AND w.is_current = true
WHERE inv.load_date >= CURRENT_DATE - INTERVAL 1 DAY
GROUP BY DATE_FORMAT(inv.load_date, 'yyyyMMdd'), p.product_key, w.warehouse_key
```

#### Rule 3: Daily Issues Aggregation
- **Description**: Sum all inventory issues (sales, transfers out, adjustments negative) per day
- **Rationale**: Tracks total inventory decreases for accurate daily position
- **Business Rule Reference**: Sales and Inventory Correlation Rules - Turnover Analysis Rule
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(o.Order_Date, 'yyyyMMdd') AS date_key,
  p.product_key,
  w.warehouse_key,
  SUM(od.Quantity_Ordered) AS total_issues
FROM silver.si_orders o
JOIN silver.si_order_details od ON o.Order_ID = od.Order_ID
JOIN Go_Product_Dimension p ON od.Product_ID = p.product_id AND p.is_current = true
JOIN Go_Warehouse_Dimension w ON o.warehouse_id = w.warehouse_id AND w.is_current = true
WHERE o.Order_Date >= CURRENT_DATE - INTERVAL 1 DAY
GROUP BY DATE_FORMAT(o.Order_Date, 'yyyyMMdd'), p.product_key, w.warehouse_key
```

#### Rule 4: Stock Adjustments Aggregation
- **Description**: Sum all stock adjustments (cycle counts, damage, obsolescence) per day
- **Rationale**: Captures non-transactional inventory changes for complete daily picture
- **Business Rule Reference**: Data Quality and Validation Rules - Anomaly Detection Rule
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(adj.adjustment_date, 'yyyyMMdd') AS date_key,
  p.product_key,
  w.warehouse_key,
  SUM(adj.adjustment_quantity) AS total_adjustments
FROM silver.si_stock_adjustments adj
JOIN Go_Product_Dimension p ON adj.Product_ID = p.product_id AND p.is_current = true
JOIN Go_Warehouse_Dimension w ON adj.Warehouse_ID = w.warehouse_id AND w.is_current = true
WHERE adj.adjustment_date >= CURRENT_DATE - INTERVAL 1 DAY
GROUP BY DATE_FORMAT(adj.adjustment_date, 'yyyyMMdd'), p.product_key, w.warehouse_key
```

#### Rule 5: Closing Balance Calculation
- **Description**: Calculate closing balance using opening balance + receipts - issues + adjustments
- **Rationale**: Provides end-of-day inventory position for reporting and next day opening
- **Business Rule Reference**: Inventory Management Rules - Stockout Prevention Rule
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  opening_balance + COALESCE(total_receipts, 0) - COALESCE(total_issues, 0) + COALESCE(total_adjustments, 0) AS closing_balance
FROM daily_inventory_movements
```

#### Rule 6: Average Cost Calculation
- **Description**: Calculate weighted average cost based on inventory movements and values
- **Rationale**: Provides accurate cost basis for inventory valuation
- **Business Rule Reference**: Data Type Constraints - Monetary Values formatting
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  CASE 
    WHEN closing_balance > 0 THEN ROUND(total_value / closing_balance, 2)
    ELSE 0 
  END AS average_cost
FROM daily_inventory_summary
```

#### Rule 7: Total Value Calculation
- **Description**: Calculate total inventory value using closing balance × average cost
- **Rationale**: Provides financial value of inventory for reporting and analysis
- **Business Rule Reference**: Range and Threshold Constraints - Inventory Turnover Ratio
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  ROUND(closing_balance * average_cost, 2) AS total_value
FROM daily_inventory_summary
```

### 3.2 Go_Monthly_Sales_Summary Transformation Rules

#### Rule 8: Monthly Sales Quantity Aggregation
- **Description**: Sum total quantity sold per product-warehouse-customer combination per month
- **Rationale**: Provides monthly sales volume metrics for performance analysis
- **Business Rule Reference**: Sales and Inventory Correlation Rules - Demand Pattern Rule
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(o.Order_Date, 'yyyyMM') AS year_month,
  p.product_key,
  w.warehouse_key,
  c.customer_key,
  SUM(od.Quantity_Ordered) AS total_quantity_sold
FROM silver.si_orders o
JOIN silver.si_order_details od ON o.Order_ID = od.Order_ID
JOIN Go_Product_Dimension p ON od.Product_ID = p.product_id AND p.is_current = true
JOIN Go_Warehouse_Dimension w ON o.warehouse_id = w.warehouse_id AND w.is_current = true
JOIN Go_Customer_Dimension c ON o.Customer_ID = c.customer_id AND c.is_current = true
WHERE o.Order_Date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 1 MONTH)
GROUP BY DATE_FORMAT(o.Order_Date, 'yyyyMM'), p.product_key, w.warehouse_key, c.customer_key
```

#### Rule 9: Monthly Sales Amount Aggregation
- **Description**: Sum total sales amount before discounts per month
- **Rationale**: Provides gross sales revenue metrics for financial analysis
- **Business Rule Reference**: Data Format Expectations - Monetary Values formatting
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  ROUND(SUM(od.Quantity_Ordered * p.list_price), 2) AS total_sales_amount
FROM monthly_sales_base msb
JOIN silver.si_order_details od ON msb.order_id = od.Order_ID
JOIN Go_Product_Dimension p ON od.Product_ID = p.product_id AND p.is_current = true
GROUP BY year_month, product_key, warehouse_key, customer_key
```

#### Rule 10: Monthly Discount Amount Calculation
- **Description**: Calculate total discount amount applied to sales per month
- **Rationale**: Tracks promotional impact and net revenue calculation
- **Business Rule Reference**: Range and Threshold Constraints - percentage validation
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  ROUND(SUM(
    CASE 
      WHEN discount_percentage > 0 THEN 
        (od.Quantity_Ordered * p.list_price * discount_percentage / 100)
      ELSE 0 
    END
  ), 2) AS total_discount_amount
FROM monthly_sales_transactions
GROUP BY year_month, product_key, warehouse_key, customer_key
```

#### Rule 11: Net Sales Amount Calculation
- **Description**: Calculate net sales amount (gross sales - discounts)
- **Rationale**: Provides actual revenue after promotional adjustments
- **Business Rule Reference**: Business Rules - KPI Threshold Rule
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  ROUND(total_sales_amount - total_discount_amount, 2) AS net_sales_amount
FROM monthly_sales_summary
```

#### Rule 12: Transaction Count Aggregation
- **Description**: Count number of distinct transactions per month
- **Rationale**: Provides transaction frequency metrics for customer behavior analysis
- **Business Rule Reference**: Uniqueness Constraints - Transaction Records
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  COUNT(DISTINCT order_id) AS number_of_transactions
FROM monthly_sales_base
GROUP BY year_month, product_key, warehouse_key, customer_key
```

#### Rule 13: Average Transaction Value Calculation
- **Description**: Calculate average transaction value (net sales / transaction count)
- **Rationale**: Provides customer spending pattern insights
- **Business Rule Reference**: Data Type Constraints - positive decimal validation
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  CASE 
    WHEN number_of_transactions > 0 THEN 
      ROUND(net_sales_amount / number_of_transactions, 2)
    ELSE 0 
  END AS average_transaction_value
FROM monthly_sales_summary
```

## 4. Data Quality and Validation Rules for Aggregated Tables

### 4.1 Granularity Checks

#### Rule 14: Daily Summary Completeness Validation
- **Description**: Ensure daily inventory summary exists for all active product-warehouse combinations
- **Rationale**: Prevents gaps in daily inventory tracking
- **SQL Example**:
```sql
SELECT 
  d.date_key,
  p.product_key,
  w.warehouse_key
FROM Go_Date_Dimension d
CROSS JOIN Go_Product_Dimension p
CROSS JOIN Go_Warehouse_Dimension w
WHERE d.date_key BETWEEN 20240101 AND 20241231
  AND p.is_current = true
  AND w.is_current = true
  AND p.product_status = 'Active'
  AND w.warehouse_status = 'Active'
EXCEPT
SELECT date_key, product_key, warehouse_key
FROM Go_Daily_Inventory_Summary
WHERE date_key BETWEEN 20240101 AND 20241231
```

#### Rule 15: Monthly Summary Consistency Validation
- **Description**: Validate monthly summaries match sum of daily transactions
- **Rationale**: Ensures aggregation accuracy and data consistency
- **SQL Example**:
```sql
WITH daily_totals AS (
  SELECT 
    DATE_FORMAT(date_key, 'yyyyMM') AS year_month,
    product_key,
    warehouse_key,
    SUM(total_issues) AS monthly_quantity_from_daily
  FROM Go_Daily_Inventory_Summary
  GROUP BY DATE_FORMAT(date_key, 'yyyyMM'), product_key, warehouse_key
)
SELECT 
  ms.year_month,
  ms.product_key,
  ms.warehouse_key,
  ms.total_quantity_sold,
  dt.monthly_quantity_from_daily,
  ABS(ms.total_quantity_sold - dt.monthly_quantity_from_daily) AS variance
FROM Go_Monthly_Sales_Summary ms
JOIN daily_totals dt ON ms.year_month = dt.year_month 
  AND ms.product_key = dt.product_key 
  AND ms.warehouse_key = dt.warehouse_key
WHERE ABS(ms.total_quantity_sold - dt.monthly_quantity_from_daily) > 0.01
```

### 4.2 Data Normalization & Formatting Rules

#### Rule 16: Decimal Precision Standardization
- **Description**: Ensure all monetary values have exactly 2 decimal places
- **Rationale**: Maintains consistent financial reporting format
- **SQL Example**:
```sql
UPDATE Go_Daily_Inventory_Summary 
SET 
  average_cost = ROUND(average_cost, 2),
  total_value = ROUND(total_value, 2)
WHERE MOD(average_cost * 100, 1) != 0 OR MOD(total_value * 100, 1) != 0
```

#### Rule 17: Date Bucketization Standardization
- **Description**: Ensure consistent date key formatting (YYYYMMDD for daily, YYYYMM for monthly)
- **Rationale**: Maintains consistent temporal dimension structure
- **SQL Example**:
```sql
-- Validation for daily date keys
SELECT date_key
FROM Go_Daily_Inventory_Summary
WHERE LENGTH(CAST(date_key AS STRING)) != 8
   OR date_key NOT RLIKE '^[0-9]{8}$'
   OR SUBSTR(CAST(date_key AS STRING), 5, 2) NOT BETWEEN '01' AND '12'
   OR SUBSTR(CAST(date_key AS STRING), 7, 2) NOT BETWEEN '01' AND '31'
```

### 4.3 Window Functions for Advanced Aggregations

#### Rule 18: Rolling Average Inventory Calculation
- **Description**: Calculate 7-day and 30-day rolling average inventory levels
- **Rationale**: Provides trend analysis and smoothed inventory patterns
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  closing_balance,
  AVG(closing_balance) OVER (
    PARTITION BY product_key, warehouse_key 
    ORDER BY date_key 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7day_avg_inventory,
  AVG(closing_balance) OVER (
    PARTITION BY product_key, warehouse_key 
    ORDER BY date_key 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS rolling_30day_avg_inventory
FROM Go_Daily_Inventory_Summary
```

#### Rule 19: Cumulative Sales Calculation
- **Description**: Calculate year-to-date and month-to-date cumulative sales
- **Rationale**: Provides progressive sales performance tracking
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  total_quantity_sold,
  SUM(total_quantity_sold) OVER (
    PARTITION BY product_key, warehouse_key, customer_key, SUBSTR(year_month, 1, 4)
    ORDER BY year_month
  ) AS ytd_cumulative_quantity,
  SUM(net_sales_amount) OVER (
    PARTITION BY product_key, warehouse_key, customer_key, SUBSTR(year_month, 1, 4)
    ORDER BY year_month
  ) AS ytd_cumulative_sales
FROM Go_Monthly_Sales_Summary
```

#### Rule 20: Inventory Velocity Calculation
- **Description**: Calculate inventory turnover velocity using window functions
- **Rationale**: Identifies fast-moving vs slow-moving products
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  closing_balance,
  total_issues,
  CASE 
    WHEN AVG(closing_balance) OVER (
      PARTITION BY product_key, warehouse_key 
      ORDER BY date_key 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) > 0 THEN
      SUM(total_issues) OVER (
        PARTITION BY product_key, warehouse_key 
        ORDER BY date_key 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) / AVG(closing_balance) OVER (
        PARTITION BY product_key, warehouse_key 
        ORDER BY date_key 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      )
    ELSE 0
  END AS inventory_turnover_30day
FROM Go_Daily_Inventory_Summary
```

## 5. Traceability Matrix

### 5.1 Source to Target Mapping

| Transformation Rule | Source Table(s) | Target Field | Business Rule Reference |
|-------------------|-----------------|--------------|------------------------|
| Rule 1 | Go_Daily_Inventory_Summary | opening_balance | Inventory Management Rules |
| Rule 2 | si_inventory, Go_Product_Dimension, Go_Warehouse_Dimension | total_receipts | Lead Time Buffer Rule |
| Rule 3 | si_orders, si_order_details, Go_Product_Dimension, Go_Warehouse_Dimension | total_issues | Turnover Analysis Rule |
| Rule 4 | si_stock_adjustments, Go_Product_Dimension, Go_Warehouse_Dimension | total_adjustments | Anomaly Detection Rule |
| Rule 5 | Daily calculations | closing_balance | Stockout Prevention Rule |
| Rule 6 | Daily calculations | average_cost | Monetary Values formatting |
| Rule 7 | Daily calculations | total_value | Inventory Turnover Ratio |
| Rule 8 | si_orders, si_order_details, dimensions | total_quantity_sold | Demand Pattern Rule |
| Rule 9 | si_order_details, Go_Product_Dimension | total_sales_amount | Monetary Values formatting |
| Rule 10 | Monthly calculations | total_discount_amount | Percentage validation |
| Rule 11 | Monthly calculations | net_sales_amount | KPI Threshold Rule |
| Rule 12 | si_orders | number_of_transactions | Transaction Records |
| Rule 13 | Monthly calculations | average_transaction_value | Positive decimal validation |

### 5.2 Conceptual Model Alignment

| KPI from Conceptual Model | Implemented in Rule | Target Table |
|---------------------------|--------------------|--------------|
| Days of Inventory Remaining | Rule 20 (Inventory Velocity) | Go_Daily_Inventory_Summary |
| Inventory Turnover Ratio | Rule 20 | Go_Daily_Inventory_Summary |
| Average Days to Sell Inventory | Rule 18 (Rolling Average) | Go_Daily_Inventory_Summary |
| Fast-Moving vs Slow-Moving Product Ratio | Rule 20 | Go_Daily_Inventory_Summary |
| Monthly Sales Performance | Rules 8-13 | Go_Monthly_Sales_Summary |

### 5.3 Constraint Compliance

| Data Constraint | Enforced by Rule | Validation Method |
|-----------------|------------------|-------------------|
| Non-negative stock levels | Rules 1, 5 | CASE statements with >= 0 checks |
| Monetary format (2 decimals) | Rules 6, 7, 9-11, 16 | ROUND() functions |
| Date format consistency | Rule 17 | Date key validation |
| Percentage range (0-100%) | Rule 10 | CASE statements with range checks |
| Referential integrity | All rules | JOIN operations with dimension tables |

## 6. Performance Optimization Recommendations

### 6.1 Partitioning Strategy
- **Daily Summary**: Partition by date_key for efficient date range queries
- **Monthly Summary**: Partition by year_month for monthly reporting performance

### 6.2 Indexing Recommendations
- **Z-ORDER optimization** on frequently queried columns:
  - Go_Daily_Inventory_Summary: (product_key, warehouse_key, date_key)
  - Go_Monthly_Sales_Summary: (product_key, customer_key, year_month)

### 6.3 Incremental Processing
- Implement delta-based loading for daily updates
- Use MERGE operations for upsert functionality
- Schedule aggregation refresh based on business requirements

## 7. Data Lineage and Audit Trail

### 7.1 Source System Tracking
- All aggregated records include source_system field
- Load_date timestamp for data freshness tracking
- Pipeline audit integration for complete lineage

### 7.2 Data Quality Monitoring
- Automated validation rules execution
- Error logging in Go_Data_Validation_Error table
- Alert thresholds for data quality metrics

## 8. Implementation Schedule

### Phase 1: Core Aggregations (Week 1-2)
- Implement Rules 1-7 for daily inventory summary
- Implement Rules 8-13 for monthly sales summary

### Phase 2: Advanced Analytics (Week 3)
- Implement Rules 18-20 for window functions
- Add rolling averages and cumulative calculations

### Phase 3: Data Quality (Week 4)
- Implement Rules 14-17 for validation
- Set up monitoring and alerting

## 9. API Cost Calculation

apiCost: 0.0425