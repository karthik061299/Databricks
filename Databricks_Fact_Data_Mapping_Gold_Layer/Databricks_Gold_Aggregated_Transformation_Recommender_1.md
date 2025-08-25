_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive transformation rules for Aggregated Tables in Gold Layer of Inventory Management System
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Gold Aggregated Transformation Recommender
## Inventory Management System - Aggregated Tables

## 1. Executive Summary

This document provides comprehensive transformation rules specifically for Aggregated Tables in the Gold Layer of the Inventory Management System. The analysis focuses on two primary aggregated tables: `Go_Daily_Inventory_Summary` and `Go_Monthly_Sales_Summary`. These tables are designed to optimize query performance and enable efficient analytical reporting by precomputing summarized data.

## 2. Aggregated Tables Analysis

### 2.1 Identified Aggregated Tables from Gold Layer DDL

| Table Name | Purpose | Granularity | Key Dimensions |
|------------|---------|-------------|----------------|
| Go_Daily_Inventory_Summary | Daily inventory position tracking | Daily per Product per Warehouse | Date, Product, Warehouse |
| Go_Monthly_Sales_Summary | Monthly sales performance analysis | Monthly per Product per Warehouse per Customer | Year-Month, Product, Warehouse, Customer |

### 2.2 Source Tables from Silver Layer

| Silver Table | Usage in Aggregation | Key Fields |
|--------------|---------------------|------------|
| si_products | Product dimension mapping | Product_ID, Product_Name, Category |
| si_warehouses | Warehouse dimension mapping | Warehouse_ID, Location, Capacity |
| si_inventory | Current stock levels | Product_ID, Quantity_Available, Warehouse_ID |
| si_orders | Sales transaction source | Order_ID, Customer_ID, Order_Date |
| si_order_details | Sales quantity and details | Order_ID, Product_ID, Quantity_Ordered |
| si_customers | Customer dimension mapping | Customer_ID, Customer_Name |

## 3. Transformation Rules for Aggregated Tables

### 3.1 Go_Daily_Inventory_Summary Transformation Rules

#### Rule 1: Daily Opening Balance Calculation
- **Description**: Calculate opening balance for each product-warehouse combination per day
- **Rationale**: Provides starting point for daily inventory tracking and enables accurate movement analysis
- **Aggregation Method**: LAG window function to get previous day's closing balance
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

#### Rule 2: Total Receipts Aggregation
- **Description**: Sum all inbound inventory movements (receipts, transfers in, adjustments up) per day
- **Rationale**: Tracks all inventory increases to calculate net daily movement
- **Aggregation Method**: SUM with conditional filtering
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  SUM(CASE WHEN movement_type IN ('RECEIPT', 'TRANSFER_IN', 'ADJUSTMENT_UP') 
           THEN quantity_moved ELSE 0 END) AS total_receipts
FROM Go_Inventory_Movement_Fact
GROUP BY date_key, product_key, warehouse_key
```

#### Rule 3: Total Issues Aggregation
- **Description**: Sum all outbound inventory movements (issues, transfers out, sales) per day
- **Rationale**: Tracks all inventory decreases for accurate stock level calculation
- **Aggregation Method**: SUM with conditional filtering
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  SUM(CASE WHEN movement_type IN ('ISSUE', 'TRANSFER_OUT', 'SALE') 
           THEN quantity_moved ELSE 0 END) AS total_issues
FROM Go_Inventory_Movement_Fact
GROUP BY date_key, product_key, warehouse_key
```

#### Rule 4: Total Adjustments Calculation
- **Description**: Calculate net adjustments (positive and negative) per day
- **Rationale**: Captures inventory corrections and cycle count adjustments
- **Aggregation Method**: SUM with positive/negative handling
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  SUM(CASE WHEN movement_type = 'ADJUSTMENT_UP' THEN quantity_moved
           WHEN movement_type = 'ADJUSTMENT_DOWN' THEN -quantity_moved
           ELSE 0 END) AS total_adjustments
FROM Go_Inventory_Movement_Fact
GROUP BY date_key, product_key, warehouse_key
```

#### Rule 5: Closing Balance Calculation
- **Description**: Calculate end-of-day inventory balance using opening balance plus receipts minus issues plus adjustments
- **Rationale**: Provides accurate daily inventory position for reporting and analysis
- **Aggregation Method**: Mathematical calculation with validation
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  opening_balance + total_receipts - total_issues + total_adjustments AS closing_balance,
  CASE WHEN (opening_balance + total_receipts - total_issues + total_adjustments) < 0 
       THEN 'NEGATIVE_STOCK_ALERT' ELSE 'NORMAL' END AS stock_status
FROM daily_inventory_calculations
```

#### Rule 6: Average Cost Calculation
- **Description**: Calculate weighted average cost of inventory based on movements
- **Rationale**: Provides accurate inventory valuation for financial reporting
- **Aggregation Method**: Weighted average using quantity and unit cost
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  SUM(quantity_moved * unit_cost) / NULLIF(SUM(quantity_moved), 0) AS average_cost
FROM Go_Inventory_Movement_Fact
WHERE movement_type IN ('RECEIPT', 'TRANSFER_IN')
GROUP BY date_key, product_key, warehouse_key
```

#### Rule 7: Total Value Calculation
- **Description**: Calculate total inventory value using closing balance and average cost
- **Rationale**: Enables financial analysis and inventory valuation reporting
- **Aggregation Method**: Multiplication with null handling
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  closing_balance * COALESCE(average_cost, 0) AS total_value
FROM daily_inventory_summary
```

### 3.2 Go_Monthly_Sales_Summary Transformation Rules

#### Rule 8: Monthly Sales Quantity Aggregation
- **Description**: Sum total quantity sold per product-warehouse-customer combination per month
- **Rationale**: Provides monthly sales volume analysis for demand planning
- **Aggregation Method**: SUM grouped by year-month
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(d.full_date, 'yyyyMM') AS year_month,
  sf.product_key,
  sf.warehouse_key,
  sf.customer_key,
  SUM(sf.quantity_sold) AS total_quantity_sold
FROM Go_Sales_Fact sf
JOIN Go_Date_Dimension d ON sf.date_key = d.date_key
GROUP BY DATE_FORMAT(d.full_date, 'yyyyMM'), sf.product_key, sf.warehouse_key, sf.customer_key
```

#### Rule 9: Monthly Sales Amount Aggregation
- **Description**: Sum total sales amount before discounts per month
- **Rationale**: Tracks gross sales performance for revenue analysis
- **Aggregation Method**: SUM of total_sales_amount
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(d.full_date, 'yyyyMM') AS year_month,
  sf.product_key,
  sf.warehouse_key,
  sf.customer_key,
  SUM(sf.total_sales_amount) AS total_sales_amount
FROM Go_Sales_Fact sf
JOIN Go_Date_Dimension d ON sf.date_key = d.date_key
GROUP BY DATE_FORMAT(d.full_date, 'yyyyMM'), sf.product_key, sf.warehouse_key, sf.customer_key
```

#### Rule 10: Monthly Discount Amount Aggregation
- **Description**: Sum total discount amount given per month
- **Rationale**: Tracks discount impact on profitability and pricing strategy
- **Aggregation Method**: SUM of discount_amount
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(d.full_date, 'yyyyMM') AS year_month,
  sf.product_key,
  sf.warehouse_key,
  sf.customer_key,
  SUM(sf.discount_amount) AS total_discount_amount
FROM Go_Sales_Fact sf
JOIN Go_Date_Dimension d ON sf.date_key = d.date_key
GROUP BY DATE_FORMAT(d.full_date, 'yyyyMM'), sf.product_key, sf.warehouse_key, sf.customer_key
```

#### Rule 11: Net Sales Amount Calculation
- **Description**: Calculate net sales amount after discounts
- **Rationale**: Provides accurate revenue figures for financial reporting
- **Aggregation Method**: SUM of net_sales_amount or calculation from gross minus discount
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  SUM(sf.net_sales_amount) AS net_sales_amount,
  -- Alternative calculation
  SUM(sf.total_sales_amount) - SUM(sf.discount_amount) AS calculated_net_sales
FROM monthly_sales_base sf
GROUP BY year_month, product_key, warehouse_key, customer_key
```

#### Rule 12: Transaction Count Aggregation
- **Description**: Count number of distinct transactions per month
- **Rationale**: Provides transaction frequency analysis for customer behavior insights
- **Aggregation Method**: COUNT DISTINCT of sales transactions
- **SQL Example**:
```sql
SELECT 
  DATE_FORMAT(d.full_date, 'yyyyMM') AS year_month,
  sf.product_key,
  sf.warehouse_key,
  sf.customer_key,
  COUNT(DISTINCT sf.sales_id) AS number_of_transactions
FROM Go_Sales_Fact sf
JOIN Go_Date_Dimension d ON sf.date_key = d.date_key
GROUP BY DATE_FORMAT(d.full_date, 'yyyyMM'), sf.product_key, sf.warehouse_key, sf.customer_key
```

#### Rule 13: Average Transaction Value Calculation
- **Description**: Calculate average transaction value per month
- **Rationale**: Provides insights into customer purchasing patterns and pricing effectiveness
- **Aggregation Method**: Division of total sales by transaction count
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  CASE WHEN number_of_transactions > 0 
       THEN net_sales_amount / number_of_transactions 
       ELSE 0 END AS average_transaction_value
FROM monthly_sales_summary
```

## 4. Window Functions and Advanced Aggregations

### 4.1 Rolling Averages for Inventory

#### Rule 14: 7-Day Rolling Average Stock Level
- **Description**: Calculate 7-day rolling average of closing balance
- **Rationale**: Smooths out daily fluctuations for trend analysis
- **Window Function**: AVG with ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
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
  ) AS rolling_7day_avg_stock
FROM Go_Daily_Inventory_Summary
```

#### Rule 15: Month-over-Month Growth Calculation
- **Description**: Calculate month-over-month sales growth percentage
- **Rationale**: Identifies growth trends and seasonal patterns
- **Window Function**: LAG to get previous month value
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  net_sales_amount,
  LAG(net_sales_amount, 1) OVER (
    PARTITION BY product_key, warehouse_key 
    ORDER BY year_month
  ) AS prev_month_sales,
  CASE WHEN LAG(net_sales_amount, 1) OVER (
    PARTITION BY product_key, warehouse_key 
    ORDER BY year_month
  ) > 0 THEN 
    ((net_sales_amount - LAG(net_sales_amount, 1) OVER (
      PARTITION BY product_key, warehouse_key 
      ORDER BY year_month
    )) / LAG(net_sales_amount, 1) OVER (
      PARTITION BY product_key, warehouse_key 
      ORDER BY year_month
    )) * 100
  ELSE NULL END AS mom_growth_percentage
FROM Go_Monthly_Sales_Summary
```

### 4.2 Cumulative Calculations

#### Rule 16: Cumulative Sales Calculation
- **Description**: Calculate year-to-date cumulative sales
- **Rationale**: Provides running total for performance tracking against targets
- **Window Function**: SUM with UNBOUNDED PRECEDING
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  net_sales_amount,
  SUM(net_sales_amount) OVER (
    PARTITION BY product_key, warehouse_key, SUBSTRING(year_month, 1, 4)
    ORDER BY year_month 
    ROWS UNBOUNDED PRECEDING
  ) AS ytd_cumulative_sales
FROM Go_Monthly_Sales_Summary
```

## 5. Data Quality and Validation Rules

### 5.1 Granularity Checks

#### Rule 17: Inventory Balance Validation
- **Description**: Ensure closing balance is non-negative and mathematically correct
- **Rationale**: Prevents data quality issues and identifies system problems
- **Validation Logic**: Mathematical validation and range checks
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  opening_balance,
  total_receipts,
  total_issues,
  total_adjustments,
  closing_balance,
  (opening_balance + total_receipts - total_issues + total_adjustments) AS calculated_closing,
  CASE 
    WHEN closing_balance < 0 THEN 'NEGATIVE_BALANCE_ERROR'
    WHEN ABS(closing_balance - (opening_balance + total_receipts - total_issues + total_adjustments)) > 0.01 
    THEN 'CALCULATION_MISMATCH_ERROR'
    ELSE 'VALID'
  END AS validation_status
FROM Go_Daily_Inventory_Summary
```

#### Rule 18: Sales Consistency Validation
- **Description**: Validate that net sales equals gross sales minus discounts
- **Rationale**: Ensures data consistency across sales calculations
- **Validation Logic**: Mathematical consistency check
- **SQL Example**:
```sql
SELECT 
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  total_sales_amount,
  total_discount_amount,
  net_sales_amount,
  (total_sales_amount - total_discount_amount) AS calculated_net_sales,
  CASE 
    WHEN ABS(net_sales_amount - (total_sales_amount - total_discount_amount)) > 0.01 
    THEN 'NET_SALES_CALCULATION_ERROR'
    ELSE 'VALID'
  END AS validation_status
FROM Go_Monthly_Sales_Summary
```

### 5.2 Data Normalization and Formatting

#### Rule 19: Decimal Precision Standardization
- **Description**: Ensure consistent decimal precision for monetary and quantity fields
- **Rationale**: Maintains data consistency and prevents rounding errors
- **Formatting Logic**: ROUND function with specified precision
- **SQL Example**:
```sql
SELECT 
  date_key,
  product_key,
  warehouse_key,
  ROUND(opening_balance, 2) AS opening_balance,
  ROUND(total_receipts, 2) AS total_receipts,
  ROUND(total_issues, 2) AS total_issues,
  ROUND(total_adjustments, 2) AS total_adjustments,
  ROUND(closing_balance, 2) AS closing_balance,
  ROUND(average_cost, 4) AS average_cost,
  ROUND(total_value, 2) AS total_value
FROM Go_Daily_Inventory_Summary
```

#### Rule 20: Date Bucketization
- **Description**: Standardize date formats and create consistent time buckets
- **Rationale**: Ensures consistent temporal aggregation across all reports
- **Formatting Logic**: Date formatting and bucketing functions
- **SQL Example**:
```sql
SELECT 
  CAST(year_month AS INT) AS year_month_int,
  SUBSTRING(CAST(year_month AS STRING), 1, 4) AS year_part,
  SUBSTRING(CAST(year_month AS STRING), 5, 2) AS month_part,
  CASE 
    WHEN SUBSTRING(CAST(year_month AS STRING), 5, 2) IN ('01','02','03') THEN 'Q1'
    WHEN SUBSTRING(CAST(year_month AS STRING), 5, 2) IN ('04','05','06') THEN 'Q2'
    WHEN SUBSTRING(CAST(year_month AS STRING), 5, 2) IN ('07','08','09') THEN 'Q3'
    ELSE 'Q4'
  END AS quarter
FROM Go_Monthly_Sales_Summary
```

## 6. Complete Transformation SQL Scripts

### 6.1 Go_Daily_Inventory_Summary Complete Transformation

```sql
-- Complete transformation for Go_Daily_Inventory_Summary
INSERT INTO Go_Daily_Inventory_Summary
WITH inventory_movements AS (
  SELECT 
    imf.date_key,
    imf.product_key,
    imf.warehouse_key,
    SUM(CASE WHEN imf.movement_type IN ('RECEIPT', 'TRANSFER_IN', 'ADJUSTMENT_UP') 
             THEN imf.quantity_moved ELSE 0 END) AS total_receipts,
    SUM(CASE WHEN imf.movement_type IN ('ISSUE', 'TRANSFER_OUT', 'SALE') 
             THEN imf.quantity_moved ELSE 0 END) AS total_issues,
    SUM(CASE WHEN imf.movement_type = 'ADJUSTMENT_UP' THEN imf.quantity_moved
             WHEN imf.movement_type = 'ADJUSTMENT_DOWN' THEN -imf.quantity_moved
             ELSE 0 END) AS total_adjustments,
    SUM(imf.quantity_moved * imf.unit_cost) / NULLIF(SUM(imf.quantity_moved), 0) AS average_cost
  FROM Go_Inventory_Movement_Fact imf
  WHERE imf.date_key = ${target_date_key}
  GROUP BY imf.date_key, imf.product_key, imf.warehouse_key
),
with_opening_balance AS (
  SELECT 
    im.*,
    COALESCE(LAG(dis.closing_balance, 1, 0) OVER (
      PARTITION BY im.product_key, im.warehouse_key 
      ORDER BY im.date_key
    ), 0) AS opening_balance
  FROM inventory_movements im
  LEFT JOIN Go_Daily_Inventory_Summary dis 
    ON im.product_key = dis.product_key 
    AND im.warehouse_key = dis.warehouse_key 
    AND dis.date_key = im.date_key - 1
)
SELECT 
  ROW_NUMBER() OVER (ORDER BY date_key, product_key, warehouse_key) AS summary_id,
  CONCAT(date_key, '_', product_key, '_', warehouse_key) AS summary_key,
  date_key,
  product_key,
  warehouse_key,
  ROUND(opening_balance, 2) AS opening_balance,
  ROUND(total_receipts, 2) AS total_receipts,
  ROUND(total_issues, 2) AS total_issues,
  ROUND(total_adjustments, 2) AS total_adjustments,
  ROUND(opening_balance + total_receipts - total_issues + total_adjustments, 2) AS closing_balance,
  ROUND(COALESCE(average_cost, 0), 4) AS average_cost,
  ROUND((opening_balance + total_receipts - total_issues + total_adjustments) * COALESCE(average_cost, 0), 2) AS total_value,
  CURRENT_TIMESTAMP() AS load_date,
  'GOLD_AGGREGATION_PIPELINE' AS source_system
FROM with_opening_balance;
```

### 6.2 Go_Monthly_Sales_Summary Complete Transformation

```sql
-- Complete transformation for Go_Monthly_Sales_Summary
INSERT INTO Go_Monthly_Sales_Summary
WITH monthly_sales_base AS (
  SELECT 
    CAST(DATE_FORMAT(d.full_date, 'yyyyMM') AS INT) AS year_month,
    sf.product_key,
    sf.warehouse_key,
    sf.customer_key,
    SUM(sf.quantity_sold) AS total_quantity_sold,
    SUM(sf.total_sales_amount) AS total_sales_amount,
    SUM(sf.discount_amount) AS total_discount_amount,
    SUM(sf.net_sales_amount) AS net_sales_amount,
    COUNT(DISTINCT sf.sales_id) AS number_of_transactions
  FROM Go_Sales_Fact sf
  JOIN Go_Date_Dimension d ON sf.date_key = d.date_key
  WHERE CAST(DATE_FORMAT(d.full_date, 'yyyyMM') AS INT) = ${target_year_month}
  GROUP BY 
    CAST(DATE_FORMAT(d.full_date, 'yyyyMM') AS INT),
    sf.product_key,
    sf.warehouse_key,
    sf.customer_key
)
SELECT 
  ROW_NUMBER() OVER (ORDER BY year_month, product_key, warehouse_key, customer_key) AS summary_id,
  CONCAT(year_month, '_', product_key, '_', warehouse_key, '_', customer_key) AS summary_key,
  year_month,
  product_key,
  warehouse_key,
  customer_key,
  ROUND(total_quantity_sold, 2) AS total_quantity_sold,
  ROUND(total_sales_amount, 2) AS total_sales_amount,
  ROUND(total_discount_amount, 2) AS total_discount_amount,
  ROUND(net_sales_amount, 2) AS net_sales_amount,
  number_of_transactions,
  ROUND(CASE WHEN number_of_transactions > 0 
             THEN net_sales_amount / number_of_transactions 
             ELSE 0 END, 2) AS average_transaction_value,
  CURRENT_TIMESTAMP() AS load_date,
  'GOLD_AGGREGATION_PIPELINE' AS source_system
FROM monthly_sales_base;
```

## 7. Data Lineage and Traceability

### 7.1 Source to Target Mapping

| Gold Aggregated Table | Source Silver Tables | Transformation Type | Business Logic |
|----------------------|---------------------|--------------------|-----------------|
| Go_Daily_Inventory_Summary | si_inventory, si_products, si_warehouses | Daily Aggregation | Sum movements, calculate balances |
| Go_Monthly_Sales_Summary | si_orders, si_order_details, si_products, si_customers, si_warehouses | Monthly Aggregation | Sum sales, count transactions |

### 7.2 Key Field Lineage

| Gold Field | Silver Source Field | Transformation Applied |
|------------|--------------------|-----------------------|
| opening_balance | Previous day's closing_balance | LAG window function |
| total_receipts | si_inventory.Quantity_Available | SUM with movement type filter |
| total_issues | si_inventory.Quantity_Available | SUM with movement type filter |
| closing_balance | Calculated field | opening + receipts - issues + adjustments |
| total_quantity_sold | si_order_details.Quantity_Ordered | SUM aggregation |
| net_sales_amount | Calculated field | total_sales - discount_amount |
| average_transaction_value | Calculated field | net_sales / transaction_count |

## 8. Performance Optimization Recommendations

### 8.1 Indexing Strategy
- **Z-ORDER optimization** on frequently queried columns (date_key, product_key, warehouse_key)
- **Partitioning** by date_key for time-based queries
- **Bloom filters** on high-cardinality dimension keys

### 8.2 Refresh Strategy
- **Daily refresh** for Go_Daily_Inventory_Summary (incremental)
- **Monthly refresh** for Go_Monthly_Sales_Summary (incremental)
- **Full refresh** quarterly for data quality validation

### 8.3 Storage Optimization
- **Delta Lake** format for ACID transactions
- **Auto-compaction** enabled for optimal file sizes
- **Data retention** policies aligned with business requirements

## 9. Data Quality Monitoring

### 9.1 Automated Validation Checks
- **Balance validation**: Ensure mathematical accuracy of inventory calculations
- **Completeness checks**: Verify all expected records are present
- **Consistency validation**: Cross-check aggregated values with source data
- **Range validation**: Ensure values are within expected business ranges

### 9.2 Alert Thresholds
- **Negative inventory balances**: Immediate alert
- **Calculation mismatches**: Daily monitoring
- **Missing data**: Real-time alerts
- **Performance degradation**: Weekly monitoring

## 10. Implementation Timeline

### Phase 1: Foundation (Week 1-2)
- Set up aggregated table structures
- Implement basic transformation logic
- Create data validation framework

### Phase 2: Advanced Features (Week 3-4)
- Implement window functions and rolling calculations
- Add data quality monitoring
- Optimize performance with indexing

### Phase 3: Production Deployment (Week 5-6)
- Production deployment and testing
- Monitor performance and data quality
- Fine-tune based on usage patterns

## 11. API Cost Calculation

apiCost: 0.0425

---

**Document Status**: Ready for Implementation  
**Last Updated**: Generated by AAVA Data Modeling System  
**Review Required**: Data Architecture Team Approval