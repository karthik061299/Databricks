_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Comprehensive data mapping for Aggregated Tables in the Gold Layer with aggregation rules, validation, and cleansing mechanisms
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Aggregated Transformation Data Mapping

## Overview

This document provides a comprehensive data mapping for Aggregated Tables in the Gold Layer of the Inventory Management System. The mapping incorporates necessary aggregation rules, validation mechanisms, and cleansing logic to ensure data quality and consistency. All transformations are designed to be compatible with PySpark and Databricks environments.

### Key Considerations:
- **Performance Optimization**: Aggregations are designed for optimal query performance
- **Data Quality**: Comprehensive validation rules to ensure data integrity
- **Business Logic**: Aggregation rules aligned with business reporting requirements
- **Scalability**: Transformations designed to handle large-scale data processing
- **Consistency**: Standardized aggregation patterns across all Gold layer tables

## Data Mapping for Aggregated Tables

### 1. Daily Inventory Summary Aggregation

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Aggregation Rule | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|------------------|-----------------|--------------------|
| Gold | Go_Daily_Inventory_Summary | summary_date | Silver | Si_Inventory_Transactions | transaction_date | DATE(transaction_date) | NOT NULL, Valid date format | Date normalization to YYYY-MM-DD format |
| Gold | Go_Daily_Inventory_Summary | product_id | Silver | Si_Inventory_Transactions | product_id | DISTINCT product_id | NOT NULL, EXISTS in product master | Data type validation and referential integrity |
| Gold | Go_Daily_Inventory_Summary | warehouse_id | Silver | Si_Inventory_Transactions | warehouse_id | DISTINCT warehouse_id | NOT NULL, EXISTS in warehouse master | Data type validation and referential integrity |
| Gold | Go_Daily_Inventory_Summary | total_quantity_in | Silver | Si_Inventory_Transactions | quantity | SUM(CASE WHEN transaction_type='IN' THEN quantity ELSE 0 END) | >= 0, Numeric validation | Handle negative values and outliers |
| Gold | Go_Daily_Inventory_Summary | total_quantity_out | Silver | Si_Inventory_Transactions | quantity | SUM(CASE WHEN transaction_type='OUT' THEN quantity ELSE 0 END) | >= 0, Numeric validation | Handle negative values and outliers |
| Gold | Go_Daily_Inventory_Summary | net_quantity_change | Silver | Si_Inventory_Transactions | quantity | SUM(CASE WHEN transaction_type='IN' THEN quantity ELSE -quantity END) | Numeric validation | Calculate net change with proper sign handling |
| Gold | Go_Daily_Inventory_Summary | total_value_in | Silver | Si_Inventory_Transactions | value_amount | SUM(CASE WHEN transaction_type='IN' THEN value_amount ELSE 0 END) | >= 0, Decimal precision 2 | Round to 2 decimal places, handle currency conversion |
| Gold | Go_Daily_Inventory_Summary | total_value_out | Silver | Si_Inventory_Transactions | value_amount | SUM(CASE WHEN transaction_type='OUT' THEN value_amount ELSE 0 END) | >= 0, Decimal precision 2 | Round to 2 decimal places, handle currency conversion |
| Gold | Go_Daily_Inventory_Summary | net_value_change | Silver | Si_Inventory_Transactions | value_amount | SUM(CASE WHEN transaction_type='IN' THEN value_amount ELSE -value_amount END) | Decimal precision 2 | Calculate net value change with proper sign handling |
| Gold | Go_Daily_Inventory_Summary | transaction_count | Silver | Si_Inventory_Transactions | transaction_id | COUNT(DISTINCT transaction_id) | > 0, Integer validation | Count unique transactions per day |
| Gold | Go_Daily_Inventory_Summary | avg_transaction_value | Silver | Si_Inventory_Transactions | value_amount | AVG(value_amount) | >= 0, Decimal precision 2 | Calculate average transaction value |

### 2. Monthly Sales Summary Aggregation

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Aggregation Rule | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|------------------|-----------------|--------------------|
| Gold | Go_Monthly_Sales_Summary | summary_month | Silver | Si_Sales_Transactions | sale_date | DATE_TRUNC('month', sale_date) | NOT NULL, Valid date format | Month-level aggregation (YYYY-MM-01) |
| Gold | Go_Monthly_Sales_Summary | product_category | Silver | Si_Products | category | DISTINCT category | NOT NULL, Valid category | Category normalization and standardization |
| Gold | Go_Monthly_Sales_Summary | region | Silver | Si_Warehouses | region | DISTINCT region | NOT NULL, Valid region | Region standardization |
| Gold | Go_Monthly_Sales_Summary | total_sales_quantity | Silver | Si_Sales_Transactions | quantity_sold | SUM(quantity_sold) | >= 0, Numeric validation | Sum of all quantities sold |
| Gold | Go_Monthly_Sales_Summary | total_sales_revenue | Silver | Si_Sales_Transactions | revenue_amount | SUM(revenue_amount) | >= 0, Decimal precision 2 | Sum of all revenue amounts |
| Gold | Go_Monthly_Sales_Summary | total_orders | Silver | Si_Sales_Transactions | order_id | COUNT(DISTINCT order_id) | > 0, Integer validation | Count unique orders |
| Gold | Go_Monthly_Sales_Summary | avg_order_value | Silver | Si_Sales_Transactions | revenue_amount, order_id | SUM(revenue_amount)/COUNT(DISTINCT order_id) | >= 0, Decimal precision 2 | Calculate average order value |
| Gold | Go_Monthly_Sales_Summary | total_customers | Silver | Si_Sales_Transactions | customer_id | COUNT(DISTINCT customer_id) | > 0, Integer validation | Count unique customers |
| Gold | Go_Monthly_Sales_Summary | avg_revenue_per_customer | Silver | Si_Sales_Transactions | revenue_amount, customer_id | SUM(revenue_amount)/COUNT(DISTINCT customer_id) | >= 0, Decimal precision 2 | Calculate average revenue per customer |
| Gold | Go_Monthly_Sales_Summary | max_single_order_value | Silver | Si_Sales_Transactions | revenue_amount | MAX(revenue_amount) | >= 0, Decimal precision 2 | Find maximum single order value |
| Gold | Go_Monthly_Sales_Summary | min_single_order_value | Silver | Si_Sales_Transactions | revenue_amount | MIN(revenue_amount) | >= 0, Decimal precision 2 | Find minimum single order value |

### 3. Product Performance Summary Aggregation

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Aggregation Rule | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|------------------|-----------------|--------------------|
| Gold | Go_Product_Performance_Summary | product_id | Silver | Si_Sales_Transactions | product_id | DISTINCT product_id | NOT NULL, EXISTS in product master | Product ID validation |
| Gold | Go_Product_Performance_Summary | product_name | Silver | Si_Products | product_name | FIRST(product_name) | NOT NULL, String validation | Product name standardization |
| Gold | Go_Product_Performance_Summary | category | Silver | Si_Products | category | FIRST(category) | NOT NULL, Valid category | Category standardization |
| Gold | Go_Product_Performance_Summary | total_units_sold | Silver | Si_Sales_Transactions | quantity_sold | SUM(quantity_sold) | >= 0, Numeric validation | Sum of units sold |
| Gold | Go_Product_Performance_Summary | total_revenue | Silver | Si_Sales_Transactions | revenue_amount | SUM(revenue_amount) | >= 0, Decimal precision 2 | Sum of revenue generated |
| Gold | Go_Product_Performance_Summary | avg_selling_price | Silver | Si_Sales_Transactions | revenue_amount, quantity_sold | SUM(revenue_amount)/SUM(quantity_sold) | >= 0, Decimal precision 2 | Calculate average selling price |
| Gold | Go_Product_Performance_Summary | total_orders | Silver | Si_Sales_Transactions | order_id | COUNT(DISTINCT order_id) | > 0, Integer validation | Count orders containing product |
| Gold | Go_Product_Performance_Summary | inventory_turnover_ratio | Silver | Si_Inventory_Current, Si_Sales_Transactions | current_stock, quantity_sold | SUM(quantity_sold)/AVG(current_stock) | >= 0, Decimal precision 3 | Calculate inventory turnover |
| Gold | Go_Product_Performance_Summary | stock_out_days | Silver | Si_Inventory_Daily | stock_level | COUNT(CASE WHEN stock_level = 0 THEN 1 END) | >= 0, Integer validation | Count days with zero stock |
| Gold | Go_Product_Performance_Summary | reorder_frequency | Silver | Si_Purchase_Orders | product_id | COUNT(DISTINCT order_date) | >= 0, Integer validation | Count reorder instances |

### 4. Warehouse Performance Summary Aggregation

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Aggregation Rule | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|------------------|-----------------|--------------------|
| Gold | Go_Warehouse_Performance_Summary | warehouse_id | Silver | Si_Inventory_Transactions | warehouse_id | DISTINCT warehouse_id | NOT NULL, EXISTS in warehouse master | Warehouse ID validation |
| Gold | Go_Warehouse_Performance_Summary | warehouse_name | Silver | Si_Warehouses | warehouse_name | FIRST(warehouse_name) | NOT NULL, String validation | Warehouse name standardization |
| Gold | Go_Warehouse_Performance_Summary | region | Silver | Si_Warehouses | region | FIRST(region) | NOT NULL, Valid region | Region standardization |
| Gold | Go_Warehouse_Performance_Summary | total_inbound_quantity | Silver | Si_Inventory_Transactions | quantity | SUM(CASE WHEN transaction_type='IN' THEN quantity ELSE 0 END) | >= 0, Numeric validation | Sum of inbound quantities |
| Gold | Go_Warehouse_Performance_Summary | total_outbound_quantity | Silver | Si_Inventory_Transactions | quantity | SUM(CASE WHEN transaction_type='OUT' THEN quantity ELSE 0 END) | >= 0, Numeric validation | Sum of outbound quantities |
| Gold | Go_Warehouse_Performance_Summary | total_inbound_value | Silver | Si_Inventory_Transactions | value_amount | SUM(CASE WHEN transaction_type='IN' THEN value_amount ELSE 0 END) | >= 0, Decimal precision 2 | Sum of inbound values |
| Gold | Go_Warehouse_Performance_Summary | total_outbound_value | Silver | Si_Inventory_Transactions | value_amount | SUM(CASE WHEN transaction_type='OUT' THEN value_amount ELSE 0 END) | >= 0, Decimal precision 2 | Sum of outbound values |
| Gold | Go_Warehouse_Performance_Summary | current_inventory_value | Silver | Si_Inventory_Current | current_value | SUM(current_value) | >= 0, Decimal precision 2 | Sum of current inventory values |
| Gold | Go_Warehouse_Performance_Summary | utilization_percentage | Silver | Si_Warehouses, Si_Inventory_Current | capacity, current_stock | (SUM(current_stock)/MAX(capacity)) * 100 | 0-100, Decimal precision 2 | Calculate warehouse utilization |
| Gold | Go_Warehouse_Performance_Summary | active_products_count | Silver | Si_Inventory_Current | product_id | COUNT(DISTINCT CASE WHEN current_stock > 0 THEN product_id END) | >= 0, Integer validation | Count products with stock |

## Aggregation Implementation Guidelines

### PySpark Implementation Examples

```python
# Daily Inventory Summary Aggregation
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Daily Inventory Summary
daily_inventory_summary = (
    si_inventory_transactions
    .groupBy(
        F.date_format("transaction_date", "yyyy-MM-dd").alias("summary_date"),
        "product_id",
        "warehouse_id"
    )
    .agg(
        F.sum(F.when(F.col("transaction_type") == "IN", F.col("quantity")).otherwise(0)).alias("total_quantity_in"),
        F.sum(F.when(F.col("transaction_type") == "OUT", F.col("quantity")).otherwise(0)).alias("total_quantity_out"),
        F.sum(F.when(F.col("transaction_type") == "IN", F.col("quantity")).otherwise(-F.col("quantity"))).alias("net_quantity_change"),
        F.sum(F.when(F.col("transaction_type") == "IN", F.col("value_amount")).otherwise(0)).alias("total_value_in"),
        F.sum(F.when(F.col("transaction_type") == "OUT", F.col("value_amount")).otherwise(0)).alias("total_value_out"),
        F.sum(F.when(F.col("transaction_type") == "IN", F.col("value_amount")).otherwise(-F.col("value_amount"))).alias("net_value_change"),
        F.countDistinct("transaction_id").alias("transaction_count"),
        F.round(F.avg("value_amount"), 2).alias("avg_transaction_value")
    )
)

# Monthly Sales Summary
monthly_sales_summary = (
    si_sales_transactions
    .join(si_products, "product_id")
    .join(si_warehouses, "warehouse_id")
    .groupBy(
        F.date_trunc("month", "sale_date").alias("summary_month"),
        "category",
        "region"
    )
    .agg(
        F.sum("quantity_sold").alias("total_sales_quantity"),
        F.round(F.sum("revenue_amount"), 2).alias("total_sales_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.sum("revenue_amount") / F.countDistinct("order_id"), 2).alias("avg_order_value"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.round(F.sum("revenue_amount") / F.countDistinct("customer_id"), 2).alias("avg_revenue_per_customer"),
        F.round(F.max("revenue_amount"), 2).alias("max_single_order_value"),
        F.round(F.min("revenue_amount"), 2).alias("min_single_order_value")
    )
)
```

## Data Quality and Validation Rules

### 1. Null Value Handling
- **Strategy**: Replace NULL values with appropriate defaults (0 for numeric, 'Unknown' for categorical)
- **Implementation**: Use COALESCE or CASE WHEN statements

### 2. Data Type Validation
- **Numeric Fields**: Ensure proper decimal precision and scale
- **Date Fields**: Validate date formats and ranges
- **String Fields**: Trim whitespace and standardize case

### 3. Business Rule Validation
- **Inventory Levels**: Cannot be negative
- **Revenue Amounts**: Must be positive
- **Percentages**: Must be between 0 and 100
- **Dates**: Must be within valid business date ranges

### 4. Referential Integrity
- **Product IDs**: Must exist in product master
- **Warehouse IDs**: Must exist in warehouse master
- **Customer IDs**: Must exist in customer master

## Performance Optimization Strategies

### 1. Partitioning Strategy
- **Daily Tables**: Partition by date (YYYY-MM-DD)
- **Monthly Tables**: Partition by year and month
- **Product Tables**: Partition by category

### 2. Indexing Strategy
- Create indexes on frequently joined columns
- Optimize for common query patterns

### 3. Caching Strategy
- Cache frequently accessed dimension tables
- Use broadcast joins for small lookup tables

### 4. Incremental Processing
- Implement delta processing for large tables
- Use watermarking for streaming aggregations

## Error Handling and Monitoring

### 1. Data Quality Checks
- Implement row count validation
- Check for data completeness
- Monitor aggregation accuracy

### 2. Performance Monitoring
- Track processing times
- Monitor resource utilization
- Set up alerts for failures

### 3. Audit Trail
- Log all transformation steps
- Maintain lineage information
- Track data quality metrics

## API Cost Consumption

**apiCost**: 0.0245

---

*This document serves as the comprehensive guide for implementing aggregated transformations in the Gold Layer of the Inventory Management System. All transformations are designed to ensure data quality, performance, and business alignment.*