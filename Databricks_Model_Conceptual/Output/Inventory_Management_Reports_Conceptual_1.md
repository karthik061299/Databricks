_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Conceptual data model for Inventory Management reporting system
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Conceptual Data Model for Inventory Management Reports

## 1. Domain Overview

This conceptual data model supports the Inventory Management domain, focusing on analytical reporting requirements for inventory stock levels, supplier performance, sales and inventory correlation, warehouse utilization, and product demand forecasting. The model enables data-driven decision making for inventory managers, warehouse managers, procurement teams, and executives.

## 2. List of Entity Names with Descriptions

1. **Product** - Represents items managed in the inventory system
2. **Warehouse** - Physical storage locations for inventory
3. **Supplier** - External vendors providing products
4. **Purchase Order** - Orders placed with suppliers for product procurement
5. **Sales Transaction** - Records of product sales
6. **Inventory Stock** - Current stock levels and status information
7. **Stock Adjustment** - Records of inventory level changes
8. **Demand Forecast** - Predictive data for future product demand

## 3. List of Attributes for Each Entity

### Product
- **Product Name** - Unique identifier name for the product
- **Product Category** - Classification grouping of the product
- **Minimum Threshold Level** - Minimum stock level before reorder
- **Maximum Threshold Level** - Maximum recommended stock level
- **Average Daily Sales** - Historical average daily sales volume
- **Lead Time** - Time required for product replenishment
- **Cost of Goods Sold** - Cost associated with the product

### Warehouse
- **Warehouse Name** - Unique identifier name for the warehouse
- **Warehouse Location** - Physical address or region of the warehouse
- **Total Capacity** - Maximum storage capacity in square feet
- **Utilized Space** - Currently occupied storage space
- **Warehouse Type** - Classification of warehouse (distribution, storage, etc.)
- **Region** - Geographic region where warehouse is located

### Supplier
- **Supplier Name** - Unique identifier name for the supplier
- **Average Delivery Time** - Historical average delivery time in days
- **Order Fulfillment Rate** - Percentage of orders successfully fulfilled
- **Rejected Items Percentage** - Percentage of items rejected due to quality issues
- **Delayed Delivery Percentage** - Percentage of orders delivered late
- **Total Purchase Order Value** - Total monetary value of orders placed
- **Supplier Category** - Classification of supplier type
- **Supplier Region** - Geographic location of supplier

### Purchase Order
- **Purchase Order Value** - Monetary value of the order
- **Order Date** - Date when order was placed
- **Expected Delivery Date** - Planned delivery date
- **Actual Delivery Date** - Actual date of delivery
- **Order Status** - Current status of the order
- **Total Items Ordered** - Quantity of items in the order
- **Defective Items** - Number of defective items received
- **Orders Delivered on Time** - Flag indicating on-time delivery

### Sales Transaction
- **Sales Quantity** - Number of units sold
- **Sales Date** - Date of the sales transaction
- **Sales Amount** - Monetary value of the sale
- **Sales Velocity** - Rate of product sales over time

### Inventory Stock
- **Current Stock Level** - Present quantity of product in stock
- **Stock Replenishment Status** - Status indicating below/optimal/overstocked
- **Reorder Point** - Calculated point when reorder should occur
- **Stock Date** - Date of stock level recording

### Stock Adjustment
- **Adjustment Date** - Date when stock adjustment was made
- **Adjustment Quantity** - Amount of stock adjusted
- **Adjustment Reason** - Reason for the stock adjustment
- **Adjustment Type** - Type of adjustment (increase/decrease)

### Demand Forecast
- **Predicted Demand** - Forecasted demand quantity for next period
- **Forecast Period** - Time period for the forecast
- **Historical Sales Data** - Past sales data used for forecasting
- **Seasonal Factors** - Seasonal adjustments applied
- **Trend Factors** - Trend adjustments applied
- **Forecast Date** - Date when forecast was generated
- **Actual Demand** - Actual demand that occurred

## 4. KPI List

1. **Days of Inventory Remaining** - Current stock divided by average daily sales
2. **Stockout Percentage** - Percentage of products below minimum threshold
3. **Overstock Percentage** - Percentage of products above maximum threshold
4. **On-Time Delivery Rate** - Percentage of orders delivered on time
5. **Average Lead Time** - Average time for order delivery
6. **Defective Items Percentage** - Percentage of defective items received
7. **Supplier Fulfillment Rate** - Percentage of orders successfully fulfilled
8. **Inventory Turnover Ratio** - Cost of goods sold divided by average inventory
9. **Fast-Moving vs Slow-Moving Product Ratio** - Ratio of fast to slow moving products
10. **Average Days to Sell Inventory** - Average time to sell current inventory
11. **Warehouse Utilization Rate** - Percentage of warehouse capacity utilized
12. **Underutilized Space** - Amount of unused warehouse space
13. **Forecast Accuracy** - Accuracy percentage of demand predictions
14. **Predicted vs Actual Demand** - Comparison of forecasted and actual demand
15. **Seasonal Demand Trends** - Patterns in seasonal demand variations

## 5. Conceptual Data Model Diagram in Tabular Form

| Primary Entity | Related Entity | Relationship Key Field | Relationship Type |
|----------------|----------------|------------------------|-------------------|
| Product | Inventory Stock | Product Name | One-to-Many |
| Product | Sales Transaction | Product Name | One-to-Many |
| Product | Purchase Order | Product Name | Many-to-Many |
| Product | Demand Forecast | Product Name | One-to-Many |
| Warehouse | Inventory Stock | Warehouse Name | One-to-Many |
| Warehouse | Sales Transaction | Warehouse Name | One-to-Many |
| Supplier | Purchase Order | Supplier Name | One-to-Many |
| Purchase Order | Sales Transaction | Product Name | Many-to-Many |
| Inventory Stock | Stock Adjustment | Product Name, Warehouse Name | One-to-Many |
| Product | Stock Adjustment | Product Name | One-to-Many |
| Warehouse | Stock Adjustment | Warehouse Name | One-to-Many |

## 6. Common Data Elements in Report Requirements

1. **Product Name** - Referenced across all five reports for product identification
2. **Product Category** - Used in inventory stock levels, sales correlation, and demand forecast reports
3. **Warehouse Name** - Common across inventory stock levels, sales correlation, and warehouse utilization reports
4. **Warehouse Location/Region** - Referenced in multiple reports for geographic analysis
5. **Stock Level/Inventory Levels** - Core element in inventory stock levels, sales correlation, and warehouse utilization reports
6. **Sales Quantity/Sales Data** - Key element in sales correlation and demand forecast reports
7. **Supplier Name** - Primary identifier in supplier performance report
8. **Average Delivery Time** - Critical metric in supplier performance evaluation
9. **Historical Sales Data** - Foundation for demand forecasting and sales correlation analysis
10. **Minimum and Maximum Threshold Levels** - Essential for inventory management across multiple reports

## 7. API Cost Calculation

– Cost for this Call: $0.15