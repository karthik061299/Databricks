____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Data constraints and business rules for Inventory Management reporting system
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Model Data Constraints for Inventory Management Reports

## 1. Data Expectations

### 1.1 Data Completeness Expectations
1. **Product Information**: All products must have complete identification data including product name and category
2. **Warehouse Data**: All warehouses must have complete capacity and location information
3. **Supplier Records**: All suppliers must have complete performance tracking data
4. **Inventory Records**: All inventory entries must have current stock levels and threshold values
5. **Sales Data**: All sales transactions must have complete quantity and date information
6. **Purchase Orders**: All purchase orders must have complete supplier, product, and delivery information

### 1.2 Data Accuracy Expectations
1. **Stock Levels**: Inventory levels must accurately reflect physical stock counts
2. **Sales Quantities**: Sales data must match actual transaction records
3. **Delivery Dates**: Actual delivery dates must be recorded accurately for supplier performance evaluation
4. **Warehouse Capacity**: Utilized space cannot exceed total warehouse capacity
5. **Supplier Metrics**: Performance percentages must be calculated based on actual delivery and quality data

### 1.3 Data Format Expectations
1. **Dates**: All dates must follow consistent format (YYYY-MM-DD)
2. **Percentages**: All percentage values must be expressed as decimal values between 0 and 1
3. **Quantities**: All quantity fields must be expressed as whole numbers
4. **Currency**: All monetary values must be in consistent currency format
5. **Names**: All name fields must follow consistent naming conventions

### 1.4 Data Consistency Expectations
1. **Product Names**: Must be consistent across all related tables and reports
2. **Warehouse Names**: Must be standardized across all inventory and sales records
3. **Supplier Names**: Must be consistent across purchase orders and delivery records
4. **Time Periods**: Reporting periods must be consistent across all time-based analyses
5. **Units of Measure**: All measurements must use consistent units (square feet, days, etc.)

## 2. Constraints

### 2.1 Mandatory Field Constraints
1. **Product Name**: Cannot be null or empty, must be unique within the system
2. **Warehouse Name**: Cannot be null or empty, must be unique within the system
3. **Supplier Name**: Cannot be null or empty, must be unique within the system
4. **Stock Level**: Cannot be null, must be present for all inventory records
5. **Sales Date**: Cannot be null for any sales transaction
6. **Purchase Order Date**: Cannot be null for any purchase order

### 2.2 Data Type Constraints
1. **Stock Level**: Must be non-negative integer
2. **Sales Quantity**: Must be non-negative integer
3. **Warehouse Capacity**: Must be positive number (square feet or cubic meters)
4. **Utilized Space**: Must be non-negative number
5. **Average Delivery Time**: Must be positive number (in days)
6. **Purchase Order Value**: Must be positive monetary value
7. **Delivery Dates**: Must be valid date format

### 2.3 Range and Boundary Constraints
1. **Order Fulfillment Rate**: Must be percentage between 0% and 100%
2. **Rejected Items Percentage**: Must be percentage between 0% and 100%
3. **Delayed Delivery Percentage**: Must be percentage between 0% and 100%
4. **Forecast Accuracy**: Must be percentage between 0% and 100%
5. **Warehouse Utilization Rate**: Must not exceed 100%
6. **Minimum Threshold**: Must be less than Maximum Threshold
7. **Utilized Space**: Cannot exceed Total Warehouse Capacity

### 2.4 Uniqueness Constraints
1. **Product Name**: Must be unique across the entire product catalog
2. **Warehouse Name**: Must be unique across all warehouse locations
3. **Supplier Name**: Must be unique across all supplier records
4. **Purchase Order ID**: Must be unique for each purchase order
5. **Product-Warehouse Combination**: Must be unique for inventory records

### 2.5 Referential Integrity Constraints
1. **Inventory-Product**: All inventory records must reference valid existing products
2. **Inventory-Warehouse**: All inventory records must reference valid existing warehouses
3. **Sales-Product**: All sales records must reference valid existing products
4. **Sales-Warehouse**: All sales records must reference valid existing warehouses
5. **Purchase Order-Supplier**: All purchase orders must reference valid existing suppliers
6. **Purchase Order-Product**: All purchase orders must reference valid existing products
7. **Delivery-Purchase Order**: All delivery records must reference valid existing purchase orders
8. **Demand Forecast-Product**: All forecast records must reference valid existing products

### 2.6 Dependency Constraints
1. **Stock Replenishment Status**: Must be calculated based on current stock level relative to minimum and maximum thresholds
2. **Reorder Point Calculation**: Depends on valid Average Daily Sales and Lead Time data
3. **Inventory Turnover Ratio**: Requires valid Cost of Goods Sold and Average Inventory values
4. **Supplier Performance Metrics**: Depend on complete delivery and quality data
5. **Demand Forecast**: Requires sufficient historical sales data for accurate predictions

## 3. Business Rules

### 3.1 Inventory Management Rules
1. **Reorder Point Calculation**: Reorder point = Average daily sales × Lead time
2. **Stock Status Determination**: 
   - Below: Current stock < Minimum threshold
   - Optimal: Minimum threshold ≤ Current stock ≤ Maximum threshold
   - Overstocked: Current stock > Maximum threshold
3. **Days of Inventory Remaining**: Current stock ÷ Average daily sales
4. **Overstock Percentage**: (Current stock - Max threshold) ÷ Max threshold × 100
5. **Safety Stock Rule**: Minimum threshold must account for lead time variability

### 3.2 Supplier Performance Rules
1. **On-Time Delivery Rate**: (Orders Delivered on Time ÷ Total Orders) × 100
2. **Defective Items Percentage**: (Defective Items ÷ Total Items Ordered) × 100
3. **Supplier Fulfillment Rate**: (Fulfilled Orders ÷ Total Orders) × 100
4. **Supplier Rating Calculation**: Weighted average of delivery performance, quality, and fulfillment rate
5. **Preferred Supplier Criteria**: On-time delivery rate > 95%, defective items < 2%

### 3.3 Sales and Inventory Correlation Rules
1. **Inventory Turnover Ratio**: Cost of Goods Sold ÷ Average Inventory
2. **Average Days to Sell**: 365 ÷ Inventory Turnover Ratio
3. **Fast-Moving Product Classification**: Turnover ratio > industry average
4. **Slow-Moving Product Classification**: Turnover ratio < 50% of industry average
5. **Stock Velocity Calculation**: Sales quantity ÷ Average inventory level

### 3.4 Warehouse Utilization Rules
1. **Warehouse Utilization Rate**: (Utilized Space ÷ Total Capacity) × 100
2. **Underutilized Space**: Total Capacity - Utilized Space
3. **Optimal Utilization Range**: 80-95% capacity utilization
4. **Space Allocation Priority**: Fast-moving products get prime warehouse locations
5. **Capacity Planning Rule**: Trigger expansion when utilization consistently exceeds 95%

### 3.5 Demand Forecasting Rules
1. **Predicted Demand Calculation**: (Historical Sales + Seasonal Factors) × Trend Factors
2. **Forecast Accuracy**: (1 - |Actual - Predicted| ÷ Actual) × 100
3. **Seasonal Adjustment**: Apply seasonal factors based on historical patterns
4. **Trend Factor Application**: Adjust for growth or decline trends
5. **Forecast Horizon**: Maximum forecast period should not exceed 12 months
6. **Minimum Historical Data**: Require at least 12 months of sales history for reliable forecasting

### 3.6 Data Processing and Transformation Rules
1. **Data Refresh Frequency**: Inventory levels updated daily, sales data updated real-time
2. **Historical Data Retention**: Maintain 3 years of historical data for trend analysis
3. **Data Aggregation Rules**: Weekly and monthly aggregations for trend reporting
4. **Exception Handling**: Flag and investigate significant variances in forecasts vs. actuals
5. **Data Quality Checks**: Automated validation of all calculations and constraint compliance

### 3.7 Reporting Logic Rules
1. **KPI Calculation Timing**: All KPIs calculated using end-of-day data
2. **Drill-Down Hierarchy**: Region → Warehouse → Product Category → Individual Product
3. **Alert Thresholds**: Automated alerts for stockouts, overstock situations, and poor supplier performance
4. **Report Access Control**: Role-based access to different levels of detail
5. **Data Comparison Periods**: Standard comparison periods (month-over-month, year-over-year)

## 4. API Cost Calculation

– Cost for this particular Api Call to LLM model: $0.18