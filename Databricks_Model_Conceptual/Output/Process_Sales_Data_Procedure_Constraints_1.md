____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Data Constraints for Sales Data Processing Procedure
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Model Data Constraints for Process Sales Data Procedure

## 1. Data Expectations

### 1.1 Data Completeness Expectations
1. **Customer Entity**: All customer records must have complete Customer Name, Email Address, and Customer Type
2. **Product Entity**: All products must have Product Name, Unit Price, and Product Status defined
3. **Sales_Order Entity**: All orders must have Order Date, Order Status, and Total Amount populated
4. **Order_Item Entity**: All order items must have Quantity Ordered and Unit Price specified
5. **Payment Entity**: All payments must have Payment Date, Payment Amount, and Payment Method recorded
6. **Sales_Representative Entity**: All representatives must have Representative Name, Employee Number, and Commission Rate
7. **Territory Entity**: All territories must have Territory Name and Territory Manager assigned
8. **Product_Category Entity**: All categories must have Category Name and Category Status defined
9. **Promotion Entity**: All promotions must have Start Date, End Date, and Discount Type specified
10. **Inventory Entity**: All inventory records must have Current Stock Level and Warehouse Location

### 1.2 Data Accuracy Expectations
1. **Email Addresses**: Must follow valid email format (contains @ symbol and valid domain)
2. **Phone Numbers**: Must follow standard phone number format for the respective country
3. **Dates**: All date fields must be in valid date format and logical (e.g., End Date after Start Date)
4. **Monetary Values**: All amount fields must be non-negative and in valid currency format
5. **Percentages**: Commission rates and discount percentages must be between 0 and 100
6. **Status Fields**: Must contain only predefined valid status values
7. **Quantities**: All quantity fields must be non-negative integers
8. **Geographic Data**: Territory boundaries must be clearly defined and non-overlapping

### 1.3 Data Format Expectations
1. **Currency Fields**: Must be formatted to 2 decimal places
2. **Date Fields**: Must follow ISO 8601 format (YYYY-MM-DD)
3. **Phone Numbers**: Must follow E.164 international format
4. **Product Codes**: Must follow standardized alphanumeric format
5. **Customer IDs**: Must follow unique identifier format
6. **Order Numbers**: Must follow sequential numbering system

### 1.4 Data Consistency Expectations
1. **Customer Information**: Customer details must be consistent across all related orders and payments
2. **Product Pricing**: Unit prices must be consistent with current product catalog
3. **Territory Assignments**: Sales representatives and customers must be assigned to valid territories
4. **Category Hierarchy**: Product categories must maintain proper hierarchical relationships
5. **Order Totals**: Order total amounts must equal sum of line items plus taxes minus discounts

## 2. Constraints

### 2.1 Mandatory Field Constraints
1. **Customer Entity**:
   - Customer Name (NOT NULL)
   - Email Address (NOT NULL, UNIQUE)
   - Customer Type (NOT NULL)
   - Registration Date (NOT NULL)

2. **Product Entity**:
   - Product Name (NOT NULL)
   - Unit Price (NOT NULL, > 0)
   - Product Status (NOT NULL)
   - Product Category (NOT NULL, FOREIGN KEY)

3. **Sales_Order Entity**:
   - Customer ID (NOT NULL, FOREIGN KEY)
   - Order Date (NOT NULL)
   - Order Status (NOT NULL)
   - Total Amount (NOT NULL, >= 0)

4. **Order_Item Entity**:
   - Order ID (NOT NULL, FOREIGN KEY)
   - Product ID (NOT NULL, FOREIGN KEY)
   - Quantity Ordered (NOT NULL, > 0)
   - Unit Price (NOT NULL, > 0)

5. **Payment Entity**:
   - Order ID (NOT NULL, FOREIGN KEY)
   - Payment Date (NOT NULL)
   - Payment Amount (NOT NULL, > 0)
   - Payment Method (NOT NULL)

### 2.2 Uniqueness Constraints
1. **Customer Email Address**: Must be unique across all customer records
2. **Employee Number**: Must be unique for all sales representatives
3. **Product Code**: Must be unique for all products
4. **Order Number**: Must be unique for all sales orders
5. **Transaction Reference**: Must be unique for all payment transactions

### 2.3 Data Type Constraints
1. **Numeric Fields**: All monetary amounts must be DECIMAL(10,2)
2. **Date Fields**: Must be DATE or DATETIME data type
3. **Text Fields**: Customer names, product names must be VARCHAR with appropriate length
4. **Boolean Fields**: Status indicators must be BOOLEAN or predefined ENUM values
5. **Integer Fields**: Quantities and counts must be INTEGER data type

### 2.4 Range Constraints
1. **Commission Rate**: Must be between 0.00 and 1.00 (0% to 100%)
2. **Discount Percentage**: Must be between 0.00 and 1.00 (0% to 100%)
3. **Credit Limit**: Must be between 0 and predefined maximum limit
4. **Order Quantity**: Must be greater than 0
5. **Stock Levels**: Cannot be negative

### 2.5 Referential Integrity Constraints
1. **Customer-Order Relationship**: All orders must reference valid customer
2. **Order-OrderItem Relationship**: All order items must reference valid order
3. **Product-OrderItem Relationship**: All order items must reference valid product
4. **Order-Payment Relationship**: All payments must reference valid order
5. **Representative-Customer Relationship**: Customer assignments must reference valid representative
6. **Territory-Representative Relationship**: Representative assignments must reference valid territory
7. **Category-Product Relationship**: All products must reference valid category

### 2.6 Dependency Constraints
1. **Order Status Dependencies**: Order status changes must follow defined workflow
2. **Payment Dependencies**: Payments cannot exceed order total amount
3. **Inventory Dependencies**: Order quantities cannot exceed available inventory
4. **Promotion Dependencies**: Promotion applications must meet minimum order requirements
5. **Territory Dependencies**: Customer and representative assignments must be within same territory

## 3. Business Rules

### 3.1 Operational Rules
1. **Order Processing Rules**:
   - Orders must be processed in chronological order
   - High-priority orders must be processed within 24 hours
   - Orders exceeding customer credit limit require approval
   - Partial shipments are allowed for backordered items

2. **Customer Management Rules**:
   - New customers require credit verification
   - Inactive customers cannot place new orders
   - Customer information updates require verification
   - Corporate customers have different pricing tiers

3. **Inventory Management Rules**:
   - Stock levels below minimum threshold trigger reorder
   - Reserved inventory cannot be allocated to new orders
   - Inventory adjustments require authorization
   - Seasonal products have special handling requirements

4. **Payment Processing Rules**:
   - Payments must be processed within 48 hours of receipt
   - Failed payments require customer notification
   - Refunds require manager approval
   - Currency conversions use daily exchange rates

### 3.2 Reporting Logic Rules
1. **Sales Revenue Calculation**:
   - Include only completed orders in revenue reports
   - Apply currency conversion for multi-currency transactions
   - Exclude cancelled orders from sales metrics
   - Include tax amounts in total revenue calculations

2. **Commission Calculation Rules**:
   - Commission calculated on net sales amount (after discounts)
   - Different commission rates for different product categories
   - Territory bonuses applied based on achievement levels
   - Commission payments processed monthly

3. **Performance Metrics Rules**:
   - KPIs calculated based on completed transactions only
   - Customer retention measured over 12-month periods
   - Territory performance compared against targets
   - Product performance measured by revenue and margin

### 3.3 Data Transformation Guidelines
1. **Data Aggregation Rules**:
   - Sales data aggregated by day, month, quarter, and year
   - Customer data aggregated by territory and segment
   - Product data aggregated by category and performance
   - Payment data aggregated by method and status

2. **Data Cleansing Rules**:
   - Remove duplicate customer records based on email and phone
   - Standardize address formats using postal service guidelines
   - Normalize product names and descriptions
   - Validate and correct phone number formats

3. **Data Integration Rules**:
   - Customer data synchronized across all systems daily
   - Product catalog updates propagated to all channels
   - Inventory levels updated in real-time
   - Payment status synchronized with financial systems

### 3.4 Compliance and Governance Rules
1. **Data Privacy Rules**:
   - Customer personal information access restricted by role
   - Payment information encrypted in storage and transit
   - Data retention policies enforced automatically
   - Customer consent tracked for marketing communications

2. **Audit and Tracking Rules**:
   - All data changes logged with user and timestamp
   - Financial transactions require dual approval
   - System access logged and monitored
   - Data export activities tracked and approved

3. **Data Quality Rules**:
   - Data quality scores calculated and monitored
   - Invalid data flagged for review and correction
   - Data completeness measured and reported
   - Data accuracy validated through automated checks

## 4. API Cost Calculation

**Cost for this particular API Call to LLM model: $0.18**