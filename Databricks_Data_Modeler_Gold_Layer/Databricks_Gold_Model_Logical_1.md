_____________________________________________
## *Author*: Ascendion AVA+
## *Created on*: 
## *Description*: Gold Layer Logical Data Model for Inventory Management Reports in Medallion Architecture
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Gold Layer Logical Data Model - Inventory Management

## 1. Gold Layer Logical Model

### 1.1 Fact Tables

#### Go_Inventory_Movement_Fact
**Description**: Central fact table capturing all inventory movements and transactions
**Table Type**: Fact
**SCD Type**: N/A

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| inventory_movement_key | STRING | Business key for inventory movement | Non-PII |
| product_key | STRING | Foreign key to product dimension | Non-PII |
| warehouse_key | STRING | Foreign key to warehouse dimension | Non-PII |
| supplier_key | STRING | Foreign key to supplier dimension | Non-PII |
| movement_date_key | STRING | Foreign key to date dimension | Non-PII |
| movement_type | STRING | Type of inventory movement (IN/OUT/TRANSFER) | Non-PII |
| quantity_moved | DECIMAL(15,2) | Quantity of items moved | Non-PII |
| unit_cost | DECIMAL(10,2) | Cost per unit at time of movement | Non-PII |
| total_cost | DECIMAL(15,2) | Total cost of movement | Non-PII |
| reference_number | STRING | Reference document number | Non-PII |
| movement_reason | STRING | Reason for inventory movement | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Sales_Fact
**Description**: Fact table for sales transactions
**Table Type**: Fact
**SCD Type**: N/A

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| sales_transaction_key | STRING | Business key for sales transaction | Non-PII |
| product_key | STRING | Foreign key to product dimension | Non-PII |
| customer_key | STRING | Foreign key to customer dimension | Non-PII |
| warehouse_key | STRING | Foreign key to warehouse dimension | Non-PII |
| sales_date_key | STRING | Foreign key to date dimension | Non-PII |
| quantity_sold | DECIMAL(15,2) | Quantity of items sold | Non-PII |
| unit_price | DECIMAL(10,2) | Price per unit | Non-PII |
| total_sales_amount | DECIMAL(15,2) | Total sales amount | Non-PII |
| discount_amount | DECIMAL(10,2) | Discount applied | Non-PII |
| tax_amount | DECIMAL(10,2) | Tax amount | Non-PII |
| net_sales_amount | DECIMAL(15,2) | Net sales amount after discount and tax | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

### 1.2 Dimension Tables

#### Go_Product_Dimension
**Description**: Product master data with historical tracking
**Table Type**: Dimension
**SCD Type**: Type 2

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| product_key | STRING | Surrogate key for product | Non-PII |
| product_code | STRING | Business key for product | Non-PII |
| product_name | STRING | Name of the product | Non-PII |
| product_description | STRING | Detailed description of product | Non-PII |
| category_name | STRING | Product category | Non-PII |
| subcategory_name | STRING | Product subcategory | Non-PII |
| brand_name | STRING | Brand of the product | Non-PII |
| unit_of_measure | STRING | Unit of measurement | Non-PII |
| standard_cost | DECIMAL(10,2) | Standard cost of product | Non-PII |
| list_price | DECIMAL(10,2) | List price of product | Non-PII |
| product_status | STRING | Status of product (Active/Inactive) | Non-PII |
| effective_start_date | DATE | Start date of record validity | Non-PII |
| effective_end_date | DATE | End date of record validity | Non-PII |
| is_current_record | BOOLEAN | Flag indicating current record | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Warehouse_Dimension
**Description**: Warehouse and location master data
**Table Type**: Dimension
**SCD Type**: Type 2

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| warehouse_key | STRING | Surrogate key for warehouse | Non-PII |
| warehouse_code | STRING | Business key for warehouse | Non-PII |
| warehouse_name | STRING | Name of the warehouse | Non-PII |
| warehouse_type | STRING | Type of warehouse | Non-PII |
| address_line1 | STRING | Address line 1 | Non-PII |
| address_line2 | STRING | Address line 2 | Non-PII |
| city | STRING | City name | Non-PII |
| state_province | STRING | State or province | Non-PII |
| postal_code | STRING | Postal code | Non-PII |
| country | STRING | Country name | Non-PII |
| warehouse_manager | STRING | Manager name | PII |
| contact_phone | STRING | Contact phone number | PII |
| contact_email | STRING | Contact email address | PII |
| warehouse_status | STRING | Status of warehouse | Non-PII |
| effective_start_date | DATE | Start date of record validity | Non-PII |
| effective_end_date | DATE | End date of record validity | Non-PII |
| is_current_record | BOOLEAN | Flag indicating current record | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Supplier_Dimension
**Description**: Supplier master data with historical tracking
**Table Type**: Dimension
**SCD Type**: Type 2

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| supplier_key | STRING | Surrogate key for supplier | Non-PII |
| supplier_code | STRING | Business key for supplier | Non-PII |
| supplier_name | STRING | Name of the supplier | Non-PII |
| supplier_type | STRING | Type of supplier | Non-PII |
| contact_person | STRING | Primary contact person | PII |
| contact_phone | STRING | Contact phone number | PII |
| contact_email | STRING | Contact email address | PII |
| address_line1 | STRING | Address line 1 | Non-PII |
| address_line2 | STRING | Address line 2 | Non-PII |
| city | STRING | City name | Non-PII |
| state_province | STRING | State or province | Non-PII |
| postal_code | STRING | Postal code | Non-PII |
| country | STRING | Country name | Non-PII |
| payment_terms | STRING | Payment terms | Non-PII |
| supplier_rating | STRING | Supplier performance rating | Non-PII |
| supplier_status | STRING | Status of supplier | Non-PII |
| effective_start_date | DATE | Start date of record validity | Non-PII |
| effective_end_date | DATE | End date of record validity | Non-PII |
| is_current_record | BOOLEAN | Flag indicating current record | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Customer_Dimension
**Description**: Customer master data with historical tracking
**Table Type**: Dimension
**SCD Type**: Type 2

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| customer_key | STRING | Surrogate key for customer | Non-PII |
| customer_code | STRING | Business key for customer | Non-PII |
| customer_name | STRING | Name of the customer | PII |
| customer_type | STRING | Type of customer (Individual/Corporate) | Non-PII |
| contact_person | STRING | Primary contact person | PII |
| contact_phone | STRING | Contact phone number | PII |
| contact_email | STRING | Contact email address | PII |
| address_line1 | STRING | Address line 1 | PII |
| address_line2 | STRING | Address line 2 | PII |
| city | STRING | City name | Non-PII |
| state_province | STRING | State or province | Non-PII |
| postal_code | STRING | Postal code | PII |
| country | STRING | Country name | Non-PII |
| customer_segment | STRING | Customer segment classification | Non-PII |
| credit_limit | DECIMAL(15,2) | Credit limit for customer | Non-PII |
| customer_status | STRING | Status of customer | Non-PII |
| effective_start_date | DATE | Start date of record validity | Non-PII |
| effective_end_date | DATE | End date of record validity | Non-PII |
| is_current_record | BOOLEAN | Flag indicating current record | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Date_Dimension
**Description**: Date dimension for time-based analysis
**Table Type**: Dimension
**SCD Type**: Type 1

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| date_key | STRING | Surrogate key for date (YYYYMMDD) | Non-PII |
| full_date | DATE | Full date value | Non-PII |
| day_of_week | INTEGER | Day of week (1-7) | Non-PII |
| day_name | STRING | Name of the day | Non-PII |
| day_of_month | INTEGER | Day of month (1-31) | Non-PII |
| day_of_year | INTEGER | Day of year (1-366) | Non-PII |
| week_of_year | INTEGER | Week of year (1-53) | Non-PII |
| month_number | INTEGER | Month number (1-12) | Non-PII |
| month_name | STRING | Name of the month | Non-PII |
| quarter_number | INTEGER | Quarter number (1-4) | Non-PII |
| quarter_name | STRING | Quarter name (Q1, Q2, Q3, Q4) | Non-PII |
| year_number | INTEGER | Year number | Non-PII |
| is_weekend | BOOLEAN | Flag indicating weekend | Non-PII |
| is_holiday | BOOLEAN | Flag indicating holiday | Non-PII |
| fiscal_year | INTEGER | Fiscal year | Non-PII |
| fiscal_quarter | INTEGER | Fiscal quarter | Non-PII |
| fiscal_month | INTEGER | Fiscal month | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

### 1.3 Process Audit Tables

#### Go_Pipeline_Audit
**Description**: Audit trail for pipeline execution details
**Table Type**: Audit
**SCD Type**: N/A

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| audit_key | STRING | Unique identifier for audit record | Non-PII |
| pipeline_name | STRING | Name of the executed pipeline | Non-PII |
| pipeline_run_id | STRING | Unique identifier for pipeline run | Non-PII |
| execution_start_time | TIMESTAMP | Pipeline execution start time | Non-PII |
| execution_end_time | TIMESTAMP | Pipeline execution end time | Non-PII |
| execution_duration_seconds | INTEGER | Duration of execution in seconds | Non-PII |
| pipeline_status | STRING | Status of pipeline execution (SUCCESS/FAILED/RUNNING) | Non-PII |
| source_table_name | STRING | Source table processed | Non-PII |
| target_table_name | STRING | Target table updated | Non-PII |
| records_read | BIGINT | Number of records read from source | Non-PII |
| records_written | BIGINT | Number of records written to target | Non-PII |
| records_updated | BIGINT | Number of records updated | Non-PII |
| records_deleted | BIGINT | Number of records deleted | Non-PII |
| error_message | STRING | Error message if pipeline failed | Non-PII |
| execution_environment | STRING | Environment where pipeline executed | Non-PII |
| executed_by | STRING | User or system that executed pipeline | Non-PII |
| load_date | TIMESTAMP | Date when audit record was created | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

### 1.4 Error Data Tables

#### Go_Data_Quality_Errors
**Description**: Data validation errors and quality issues
**Table Type**: Error Data
**SCD Type**: N/A

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| error_key | STRING | Unique identifier for error record | Non-PII |
| pipeline_run_id | STRING | Pipeline run identifier where error occurred | Non-PII |
| table_name | STRING | Table name where error was found | Non-PII |
| column_name | STRING | Column name with data quality issue | Non-PII |
| error_type | STRING | Type of data quality error | Non-PII |
| error_category | STRING | Category of error (VALIDATION/TRANSFORMATION/BUSINESS_RULE) | Non-PII |
| error_severity | STRING | Severity level (CRITICAL/HIGH/MEDIUM/LOW) | Non-PII |
| error_description | STRING | Detailed description of the error | Non-PII |
| error_rule | STRING | Business rule or validation rule that failed | Non-PII |
| record_identifier | STRING | Identifier of the record with error | Non-PII |
| invalid_value | STRING | The invalid value that caused the error | Non-PII |
| expected_value | STRING | Expected value or format | Non-PII |
| error_count | INTEGER | Number of records with same error | Non-PII |
| error_timestamp | TIMESTAMP | Timestamp when error was detected | Non-PII |
| resolution_status | STRING | Status of error resolution | Non-PII |
| resolution_notes | STRING | Notes on error resolution | Non-PII |
| resolved_by | STRING | User who resolved the error | Non-PII |
| resolved_timestamp | TIMESTAMP | Timestamp when error was resolved | Non-PII |
| load_date | TIMESTAMP | Date when error record was created | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

### 1.5 Aggregated Tables

#### Go_Inventory_Summary_Daily
**Description**: Daily aggregated inventory levels and movements
**Table Type**: Aggregated
**SCD Type**: N/A

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| summary_date_key | STRING | Date key for summary | Non-PII |
| product_key | STRING | Product identifier | Non-PII |
| warehouse_key | STRING | Warehouse identifier | Non-PII |
| opening_balance | DECIMAL(15,2) | Opening inventory balance | Non-PII |
| total_receipts | DECIMAL(15,2) | Total inventory received | Non-PII |
| total_issues | DECIMAL(15,2) | Total inventory issued | Non-PII |
| total_adjustments | DECIMAL(15,2) | Total inventory adjustments | Non-PII |
| closing_balance | DECIMAL(15,2) | Closing inventory balance | Non-PII |
| average_cost | DECIMAL(10,2) | Average cost per unit | Non-PII |
| total_value | DECIMAL(15,2) | Total inventory value | Non-PII |
| reorder_point | DECIMAL(15,2) | Reorder point for product | Non-PII |
| stock_status | STRING | Stock status (IN_STOCK/LOW_STOCK/OUT_OF_STOCK) | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Sales_Summary_Monthly
**Description**: Monthly aggregated sales performance
**Table Type**: Aggregated
**SCD Type**: N/A

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|-------------------|
| summary_month_key | STRING | Month key for summary (YYYYMM) | Non-PII |
| product_key | STRING | Product identifier | Non-PII |
| customer_key | STRING | Customer identifier | Non-PII |
| warehouse_key | STRING | Warehouse identifier | Non-PII |
| total_quantity_sold | DECIMAL(15,2) | Total quantity sold | Non-PII |
| total_sales_amount | DECIMAL(15,2) | Total sales amount | Non-PII |
| total_discount_amount | DECIMAL(15,2) | Total discount given | Non-PII |
| total_tax_amount | DECIMAL(15,2) | Total tax collected | Non-PII |
| net_sales_amount | DECIMAL(15,2) | Net sales amount | Non-PII |
| average_unit_price | DECIMAL(10,2) | Average selling price per unit | Non-PII |
| number_of_transactions | INTEGER | Number of sales transactions | Non-PII |
| unique_customers | INTEGER | Number of unique customers | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

## 2. Conceptual Data Model Diagram (Tabular Relationships)

| Source Table | Target Table | Relationship Key | Relationship Type |
|--------------|--------------|------------------|-------------------|
| Go_Inventory_Movement_Fact | Go_Product_Dimension | product_key | Many-to-One |
| Go_Inventory_Movement_Fact | Go_Warehouse_Dimension | warehouse_key | Many-to-One |
| Go_Inventory_Movement_Fact | Go_Supplier_Dimension | supplier_key | Many-to-One |
| Go_Inventory_Movement_Fact | Go_Date_Dimension | movement_date_key | Many-to-One |
| Go_Sales_Fact | Go_Product_Dimension | product_key | Many-to-One |
| Go_Sales_Fact | Go_Customer_Dimension | customer_key | Many-to-One |
| Go_Sales_Fact | Go_Warehouse_Dimension | warehouse_key | Many-to-One |
| Go_Sales_Fact | Go_Date_Dimension | sales_date_key | Many-to-One |
| Go_Inventory_Summary_Daily | Go_Product_Dimension | product_key | Many-to-One |
| Go_Inventory_Summary_Daily | Go_Warehouse_Dimension | warehouse_key | Many-to-One |
| Go_Inventory_Summary_Daily | Go_Date_Dimension | summary_date_key | Many-to-One |
| Go_Sales_Summary_Monthly | Go_Product_Dimension | product_key | Many-to-One |
| Go_Sales_Summary_Monthly | Go_Customer_Dimension | customer_key | Many-to-One |
| Go_Sales_Summary_Monthly | Go_Warehouse_Dimension | warehouse_key | Many-to-One |
| Go_Pipeline_Audit | Go_Data_Quality_Errors | pipeline_run_id | One-to-Many |

## 3. Design Rationale and Assumptions

### 3.1 Key Design Decisions

1. **Dimensional Modeling**: Implemented star schema design with fact tables at the center and dimension tables providing context
2. **SCD Implementation**: 
   - Type 2 for Product, Warehouse, Supplier, and Customer dimensions to track historical changes
   - Type 1 for Date dimension as dates don't change
3. **Naming Convention**: All Gold layer tables prefixed with 'Go_' for easy identification
4. **Audit and Error Tracking**: Dedicated tables for pipeline execution audit and data quality error tracking
5. **Aggregation Strategy**: Pre-calculated daily and monthly summaries for improved query performance

### 3.2 Assumptions Made

1. **Business Keys**: Assumed standard business keys exist for all master data entities
2. **Data Sources**: Assumed data flows from Bronze to Silver to Gold layers in medallion architecture
3. **Historical Tracking**: Assumed business requirement for tracking changes in master data
4. **Reporting Requirements**: Assumed need for both detailed transactional and summarized reporting
5. **Data Quality**: Assumed comprehensive data validation rules exist in the pipeline

### 3.3 Performance Considerations

1. **Partitioning**: Tables should be partitioned by date columns for optimal performance
2. **Indexing**: Appropriate indexes on foreign key columns and frequently queried fields
3. **Aggregation**: Pre-aggregated tables reduce query complexity and improve response times
4. **Data Retention**: Implement appropriate data retention policies for audit and error tables

## apiCost: 0.0234