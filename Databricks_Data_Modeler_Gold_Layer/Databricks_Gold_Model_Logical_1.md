_____________________________________________
## *Author*: Ascendion AVA+
## *Created on*: 
## *Description*: Gold Layer Logical Data Model for Inventory Management Reports with dimensional modeling, audit and error tracking capabilities
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Gold Layer Logical Data Model - Inventory Management

## 1. Gold Layer Logical Model

### 1.1 Fact Tables

#### Go_Inventory_Movement_Fact
**Description**: Central fact table capturing all inventory movements and transactions
**Table Type**: Fact
**SCD Type**: N/A (Fact table)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| inventory_movement_key | BIGINT | Surrogate key for inventory movement | Non-PII |
| product_key | BIGINT | Foreign key to product dimension | Non-PII |
| warehouse_key | BIGINT | Foreign key to warehouse dimension | Non-PII |
| supplier_key | BIGINT | Foreign key to supplier dimension | Non-PII |
| date_key | INT | Foreign key to date dimension | Non-PII |
| movement_type | STRING | Type of inventory movement (IN/OUT/TRANSFER) | Non-PII |
| quantity_moved | DECIMAL(15,2) | Quantity of items moved | Non-PII |
| unit_cost | DECIMAL(10,2) | Cost per unit at time of movement | Non-PII |
| total_value | DECIMAL(15,2) | Total value of movement | Non-PII |
| reference_number | STRING | Transaction reference number | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Sales_Fact
**Description**: Fact table for sales transactions and revenue tracking
**Table Type**: Fact
**SCD Type**: N/A (Fact table)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| sales_key | BIGINT | Surrogate key for sales transaction | Non-PII |
| product_key | BIGINT | Foreign key to product dimension | Non-PII |
| customer_key | BIGINT | Foreign key to customer dimension | Non-PII |
| warehouse_key | BIGINT | Foreign key to warehouse dimension | Non-PII |
| date_key | INT | Foreign key to date dimension | Non-PII |
| quantity_sold | DECIMAL(15,2) | Quantity of items sold | Non-PII |
| unit_price | DECIMAL(10,2) | Price per unit | Non-PII |
| total_sales_amount | DECIMAL(15,2) | Total sales amount | Non-PII |
| discount_amount | DECIMAL(10,2) | Discount applied | Non-PII |
| tax_amount | DECIMAL(10,2) | Tax amount | Non-PII |
| net_sales_amount | DECIMAL(15,2) | Net sales after discount and tax | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

### 1.2 Dimension Tables

#### Go_Product_Dimension
**Description**: Product master data with hierarchical information
**Table Type**: Dimension
**SCD Type**: Type 2 (Track historical changes)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| product_key | BIGINT | Surrogate key for product | Non-PII |
| product_code | STRING | Business key for product | Non-PII |
| product_name | STRING | Name of the product | Non-PII |
| product_description | STRING | Detailed description of product | Non-PII |
| category_name | STRING | Product category | Non-PII |
| subcategory_name | STRING | Product subcategory | Non-PII |
| brand_name | STRING | Brand name | Non-PII |
| unit_of_measure | STRING | Unit of measurement | Non-PII |
| standard_cost | DECIMAL(10,2) | Standard cost of product | Non-PII |
| list_price | DECIMAL(10,2) | List price of product | Non-PII |
| product_status | STRING | Active/Inactive status | Non-PII |
| effective_start_date | DATE | Start date of record validity | Non-PII |
| effective_end_date | DATE | End date of record validity | Non-PII |
| is_current | BOOLEAN | Flag indicating current record | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Warehouse_Dimension
**Description**: Warehouse and location master data
**Table Type**: Dimension
**SCD Type**: Type 2 (Track historical changes)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| warehouse_key | BIGINT | Surrogate key for warehouse | Non-PII |
| warehouse_code | STRING | Business key for warehouse | Non-PII |
| warehouse_name | STRING | Name of the warehouse | Non-PII |
| warehouse_type | STRING | Type of warehouse (Main/Regional/Local) | Non-PII |
| address_line1 | STRING | Address line 1 | Non-PII |
| address_line2 | STRING | Address line 2 | Non-PII |
| city | STRING | City name | Non-PII |
| state_province | STRING | State or province | Non-PII |
| postal_code | STRING | Postal code | Non-PII |
| country | STRING | Country name | Non-PII |
| warehouse_manager | STRING | Manager name | PII |
| contact_phone | STRING | Contact phone number | PII |
| contact_email | STRING | Contact email address | PII |
| warehouse_status | STRING | Active/Inactive status | Non-PII |
| effective_start_date | DATE | Start date of record validity | Non-PII |
| effective_end_date | DATE | End date of record validity | Non-PII |
| is_current | BOOLEAN | Flag indicating current record | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Supplier_Dimension
**Description**: Supplier master data with contact information
**Table Type**: Dimension
**SCD Type**: Type 2 (Track historical changes)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| supplier_key | BIGINT | Surrogate key for supplier | Non-PII |
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
| supplier_status | STRING | Active/Inactive status | Non-PII |
| effective_start_date | DATE | Start date of record validity | Non-PII |
| effective_end_date | DATE | End date of record validity | Non-PII |
| is_current | BOOLEAN | Flag indicating current record | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Customer_Dimension
**Description**: Customer master data for sales analysis
**Table Type**: Dimension
**SCD Type**: Type 2 (Track historical changes)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| customer_key | BIGINT | Surrogate key for customer | Non-PII |
| customer_code | STRING | Business key for customer | Non-PII |
| customer_name | STRING | Name of the customer | PII |
| customer_type | STRING | Type of customer (Retail/Wholesale) | Non-PII |
| contact_person | STRING | Primary contact person | PII |
| contact_phone | STRING | Contact phone number | PII |
| contact_email | STRING | Contact email address | PII |
| address_line1 | STRING | Address line 1 | PII |
| address_line2 | STRING | Address line 2 | PII |
| city | STRING | City name | Non-PII |
| state_province | STRING | State or province | Non-PII |
| postal_code | STRING | Postal code | PII |
| country | STRING | Country name | Non-PII |
| credit_limit | DECIMAL(15,2) | Credit limit for customer | Non-PII |
| customer_segment | STRING | Customer segment classification | Non-PII |
| customer_status | STRING | Active/Inactive status | Non-PII |
| effective_start_date | DATE | Start date of record validity | Non-PII |
| effective_end_date | DATE | End date of record validity | Non-PII |
| is_current | BOOLEAN | Flag indicating current record | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Date_Dimension
**Description**: Date dimension for time-based analysis
**Table Type**: Dimension
**SCD Type**: Type 1 (No historical tracking needed)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| date_key | INT | Surrogate key for date (YYYYMMDD) | Non-PII |
| full_date | DATE | Full date value | Non-PII |
| day_of_week | INT | Day of week (1-7) | Non-PII |
| day_name | STRING | Name of the day | Non-PII |
| day_of_month | INT | Day of month (1-31) | Non-PII |
| day_of_year | INT | Day of year (1-366) | Non-PII |
| week_of_year | INT | Week of year (1-53) | Non-PII |
| month_number | INT | Month number (1-12) | Non-PII |
| month_name | STRING | Name of the month | Non-PII |
| quarter_number | INT | Quarter number (1-4) | Non-PII |
| quarter_name | STRING | Quarter name (Q1, Q2, Q3, Q4) | Non-PII |
| year_number | INT | Year number | Non-PII |
| is_weekend | BOOLEAN | Flag indicating weekend | Non-PII |
| is_holiday | BOOLEAN | Flag indicating holiday | Non-PII |
| fiscal_year | INT | Fiscal year | Non-PII |
| fiscal_quarter | INT | Fiscal quarter | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

### 1.3 Code Tables

#### Go_Movement_Type_Code
**Description**: Code table for inventory movement types
**Table Type**: Code Table
**SCD Type**: Type 1 (Overwrite changes)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| movement_type_code | STRING | Code for movement type | Non-PII |
| movement_type_name | STRING | Name of movement type | Non-PII |
| movement_type_description | STRING | Description of movement type | Non-PII |
| is_active | BOOLEAN | Active status flag | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| update_date | TIMESTAMP | Date when record was last updated | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

### 1.4 Process Audit Tables

#### Go_Process_Audit
**Description**: Audit trail for pipeline execution and data processing
**Table Type**: Audit
**SCD Type**: N/A (Audit table)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| audit_key | BIGINT | Surrogate key for audit record | Non-PII |
| process_name | STRING | Name of the executed process | Non-PII |
| process_type | STRING | Type of process (ETL/ELT/Validation) | Non-PII |
| pipeline_name | STRING | Name of the pipeline | Non-PII |
| pipeline_run_id | STRING | Unique identifier for pipeline run | Non-PII |
| job_name | STRING | Name of the job | Non-PII |
| step_name | STRING | Name of the processing step | Non-PII |
| source_table | STRING | Source table name | Non-PII |
| target_table | STRING | Target table name | Non-PII |
| records_read | BIGINT | Number of records read | Non-PII |
| records_processed | BIGINT | Number of records processed | Non-PII |
| records_inserted | BIGINT | Number of records inserted | Non-PII |
| records_updated | BIGINT | Number of records updated | Non-PII |
| records_deleted | BIGINT | Number of records deleted | Non-PII |
| records_rejected | BIGINT | Number of records rejected | Non-PII |
| process_start_time | TIMESTAMP | Process start timestamp | Non-PII |
| process_end_time | TIMESTAMP | Process end timestamp | Non-PII |
| process_duration_seconds | INT | Process duration in seconds | Non-PII |
| process_status | STRING | Status (SUCCESS/FAILED/WARNING) | Non-PII |
| error_message | STRING | Error message if any | Non-PII |
| user_name | STRING | User who executed the process | PII |
| load_date | TIMESTAMP | Date when audit record was created | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

### 1.5 Error Data Tables

#### Go_Data_Validation_Error
**Description**: Error data from data validation processes
**Table Type**: Error Data
**SCD Type**: N/A (Error table)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| error_key | BIGINT | Surrogate key for error record | Non-PII |
| pipeline_run_id | STRING | Pipeline run identifier | Non-PII |
| table_name | STRING | Table where error occurred | Non-PII |
| column_name | STRING | Column where error occurred | Non-PII |
| record_identifier | STRING | Identifier of the problematic record | Non-PII |
| error_type | STRING | Type of validation error | Non-PII |
| error_category | STRING | Category of error (Data Quality/Business Rule) | Non-PII |
| error_severity | STRING | Severity level (CRITICAL/HIGH/MEDIUM/LOW) | Non-PII |
| error_description | STRING | Detailed error description | Non-PII |
| expected_value | STRING | Expected value | Non-PII |
| actual_value | STRING | Actual value that caused error | Non-PII |
| validation_rule | STRING | Validation rule that failed | Non-PII |
| error_timestamp | TIMESTAMP | When the error occurred | Non-PII |
| resolution_status | STRING | Status of error resolution | Non-PII |
| resolution_notes | STRING | Notes on error resolution | Non-PII |
| resolved_by | STRING | User who resolved the error | PII |
| resolved_timestamp | TIMESTAMP | When the error was resolved | Non-PII |
| load_date | TIMESTAMP | Date when error record was created | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

### 1.6 Aggregated Tables

#### Go_Daily_Inventory_Summary
**Description**: Daily aggregated inventory levels and movements
**Table Type**: Aggregated
**SCD Type**: N/A (Aggregated table)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| summary_key | BIGINT | Surrogate key for summary record | Non-PII |
| date_key | INT | Foreign key to date dimension | Non-PII |
| product_key | BIGINT | Foreign key to product dimension | Non-PII |
| warehouse_key | BIGINT | Foreign key to warehouse dimension | Non-PII |
| opening_balance | DECIMAL(15,2) | Opening inventory balance | Non-PII |
| total_receipts | DECIMAL(15,2) | Total receipts for the day | Non-PII |
| total_issues | DECIMAL(15,2) | Total issues for the day | Non-PII |
| total_adjustments | DECIMAL(15,2) | Total adjustments for the day | Non-PII |
| closing_balance | DECIMAL(15,2) | Closing inventory balance | Non-PII |
| average_cost | DECIMAL(10,2) | Average cost per unit | Non-PII |
| total_value | DECIMAL(15,2) | Total inventory value | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

#### Go_Monthly_Sales_Summary
**Description**: Monthly aggregated sales performance metrics
**Table Type**: Aggregated
**SCD Type**: N/A (Aggregated table)

| Column Name | Data Type | Description | PII Classification |
|-------------|-----------|-------------|--------------------|
| summary_key | BIGINT | Surrogate key for summary record | Non-PII |
| year_month | INT | Year and month (YYYYMM) | Non-PII |
| product_key | BIGINT | Foreign key to product dimension | Non-PII |
| warehouse_key | BIGINT | Foreign key to warehouse dimension | Non-PII |
| customer_key | BIGINT | Foreign key to customer dimension | Non-PII |
| total_quantity_sold | DECIMAL(15,2) | Total quantity sold | Non-PII |
| total_sales_amount | DECIMAL(15,2) | Total sales amount | Non-PII |
| total_discount_amount | DECIMAL(15,2) | Total discount amount | Non-PII |
| net_sales_amount | DECIMAL(15,2) | Net sales amount | Non-PII |
| number_of_transactions | INT | Number of sales transactions | Non-PII |
| average_transaction_value | DECIMAL(10,2) | Average transaction value | Non-PII |
| load_date | TIMESTAMP | Date when record was loaded | Non-PII |
| source_system | STRING | Source system identifier | Non-PII |

## 2. Conceptual Data Model Diagram

### 2.1 Table Relationships

| Source Table | Target Table | Relationship Key | Relationship Type |
|--------------|--------------|------------------|-------------------|
| Go_Inventory_Movement_Fact | Go_Product_Dimension | product_key | Many-to-One |
| Go_Inventory_Movement_Fact | Go_Warehouse_Dimension | warehouse_key | Many-to-One |
| Go_Inventory_Movement_Fact | Go_Supplier_Dimension | supplier_key | Many-to-One |
| Go_Inventory_Movement_Fact | Go_Date_Dimension | date_key | Many-to-One |
| Go_Sales_Fact | Go_Product_Dimension | product_key | Many-to-One |
| Go_Sales_Fact | Go_Customer_Dimension | customer_key | Many-to-One |
| Go_Sales_Fact | Go_Warehouse_Dimension | warehouse_key | Many-to-One |
| Go_Sales_Fact | Go_Date_Dimension | date_key | Many-to-One |
| Go_Daily_Inventory_Summary | Go_Product_Dimension | product_key | Many-to-One |
| Go_Daily_Inventory_Summary | Go_Warehouse_Dimension | warehouse_key | Many-to-One |
| Go_Daily_Inventory_Summary | Go_Date_Dimension | date_key | Many-to-One |
| Go_Monthly_Sales_Summary | Go_Product_Dimension | product_key | Many-to-One |
| Go_Monthly_Sales_Summary | Go_Warehouse_Dimension | warehouse_key | Many-to-One |
| Go_Monthly_Sales_Summary | Go_Customer_Dimension | customer_key | Many-to-One |
| Go_Process_Audit | Go_Data_Validation_Error | pipeline_run_id | One-to-Many |

### 2.2 Data Model Architecture

**Star Schema Design**: The model follows a star schema pattern with:
- **Central Fact Tables**: Go_Inventory_Movement_Fact and Go_Sales_Fact serve as the central transaction tables
- **Dimension Tables**: Product, Warehouse, Supplier, Customer, and Date dimensions provide descriptive context
- **Conformed Dimensions**: Date, Product, and Warehouse dimensions are shared across multiple fact tables
- **Audit Framework**: Process audit and error tracking tables provide data lineage and quality monitoring
- **Aggregated Tables**: Pre-calculated summaries for improved query performance

### 2.3 Design Rationale

1. **Dimensional Modeling**: Implemented star schema for optimal query performance and business user understanding
2. **SCD Implementation**: Type 2 SCD for dimensions requiring historical tracking, Type 1 for reference data
3. **Audit Trail**: Comprehensive audit framework for data governance and troubleshooting
4. **Error Handling**: Dedicated error tracking for data quality management
5. **Performance Optimization**: Aggregated tables for common reporting scenarios
6. **Data Governance**: PII classification for compliance with data privacy regulations

## 3. apiCost

**apiCost**: 0.0000 // Cost consumed by the API for this call (in USD)