_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Databricks Silver Layer Logical Data Model for Inventory Management System
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Silver Layer Logical Data Model
## Inventory Management System

## 1. Silver Layer Logical Model

### 1.1 Si_Products
**Description**: Silver layer table containing cleansed and standardized product information

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Product_Name | Name of the product | String |
| Category | Product category classification | String |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 1.2 Si_Suppliers
**Description**: Silver layer table containing cleansed and standardized supplier information

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Supplier_Name | Name of the supplier organization | String |
| Contact_Number | Primary contact telephone number for supplier | String |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 1.3 Si_Warehouses
**Description**: Silver layer table containing cleansed and standardized warehouse facility information

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Location | Physical address or location identifier of warehouse | String |
| Capacity | Maximum storage capacity of warehouse facility | Integer |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 1.4 Si_Inventory
**Description**: Silver layer table containing cleansed and standardized inventory records

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Quantity_Available | Current available quantity of product in inventory | Integer |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 1.5 Si_Orders
**Description**: Silver layer table containing cleansed and standardized customer order information

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Order_Date | Date when customer order was placed | Date |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 1.6 Si_Order_Details
**Description**: Silver layer table containing cleansed and standardized order line item details

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Quantity_Ordered | Quantity of specific product ordered by customer | Integer |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 1.7 Si_Shipments
**Description**: Silver layer table containing cleansed and standardized shipment tracking information

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Shipment_Date | Date when order shipment was dispatched | Date |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 1.8 Si_Returns
**Description**: Silver layer table containing cleansed and standardized product return information

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Return_Reason | Reason code or description for product return | String |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 1.9 Si_Stock_Levels
**Description**: Silver layer table containing cleansed and standardized stock level monitoring data

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Reorder_Threshold | Minimum quantity threshold that triggers reorder process | Integer |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 1.10 Si_Customers
**Description**: Silver layer table containing cleansed and standardized customer information

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Customer_Name | Full name of the customer | String |
| Email | Primary email address for customer communication | String |
| load_timestamp | Timestamp when record was loaded into Silver layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

## 2. Error Data Structure

### 2.1 Si_Data_Quality_Errors
**Description**: Silver layer table containing error data from data validation and quality checks

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| error_timestamp | Timestamp when error was detected | Timestamp |
| source_table | Name of the source table where error occurred | String |
| error_type | Type of data quality error (NULL_VALUE, INVALID_FORMAT, CONSTRAINT_VIOLATION, etc.) | String |
| error_description | Detailed description of the data quality error | String |
| error_column | Column name where error was detected | String |
| error_value | Actual value that caused the error | String |
| record_identifier | Unique identifier of the record with error | String |
| severity_level | Severity level of the error (CRITICAL, HIGH, MEDIUM, LOW) | String |
| validation_rule | Name of the validation rule that failed | String |
| error_count | Number of similar errors in the batch | Integer |
| processed_by | Process or system that detected the error | String |
| resolution_status | Status of error resolution (OPEN, IN_PROGRESS, RESOLVED, IGNORED) | String |
| resolution_timestamp | Timestamp when error was resolved | Timestamp |
| load_timestamp | Timestamp when error record was loaded | Timestamp |
| source_system | Source system where error originated | String |

## 3. Audit Data Structure

### 3.1 Si_Pipeline_Audit
**Description**: Silver layer table containing audit details from pipeline execution

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| audit_timestamp | Timestamp when audit record was created | Timestamp |
| pipeline_name | Name of the data pipeline that was executed | String |
| pipeline_run_id | Unique identifier for the pipeline execution run | String |
| source_table | Name of the source table being processed | String |
| target_table | Name of the target table being populated | String |
| records_read | Number of records read from source | Integer |
| records_processed | Number of records successfully processed | Integer |
| records_inserted | Number of records inserted into target table | Integer |
| records_updated | Number of records updated in target table | Integer |
| records_deleted | Number of records deleted from target table | Integer |
| records_rejected | Number of records rejected due to quality issues | Integer |
| processing_start_time | Timestamp when processing started | Timestamp |
| processing_end_time | Timestamp when processing completed | Timestamp |
| processing_duration | Duration of processing in seconds | Integer |
| pipeline_status | Status of pipeline execution (SUCCESS, FAILED, PARTIAL_SUCCESS) | String |
| error_message | Error message if pipeline failed | String |
| data_quality_score | Overall data quality score for the batch | Float |
| processed_by | User or system that executed the pipeline | String |
| environment | Environment where pipeline was executed (DEV, TEST, PROD) | String |
| pipeline_version | Version of the pipeline that was executed | String |
| load_timestamp | Timestamp when audit record was loaded | Timestamp |
| source_system | Source system identifier | String |

## 4. Conceptual Data Model Diagram in Tabular Form

| Primary Table | Related Table | Relationship Key Field | Relationship Type |
|---------------|---------------|------------------------|-------------------|
| Si_Products | Si_Inventory | Product_Name | One-to-Many |
| Si_Products | Si_Order_Details | Product_Name | One-to-Many |
| Si_Products | Si_Stock_Levels | Product_Name | One-to-Many |
| Si_Products | Si_Suppliers | Product_Name | Many-to-Many |
| Si_Warehouses | Si_Inventory | Location | One-to-Many |
| Si_Warehouses | Si_Stock_Levels | Location | One-to-Many |
| Si_Customers | Si_Orders | Customer_Name | One-to-Many |
| Si_Orders | Si_Order_Details | Order_Date | One-to-Many |
| Si_Orders | Si_Shipments | Order_Date | One-to-One |
| Si_Orders | Si_Returns | Order_Date | One-to-Many |
| Si_Data_Quality_Errors | Si_Products | source_table | Many-to-One |
| Si_Data_Quality_Errors | Si_Suppliers | source_table | Many-to-One |
| Si_Data_Quality_Errors | Si_Warehouses | source_table | Many-to-One |
| Si_Data_Quality_Errors | Si_Inventory | source_table | Many-to-One |
| Si_Data_Quality_Errors | Si_Orders | source_table | Many-to-One |
| Si_Data_Quality_Errors | Si_Order_Details | source_table | Many-to-One |
| Si_Data_Quality_Errors | Si_Shipments | source_table | Many-to-One |
| Si_Data_Quality_Errors | Si_Returns | source_table | Many-to-One |
| Si_Data_Quality_Errors | Si_Stock_Levels | source_table | Many-to-One |
| Si_Data_Quality_Errors | Si_Customers | source_table | Many-to-One |
| Si_Pipeline_Audit | Si_Products | target_table | Many-to-One |
| Si_Pipeline_Audit | Si_Suppliers | target_table | Many-to-One |
| Si_Pipeline_Audit | Si_Warehouses | target_table | Many-to-One |
| Si_Pipeline_Audit | Si_Inventory | target_table | Many-to-One |
| Si_Pipeline_Audit | Si_Orders | target_table | Many-to-One |
| Si_Pipeline_Audit | Si_Order_Details | target_table | Many-to-One |
| Si_Pipeline_Audit | Si_Shipments | target_table | Many-to-One |
| Si_Pipeline_Audit | Si_Returns | target_table | Many-to-One |
| Si_Pipeline_Audit | Si_Stock_Levels | target_table | Many-to-One |
| Si_Pipeline_Audit | Si_Customers | target_table | Many-to-One |

## 5. Design Rationale and Key Decisions

### 5.1 Naming Convention
- **Si_ Prefix**: All Silver layer tables use "Si_" prefix to clearly identify the data layer and distinguish from Bronze layer
- **Descriptive Names**: Table names reflect the business entity they represent with consistent naming
- **Error and Audit Tables**: Specialized tables for data quality errors and pipeline audit information

### 5.2 Data Structure Decisions
- **No Primary/Foreign Keys**: Silver layer excludes primary key and foreign key fields as per requirements, focusing on cleansed business data
- **Metadata Columns**: Standard metadata columns (load_timestamp, update_timestamp, source_system) maintained from Bronze layer
- **Data Type Standardization**: Consistent data types applied across all tables for improved data quality
- **Error Handling**: Comprehensive error data structure to capture data validation failures
- **Audit Trail**: Detailed audit structure to track pipeline execution and data lineage

### 5.3 Data Quality and Validation
- **Error Classification**: Multiple error types and severity levels for comprehensive data quality monitoring
- **Resolution Tracking**: Status tracking for error resolution and data quality improvement
- **Validation Rules**: Support for named validation rules and constraint checking

### 5.4 Pipeline Audit and Governance
- **Execution Tracking**: Complete pipeline execution metrics including record counts and processing times
- **Data Quality Scoring**: Overall data quality score calculation for batch processing
- **Environment Tracking**: Support for multiple environments (DEV, TEST, PROD)
- **Version Control**: Pipeline version tracking for change management

### 5.5 Key Assumptions
- **Data Cleansing**: Silver layer assumes data has been cleansed and standardized from Bronze layer
- **Business Rules**: Data conforms to business rules and constraints defined in the requirements
- **Referential Integrity**: Logical relationships maintained through business keys rather than technical keys
- **Data Freshness**: Regular updates from Bronze layer with appropriate timestamp tracking

### 5.6 Data Integration Approach
- **Medallion Architecture**: Follows medallion architecture principles with clear separation between Bronze and Silver layers
- **Incremental Processing**: Support for incremental data processing with timestamp-based change detection
- **Error Recovery**: Comprehensive error handling and recovery mechanisms
- **Data Lineage**: Complete audit trail for data governance and compliance requirements

## 6. Implementation Guidelines

### 6.1 Data Processing
- **Transformation Logic**: Apply business rules and data quality checks during Bronze to Silver transformation
- **Deduplication**: Remove duplicate records based on business logic
- **Standardization**: Apply consistent formatting and data type conversions
- **Validation**: Implement comprehensive data validation rules

### 6.2 Error Handling
- **Error Capture**: Capture all data quality errors in Si_Data_Quality_Errors table
- **Error Classification**: Classify errors by type and severity for prioritized resolution
- **Error Reporting**: Generate data quality reports for business users
- **Error Resolution**: Track error resolution status and timeline

### 6.3 Audit and Monitoring
- **Pipeline Monitoring**: Track all pipeline executions in Si_Pipeline_Audit table
- **Performance Metrics**: Monitor processing times and record counts
- **Data Quality Metrics**: Calculate and track data quality scores
- **Alerting**: Implement alerting for pipeline failures and data quality issues

## 7. API Cost

apiCost: 0.000375