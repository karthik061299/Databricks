_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Enhanced Databricks Bronze Layer Logical Data Model for Inventory Management System
## *Version*: 2
## *Updated on*: 
_____________________________________________

# Databricks Bronze Layer Logical Data Model
## Inventory Management System - Enhanced Version

## 1. PII Classification

| Column Names | Reason why it is classified as PII |
|--------------|------------------------------------|  
| Customer_Name | Contains personal identifiable information - customer's full name which can directly identify individuals |
| Email | Contains personal contact information (email addresses) that can uniquely identify and contact individuals |
| Contact_Number | Contains personal contact information (phone numbers) that can identify suppliers/individuals and enable direct contact |
| Supplier_Name | Contains business entity names that may include personal information for sole proprietorships or individual contractors |

## 2. Bronze Layer Logical Model

### 2.1 Bz_Products
**Description**: Bronze layer table containing raw product information from source systems with complete product catalog data

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Product_Name | Name of the product as provided by source system | String |
| Category | Product category classification for inventory organization | String |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

### 2.2 Bz_Suppliers
**Description**: Bronze layer table containing raw supplier information from source systems including contact details

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Supplier_Name | Name of the supplier organization or individual | String |
| Contact_Number | Primary contact telephone number for supplier communication | String |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

### 2.3 Bz_Warehouses
**Description**: Bronze layer table containing raw warehouse facility information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Location | Physical address or location identifier of warehouse facility | String |
| Capacity | Maximum storage capacity of warehouse facility in units | Integer |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

### 2.4 Bz_Inventory
**Description**: Bronze layer table containing raw inventory records from source systems with current stock information

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Quantity_Available | Current available quantity of product in inventory | Integer |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

### 2.5 Bz_Orders
**Description**: Bronze layer table containing raw customer order information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Order_Date | Date when customer order was placed in source system | Date |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

### 2.6 Bz_Order_Details
**Description**: Bronze layer table containing raw order line item details from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Quantity_Ordered | Quantity of specific product ordered by customer | Integer |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

### 2.7 Bz_Shipments
**Description**: Bronze layer table containing raw shipment tracking information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Shipment_Date | Date when order shipment was dispatched from warehouse | Date |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

### 2.8 Bz_Returns
**Description**: Bronze layer table containing raw product return information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Return_Reason | Reason code or description for product return from customer | String |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

### 2.9 Bz_Stock_Levels
**Description**: Bronze layer table containing raw stock level monitoring data from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Reorder_Threshold | Minimum quantity threshold that triggers reorder process | Integer |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

### 2.10 Bz_Customers
**Description**: Bronze layer table containing raw customer information from source systems with PII data

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Customer_Name | Full name of the customer (PII - requires encryption) | String |
| Email | Primary email address for customer communication (PII - requires encryption) | String |
| load_timestamp | Timestamp when record was initially loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated or modified | Timestamp |
| source_system | Source system identifier indicating origin of data | String |
| record_status | Status of the record (ACTIVE, INACTIVE, DELETED) | String |
| data_quality_score | Quality score of the source data (0-100) | Integer |

## 3. Audit Table Design

### Bz_Audit_Log
**Description**: Comprehensive audit trail for all Bronze layer data processing activities with enhanced tracking

| Field Name | Business Description | Data Type |
|------------|---------------------|----------|
| record_id | Unique identifier for each audit record (UUID format) | String |
| source_table | Name of the source table being processed | String |
| target_table | Name of the Bronze layer table being updated | String |
| operation_type | Type of operation performed (INSERT, UPDATE, DELETE, MERGE) | String |
| load_timestamp | Timestamp when data loading process started | Timestamp |
| processed_by | Identifier of the process, job, or user performing the operation | String |
| processing_time | Duration taken to complete the processing operation in seconds | Integer |
| records_processed | Number of records processed in the operation | Integer |
| records_failed | Number of records that failed processing | Integer |
| status | Processing status indicator (SUCCESS, FAILED, IN_PROGRESS, PARTIAL) | String |
| error_message | Detailed error message if processing failed | String |
| data_lineage_id | Unique identifier for tracking data lineage across layers | String |

## 4. Conceptual Data Model Diagram (Tabular Form)

| Source Table | Connected To | Key Field Relationship | Relationship Type |
|--------------|--------------|------------------------|-------------------|
| Bz_Products | Bz_Inventory | Product reference | One-to-Many |
| Bz_Products | Bz_Order_Details | Product reference | One-to-Many |
| Bz_Products | Bz_Stock_Levels | Product reference | One-to-Many |
| Bz_Products | Bz_Suppliers | Product reference | Many-to-Many |
| Bz_Warehouses | Bz_Inventory | Warehouse reference | One-to-Many |
| Bz_Warehouses | Bz_Stock_Levels | Warehouse reference | One-to-Many |
| Bz_Customers | Bz_Orders | Customer reference | One-to-Many |
| Bz_Orders | Bz_Order_Details | Order reference | One-to-Many |
| Bz_Orders | Bz_Shipments | Order reference | One-to-One |
| Bz_Orders | Bz_Returns | Order reference | One-to-Many |

## 5. Enhanced Design Rationale and Key Decisions

### 5.1 Naming Convention
- **Bz_ Prefix**: All Bronze layer tables use "Bz_" prefix to clearly identify the data layer and distinguish from Silver/Gold layers
- **Descriptive Names**: Table names reflect the business entity they represent for better understanding
- **Consistent Formatting**: Underscore separation for multi-word table names

### 5.2 Enhanced Data Structure Decisions
- **No Primary/Foreign Keys**: Bronze layer excludes key fields as per requirements, focusing on raw data attributes
- **Enhanced Metadata Columns**: Added record_status and data_quality_score for better data governance
- **Data Type Mapping**: Logical data types used (String, Integer, Date, Timestamp) rather than physical storage types
- **Quality Tracking**: Data quality scores enable monitoring of source data integrity

### 5.3 Enhanced PII Handling
- **Comprehensive Identification**: Customer names, emails, and contact numbers identified as PII with detailed reasoning
- **Compliance Ready**: Supports GDPR, CCPA, and other data privacy requirements
- **Encryption Recommendations**: PII fields marked for encryption in implementation
- **Audit Trail**: Enhanced audit logging for compliance tracking and data lineage

### 5.4 Enhanced Audit Capabilities
- **Comprehensive Tracking**: Enhanced audit table with operation types, record counts, and error handling
- **Data Lineage**: Unique identifiers for tracking data flow across medallion architecture layers
- **Performance Monitoring**: Processing time and record count tracking for optimization
- **Error Handling**: Detailed error logging for troubleshooting and data quality improvement

### 5.5 Assumptions and Constraints
- **Source Data Quality**: Source systems provide structured data with consistent formats
- **Referential Integrity**: Source systems maintain referential integrity at the application level
- **Bronze Layer Purpose**: Serves as exact replica of source data structure with minimal transformation
- **Metadata Population**: Metadata columns will be populated by the ingestion framework automatically
- **Scalability**: Design supports high-volume data ingestion and processing
- **Delta Lake Format**: Tables will be implemented using Delta Lake format for ACID transactions

### 5.6 Implementation Considerations
- **Partitioning Strategy**: Tables should be partitioned by load_timestamp for optimal query performance
- **Retention Policy**: Implement data retention policies based on business requirements
- **Monitoring**: Set up monitoring for data quality scores and processing failures
- **Security**: Implement column-level security for PII fields

## 6. API Cost

apiCost: 0.000187