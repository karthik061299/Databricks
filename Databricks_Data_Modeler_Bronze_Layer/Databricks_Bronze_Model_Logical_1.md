_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Databricks Bronze Layer Logical Data Model for Inventory Management System
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Bronze Layer Logical Data Model
## Inventory Management System

## 1. PII Classification

| Column Names | Reason why it is classified as PII |
|--------------|------------------------------------|
| Customer_Name | Contains personal identifiable information - customer's full name |
| Email | Contains personal contact information that can identify individuals |
| Contact_Number | Contains personal contact information that can identify suppliers/individuals |
| Supplier_Name | Contains business entity names that may include personal information for sole proprietorships |

## 2. Bronze Layer Logical Model

### 2.1 Bz_Products
**Description**: Bronze layer table containing raw product information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Product_Name | Name of the product | String |
| Category | Product category classification | String |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 2.2 Bz_Suppliers
**Description**: Bronze layer table containing raw supplier information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Supplier_Name | Name of the supplier organization | String |
| Contact_Number | Primary contact telephone number for supplier | String |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 2.3 Bz_Warehouses
**Description**: Bronze layer table containing raw warehouse facility information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Location | Physical address or location identifier of warehouse | String |
| Capacity | Maximum storage capacity of warehouse facility | Integer |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 2.4 Bz_Inventory
**Description**: Bronze layer table containing raw inventory records from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Quantity_Available | Current available quantity of product in inventory | Integer |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 2.5 Bz_Orders
**Description**: Bronze layer table containing raw customer order information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Order_Date | Date when customer order was placed | Date |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 2.6 Bz_Order_Details
**Description**: Bronze layer table containing raw order line item details from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Quantity_Ordered | Quantity of specific product ordered by customer | Integer |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 2.7 Bz_Shipments
**Description**: Bronze layer table containing raw shipment tracking information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Shipment_Date | Date when order shipment was dispatched | Date |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 2.8 Bz_Returns
**Description**: Bronze layer table containing raw product return information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Return_Reason | Reason code or description for product return | String |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 2.9 Bz_Stock_Levels
**Description**: Bronze layer table containing raw stock level monitoring data from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Reorder_Threshold | Minimum quantity threshold that triggers reorder process | Integer |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

### 2.10 Bz_Customers
**Description**: Bronze layer table containing raw customer information from source systems

| Column Name | Business Description | Data Type |
|-------------|---------------------|----------|
| Customer_Name | Full name of the customer | String |
| Email | Primary email address for customer communication | String |
| load_timestamp | Timestamp when record was loaded into Bronze layer | Timestamp |
| update_timestamp | Timestamp when record was last updated | Timestamp |
| source_system | Source system identifier | String |

## 3. Audit Table Design

### Bz_Audit_Log
**Description**: Comprehensive audit trail for all Bronze layer data processing activities

| Field Name | Business Description | Data Type |
|------------|---------------------|----------|
| record_id | Unique identifier for each audit record | String |
| source_table | Name of the source table being processed | String |
| load_timestamp | Timestamp when data loading process started | Timestamp |
| processed_by | Identifier of the process or user performing the operation | String |
| processing_time | Duration taken to complete the processing operation | Integer |
| status | Processing status indicator (SUCCESS, FAILED, IN_PROGRESS) | String |

## 4. Conceptual Data Model Diagram (Tabular Form)

| Source Table | Connected To | Key Field Relationship |
|--------------|--------------|------------------------|
| Bz_Products | Bz_Inventory | Product reference |
| Bz_Products | Bz_Order_Details | Product reference |
| Bz_Products | Bz_Stock_Levels | Product reference |
| Bz_Products | Bz_Suppliers | Product reference |
| Bz_Warehouses | Bz_Inventory | Warehouse reference |
| Bz_Warehouses | Bz_Stock_Levels | Warehouse reference |
| Bz_Customers | Bz_Orders | Customer reference |
| Bz_Orders | Bz_Order_Details | Order reference |
| Bz_Orders | Bz_Shipments | Order reference |
| Bz_Orders | Bz_Returns | Order reference |

## 5. Design Rationale and Key Decisions

### 5.1 Naming Convention
- **Bz_ Prefix**: All Bronze layer tables use "Bz_" prefix to clearly identify the data layer
- **Descriptive Names**: Table names reflect the business entity they represent

### 5.2 Data Structure Decisions
- **No Primary/Foreign Keys**: Bronze layer excludes key fields as per requirements, focusing on raw data attributes
- **Metadata Columns**: Standard metadata columns (load_timestamp, update_timestamp, source_system) added to all tables for data lineage
- **Data Type Mapping**: Logical data types used (String, Integer, Date, Timestamp) rather than physical storage types

### 5.3 PII Handling
- **Identification**: Customer names, emails, and contact numbers identified as PII
- **Compliance**: Supports GDPR and data privacy requirements
- **Audit Trail**: Comprehensive audit logging for compliance tracking

### 5.4 Assumptions
- Source systems provide clean, structured data
- All tables maintain referential integrity at the source level
- Bronze layer serves as exact replica of source data structure
- Metadata columns will be populated by the ingestion framework

## 6. API Cost

apiCost: 0.000125