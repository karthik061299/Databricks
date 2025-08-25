_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive data mapping for Dimension tables in Gold Layer with transformations, validations, and cleansing rules
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Dim Transformation Data Mapping
## Inventory Management System - Dimension Tables

## 1. Overview

This document provides a comprehensive data mapping for Dimension tables in the Gold Layer of the Inventory Management System. The mapping incorporates necessary transformations, validations, and cleansing rules at the attribute level, ensuring high data quality and consistency for business intelligence applications.

### Key Considerations:
- **Source-to-Target Mapping**: Complete field-level mapping from Silver to Gold layer
- **Data Quality**: Comprehensive validation and cleansing rules
- **Business Rules**: Implementation of hierarchical relationships and standardization
- **Performance**: Optimized transformations compatible with PySpark and Databricks
- **Governance**: Audit trail and error handling capabilities

## 2. Data Mapping for Dimension Tables

### 2.1 Go_Product_Dimension Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Gold | Go_Product_Dimension | product_id | Silver | si_products | Product_ID | NOT NULL, Unique | ROW_NUMBER() OVER (ORDER BY Product_ID, load_date) |
| Gold | Go_Product_Dimension | product_key | Silver | si_products | Product_ID | NOT NULL, Unique | DENSE_RANK() OVER (ORDER BY Product_ID) |
| Gold | Go_Product_Dimension | product_code | Silver | si_products | Product_ID | NOT NULL, Format validation | CONCAT('PRD_', LPAD(Product_ID, 6, '0')) |
| Gold | Go_Product_Dimension | product_name | Silver | si_products | Product_Name | NOT NULL, Length > 0 | CASE WHEN LENGTH(TRIM(Product_Name)) = 0 THEN 'UNKNOWN_PRODUCT' WHEN Product_Name IS NULL THEN 'UNKNOWN_PRODUCT' ELSE INITCAP(TRIM(Product_Name)) END |
| Gold | Go_Product_Dimension | product_description | Silver | si_products | Product_Name | Length validation | CONCAT('Product: ', INITCAP(TRIM(Product_Name))) |
| Gold | Go_Product_Dimension | category_name | Silver | si_products | Category | NOT NULL, Standardization | CASE WHEN Category LIKE '%Electronics%' THEN 'Electronics' WHEN Category LIKE '%Furniture%' THEN 'Furniture' WHEN Category LIKE '%Clothing%' THEN 'Apparel' ELSE 'General' END |
| Gold | Go_Product_Dimension | subcategory_name | Silver | si_products | Category | Hierarchy mapping | CASE WHEN Category LIKE '%Mobile%' THEN 'Mobile Devices' WHEN Category LIKE '%Laptop%' THEN 'Computing' ELSE 'Other' END |
| Gold | Go_Product_Dimension | brand_name | Silver | si_products | Product_Name | Brand extraction | SPLIT(Product_Name, ' ')[0] |
| Gold | Go_Product_Dimension | unit_of_measure | System Generated | N/A | N/A | Default value | 'EACH' |
| Gold | Go_Product_Dimension | standard_cost | System Generated | N/A | N/A | Numeric validation | 0.00 |
| Gold | Go_Product_Dimension | list_price | System Generated | N/A | N/A | Numeric validation | 0.00 |
| Gold | Go_Product_Dimension | product_status | Silver | si_products | update_date | Business rule validation | CASE WHEN update_date >= date_sub(current_date(), 90) THEN 'ACTIVE' WHEN update_date >= date_sub(current_date(), 365) THEN 'INACTIVE' ELSE 'DISCONTINUED' END |
| Gold | Go_Product_Dimension | effective_start_date | Silver | si_products | load_date | Date validation | COALESCE(load_date, current_timestamp()) |
| Gold | Go_Product_Dimension | effective_end_date | System Generated | N/A | N/A | SCD Type 2 | CAST('9999-12-31' as DATE) |
| Gold | Go_Product_Dimension | is_current | System Generated | N/A | N/A | SCD Type 2 | TRUE |
| Gold | Go_Product_Dimension | load_date | Silver | si_products | load_date | Timestamp validation | load_date |
| Gold | Go_Product_Dimension | update_date | Silver | si_products | update_date | Timestamp validation | update_date |
| Gold | Go_Product_Dimension | source_system | Silver | si_products | source_system | NOT NULL | source_system |

### 2.2 Go_Warehouse_Dimension Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Gold | Go_Warehouse_Dimension | warehouse_id | Silver | si_warehouses | Warehouse_ID | NOT NULL, Unique | ROW_NUMBER() OVER (ORDER BY Warehouse_ID, load_date) |
| Gold | Go_Warehouse_Dimension | warehouse_key | Silver | si_warehouses | Warehouse_ID | NOT NULL, Unique | DENSE_RANK() OVER (ORDER BY Warehouse_ID) |
| Gold | Go_Warehouse_Dimension | warehouse_code | Silver | si_warehouses | Warehouse_ID | NOT NULL, Format validation | CONCAT('WH_', LPAD(Warehouse_ID, 6, '0')) |
| Gold | Go_Warehouse_Dimension | warehouse_name | Silver | si_warehouses | Location | NOT NULL, Length > 0 | CONCAT('Warehouse ', Warehouse_ID, ' - ', SPLIT(Location, ',')[0]) |
| Gold | Go_Warehouse_Dimension | warehouse_type | Silver | si_warehouses | Capacity | Business rule validation | CASE WHEN Capacity >= 100000 THEN 'LARGE_DISTRIBUTION' WHEN Capacity >= 50000 THEN 'MEDIUM_DISTRIBUTION' WHEN Capacity >= 10000 THEN 'SMALL_DISTRIBUTION' ELSE 'LOCAL_STORAGE' END |
| Gold | Go_Warehouse_Dimension | address_line1 | Silver | si_warehouses | Location | Address parsing | SPLIT(Location, ',')[0] |
| Gold | Go_Warehouse_Dimension | address_line2 | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Warehouse_Dimension | city | Silver | si_warehouses | Location | Address parsing | CASE WHEN SIZE(SPLIT(Location, ',')) > 1 THEN SPLIT(Location, ',')[1] ELSE NULL END |
| Gold | Go_Warehouse_Dimension | state_province | Silver | si_warehouses | Location | Address parsing | CASE WHEN SIZE(SPLIT(Location, ',')) > 2 THEN SPLIT(Location, ',')[2] ELSE NULL END |
| Gold | Go_Warehouse_Dimension | postal_code | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Warehouse_Dimension | country | System Generated | N/A | N/A | Default value | 'USA' |
| Gold | Go_Warehouse_Dimension | warehouse_manager | System Generated | N/A | N/A | Default value | 'TBD' |
| Gold | Go_Warehouse_Dimension | contact_phone | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Warehouse_Dimension | contact_email | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Warehouse_Dimension | warehouse_status | System Generated | N/A | N/A | Default value | 'ACTIVE' |
| Gold | Go_Warehouse_Dimension | effective_start_date | Silver | si_warehouses | load_date | Date validation | COALESCE(load_date, current_timestamp()) |
| Gold | Go_Warehouse_Dimension | effective_end_date | System Generated | N/A | N/A | SCD Type 2 | CAST('9999-12-31' as DATE) |
| Gold | Go_Warehouse_Dimension | is_current | System Generated | N/A | N/A | SCD Type 2 | TRUE |
| Gold | Go_Warehouse_Dimension | load_date | Silver | si_warehouses | load_date | Timestamp validation | load_date |
| Gold | Go_Warehouse_Dimension | update_date | Silver | si_warehouses | update_date | Timestamp validation | update_date |
| Gold | Go_Warehouse_Dimension | source_system | Silver | si_warehouses | source_system | NOT NULL | source_system |

### 2.3 Go_Supplier_Dimension Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Gold | Go_Supplier_Dimension | supplier_id | Silver | si_suppliers | Supplier_ID | NOT NULL, Unique | ROW_NUMBER() OVER (ORDER BY Supplier_ID, load_date) |
| Gold | Go_Supplier_Dimension | supplier_key | Silver | si_suppliers | Supplier_ID | NOT NULL, Unique | DENSE_RANK() OVER (ORDER BY Supplier_ID) |
| Gold | Go_Supplier_Dimension | supplier_code | Silver | si_suppliers | Supplier_ID | NOT NULL, Format validation | CONCAT('SUP_', LPAD(Supplier_ID, 6, '0')) |
| Gold | Go_Supplier_Dimension | supplier_name | Silver | si_suppliers | Supplier_Name | NOT NULL, Length > 0, Standardization | CASE WHEN Supplier_Name IS NULL OR LENGTH(TRIM(Supplier_Name)) = 0 THEN 'UNKNOWN_SUPPLIER' ELSE INITCAP(TRIM(Supplier_Name)) END |
| Gold | Go_Supplier_Dimension | supplier_type | System Generated | N/A | N/A | Default value | 'STANDARD' |
| Gold | Go_Supplier_Dimension | contact_person | Silver | si_suppliers | Supplier_Name | Name standardization | INITCAP(TRIM(Supplier_Name)) |
| Gold | Go_Supplier_Dimension | contact_phone | Silver | si_suppliers | Contact_Number | Phone validation | CASE WHEN Contact_Number RLIKE '^[0-9+\\-\\s\\(\\)]+$' THEN Contact_Number ELSE NULL END |
| Gold | Go_Supplier_Dimension | contact_email | Silver | si_suppliers | Contact_Number | Email validation | CASE WHEN Contact_Number RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN Contact_Number ELSE NULL END |
| Gold | Go_Supplier_Dimension | address_line1 | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Supplier_Dimension | address_line2 | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Supplier_Dimension | city | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Supplier_Dimension | state_province | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Supplier_Dimension | postal_code | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Supplier_Dimension | country | System Generated | N/A | N/A | Default value | 'USA' |
| Gold | Go_Supplier_Dimension | payment_terms | System Generated | N/A | N/A | Default value | 'NET_30' |
| Gold | Go_Supplier_Dimension | supplier_rating | Silver | si_suppliers | load_date | Business rule validation | CASE WHEN load_date >= date_sub(current_date(), 30) THEN 'NEW_SUPPLIER' WHEN update_date >= date_sub(current_date(), 90) THEN 'ACTIVE' ELSE 'INACTIVE' END |
| Gold | Go_Supplier_Dimension | supplier_status | System Generated | N/A | N/A | Default value | 'ACTIVE' |
| Gold | Go_Supplier_Dimension | effective_start_date | Silver | si_suppliers | load_date | Date validation | COALESCE(load_date, current_timestamp()) |
| Gold | Go_Supplier_Dimension | effective_end_date | System Generated | N/A | N/A | SCD Type 2 | CAST('9999-12-31' as DATE) |
| Gold | Go_Supplier_Dimension | is_current | System Generated | N/A | N/A | SCD Type 2 | TRUE |
| Gold | Go_Supplier_Dimension | load_date | Silver | si_suppliers | load_date | Timestamp validation | load_date |
| Gold | Go_Supplier_Dimension | update_date | Silver | si_suppliers | update_date | Timestamp validation | update_date |
| Gold | Go_Supplier_Dimension | source_system | Silver | si_suppliers | source_system | NOT NULL | source_system |

### 2.4 Go_Customer_Dimension Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Gold | Go_Customer_Dimension | customer_id | Silver | si_customers | Customer_ID | NOT NULL, Unique | ROW_NUMBER() OVER (ORDER BY Customer_ID, load_date) |
| Gold | Go_Customer_Dimension | customer_key | Silver | si_customers | Customer_ID | NOT NULL, Unique | DENSE_RANK() OVER (ORDER BY Customer_ID) |
| Gold | Go_Customer_Dimension | customer_code | Silver | si_customers | Customer_ID | NOT NULL, Format validation | CONCAT('CUST_', LPAD(Customer_ID, 8, '0')) |
| Gold | Go_Customer_Dimension | customer_name | Silver | si_customers | Customer_Name | NOT NULL, Length > 0, Standardization | CASE WHEN Customer_Name IS NULL OR LENGTH(TRIM(Customer_Name)) = 0 THEN 'UNKNOWN_CUSTOMER' ELSE INITCAP(TRIM(Customer_Name)) END |
| Gold | Go_Customer_Dimension | customer_type | System Generated | N/A | N/A | Default value | 'RETAIL' |
| Gold | Go_Customer_Dimension | contact_person | Silver | si_customers | Customer_Name | Name standardization | INITCAP(TRIM(Customer_Name)) |
| Gold | Go_Customer_Dimension | contact_phone | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Customer_Dimension | contact_email | Silver | si_customers | Email | Email validation | CASE WHEN Email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN LOWER(TRIM(Email)) ELSE NULL END |
| Gold | Go_Customer_Dimension | address_line1 | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Customer_Dimension | address_line2 | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Customer_Dimension | city | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Customer_Dimension | state_province | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Customer_Dimension | postal_code | System Generated | N/A | N/A | Default value | NULL |
| Gold | Go_Customer_Dimension | country | System Generated | N/A | N/A | Default value | 'USA' |
| Gold | Go_Customer_Dimension | credit_limit | System Generated | N/A | N/A | Numeric validation | 10000.00 |
| Gold | Go_Customer_Dimension | customer_segment | Silver | si_customers | load_date | Business rule validation | CASE WHEN load_date >= date_sub(current_date(), 90) THEN 'NEW_CUSTOMER' WHEN update_date >= date_sub(current_date(), 365) THEN 'ACTIVE_CUSTOMER' ELSE 'INACTIVE_CUSTOMER' END |
| Gold | Go_Customer_Dimension | customer_status | System Generated | N/A | N/A | Default value | 'ACTIVE' |
| Gold | Go_Customer_Dimension | effective_start_date | Silver | si_customers | load_date | Date validation | COALESCE(load_date, current_timestamp()) |
| Gold | Go_Customer_Dimension | effective_end_date | System Generated | N/A | N/A | SCD Type 2 | CAST('9999-12-31' as DATE) |
| Gold | Go_Customer_Dimension | is_current | System Generated | N/A | N/A | SCD Type 2 | TRUE |
| Gold | Go_Customer_Dimension | load_date | Silver | si_customers | load_date | Timestamp validation | load_date |
| Gold | Go_Customer_Dimension | update_date | Silver | si_customers | update_date | Timestamp validation | update_date |
| Gold | Go_Customer_Dimension | source_system | Silver | si_customers | source_system | NOT NULL | source_system |

### 2.5 Go_Date_Dimension Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Validation Rule | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|-----------------|--------------------|
| Gold | Go_Date_Dimension | date_id | System Generated | N/A | N/A | Unique, Sequential | ROW_NUMBER() OVER (ORDER BY full_date) |
| Gold | Go_Date_Dimension | date_key | System Generated | N/A | N/A | Unique, YYYYMMDD format | CAST(date_format(full_date, 'yyyyMMdd') as INT) |
| Gold | Go_Date_Dimension | full_date | System Generated | N/A | N/A | Date validation | explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) |
| Gold | Go_Date_Dimension | day_of_week | System Generated | N/A | N/A | Range 1-7 | dayofweek(full_date) |
| Gold | Go_Date_Dimension | day_name | System Generated | N/A | N/A | Valid day name | date_format(full_date, 'EEEE') |
| Gold | Go_Date_Dimension | day_of_month | System Generated | N/A | N/A | Range 1-31 | dayofmonth(full_date) |
| Gold | Go_Date_Dimension | day_of_year | System Generated | N/A | N/A | Range 1-366 | dayofyear(full_date) |
| Gold | Go_Date_Dimension | week_of_year | System Generated | N/A | N/A | Range 1-53 | weekofyear(full_date) |
| Gold | Go_Date_Dimension | month_number | System Generated | N/A | N/A | Range 1-12 | month(full_date) |
| Gold | Go_Date_Dimension | month_name | System Generated | N/A | N/A | Valid month name | date_format(full_date, 'MMMM') |
| Gold | Go_Date_Dimension | quarter_number | System Generated | N/A | N/A | Range 1-4 | quarter(full_date) |
| Gold | Go_Date_Dimension | quarter_name | System Generated | N/A | N/A | Q1-Q4 format | CONCAT('Q', quarter(full_date)) |
| Gold | Go_Date_Dimension | year_number | System Generated | N/A | N/A | Valid year | year(full_date) |
| Gold | Go_Date_Dimension | is_weekend | System Generated | N/A | N/A | Boolean validation | CASE WHEN dayofweek(full_date) IN (1, 7) THEN TRUE ELSE FALSE END |
| Gold | Go_Date_Dimension | is_holiday | System Generated | N/A | N/A | Boolean validation | CASE WHEN full_date IN ('2024-01-01', '2024-07-04', '2024-12-25') THEN TRUE ELSE FALSE END |
| Gold | Go_Date_Dimension | fiscal_year | System Generated | N/A | N/A | Fiscal year calculation | CASE WHEN month(full_date) <= 3 THEN year(full_date) ELSE year(full_date) + 1 END |
| Gold | Go_Date_Dimension | fiscal_quarter | System Generated | N/A | N/A | Fiscal quarter calculation | CASE WHEN month(full_date) <= 3 THEN quarter(full_date) + 1 WHEN month(full_date) <= 6 THEN 1 WHEN month(full_date) <= 9 THEN 2 ELSE 3 END |
| Gold | Go_Date_Dimension | load_date | System Generated | N/A | N/A | Timestamp validation | current_timestamp() |
| Gold | Go_Date_Dimension | source_system | System Generated | N/A | N/A | Default value | 'SYSTEM_GENERATED' |

## 3. Data Quality and Validation Framework

### 3.1 Mandatory Field Validations
- **Product Dimension**: product_name, category_name cannot be null
- **Warehouse Dimension**: warehouse_name, location cannot be null  
- **Supplier Dimension**: supplier_name cannot be null
- **Customer Dimension**: customer_name cannot be null
- **Date Dimension**: full_date, date_key cannot be null

### 3.2 Data Standardization Rules
- **Name Fields**: Apply INITCAP transformation for consistent formatting
- **Email Fields**: Convert to lowercase and validate format
- **Phone Fields**: Validate numeric format with allowed special characters
- **Code Fields**: Apply consistent prefixing and padding

### 3.3 Business Rule Validations
- **Product Status**: Derive based on update_date recency
- **Warehouse Type**: Classify based on capacity thresholds
- **Supplier Rating**: Determine based on activity and load dates
- **Customer Segment**: Categorize based on registration and activity dates

### 3.4 SCD Type 2 Implementation
- **Effective Dates**: Track record validity periods
- **Current Flag**: Identify active records
- **Surrogate Keys**: Maintain historical versions

## 4. Transformation Implementation Guidelines

### 4.1 PySpark Compatibility
- All transformations use Spark SQL functions
- Compatible with Databricks runtime
- Optimized for distributed processing

### 4.2 Performance Considerations
- Use broadcast joins for dimension lookups
- Implement incremental loading strategies
- Apply Z-ORDER optimization on key columns

### 4.3 Error Handling
- Capture transformation errors in audit tables
- Implement data quality scoring
- Provide detailed error descriptions

## 5. Monitoring and Maintenance

### 5.1 Data Quality Metrics
- Track null value percentages
- Monitor standardization success rates
- Validate referential integrity

### 5.2 Performance Metrics
- Monitor transformation execution times
- Track resource utilization
- Validate optimization effectiveness

## 6. API Cost Calculation

**apiCost**: 0.0485

---

*This comprehensive data mapping ensures high-quality, standardized dimension data in the Gold layer, supporting robust business intelligence and analytics capabilities for the Inventory Management System.*