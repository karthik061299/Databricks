_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive transformation rules for Dimension tables in Gold layer of Inventory Management System
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Databricks Gold Dim Transformation Recommender
## Inventory Management System - Dimension Tables

## 1. Overview

This document provides comprehensive transformation rules specifically for Dimension tables in the Gold layer of the Inventory Management System. The transformations ensure data integrity, standardization, and consistency for analytical reporting requirements.

## 2. Source Analysis

### 2.1 Silver Layer Dimension Sources
- **si_products** → Go_Product_Dimension
- **si_warehouses** → Go_Warehouse_Dimension  
- **si_suppliers** → Go_Supplier_Dimension
- **si_customers** → Go_Customer_Dimension
- **System Generated** → Go_Date_Dimension

### 2.2 Key Business Requirements
- Maintain product hierarchies and categorization
- Standardize warehouse location and capacity information
- Ensure supplier performance tracking capabilities
- Support customer segmentation and analysis
- Provide comprehensive date attributes for time-based analysis

## 3. Transformation Rules for Dimension Tables

### 3.1 Go_Product_Dimension Transformations

#### Rule 1: Product Key Generation
- **Description**: Generate surrogate keys for product dimension
- **Rationale**: Ensure unique identification and support SCD Type 2 implementation
- **SQL Example**:
```sql
SELECT 
    ROW_NUMBER() OVER (ORDER BY Product_ID, load_date) as product_id,
    DENSE_RANK() OVER (ORDER BY Product_ID) as product_key,
    Product_ID as product_code,
    Product_Name as product_name
FROM silver.si_products
```

#### Rule 2: Product Name Standardization
- **Description**: Standardize product names with proper case formatting
- **Rationale**: Ensure consistent naming conventions across reports as per data constraints
- **SQL Example**:
```sql
SELECT 
    INITCAP(TRIM(Product_Name)) as product_name,
    CASE 
        WHEN LENGTH(TRIM(Product_Name)) = 0 THEN 'UNKNOWN_PRODUCT'
        WHEN Product_Name IS NULL THEN 'UNKNOWN_PRODUCT'
        ELSE INITCAP(TRIM(Product_Name))
    END as standardized_product_name
FROM silver.si_products
```

#### Rule 3: Category Hierarchy Mapping
- **Description**: Create category and subcategory hierarchies from single category field
- **Rationale**: Support hierarchical analysis and drill-down reporting capabilities
- **SQL Example**:
```sql
SELECT 
    Category,
    CASE 
        WHEN Category LIKE '%Electronics%' THEN 'Electronics'
        WHEN Category LIKE '%Furniture%' THEN 'Furniture'
        WHEN Category LIKE '%Clothing%' THEN 'Apparel'
        ELSE 'General'
    END as category_name,
    CASE 
        WHEN Category LIKE '%Mobile%' THEN 'Mobile Devices'
        WHEN Category LIKE '%Laptop%' THEN 'Computing'
        ELSE 'Other'
    END as subcategory_name
FROM silver.si_products
```

#### Rule 4: Product Status Derivation
- **Description**: Derive product status based on business rules
- **Rationale**: Support inventory management and product lifecycle tracking
- **SQL Example**:
```sql
SELECT 
    Product_ID,
    CASE 
        WHEN update_date >= date_sub(current_date(), 90) THEN 'ACTIVE'
        WHEN update_date >= date_sub(current_date(), 365) THEN 'INACTIVE'
        ELSE 'DISCONTINUED'
    END as product_status
FROM silver.si_products
```

#### Rule 5: SCD Type 2 Implementation
- **Description**: Implement Slowly Changing Dimension Type 2 for product changes
- **Rationale**: Maintain historical product information for trend analysis
- **SQL Example**:
```sql
SELECT 
    *,
    COALESCE(load_date, current_timestamp()) as effective_start_date,
    CAST('9999-12-31' as DATE) as effective_end_date,
    TRUE as is_current
FROM transformed_products
WHERE NOT EXISTS (
    SELECT 1 FROM Go_Product_Dimension gp 
    WHERE gp.product_code = transformed_products.product_code 
    AND gp.is_current = TRUE
)
```

### 3.2 Go_Warehouse_Dimension Transformations

#### Rule 6: Warehouse Key Generation
- **Description**: Generate surrogate keys for warehouse dimension
- **Rationale**: Ensure unique identification and referential integrity
- **SQL Example**:
```sql
SELECT 
    ROW_NUMBER() OVER (ORDER BY Warehouse_ID, load_date) as warehouse_id,
    DENSE_RANK() OVER (ORDER BY Warehouse_ID) as warehouse_key,
    CONCAT('WH_', LPAD(Warehouse_ID, 6, '0')) as warehouse_code
FROM silver.si_warehouses
```

#### Rule 7: Location Standardization
- **Description**: Parse and standardize warehouse location information
- **Rationale**: Enable geographic analysis and location-based reporting
- **SQL Example**:
```sql
SELECT 
    Location,
    SPLIT(Location, ',')[0] as address_line1,
    CASE 
        WHEN SIZE(SPLIT(Location, ',')) > 1 THEN SPLIT(Location, ',')[1]
        ELSE NULL
    END as city,
    CASE 
        WHEN SIZE(SPLIT(Location, ',')) > 2 THEN SPLIT(Location, ',')[2]
        ELSE NULL
    END as state_province,
    UPPER(TRIM(Location)) as standardized_location
FROM silver.si_warehouses
```

#### Rule 8: Warehouse Type Classification
- **Description**: Classify warehouses based on capacity and location
- **Rationale**: Support warehouse utilization analysis and operational planning
- **SQL Example**:
```sql
SELECT 
    Warehouse_ID,
    Capacity,
    CASE 
        WHEN Capacity >= 100000 THEN 'LARGE_DISTRIBUTION'
        WHEN Capacity >= 50000 THEN 'MEDIUM_DISTRIBUTION'
        WHEN Capacity >= 10000 THEN 'SMALL_DISTRIBUTION'
        ELSE 'LOCAL_STORAGE'
    END as warehouse_type
FROM silver.si_warehouses
```

#### Rule 9: Capacity Validation and Standardization
- **Description**: Validate and standardize warehouse capacity values
- **Rationale**: Ensure data quality and support capacity planning calculations
- **SQL Example**:
```sql
SELECT 
    Warehouse_ID,
    CASE 
        WHEN Capacity IS NULL OR Capacity <= 0 THEN 1000
        WHEN Capacity > 1000000 THEN 1000000
        ELSE Capacity
    END as validated_capacity,
    CAST(Capacity as DECIMAL(15,2)) as total_capacity
FROM silver.si_warehouses
```

### 3.3 Go_Supplier_Dimension Transformations

#### Rule 10: Supplier Key Generation
- **Description**: Generate surrogate keys for supplier dimension
- **Rationale**: Support supplier performance tracking and analysis
- **SQL Example**:
```sql
SELECT 
    ROW_NUMBER() OVER (ORDER BY Supplier_ID, load_date) as supplier_id,
    DENSE_RANK() OVER (ORDER BY Supplier_ID) as supplier_key,
    CONCAT('SUP_', LPAD(Supplier_ID, 6, '0')) as supplier_code
FROM silver.si_suppliers
```

#### Rule 11: Supplier Name Standardization
- **Description**: Standardize supplier names and remove duplicates
- **Rationale**: Ensure consistent supplier identification across procurement records
- **SQL Example**:
```sql
SELECT 
    Supplier_ID,
    UPPER(TRIM(REGEXP_REPLACE(Supplier_Name, '[^A-Za-z0-9\\s]', ''))) as supplier_name,
    INITCAP(TRIM(Supplier_Name)) as display_supplier_name
FROM silver.si_suppliers
WHERE Supplier_Name IS NOT NULL AND LENGTH(TRIM(Supplier_Name)) > 0
```

#### Rule 12: Contact Information Validation
- **Description**: Validate and format supplier contact information
- **Rationale**: Ensure reliable communication channels for procurement activities
- **SQL Example**:
```sql
SELECT 
    Supplier_ID,
    CASE 
        WHEN Contact_Number RLIKE '^[0-9+\\-\\s\\(\\)]+$' THEN Contact_Number
        ELSE NULL
    END as contact_phone,
    CASE 
        WHEN Contact_Number RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN Contact_Number
        ELSE NULL
    END as contact_email
FROM silver.si_suppliers
```

#### Rule 13: Supplier Rating Derivation
- **Description**: Derive supplier rating based on performance metrics
- **Rationale**: Support supplier evaluation and preferred supplier identification
- **SQL Example**:
```sql
SELECT 
    Supplier_ID,
    CASE 
        WHEN load_date >= date_sub(current_date(), 30) THEN 'NEW_SUPPLIER'
        WHEN update_date >= date_sub(current_date(), 90) THEN 'ACTIVE'
        ELSE 'INACTIVE'
    END as supplier_rating,
    'ACTIVE' as supplier_status
FROM silver.si_suppliers
```

### 3.4 Go_Customer_Dimension Transformations

#### Rule 14: Customer Key Generation
- **Description**: Generate surrogate keys for customer dimension
- **Rationale**: Support customer analysis and segmentation reporting
- **SQL Example**:
```sql
SELECT 
    ROW_NUMBER() OVER (ORDER BY Customer_ID, load_date) as customer_id,
    DENSE_RANK() OVER (ORDER BY Customer_ID) as customer_key,
    CONCAT('CUST_', LPAD(Customer_ID, 8, '0')) as customer_code
FROM silver.si_customers
```

#### Rule 15: Customer Name Standardization
- **Description**: Standardize customer names with proper formatting
- **Rationale**: Ensure consistent customer identification and reporting
- **SQL Example**:
```sql
SELECT 
    Customer_ID,
    INITCAP(TRIM(Customer_Name)) as customer_name,
    UPPER(TRIM(Customer_Name)) as search_customer_name
FROM silver.si_customers
WHERE Customer_Name IS NOT NULL AND LENGTH(TRIM(Customer_Name)) > 0
```

#### Rule 16: Email Validation and Standardization
- **Description**: Validate and standardize customer email addresses
- **Rationale**: Ensure data quality for customer communication and analysis
- **SQL Example**:
```sql
SELECT 
    Customer_ID,
    CASE 
        WHEN Email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' 
        THEN LOWER(TRIM(Email))
        ELSE NULL
    END as contact_email,
    CASE 
        WHEN Email IS NOT NULL AND Email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' 
        THEN 'VALID'
        ELSE 'INVALID'
    END as email_status
FROM silver.si_customers
```

#### Rule 17: Customer Segmentation
- **Description**: Derive customer segments based on business rules
- **Rationale**: Support customer analysis and targeted marketing strategies
- **SQL Example**:
```sql
SELECT 
    Customer_ID,
    CASE 
        WHEN load_date >= date_sub(current_date(), 90) THEN 'NEW_CUSTOMER'
        WHEN update_date >= date_sub(current_date(), 365) THEN 'ACTIVE_CUSTOMER'
        ELSE 'INACTIVE_CUSTOMER'
    END as customer_segment,
    'ACTIVE' as customer_status
FROM silver.si_customers
```

### 3.5 Go_Date_Dimension Transformations

#### Rule 18: Date Key Generation
- **Description**: Generate date keys in YYYYMMDD format
- **Rationale**: Provide efficient date-based partitioning and joining
- **SQL Example**:
```sql
SELECT 
    ROW_NUMBER() OVER (ORDER BY full_date) as date_id,
    CAST(date_format(full_date, 'yyyyMMdd') as INT) as date_key,
    full_date
FROM (
    SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) as full_date
)
```

#### Rule 19: Date Attribute Derivation
- **Description**: Derive comprehensive date attributes for analysis
- **Rationale**: Support time-based analysis and seasonal reporting
- **SQL Example**:
```sql
SELECT 
    full_date,
    dayofweek(full_date) as day_of_week,
    date_format(full_date, 'EEEE') as day_name,
    dayofmonth(full_date) as day_of_month,
    dayofyear(full_date) as day_of_year,
    weekofyear(full_date) as week_of_year,
    month(full_date) as month_number,
    date_format(full_date, 'MMMM') as month_name,
    quarter(full_date) as quarter_number,
    CONCAT('Q', quarter(full_date)) as quarter_name,
    year(full_date) as year_number
FROM date_sequence
```

#### Rule 20: Business Calendar Attributes
- **Description**: Add business calendar attributes including weekends and holidays
- **Rationale**: Support business day calculations and operational reporting
- **SQL Example**:
```sql
SELECT 
    full_date,
    CASE 
        WHEN dayofweek(full_date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END as is_weekend,
    CASE 
        WHEN full_date IN ('2024-01-01', '2024-07-04', '2024-12-25') THEN TRUE
        ELSE FALSE
    END as is_holiday,
    CASE 
        WHEN month(full_date) <= 3 THEN year(full_date)
        ELSE year(full_date) + 1
    END as fiscal_year,
    CASE 
        WHEN month(full_date) <= 3 THEN quarter(full_date) + 1
        WHEN month(full_date) <= 6 THEN 1
        WHEN month(full_date) <= 9 THEN 2
        ELSE 3
    END as fiscal_quarter
FROM date_sequence
```

## 4. Data Quality and Validation Rules

### 4.1 Mandatory Field Validations
- **Product Dimension**: product_name, category_name cannot be null
- **Warehouse Dimension**: warehouse_name, location cannot be null
- **Supplier Dimension**: supplier_name cannot be null
- **Customer Dimension**: customer_name cannot be null
- **Date Dimension**: full_date, date_key cannot be null

### 4.2 Data Type Conversions
- Convert all ID fields to BIGINT for consistency
- Standardize all monetary fields to DECIMAL(15,2)
- Convert all date fields to DATE type
- Standardize all text fields to appropriate VARCHAR lengths

### 4.3 Referential Integrity Checks
- Validate all dimension keys exist before fact table loading
- Ensure SCD effective dates are properly sequenced
- Verify no duplicate active records in SCD Type 2 dimensions

## 5. Performance Optimization Recommendations

### 5.1 Partitioning Strategy
- **Product Dimension**: No partitioning (relatively small table)
- **Warehouse Dimension**: No partitioning (small reference table)
- **Supplier Dimension**: No partitioning (small reference table)
- **Customer Dimension**: Consider partitioning by customer_segment for large datasets
- **Date Dimension**: No partitioning (pre-calculated reference table)

### 5.2 Indexing Recommendations
- Apply Z-ORDER optimization on frequently queried columns
- Product Dimension: Z-ORDER BY (product_key, category_name)
- Warehouse Dimension: Z-ORDER BY (warehouse_key, warehouse_type)
- Supplier Dimension: Z-ORDER BY (supplier_key, supplier_rating)
- Customer Dimension: Z-ORDER BY (customer_key, customer_segment)
- Date Dimension: Z-ORDER BY (date_key, year_number, quarter_number)

## 6. Implementation Sequence

1. **Phase 1**: Create and populate Go_Date_Dimension (independent table)
2. **Phase 2**: Transform and load Go_Product_Dimension
3. **Phase 3**: Transform and load Go_Warehouse_Dimension
4. **Phase 4**: Transform and load Go_Supplier_Dimension
5. **Phase 5**: Transform and load Go_Customer_Dimension
6. **Phase 6**: Apply Z-ORDER optimization to all dimension tables
7. **Phase 7**: Validate data quality and referential integrity

## 7. Monitoring and Maintenance

### 7.1 Data Quality Monitoring
- Monitor null values in mandatory fields
- Track data standardization success rates
- Validate SCD Type 2 implementation integrity
- Monitor dimension key uniqueness

### 7.2 Performance Monitoring
- Track dimension table query performance
- Monitor storage utilization
- Validate optimization effectiveness
- Track data loading times

## 8. Traceability Matrix

| Transformation Rule | Source Requirement | Conceptual Model Reference | Constraint Reference |
|-------------------|-------------------|---------------------------|---------------------|
| Product Key Generation | Unique Product Identification | Product Entity | Uniqueness Constraints |
| Product Name Standardization | Consistent Naming | Product Name Attribute | Data Format Expectations |
| Category Hierarchy | Product Classification | Product Category Attribute | Business Rules |
| Warehouse Location Parsing | Geographic Analysis | Warehouse Location Attribute | Data Consistency Expectations |
| Supplier Performance Rating | Supplier Evaluation | Supplier Performance Metrics | Supplier Performance Rules |
| Customer Segmentation | Customer Analysis | Customer Classification | Business Rules |
| Date Attribute Derivation | Time-based Analysis | Date/Time Requirements | Data Format Expectations |

## 9. API Cost Calculation

**apiCost**: 0.0425

---

*This document provides comprehensive transformation rules for all Dimension tables in the Gold layer, ensuring data quality, consistency, and analytical readiness for the Inventory Management System.*