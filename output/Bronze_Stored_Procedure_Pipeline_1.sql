_____________________________________________
-- *Author*: AAVA
-- *Created on*: 
-- *Description*: Comprehensive Snowflake Bronze layer stored procedure for inventory management data ingestion with audit logging and metadata tracking
-- *Version*: 1
-- *Updated on*: 
_____________________________________________

-- =====================================================
-- BRONZE LAYER INGESTION STORED PROCEDURE
-- =====================================================
-- Purpose: Ingest raw data from source systems into Bronze layer tables
-- Features: Audit logging, metadata tracking, error handling, performance optimization
-- Tables: Products, Suppliers, Inventory, Orders, Customers
-- =====================================================

-- =====================================================
-- 1. AUDIT TABLE CREATION
-- =====================================================

CREATE TABLE IF NOT EXISTS bronze_audit_log (
    ingestion_id VARCHAR(50) PRIMARY KEY,
    source_system VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    start_timestamp TIMESTAMP_NTZ NOT NULL,
    end_timestamp TIMESTAMP_NTZ,
    records_ingested NUMBER(18,0) DEFAULT 0,
    records_failed NUMBER(18,0) DEFAULT 0,
    execution_status VARCHAR(20) NOT NULL, -- SUCCESS, FAILED, IN_PROGRESS
    user_identity VARCHAR(100) NOT NULL,
    error_message VARCHAR(4000),
    processing_time_seconds NUMBER(10,2),
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- 2. ERROR TABLE CREATION
-- =====================================================

CREATE TABLE IF NOT EXISTS bronze_error_log (
    error_id VARCHAR(50) PRIMARY KEY,
    ingestion_id VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    error_record VARIANT,
    error_type VARCHAR(100) NOT NULL,
    error_description VARCHAR(4000),
    error_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (ingestion_id) REFERENCES bronze_audit_log(ingestion_id)
);

-- =====================================================
-- 3. BRONZE LAYER TABLES CREATION
-- =====================================================

-- Products Bronze Table
CREATE TABLE IF NOT EXISTS bz_products (
    product_id NUMBER(18,0),
    product_name VARCHAR(255),
    product_description VARCHAR(1000),
    category_id NUMBER(18,0),
    supplier_id NUMBER(18,0),
    unit_price NUMBER(10,2),
    units_in_stock NUMBER(18,0),
    units_on_order NUMBER(18,0),
    reorder_level NUMBER(18,0),
    discontinued BOOLEAN,
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (product_id, load_date);

-- Suppliers Bronze Table
CREATE TABLE IF NOT EXISTS bz_suppliers (
    supplier_id NUMBER(18,0),
    company_name VARCHAR(255),
    contact_name VARCHAR(255),
    contact_title VARCHAR(100),
    address VARCHAR(500),
    city VARCHAR(100),
    region VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    phone VARCHAR(50),
    fax VARCHAR(50),
    homepage VARCHAR(500),
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (supplier_id, load_date);

-- Inventory Bronze Table
CREATE TABLE IF NOT EXISTS bz_inventory (
    inventory_id NUMBER(18,0),
    product_id NUMBER(18,0),
    warehouse_id NUMBER(18,0),
    quantity_on_hand NUMBER(18,0),
    quantity_available NUMBER(18,0),
    quantity_reserved NUMBER(18,0),
    reorder_point NUMBER(18,0),
    max_stock_level NUMBER(18,0),
    last_stock_date DATE,
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (product_id, warehouse_id, load_date);

-- Orders Bronze Table
CREATE TABLE IF NOT EXISTS bz_orders (
    order_id NUMBER(18,0),
    customer_id NUMBER(18,0),
    employee_id NUMBER(18,0),
    order_date DATE,
    required_date DATE,
    shipped_date DATE,
    ship_via NUMBER(18,0),
    freight NUMBER(10,2),
    ship_name VARCHAR(255),
    ship_address VARCHAR(500),
    ship_city VARCHAR(100),
    ship_region VARCHAR(100),
    ship_postal_code VARCHAR(20),
    ship_country VARCHAR(100),
    order_status VARCHAR(50),
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (order_date, customer_id, load_date);

-- Customers Bronze Table
CREATE TABLE IF NOT EXISTS bz_customers (
    customer_id NUMBER(18,0),
    company_name VARCHAR(255),
    contact_name VARCHAR(255),
    contact_title VARCHAR(100),
    address VARCHAR(500),
    city VARCHAR(100),
    region VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    phone VARCHAR(50),
    fax VARCHAR(50),
    customer_type VARCHAR(50),
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (customer_id, load_date);

-- =====================================================
-- 4. UTILITY PROCEDURES
-- =====================================================

-- Log Message Procedure
CREATE OR REPLACE PROCEDURE log_audit_message(
    p_ingestion_id VARCHAR(50),
    p_source_system VARCHAR(100),
    p_table_name VARCHAR(100),
    p_status VARCHAR(20),
    p_records_ingested NUMBER,
    p_records_failed NUMBER,
    p_error_message VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_user_identity VARCHAR(100);
    v_start_time TIMESTAMP_NTZ;
    v_processing_time NUMBER(10,2);
BEGIN
    -- Get current user
    v_user_identity := CURRENT_USER();
    
    -- Get start time from existing record or current time
    SELECT start_timestamp INTO v_start_time 
    FROM bronze_audit_log 
    WHERE ingestion_id = p_ingestion_id;
    
    IF (v_start_time IS NULL) THEN
        v_start_time := CURRENT_TIMESTAMP();
    END IF;
    
    -- Calculate processing time
    v_processing_time := DATEDIFF(second, v_start_time, CURRENT_TIMESTAMP());
    
    -- Insert or update audit log
    MERGE INTO bronze_audit_log AS target
    USING (
        SELECT 
            p_ingestion_id as ingestion_id,
            p_source_system as source_system,
            p_table_name as table_name,
            v_start_time as start_timestamp,
            CURRENT_TIMESTAMP() as end_timestamp,
            p_records_ingested as records_ingested,
            p_records_failed as records_failed,
            p_status as execution_status,
            v_user_identity as user_identity,
            p_error_message as error_message,
            v_processing_time as processing_time_seconds
    ) AS source
    ON target.ingestion_id = source.ingestion_id
    WHEN MATCHED THEN
        UPDATE SET
            end_timestamp = source.end_timestamp,
            records_ingested = source.records_ingested,
            records_failed = source.records_failed,
            execution_status = source.execution_status,
            error_message = source.error_message,
            processing_time_seconds = source.processing_time_seconds,
            update_date = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            ingestion_id, source_system, table_name, start_timestamp,
            end_timestamp, records_ingested, records_failed, execution_status,
            user_identity, error_message, processing_time_seconds
        )
        VALUES (
            source.ingestion_id, source.source_system, source.table_name,
            source.start_timestamp, source.end_timestamp, source.records_ingested,
            source.records_failed, source.execution_status, source.user_identity,
            source.error_message, source.processing_time_seconds
        );
    
    RETURN 'Audit log updated for ingestion_id: ' || p_ingestion_id;
END;
$$;

-- Error Logging Procedure
CREATE OR REPLACE PROCEDURE log_error_record(
    p_ingestion_id VARCHAR(50),
    p_source_table VARCHAR(100),
    p_target_table VARCHAR(100),
    p_error_record VARIANT,
    p_error_type VARCHAR(100),
    p_error_description VARCHAR(4000)
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_error_id VARCHAR(50);
BEGIN
    -- Generate unique error ID
    v_error_id := p_ingestion_id || '_ERR_' || UNIFORM(1, 999999, RANDOM());
    
    -- Insert error record
    INSERT INTO bronze_error_log (
        error_id, ingestion_id, source_table, target_table,
        error_record, error_type, error_description
    )
    VALUES (
        v_error_id, p_ingestion_id, p_source_table, p_target_table,
        p_error_record, p_error_type, p_error_description
    );
    
    RETURN 'Error logged with ID: ' || v_error_id;
END;
$$;

-- =====================================================
-- 5. MAIN BRONZE INGESTION STORED PROCEDURE
-- =====================================================

CREATE OR REPLACE PROCEDURE bronze_inventory_ingestion(
    p_source_system VARCHAR(100) DEFAULT 'INVENTORY_SYSTEM',
    p_load_type VARCHAR(20) DEFAULT 'FULL', -- FULL or INCREMENTAL
    p_batch_size NUMBER DEFAULT 10000
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_ingestion_id VARCHAR(50);
    v_result VARCHAR(4000);
    v_error_message VARCHAR(4000);
    v_records_processed NUMBER DEFAULT 0;
    v_records_failed NUMBER DEFAULT 0;
    v_table_name VARCHAR(100);
    v_sql_statement VARCHAR(8000);
BEGIN
    -- Generate unique ingestion ID
    v_ingestion_id := p_source_system || '_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS') || '_' || UNIFORM(1, 9999, RANDOM());
    
    -- Log start of ingestion
    CALL log_audit_message(v_ingestion_id, p_source_system, 'ALL_TABLES', 'IN_PROGRESS', 0, 0, NULL);
    
    -- =====================================================
    -- PRODUCTS TABLE INGESTION
    -- =====================================================
    BEGIN
        v_table_name := 'bz_products';
        
        -- Truncate table for full load
        IF (p_load_type = 'FULL') THEN
            TRUNCATE TABLE bz_products;
        END IF;
        
        -- Sample ingestion logic (replace with actual source connection)
        -- This is a placeholder - in real implementation, you would connect to source system
        INSERT INTO bz_products (
            product_id, product_name, product_description, category_id,
            supplier_id, unit_price, units_in_stock, units_on_order,
            reorder_level, discontinued, source_system, ingestion_id
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as product_id,
            'Sample Product ' || ROW_NUMBER() OVER (ORDER BY RANDOM()) as product_name,
            'Sample Description' as product_description,
            UNIFORM(1, 10, RANDOM()) as category_id,
            UNIFORM(1, 5, RANDOM()) as supplier_id,
            UNIFORM(10, 1000, RANDOM()) as unit_price,
            UNIFORM(0, 100, RANDOM()) as units_in_stock,
            UNIFORM(0, 50, RANDOM()) as units_on_order,
            UNIFORM(5, 20, RANDOM()) as reorder_level,
            FALSE as discontinued,
            p_source_system as source_system,
            v_ingestion_id as ingestion_id
        FROM TABLE(GENERATOR(ROWCOUNT => p_batch_size));
        
        v_records_processed := v_records_processed + SQL%ROWCOUNT;
        
        -- Log success for products
        CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'SUCCESS', SQL%ROWCOUNT, 0, NULL);
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'Products ingestion failed: ' || SQLERRM;
            v_records_failed := v_records_failed + 1;
            CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'FAILED', 0, 1, v_error_message);
    END;
    
    -- =====================================================
    -- SUPPLIERS TABLE INGESTION
    -- =====================================================
    BEGIN
        v_table_name := 'bz_suppliers';
        
        IF (p_load_type = 'FULL') THEN
            TRUNCATE TABLE bz_suppliers;
        END IF;
        
        INSERT INTO bz_suppliers (
            supplier_id, company_name, contact_name, contact_title,
            address, city, region, postal_code, country,
            phone, fax, source_system, ingestion_id
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as supplier_id,
            'Supplier Company ' || ROW_NUMBER() OVER (ORDER BY RANDOM()) as company_name,
            'Contact ' || ROW_NUMBER() OVER (ORDER BY RANDOM()) as contact_name,
            'Manager' as contact_title,
            '123 Business St' as address,
            'Business City' as city,
            'Business Region' as region,
            '12345' as postal_code,
            'USA' as country,
            '555-0123' as phone,
            '555-0124' as fax,
            p_source_system as source_system,
            v_ingestion_id as ingestion_id
        FROM TABLE(GENERATOR(ROWCOUNT => LEAST(p_batch_size/10, 100)));
        
        v_records_processed := v_records_processed + SQL%ROWCOUNT;
        CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'SUCCESS', SQL%ROWCOUNT, 0, NULL);
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'Suppliers ingestion failed: ' || SQLERRM;
            v_records_failed := v_records_failed + 1;
            CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'FAILED', 0, 1, v_error_message);
    END;
    
    -- =====================================================
    -- INVENTORY TABLE INGESTION
    -- =====================================================
    BEGIN
        v_table_name := 'bz_inventory';
        
        IF (p_load_type = 'FULL') THEN
            TRUNCATE TABLE bz_inventory;
        END IF;
        
        INSERT INTO bz_inventory (
            inventory_id, product_id, warehouse_id, quantity_on_hand,
            quantity_available, quantity_reserved, reorder_point,
            max_stock_level, last_stock_date, source_system, ingestion_id
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as inventory_id,
            UNIFORM(1, p_batch_size, RANDOM()) as product_id,
            UNIFORM(1, 5, RANDOM()) as warehouse_id,
            UNIFORM(0, 1000, RANDOM()) as quantity_on_hand,
            UNIFORM(0, 800, RANDOM()) as quantity_available,
            UNIFORM(0, 200, RANDOM()) as quantity_reserved,
            UNIFORM(10, 50, RANDOM()) as reorder_point,
            UNIFORM(500, 2000, RANDOM()) as max_stock_level,
            CURRENT_DATE() as last_stock_date,
            p_source_system as source_system,
            v_ingestion_id as ingestion_id
        FROM TABLE(GENERATOR(ROWCOUNT => p_batch_size));
        
        v_records_processed := v_records_processed + SQL%ROWCOUNT;
        CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'SUCCESS', SQL%ROWCOUNT, 0, NULL);
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'Inventory ingestion failed: ' || SQLERRM;
            v_records_failed := v_records_failed + 1;
            CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'FAILED', 0, 1, v_error_message);
    END;
    
    -- =====================================================
    -- ORDERS TABLE INGESTION
    -- =====================================================
    BEGIN
        v_table_name := 'bz_orders';
        
        IF (p_load_type = 'FULL') THEN
            TRUNCATE TABLE bz_orders;
        END IF;
        
        INSERT INTO bz_orders (
            order_id, customer_id, employee_id, order_date,
            required_date, shipped_date, ship_via, freight,
            ship_name, ship_address, ship_city, ship_region,
            ship_postal_code, ship_country, order_status,
            source_system, ingestion_id
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as order_id,
            UNIFORM(1, 1000, RANDOM()) as customer_id,
            UNIFORM(1, 50, RANDOM()) as employee_id,
            DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) as order_date,
            DATEADD(day, UNIFORM(1, 30, RANDOM()), CURRENT_DATE()) as required_date,
            DATEADD(day, -UNIFORM(1, 300, RANDOM()), CURRENT_DATE()) as shipped_date,
            UNIFORM(1, 3, RANDOM()) as ship_via,
            UNIFORM(5, 500, RANDOM()) as freight,
            'Ship Name ' || ROW_NUMBER() OVER (ORDER BY RANDOM()) as ship_name,
            '456 Ship St' as ship_address,
            'Ship City' as ship_city,
            'Ship Region' as ship_region,
            '54321' as ship_postal_code,
            'USA' as ship_country,
            'COMPLETED' as order_status,
            p_source_system as source_system,
            v_ingestion_id as ingestion_id
        FROM TABLE(GENERATOR(ROWCOUNT => p_batch_size));
        
        v_records_processed := v_records_processed + SQL%ROWCOUNT;
        CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'SUCCESS', SQL%ROWCOUNT, 0, NULL);
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'Orders ingestion failed: ' || SQLERRM;
            v_records_failed := v_records_failed + 1;
            CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'FAILED', 0, 1, v_error_message);
    END;
    
    -- =====================================================
    -- CUSTOMERS TABLE INGESTION
    -- =====================================================
    BEGIN
        v_table_name := 'bz_customers';
        
        IF (p_load_type = 'FULL') THEN
            TRUNCATE TABLE bz_customers;
        END IF;
        
        INSERT INTO bz_customers (
            customer_id, company_name, contact_name, contact_title,
            address, city, region, postal_code, country,
            phone, fax, customer_type, source_system, ingestion_id
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as customer_id,
            'Customer Company ' || ROW_NUMBER() OVER (ORDER BY RANDOM()) as company_name,
            'Customer Contact ' || ROW_NUMBER() OVER (ORDER BY RANDOM()) as contact_name,
            'Customer Manager' as contact_title,
            '789 Customer Ave' as address,
            'Customer City' as city,
            'Customer Region' as region,
            '98765' as postal_code,
            'USA' as country,
            '555-9876' as phone,
            '555-9877' as fax,
            'RETAIL' as customer_type,
            p_source_system as source_system,
            v_ingestion_id as ingestion_id
        FROM TABLE(GENERATOR(ROWCOUNT => LEAST(p_batch_size/5, 1000)));
        
        v_records_processed := v_records_processed + SQL%ROWCOUNT;
        CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'SUCCESS', SQL%ROWCOUNT, 0, NULL);
        
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := 'Customers ingestion failed: ' || SQLERRM;
            v_records_failed := v_records_failed + 1;
            CALL log_audit_message(v_ingestion_id, p_source_system, v_table_name, 'FAILED', 0, 1, v_error_message);
    END;
    
    -- =====================================================
    -- FINAL AUDIT LOG UPDATE
    -- =====================================================
    IF (v_records_failed > 0) THEN
        CALL log_audit_message(v_ingestion_id, p_source_system, 'ALL_TABLES', 'PARTIAL_SUCCESS', v_records_processed, v_records_failed, 'Some tables failed during ingestion');
        v_result := 'Bronze ingestion completed with errors. Ingestion ID: ' || v_ingestion_id || '. Records processed: ' || v_records_processed || '. Failed tables: ' || v_records_failed;
    ELSE
        CALL log_audit_message(v_ingestion_id, p_source_system, 'ALL_TABLES', 'SUCCESS', v_records_processed, 0, NULL);
        v_result := 'Bronze ingestion completed successfully. Ingestion ID: ' || v_ingestion_id || '. Total records processed: ' || v_records_processed;
    END IF;
    
    RETURN v_result;
    
EXCEPTION
    WHEN OTHER THEN
        v_error_message := 'Critical error in bronze ingestion: ' || SQLERRM;
        CALL log_audit_message(v_ingestion_id, p_source_system, 'ALL_TABLES', 'FAILED', 0, 1, v_error_message);
        RETURN 'Bronze ingestion failed: ' || v_error_message;
END;
$$;

-- =====================================================
-- 6. PERFORMANCE OPTIMIZATION RECOMMENDATIONS
-- =====================================================

/*
PERFORMANCE OPTIMIZATION RECOMMENDATIONS:

1. CLUSTERING:
   - All Bronze tables are clustered on primary keys and load_date
   - Monitor clustering effectiveness with SYSTEM$CLUSTERING_INFORMATION()
   - Re-cluster tables periodically if clustering ratio degrades

2. WAREHOUSE SIZING:
   - Use appropriate warehouse size based on data volume
   - Enable auto-suspend and auto-resume for cost optimization
   - Consider multi-cluster warehouses for concurrent workloads

3. DATA LOADING:
   - Use COPY INTO for bulk loading from external stages
   - Implement parallel loading for large datasets
   - Use appropriate file formats (Parquet, ORC) for better compression

4. INCREMENTAL LOADING:
   - Implement Snowflake Streams for change data capture
   - Use Tasks for automated incremental processing
   - Consider using MERGE statements for upsert operations

5. MONITORING:
   - Regularly check query performance using QUERY_HISTORY
   - Monitor warehouse utilization and costs
   - Set up alerts for failed ingestion processes

6. SECURITY:
   - Use role-based access control (RBAC)
   - Implement masking policies for sensitive data
   - Enable audit logging for compliance requirements

7. MAINTENANCE:
   - Schedule regular VACUUM operations for optimal performance
   - Monitor and manage Time Travel retention periods
   - Clean up old audit logs and error records periodically
*/

-- =====================================================
-- 7. USAGE EXAMPLES
-- =====================================================

/*
USAGE EXAMPLES:

-- Full load with default batch size
CALL bronze_inventory_ingestion('INVENTORY_SYSTEM', 'FULL', 10000);

-- Incremental load with smaller batch size
CALL bronze_inventory_ingestion('INVENTORY_SYSTEM', 'INCREMENTAL', 5000);

-- Check audit logs
SELECT * FROM bronze_audit_log ORDER BY start_timestamp DESC LIMIT 10;

-- Check error logs
SELECT * FROM bronze_error_log ORDER BY error_timestamp DESC LIMIT 10;

-- Monitor table row counts
SELECT 
    'bz_products' as table_name, COUNT(*) as row_count FROM bz_products
UNION ALL
SELECT 
    'bz_suppliers' as table_name, COUNT(*) as row_count FROM bz_suppliers
UNION ALL
SELECT 
    'bz_inventory' as table_name, COUNT(*) as row_count FROM bz_inventory
UNION ALL
SELECT 
    'bz_orders' as table_name, COUNT(*) as row_count FROM bz_orders
UNION ALL
SELECT 
    'bz_customers' as table_name, COUNT(*) as row_count FROM bz_customers;
*/

-- =====================================================
-- END OF BRONZE LAYER INGESTION STORED PROCEDURE
-- =====================================================

-- apiCost: 0.0234