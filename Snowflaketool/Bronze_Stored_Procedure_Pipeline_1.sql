_____________________________________________
-- *Author*: AAVA
-- *Created on*: 
-- *Description*: Snowflake Bronze layer stored procedure for inventory management data ingestion
-- *Version*: 1
-- *Updated on*: 
_____________________________________________

-- =====================================================
-- BRONZE LAYER INVENTORY MANAGEMENT INGESTION PIPELINE
-- =====================================================

-- Create audit logging table
CREATE TABLE IF NOT EXISTS bronze_audit_log (
    ingestion_id VARCHAR(50) PRIMARY KEY,
    source_system VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    start_timestamp TIMESTAMP_NTZ NOT NULL,
    end_timestamp TIMESTAMP_NTZ,
    records_ingested INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    execution_status VARCHAR(20) DEFAULT 'RUNNING',
    user_identity VARCHAR(100) NOT NULL,
    error_message VARCHAR(5000),
    processing_time_seconds INTEGER,
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create error records table
CREATE TABLE IF NOT EXISTS bronze_error_records (
    error_id VARCHAR(50) PRIMARY KEY,
    ingestion_id VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    error_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    error_type VARCHAR(100),
    error_description VARCHAR(5000),
    rejected_record VARIANT,
    source_system VARCHAR(100),
    FOREIGN KEY (ingestion_id) REFERENCES bronze_audit_log(ingestion_id)
);

-- =====================================================
-- BRONZE LAYER TABLE DEFINITIONS
-- =====================================================

-- Bronze Products Table
CREATE TABLE IF NOT EXISTS bz_products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category_id INTEGER,
    supplier_id INTEGER,
    unit_price DECIMAL(10,2),
    units_in_stock INTEGER DEFAULT 0,
    units_on_order INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 0,
    discontinued BOOLEAN DEFAULT FALSE,
    product_description VARCHAR(1000),
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (category_id, supplier_id);

-- Bronze Suppliers Table
CREATE TABLE IF NOT EXISTS bz_suppliers (
    supplier_id INTEGER PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    contact_name VARCHAR(100),
    contact_title VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    phone VARCHAR(50),
    fax VARCHAR(50),
    homepage VARCHAR(255),
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (country, city);

-- Bronze Categories Table
CREATE TABLE IF NOT EXISTS bz_categories (
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    description VARCHAR(1000),
    picture VARIANT,
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
);

-- Bronze Orders Table
CREATE TABLE IF NOT EXISTS bz_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id VARCHAR(10),
    employee_id INTEGER,
    order_date DATE,
    required_date DATE,
    shipped_date DATE,
    ship_via INTEGER,
    freight DECIMAL(10,2) DEFAULT 0,
    ship_name VARCHAR(255),
    ship_address VARCHAR(255),
    ship_city VARCHAR(100),
    ship_region VARCHAR(100),
    ship_postal_code VARCHAR(20),
    ship_country VARCHAR(100),
    order_status VARCHAR(50) DEFAULT 'PENDING',
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (order_date, customer_id);

-- Bronze Order Details Table
CREATE TABLE IF NOT EXISTS bz_order_details (
    order_detail_id VARCHAR(50) PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    quantity INTEGER NOT NULL,
    discount DECIMAL(5,2) DEFAULT 0,
    line_total DECIMAL(12,2),
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (order_id, product_id);

-- Bronze Inventory Table
CREATE TABLE IF NOT EXISTS bz_inventory (
    inventory_id VARCHAR(50) PRIMARY KEY,
    product_id INTEGER NOT NULL,
    warehouse_id INTEGER,
    quantity_on_hand INTEGER DEFAULT 0,
    quantity_allocated INTEGER DEFAULT 0,
    quantity_available INTEGER DEFAULT 0,
    reorder_point INTEGER DEFAULT 0,
    max_stock_level INTEGER DEFAULT 0,
    last_stock_date DATE,
    cost_per_unit DECIMAL(10,2),
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (warehouse_id, product_id);

-- =====================================================
-- UTILITY PROCEDURES
-- =====================================================

-- Procedure to log audit information
CREATE OR REPLACE PROCEDURE log_audit_start(
    p_ingestion_id VARCHAR,
    p_source_system VARCHAR,
    p_table_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO bronze_audit_log (
        ingestion_id,
        source_system,
        table_name,
        start_timestamp,
        user_identity,
        execution_status
    )
    VALUES (
        p_ingestion_id,
        p_source_system,
        p_table_name,
        CURRENT_TIMESTAMP(),
        CURRENT_USER(),
        'RUNNING'
    );
    
    RETURN 'Audit log started for: ' || p_table_name;
END;
$$;

-- Procedure to update audit log on completion
CREATE OR REPLACE PROCEDURE log_audit_complete(
    p_ingestion_id VARCHAR,
    p_records_ingested INTEGER,
    p_records_failed INTEGER,
    p_status VARCHAR,
    p_error_message VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_start_time TIMESTAMP_NTZ;
    v_processing_time INTEGER;
BEGIN
    -- Get start time
    SELECT start_timestamp INTO v_start_time
    FROM bronze_audit_log
    WHERE ingestion_id = p_ingestion_id;
    
    -- Calculate processing time
    v_processing_time := DATEDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP());
    
    -- Update audit log
    UPDATE bronze_audit_log
    SET 
        end_timestamp = CURRENT_TIMESTAMP(),
        records_ingested = p_records_ingested,
        records_failed = p_records_failed,
        execution_status = p_status,
        error_message = p_error_message,
        processing_time_seconds = v_processing_time,
        update_date = CURRENT_TIMESTAMP()
    WHERE ingestion_id = p_ingestion_id;
    
    RETURN 'Audit log completed for ingestion: ' || p_ingestion_id;
END;
$$;

-- Procedure to log error records
CREATE OR REPLACE PROCEDURE log_error_record(
    p_ingestion_id VARCHAR,
    p_table_name VARCHAR,
    p_error_type VARCHAR,
    p_error_description VARCHAR,
    p_rejected_record VARIANT,
    p_source_system VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_error_id VARCHAR;
BEGIN
    v_error_id := 'ERR_' || p_ingestion_id || '_' || UNIFORM(1, 999999, RANDOM());
    
    INSERT INTO bronze_error_records (
        error_id,
        ingestion_id,
        table_name,
        error_type,
        error_description,
        rejected_record,
        source_system
    )
    VALUES (
        v_error_id,
        p_ingestion_id,
        p_table_name,
        p_error_type,
        p_error_description,
        p_rejected_record,
        p_source_system
    );
    
    RETURN 'Error logged with ID: ' || v_error_id;
END;
$$;

-- =====================================================
-- DATA INGESTION PROCEDURES
-- =====================================================

-- Procedure to ingest products data
CREATE OR REPLACE PROCEDURE ingest_products_data(
    p_ingestion_id VARCHAR,
    p_source_system VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_records_processed INTEGER DEFAULT 0;
    v_records_failed INTEGER DEFAULT 0;
    v_result VARCHAR;
BEGIN
    -- Start audit logging
    CALL log_audit_start(p_ingestion_id, p_source_system, 'bz_products');
    
    BEGIN
        -- Sample data insertion (replace with actual source data extraction)
        INSERT INTO bz_products (
            product_id, product_name, category_id, supplier_id, unit_price,
            units_in_stock, units_on_order, reorder_level, discontinued,
            product_description, source_system, ingestion_id
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY 1) as product_id,
            'Sample Product ' || ROW_NUMBER() OVER (ORDER BY 1) as product_name,
            UNIFORM(1, 10, RANDOM()) as category_id,
            UNIFORM(1, 20, RANDOM()) as supplier_id,
            UNIFORM(10, 1000, RANDOM()) / 100.0 as unit_price,
            UNIFORM(0, 100, RANDOM()) as units_in_stock,
            UNIFORM(0, 50, RANDOM()) as units_on_order,
            UNIFORM(5, 25, RANDOM()) as reorder_level,
            FALSE as discontinued,
            'Sample product description' as product_description,
            p_source_system as source_system,
            p_ingestion_id as ingestion_id
        FROM TABLE(GENERATOR(ROWCOUNT => 100));
        
        GET DIAGNOSTICS v_records_processed = ROW_COUNT;
        
        -- Complete audit logging
        CALL log_audit_complete(p_ingestion_id, v_records_processed, v_records_failed, 'SUCCESS');
        
        v_result := 'Products ingestion completed. Records processed: ' || v_records_processed;
        
    EXCEPTION
        WHEN OTHER THEN
            v_records_failed := 1;
            CALL log_audit_complete(p_ingestion_id, 0, v_records_failed, 'FAILED', SQLERRM);
            v_result := 'Products ingestion failed: ' || SQLERRM;
    END;
    
    RETURN v_result;
END;
$$;

-- Procedure to ingest suppliers data
CREATE OR REPLACE PROCEDURE ingest_suppliers_data(
    p_ingestion_id VARCHAR,
    p_source_system VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_records_processed INTEGER DEFAULT 0;
    v_records_failed INTEGER DEFAULT 0;
    v_result VARCHAR;
BEGIN
    -- Start audit logging
    CALL log_audit_start(p_ingestion_id, p_source_system, 'bz_suppliers');
    
    BEGIN
        -- Sample data insertion (replace with actual source data extraction)
        INSERT INTO bz_suppliers (
            supplier_id, company_name, contact_name, contact_title,
            address, city, region, postal_code, country, phone,
            source_system, ingestion_id
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY 1) as supplier_id,
            'Supplier Company ' || ROW_NUMBER() OVER (ORDER BY 1) as company_name,
            'Contact ' || ROW_NUMBER() OVER (ORDER BY 1) as contact_name,
            'Manager' as contact_title,
            '123 Business St' as address,
            'Business City' as city,
            'Business Region' as region,
            '12345' as postal_code,
            'USA' as country,
            '555-0123' as phone,
            p_source_system as source_system,
            p_ingestion_id as ingestion_id
        FROM TABLE(GENERATOR(ROWCOUNT => 20));
        
        GET DIAGNOSTICS v_records_processed = ROW_COUNT;
        
        -- Complete audit logging
        CALL log_audit_complete(p_ingestion_id, v_records_processed, v_records_failed, 'SUCCESS');
        
        v_result := 'Suppliers ingestion completed. Records processed: ' || v_records_processed;
        
    EXCEPTION
        WHEN OTHER THEN
            v_records_failed := 1;
            CALL log_audit_complete(p_ingestion_id, 0, v_records_failed, 'FAILED', SQLERRM);
            v_result := 'Suppliers ingestion failed: ' || SQLERRM;
    END;
    
    RETURN v_result;
END;
$$;

-- Procedure to ingest categories data
CREATE OR REPLACE PROCEDURE ingest_categories_data(
    p_ingestion_id VARCHAR,
    p_source_system VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_records_processed INTEGER DEFAULT 0;
    v_records_failed INTEGER DEFAULT 0;
    v_result VARCHAR;
BEGIN
    -- Start audit logging
    CALL log_audit_start(p_ingestion_id, p_source_system, 'bz_categories');
    
    BEGIN
        -- Sample data insertion (replace with actual source data extraction)
        INSERT INTO bz_categories (
            category_id, category_name, description,
            source_system, ingestion_id
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY 1) as category_id,
            'Category ' || ROW_NUMBER() OVER (ORDER BY 1) as category_name,
            'Description for category ' || ROW_NUMBER() OVER (ORDER BY 1) as description,
            p_source_system as source_system,
            p_ingestion_id as ingestion_id
        FROM TABLE(GENERATOR(ROWCOUNT => 10));
        
        GET DIAGNOSTICS v_records_processed = ROW_COUNT;
        
        -- Complete audit logging
        CALL log_audit_complete(p_ingestion_id, v_records_processed, v_records_failed, 'SUCCESS');
        
        v_result := 'Categories ingestion completed. Records processed: ' || v_records_processed;
        
    EXCEPTION
        WHEN OTHER THEN
            v_records_failed := 1;
            CALL log_audit_complete(p_ingestion_id, 0, v_records_failed, 'FAILED', SQLERRM);
            v_result := 'Categories ingestion failed: ' || SQLERRM;
    END;
    
    RETURN v_result;
END;
$$;

-- Procedure to ingest inventory data
CREATE OR REPLACE PROCEDURE ingest_inventory_data(
    p_ingestion_id VARCHAR,
    p_source_system VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_records_processed INTEGER DEFAULT 0;
    v_records_failed INTEGER DEFAULT 0;
    v_result VARCHAR;
BEGIN
    -- Start audit logging
    CALL log_audit_start(p_ingestion_id, p_source_system, 'bz_inventory');
    
    BEGIN
        -- Sample data insertion (replace with actual source data extraction)
        INSERT INTO bz_inventory (
            inventory_id, product_id, warehouse_id, quantity_on_hand,
            quantity_allocated, quantity_available, reorder_point,
            max_stock_level, last_stock_date, cost_per_unit,
            source_system, ingestion_id
        )
        SELECT 
            'INV_' || ROW_NUMBER() OVER (ORDER BY 1) as inventory_id,
            UNIFORM(1, 100, RANDOM()) as product_id,
            UNIFORM(1, 5, RANDOM()) as warehouse_id,
            UNIFORM(0, 500, RANDOM()) as quantity_on_hand,
            UNIFORM(0, 50, RANDOM()) as quantity_allocated,
            UNIFORM(0, 450, RANDOM()) as quantity_available,
            UNIFORM(10, 50, RANDOM()) as reorder_point,
            UNIFORM(100, 1000, RANDOM()) as max_stock_level,
            CURRENT_DATE() as last_stock_date,
            UNIFORM(5, 100, RANDOM()) / 10.0 as cost_per_unit,
            p_source_system as source_system,
            p_ingestion_id as ingestion_id
        FROM TABLE(GENERATOR(ROWCOUNT => 200));
        
        GET DIAGNOSTICS v_records_processed = ROW_COUNT;
        
        -- Complete audit logging
        CALL log_audit_complete(p_ingestion_id, v_records_processed, v_records_failed, 'SUCCESS');
        
        v_result := 'Inventory ingestion completed. Records processed: ' || v_records_processed;
        
    EXCEPTION
        WHEN OTHER THEN
            v_records_failed := 1;
            CALL log_audit_complete(p_ingestion_id, 0, v_records_failed, 'FAILED', SQLERRM);
            v_result := 'Inventory ingestion failed: ' || SQLERRM;
    END;
    
    RETURN v_result;
END;
$$;

-- =====================================================
-- MAIN ORCHESTRATION PROCEDURE
-- =====================================================

CREATE OR REPLACE PROCEDURE bronze_inventory_ingestion_pipeline(
    p_source_system VARCHAR DEFAULT 'INVENTORY_DB',
    p_load_type VARCHAR DEFAULT 'FULL'
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_ingestion_id VARCHAR;
    v_result VARCHAR DEFAULT '';
    v_temp_result VARCHAR;
    v_total_records INTEGER DEFAULT 0;
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
BEGIN
    -- Generate unique ingestion ID
    v_ingestion_id := 'ING_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS') || '_' || UNIFORM(1000, 9999, RANDOM());
    v_start_time := CURRENT_TIMESTAMP();
    
    v_result := 'Starting Bronze Layer Ingestion Pipeline\n';
    v_result := v_result || 'Ingestion ID: ' || v_ingestion_id || '\n';
    v_result := v_result || 'Source System: ' || p_source_system || '\n';
    v_result := v_result || 'Load Type: ' || p_load_type || '\n';
    v_result := v_result || 'Start Time: ' || v_start_time || '\n\n';
    
    BEGIN
        -- Ingest Categories (reference data first)
        CALL ingest_categories_data(v_ingestion_id || '_CAT', p_source_system);
        v_result := v_result || 'Categories ingestion completed\n';
        
        -- Ingest Suppliers (reference data)
        CALL ingest_suppliers_data(v_ingestion_id || '_SUP', p_source_system);
        v_result := v_result || 'Suppliers ingestion completed\n';
        
        -- Ingest Products (depends on categories and suppliers)
        CALL ingest_products_data(v_ingestion_id || '_PRD', p_source_system);
        v_result := v_result || 'Products ingestion completed\n';
        
        -- Ingest Inventory (depends on products)
        CALL ingest_inventory_data(v_ingestion_id || '_INV', p_source_system);
        v_result := v_result || 'Inventory ingestion completed\n';
        
        v_end_time := CURRENT_TIMESTAMP();
        
        -- Get total records processed
        SELECT SUM(records_ingested) INTO v_total_records
        FROM bronze_audit_log
        WHERE ingestion_id LIKE v_ingestion_id || '%';
        
        v_result := v_result || '\n=== PIPELINE SUMMARY ===\n';
        v_result := v_result || 'Total Records Processed: ' || COALESCE(v_total_records, 0) || '\n';
        v_result := v_result || 'Total Processing Time: ' || DATEDIFF(SECOND, v_start_time, v_end_time) || ' seconds\n';
        v_result := v_result || 'End Time: ' || v_end_time || '\n';
        v_result := v_result || 'Status: SUCCESS\n';
        
    EXCEPTION
        WHEN OTHER THEN
            v_result := v_result || '\nPIPELINE FAILED: ' || SQLERRM;
            -- Log pipeline failure
            INSERT INTO bronze_audit_log (
                ingestion_id,
                source_system,
                table_name,
                start_timestamp,
                end_timestamp,
                execution_status,
                error_message,
                user_identity
            )
            VALUES (
                v_ingestion_id || '_PIPELINE',
                p_source_system,
                'PIPELINE_ORCHESTRATION',
                v_start_time,
                CURRENT_TIMESTAMP(),
                'FAILED',
                SQLERRM,
                CURRENT_USER()
            );
    END;
    
    RETURN v_result;
END;
$$;

-- =====================================================
-- MONITORING AND STATISTICS PROCEDURES
-- =====================================================

CREATE OR REPLACE PROCEDURE get_ingestion_statistics(
    p_days_back INTEGER DEFAULT 7
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_result VARCHAR DEFAULT '';
    v_total_ingestions INTEGER;
    v_successful_ingestions INTEGER;
    v_failed_ingestions INTEGER;
    v_total_records INTEGER;
    v_avg_processing_time DECIMAL(10,2);
BEGIN
    -- Get statistics
    SELECT 
        COUNT(*),
        SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END),
        SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END),
        SUM(records_ingested),
        AVG(processing_time_seconds)
    INTO 
        v_total_ingestions,
        v_successful_ingestions,
        v_failed_ingestions,
        v_total_records,
        v_avg_processing_time
    FROM bronze_audit_log
    WHERE start_timestamp >= DATEADD(DAY, -p_days_back, CURRENT_TIMESTAMP());
    
    v_result := '=== BRONZE LAYER INGESTION STATISTICS ===\n';
    v_result := v_result || 'Period: Last ' || p_days_back || ' days\n';
    v_result := v_result || 'Total Ingestions: ' || COALESCE(v_total_ingestions, 0) || '\n';
    v_result := v_result || 'Successful Ingestions: ' || COALESCE(v_successful_ingestions, 0) || '\n';
    v_result := v_result || 'Failed Ingestions: ' || COALESCE(v_failed_ingestions, 0) || '\n';
    v_result := v_result || 'Success Rate: ' || ROUND((COALESCE(v_successful_ingestions, 0) * 100.0 / NULLIF(v_total_ingestions, 0)), 2) || '%\n';
    v_result := v_result || 'Total Records Processed: ' || COALESCE(v_total_records, 0) || '\n';
    v_result := v_result || 'Average Processing Time: ' || COALESCE(v_avg_processing_time, 0) || ' seconds\n';
    
    RETURN v_result;
END;
$$;

-- =====================================================
-- USAGE EXAMPLES AND TESTING
-- =====================================================

/*
-- Example usage:

-- 1. Run the full pipeline
CALL bronze_inventory_ingestion_pipeline('INVENTORY_DB', 'FULL');

-- 2. Get ingestion statistics
CALL get_ingestion_statistics(30);

-- 3. Check audit logs
SELECT * FROM bronze_audit_log ORDER BY start_timestamp DESC LIMIT 10;

-- 4. Check error records
SELECT * FROM bronze_error_records ORDER BY error_timestamp DESC LIMIT 10;

-- 5. Verify data ingestion
SELECT COUNT(*) FROM bz_products;
SELECT COUNT(*) FROM bz_suppliers;
SELECT COUNT(*) FROM bz_categories;
SELECT COUNT(*) FROM bz_inventory;

-- 6. Check clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('bz_products', '(category_id, supplier_id)');
SELECT SYSTEM$CLUSTERING_INFORMATION('bz_suppliers', '(country, city)');

*/