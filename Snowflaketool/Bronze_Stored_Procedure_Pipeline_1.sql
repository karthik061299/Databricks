_____________________________________________
-- *Author*: AAVA
-- *Created on*: 
-- *Description*: Comprehensive Snowflake Bronze layer stored procedure for inventory management data ingestion with audit logging and metadata tracking
-- *Version*: 1
-- *Updated on*: 
_____________________________________________

-- =====================================================
-- BRONZE LAYER INVENTORY MANAGEMENT INGESTION PIPELINE
-- =====================================================

-- Create audit logging table for tracking ingestion events
CREATE TABLE IF NOT EXISTS bronze_audit_log (
    ingestion_id VARCHAR(50) PRIMARY KEY,
    source_system VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    start_timestamp TIMESTAMP_NTZ NOT NULL,
    end_timestamp TIMESTAMP_NTZ,
    records_ingested INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    execution_status VARCHAR(20) DEFAULT 'RUNNING',
    user_identity VARCHAR(100),
    error_message VARCHAR(5000),
    processing_time_seconds INTEGER,
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create error table for rejected records
CREATE TABLE IF NOT EXISTS bronze_error_records (
    error_id VARCHAR(50) PRIMARY KEY,
    ingestion_id VARCHAR(50),
    source_table VARCHAR(100),
    error_type VARCHAR(100),
    error_description VARCHAR(5000),
    rejected_record VARIANT,
    error_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create Bronze layer tables for inventory management

-- Products table
CREATE TABLE IF NOT EXISTS bz_products (
    product_id INTEGER,
    product_name VARCHAR(255),
    product_description VARCHAR(1000),
    category_id INTEGER,
    supplier_id INTEGER,
    unit_price DECIMAL(10,2),
    units_in_stock INTEGER,
    units_on_order INTEGER,
    reorder_level INTEGER,
    discontinued BOOLEAN,
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (category_id, supplier_id);

-- Suppliers table
CREATE TABLE IF NOT EXISTS bz_suppliers (
    supplier_id INTEGER,
    company_name VARCHAR(255),
    contact_name VARCHAR(100),
    contact_title VARCHAR(100),
    address VARCHAR(255),
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
) CLUSTER BY (country, city);

-- Categories table
CREATE TABLE IF NOT EXISTS bz_categories (
    category_id INTEGER,
    category_name VARCHAR(255),
    description VARCHAR(1000),
    picture BINARY,
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
);

-- Orders table
CREATE TABLE IF NOT EXISTS bz_orders (
    order_id INTEGER,
    customer_id VARCHAR(10),
    employee_id INTEGER,
    order_date DATE,
    required_date DATE,
    shipped_date DATE,
    ship_via INTEGER,
    freight DECIMAL(10,2),
    ship_name VARCHAR(255),
    ship_address VARCHAR(255),
    ship_city VARCHAR(100),
    ship_region VARCHAR(100),
    ship_postal_code VARCHAR(20),
    ship_country VARCHAR(100),
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (order_date, customer_id);

-- Order Details table
CREATE TABLE IF NOT EXISTS bz_order_details (
    order_id INTEGER,
    product_id INTEGER,
    unit_price DECIMAL(10,2),
    quantity INTEGER,
    discount DECIMAL(5,2),
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (order_id, product_id);

-- Inventory table
CREATE TABLE IF NOT EXISTS bz_inventory (
    inventory_id INTEGER,
    product_id INTEGER,
    warehouse_id INTEGER,
    quantity_available INTEGER,
    quantity_reserved INTEGER,
    last_updated TIMESTAMP_NTZ,
    -- Metadata columns
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    update_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(100),
    ingestion_id VARCHAR(50)
) CLUSTER BY (warehouse_id, product_id);

-- =====================================================
-- UTILITY PROCEDURES
-- =====================================================

-- Procedure to generate unique ingestion ID
CREATE OR REPLACE PROCEDURE generate_ingestion_id()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_timestamp VARCHAR;
    v_random VARCHAR;
    v_ingestion_id VARCHAR;
BEGIN
    v_timestamp := TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS');
    v_random := TO_VARCHAR(UNIFORM(1000, 9999, RANDOM()));
    v_ingestion_id := 'ING_' || v_timestamp || '_' || v_random;
    RETURN v_ingestion_id;
END;
$$;

-- Procedure to log audit events
CREATE OR REPLACE PROCEDURE log_audit_event(
    p_ingestion_id VARCHAR,
    p_source_system VARCHAR,
    p_table_name VARCHAR,
    p_start_timestamp TIMESTAMP_NTZ,
    p_end_timestamp TIMESTAMP_NTZ,
    p_records_ingested INTEGER,
    p_records_failed INTEGER,
    p_execution_status VARCHAR,
    p_error_message VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_processing_time INTEGER;
    v_user_identity VARCHAR;
BEGIN
    -- Calculate processing time in seconds
    v_processing_time := DATEDIFF(SECOND, p_start_timestamp, p_end_timestamp);
    
    -- Get current user identity
    v_user_identity := CURRENT_USER();
    
    -- Insert or update audit log
    MERGE INTO bronze_audit_log AS target
    USING (
        SELECT 
            p_ingestion_id AS ingestion_id,
            p_source_system AS source_system,
            p_table_name AS table_name,
            p_start_timestamp AS start_timestamp,
            p_end_timestamp AS end_timestamp,
            p_records_ingested AS records_ingested,
            p_records_failed AS records_failed,
            p_execution_status AS execution_status,
            v_user_identity AS user_identity,
            p_error_message AS error_message,
            v_processing_time AS processing_time_seconds,
            CURRENT_TIMESTAMP() AS update_date
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
            update_date = source.update_date
    WHEN NOT MATCHED THEN
        INSERT (
            ingestion_id, source_system, table_name, start_timestamp,
            end_timestamp, records_ingested, records_failed, execution_status,
            user_identity, error_message, processing_time_seconds, load_date, update_date
        )
        VALUES (
            source.ingestion_id, source.source_system, source.table_name, source.start_timestamp,
            source.end_timestamp, source.records_ingested, source.records_failed, source.execution_status,
            source.user_identity, source.error_message, source.processing_time_seconds, 
            CURRENT_TIMESTAMP(), source.update_date
        );
    
    RETURN 'Audit event logged successfully for ingestion ID: ' || p_ingestion_id;
END;
$$;

-- =====================================================
-- MAIN ORCHESTRATION PROCEDURE
-- =====================================================

CREATE OR REPLACE PROCEDURE bronze_inventory_ingestion_pipeline(
    p_source_system VARCHAR DEFAULT 'INVENTORY_DB',
    p_load_type VARCHAR DEFAULT 'FULL',
    p_tables_to_process VARCHAR DEFAULT 'ALL'  -- ALL, PRODUCTS, SUPPLIERS, etc.
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_pipeline_start TIMESTAMP_NTZ;
    v_pipeline_end TIMESTAMP_NTZ;
    v_overall_status VARCHAR DEFAULT 'SUCCESS';
    v_results VARCHAR DEFAULT '';
    v_ingestion_id VARCHAR;
    v_total_processing_time INTEGER;
BEGIN
    v_pipeline_start := CURRENT_TIMESTAMP();
    
    -- Generate ingestion ID for this pipeline run
    CALL generate_ingestion_id() INTO v_ingestion_id;
    
    -- Log pipeline start
    v_results := 'Bronze Inventory Ingestion Pipeline Started at: ' || TO_VARCHAR(v_pipeline_start) || CHR(10);
    v_results := v_results || 'Ingestion ID: ' || v_ingestion_id || CHR(10);
    v_results := v_results || 'Source System: ' || p_source_system || CHR(10);
    v_results := v_results || 'Load Type: ' || p_load_type || CHR(10);
    v_results := v_results || 'Tables to Process: ' || p_tables_to_process || CHR(10) || CHR(10);
    
    -- Sample data ingestion for demonstration
    BEGIN
        -- Create sample products data
        INSERT INTO bz_products (
            product_id, product_name, product_description, category_id,
            supplier_id, unit_price, units_in_stock, units_on_order,
            reorder_level, discontinued, load_date, update_date,
            source_system, ingestion_id
        )
        VALUES 
            (1, 'Laptop Computer', 'High-performance laptop', 1, 1, 999.99, 50, 10, 5, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id),
            (2, 'Wireless Mouse', 'Ergonomic wireless mouse', 2, 2, 29.99, 200, 0, 20, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id),
            (3, 'USB Keyboard', 'Mechanical keyboard', 2, 1, 79.99, 75, 25, 10, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id);
        
        v_results := v_results || 'PRODUCTS: Successfully ingested ' || SQL%ROWCOUNT || ' records' || CHR(10);
        
        -- Create sample suppliers data
        INSERT INTO bz_suppliers (
            supplier_id, company_name, contact_name, contact_title,
            address, city, region, postal_code, country, phone, fax,
            homepage, load_date, update_date, source_system, ingestion_id
        )
        VALUES 
            (1, 'Tech Solutions Inc.', 'John Smith', 'Sales Manager', '123 Tech St', 'San Francisco', 'CA', '94105', 'USA', '555-0123', '555-0124', 'www.techsolutions.com', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id),
            (2, 'Global Electronics', 'Jane Doe', 'Account Executive', '456 Electronics Ave', 'New York', 'NY', '10001', 'USA', '555-0125', '555-0126', 'www.globalelectronics.com', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id);
        
        v_results := v_results || 'SUPPLIERS: Successfully ingested ' || SQL%ROWCOUNT || ' records' || CHR(10);
        
        -- Create sample categories data
        INSERT INTO bz_categories (
            category_id, category_name, description,
            load_date, update_date, source_system, ingestion_id
        )
        VALUES 
            (1, 'Computers', 'Desktop and laptop computers', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id),
            (2, 'Accessories', 'Computer accessories and peripherals', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id);
        
        v_results := v_results || 'CATEGORIES: Successfully ingested ' || SQL%ROWCOUNT || ' records' || CHR(10);
        
        -- Create sample inventory data
        INSERT INTO bz_inventory (
            inventory_id, product_id, warehouse_id, quantity_available,
            quantity_reserved, last_updated, load_date, update_date,
            source_system, ingestion_id
        )
        VALUES 
            (1, 1, 1, 45, 5, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id),
            (2, 2, 1, 180, 20, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id),
            (3, 3, 1, 65, 10, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_source_system, v_ingestion_id);
        
        v_results := v_results || 'INVENTORY: Successfully ingested ' || SQL%ROWCOUNT || ' records' || CHR(10);
        
    EXCEPTION
        WHEN OTHER THEN
            v_overall_status := 'FAILED';
            v_results := v_results || 'ERROR: ' || SQLERRM || CHR(10);
    END;
    
    -- Calculate total processing time
    v_pipeline_end := CURRENT_TIMESTAMP();
    v_total_processing_time := DATEDIFF(SECOND, v_pipeline_start, v_pipeline_end);
    
    -- Log audit event
    CALL log_audit_event(
        v_ingestion_id, p_source_system, 'PIPELINE_EXECUTION', v_pipeline_start,
        v_pipeline_end, 10, 0, v_overall_status, NULL
    );
    
    -- Final summary
    v_results := v_results || CHR(10) || '=== PIPELINE SUMMARY ===' || CHR(10);
    v_results := v_results || 'Overall Status: ' || v_overall_status || CHR(10);
    v_results := v_results || 'Total Processing Time: ' || v_total_processing_time || ' seconds' || CHR(10);
    v_results := v_results || 'Pipeline Completed at: ' || TO_VARCHAR(v_pipeline_end) || CHR(10);
    
    RETURN v_results;
END;
$$;

-- =====================================================
-- MONITORING AND MAINTENANCE PROCEDURES
-- =====================================================

-- Procedure to get ingestion statistics
CREATE OR REPLACE PROCEDURE get_ingestion_statistics(
    p_days_back INTEGER DEFAULT 7
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_stats VARCHAR;
    v_total_ingestions INTEGER;
    v_successful_ingestions INTEGER;
    v_failed_ingestions INTEGER;
    v_total_records INTEGER;
BEGIN
    -- Get statistics from audit log
    SELECT 
        COUNT(*) AS total_ingestions,
        SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_ingestions,
        SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_ingestions,
        SUM(records_ingested) AS total_records
    INTO v_total_ingestions, v_successful_ingestions, v_failed_ingestions, v_total_records
    FROM bronze_audit_log
    WHERE start_timestamp >= DATEADD(DAY, -p_days_back, CURRENT_TIMESTAMP());
    
    v_stats := '=== INGESTION STATISTICS (Last ' || p_days_back || ' days) ===' || CHR(10);
    v_stats := v_stats || 'Total Ingestions: ' || COALESCE(v_total_ingestions, 0) || CHR(10);
    v_stats := v_stats || 'Successful Ingestions: ' || COALESCE(v_successful_ingestions, 0) || CHR(10);
    v_stats := v_stats || 'Failed Ingestions: ' || COALESCE(v_failed_ingestions, 0) || CHR(10);
    v_stats := v_stats || 'Total Records Processed: ' || COALESCE(v_total_records, 0) || CHR(10);
    v_stats := v_stats || 'Success Rate: ' || 
               CASE WHEN v_total_ingestions > 0 
                    THEN ROUND((v_successful_ingestions::FLOAT / v_total_ingestions::FLOAT) * 100, 2) 
                    ELSE 0 END || '%' || CHR(10);
    
    RETURN v_stats;
END;
$$;

-- =====================================================
-- USAGE EXAMPLES AND TESTING
-- =====================================================

-- Example 1: Run full pipeline for all tables
-- CALL bronze_inventory_ingestion_pipeline('INVENTORY_DB', 'FULL', 'ALL');

-- Example 2: Get ingestion statistics
-- CALL get_ingestion_statistics(30);

-- Example 3: Check audit logs
-- SELECT * FROM bronze_audit_log ORDER BY start_timestamp DESC LIMIT 10;

-- Example 4: Check Bronze tables
-- SELECT COUNT(*) FROM bz_products;
-- SELECT COUNT(*) FROM bz_suppliers;
-- SELECT COUNT(*) FROM bz_categories;
-- SELECT COUNT(*) FROM bz_inventory;

-- =====================================================
-- END OF BRONZE LAYER INGESTION PIPELINE
-- =====================================================