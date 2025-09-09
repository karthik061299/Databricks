_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Snowflake stored procedure for Bronze layer inventory management data ingestion
## *Version*: 1
## *Updated on*: 
_____________________________________________

-- =====================================================
-- SNOWFLAKE BRONZE LAYER DATA INGESTION STORED PROCEDURE
-- FOR INVENTORY MANAGEMENT SYSTEM
-- =====================================================

-- Create Bronze Schema if not exists
CREATE SCHEMA IF NOT EXISTS BRONZE;
USE SCHEMA BRONZE;

-- =====================================================
-- AUDIT AND ERROR TABLES CREATION
-- =====================================================

-- Create Audit Log Table
CREATE TABLE IF NOT EXISTS bz_audit_log (
    ingestion_id STRING DEFAULT CONCAT('ING_'