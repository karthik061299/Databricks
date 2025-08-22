_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive Bronze Layer Data Mapping for Inventory Management System
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Bronze Model Data Mapping
## Inventory Management System - Bronze Layer Implementation

## 1. Data Mapping Overview

This document provides comprehensive data mapping between source system tables and Bronze layer tables in the Medallion architecture implementation for Databricks. The Bronze layer preserves raw data structure with minimal transformation while adding essential metadata for data governance and lineage tracking.

## 2. Data Mapping for Bronze Layer

### 2.1 Products Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Products | Product_ID | Source | Products | Product_ID | 1-1 Mapping |
| Bronze | Bz_Products | Product_Name | Source | Products | Product_Name | 1-1 Mapping |
| Bronze | Bz_Products | Category | Source | Products | Category | 1-1 Mapping |
| Bronze | Bz_Products | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Products | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Products | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Products | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Products | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

### 2.2 Suppliers Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Suppliers | Supplier_ID | Source | Suppliers | Supplier_ID | 1-1 Mapping |
| Bronze | Bz_Suppliers | Supplier_Name | Source | Suppliers | Supplier_Name | 1-1 Mapping |
| Bronze | Bz_Suppliers | Contact_Number | Source | Suppliers | Contact_Number | 1-1 Mapping |
| Bronze | Bz_Suppliers | Product_ID | Source | Suppliers | Product_ID | 1-1 Mapping |
| Bronze | Bz_Suppliers | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Suppliers | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Suppliers | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Suppliers | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Suppliers | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

### 2.3 Warehouses Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Warehouses | Warehouse_ID | Source | Warehouses | Warehouse_ID | 1-1 Mapping |
| Bronze | Bz_Warehouses | Location | Source | Warehouses | Location | 1-1 Mapping |
| Bronze | Bz_Warehouses | Capacity | Source | Warehouses | Capacity | 1-1 Mapping |
| Bronze | Bz_Warehouses | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Warehouses | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Warehouses | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Warehouses | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Warehouses | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

### 2.4 Inventory Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Inventory | Inventory_ID | Source | Inventory | Inventory_ID | 1-1 Mapping |
| Bronze | Bz_Inventory | Product_ID | Source | Inventory | Product_ID | 1-1 Mapping |
| Bronze | Bz_Inventory | Quantity_Available | Source | Inventory | Quantity_Available | 1-1 Mapping |
| Bronze | Bz_Inventory | Warehouse_ID | Source | Inventory | Warehouse_ID | 1-1 Mapping |
| Bronze | Bz_Inventory | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Inventory | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Inventory | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Inventory | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Inventory | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

### 2.5 Orders Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Orders | Order_ID | Source | Orders | Order_ID | 1-1 Mapping |
| Bronze | Bz_Orders | Customer_ID | Source | Orders | Customer_ID | 1-1 Mapping |
| Bronze | Bz_Orders | Order_Date | Source | Orders | Order_Date | 1-1 Mapping |
| Bronze | Bz_Orders | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Orders | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Orders | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Orders | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Orders | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

### 2.6 Order Details Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Order_Details | Order_Detail_ID | Source | Order_Details | Order_Detail_ID | 1-1 Mapping |
| Bronze | Bz_Order_Details | Order_ID | Source | Order_Details | Order_ID | 1-1 Mapping |
| Bronze | Bz_Order_Details | Product_ID | Source | Order_Details | Product_ID | 1-1 Mapping |
| Bronze | Bz_Order_Details | Quantity_Ordered | Source | Order_Details | Quantity_Ordered | 1-1 Mapping |
| Bronze | Bz_Order_Details | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Order_Details | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Order_Details | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Order_Details | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Order_Details | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

### 2.7 Shipments Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Shipments | Shipment_ID | Source | Shipments | Shipment_ID | 1-1 Mapping |
| Bronze | Bz_Shipments | Order_ID | Source | Shipments | Order_ID | 1-1 Mapping |
| Bronze | Bz_Shipments | Shipment_Date | Source | Shipments | Shipment_Date | 1-1 Mapping |
| Bronze | Bz_Shipments | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Shipments | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Shipments | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Shipments | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Shipments | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

### 2.8 Returns Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Returns | Return_ID | Source | Returns | Return_ID | 1-1 Mapping |
| Bronze | Bz_Returns | Order_ID | Source | Returns | Order_ID | 1-1 Mapping |
| Bronze | Bz_Returns | Return_Reason | Source | Returns | Return_Reason | 1-1 Mapping |
| Bronze | Bz_Returns | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Returns | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Returns | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Returns | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Returns | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

### 2.9 Stock Levels Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Stock_Levels | Stock_Level_ID | Source | Stock_Levels | Stock_Level_ID | 1-1 Mapping |
| Bronze | Bz_Stock_Levels | Warehouse_ID | Source | Stock_Levels | Warehouse_ID | 1-1 Mapping |
| Bronze | Bz_Stock_Levels | Product_ID | Source | Stock_Levels | Product_ID | 1-1 Mapping |
| Bronze | Bz_Stock_Levels | Reorder_Threshold | Source | Stock_Levels | Reorder_Threshold | 1-1 Mapping |
| Bronze | Bz_Stock_Levels | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Stock_Levels | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Stock_Levels | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Stock_Levels | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Stock_Levels | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

### 2.10 Customers Table Mapping

| Target Layer | Target Table | Target Field | Source Layer | Source Table | Source Field | Transformation Rule |
|--------------|--------------|--------------|--------------|--------------|--------------|--------------------|
| Bronze | Bz_Customers | Customer_ID | Source | Customers | Customer_ID | 1-1 Mapping |
| Bronze | Bz_Customers | Customer_Name | Source | Customers | Customer_Name | 1-1 Mapping (PII - Requires Encryption) |
| Bronze | Bz_Customers | Email | Source | Customers | Email | 1-1 Mapping (PII - Requires Encryption) |
| Bronze | Bz_Customers | load_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Customers | update_timestamp | Source | System Generated | N/A | System Generated - Current Timestamp |
| Bronze | Bz_Customers | source_system | Source | System Generated | N/A | System Generated - Source System Identifier |
| Bronze | Bz_Customers | record_status | Source | System Generated | N/A | System Generated - Default 'ACTIVE' |
| Bronze | Bz_Customers | data_quality_score | Source | System Generated | N/A | System Generated - Quality Score (0-100) |

## 3. Data Type Mapping

### 3.1 Source to Bronze Data Type Conversion

| Source Data Type | Bronze Layer Data Type | Databricks SQL Type | PySpark Type | Rationale |
|------------------|------------------------|--------------------|--------------|-----------|
| INT | INT | INT | IntegerType() | Direct mapping for integer values |
| VARCHAR(255) | STRING | STRING | StringType() | Databricks STRING handles variable length text |
| VARCHAR(100) | STRING | STRING | StringType() | Databricks STRING handles variable length text |
| VARCHAR(20) | STRING | STRING | StringType() | Databricks STRING handles variable length text |
| DATE | DATE | DATE | DateType() | Direct mapping for date values |
| TIMESTAMP | TIMESTAMP | TIMESTAMP | TimestampType() | System-generated timestamps |

## 4. Data Ingestion Rules

### 4.1 Raw Data Ingestion Process

| Process Step | Description | Implementation |
|--------------|-------------|----------------|
| **Extract** | Read data from source systems | Use Databricks Auto Loader for streaming ingestion |
| **Load** | Write data to Bronze layer tables | Use Delta Lake MERGE operations for upserts |
| **Metadata Population** | Add system-generated metadata | Populate load_timestamp, source_system, record_status |
| **Quality Scoring** | Calculate data quality metrics | Implement validation rules and assign quality scores |
| **Audit Logging** | Track ingestion process | Log all operations to Bz_Audit_Log table |

### 4.2 Initial Data Validation Rules

| Validation Type | Rule Description | Action on Failure |
|----------------|------------------|-------------------|
| **Schema Validation** | Verify column names and data types match expected schema | Log error, set data_quality_score to 0 |
| **Null Check** | Identify null values in required fields | Log warning, set data_quality_score based on null percentage |
| **Data Format** | Validate date formats and numeric ranges | Log error, set data_quality_score based on format violations |
| **Referential Integrity** | Check for orphaned records (informational only) | Log warning, maintain record but flag in audit |
| **Duplicate Detection** | Identify potential duplicate records | Log warning, maintain all records but flag duplicates |

## 5. Metadata Management

### 5.1 System-Generated Metadata Fields

| Metadata Field | Purpose | Population Rule | Data Type |
|----------------|---------|-----------------|----------|
| **load_timestamp** | Track initial data load time | Set during INSERT operations | TIMESTAMP |
| **update_timestamp** | Track last modification time | Updated during MERGE/UPDATE operations | TIMESTAMP |
| **source_system** | Identify data origin | Set based on ingestion pipeline configuration | STRING |
| **record_status** | Track record lifecycle | Default 'ACTIVE', managed by lifecycle processes | STRING |
| **data_quality_score** | Measure data quality | Calculated based on validation rules (0-100) | INT |

### 5.2 Data Lineage Tracking

| Lineage Component | Description | Implementation |
|-------------------|-------------|----------------|
| **Source Identification** | Track original source system and table | Populate source_system field |
| **Load Tracking** | Monitor data ingestion batches | Use load_timestamp for batch identification |
| **Quality Monitoring** | Track data quality over time | Monitor data_quality_score trends |
| **Audit Trail** | Comprehensive operation logging | Maintain detailed audit log in Bz_Audit_Log |

## 6. PII Data Handling

### 6.1 PII Classification and Protection

| PII Field | Table | Classification Level | Protection Method |
|-----------|-------|---------------------|-------------------|
| Customer_Name | Bz_Customers | High | Column-level encryption at rest |
| Email | Bz_Customers | High | Column-level encryption at rest |
| Contact_Number | Bz_Suppliers | Medium | Column-level encryption at rest |
| Supplier_Name | Bz_Suppliers | Low | Access control and audit logging |

### 6.2 Compliance Requirements

| Requirement | Implementation | Monitoring |
|-------------|----------------|------------|
| **GDPR Compliance** | Implement right to be forgotten | Track data retention and deletion requests |
| **Data Encryption** | Encrypt PII fields at rest and in transit | Monitor encryption status and key rotation |
| **Access Logging** | Log all access to PII data | Audit trail for compliance reporting |
| **Data Masking** | Mask PII in non-production environments | Implement dynamic data masking |

## 7. Performance Optimization

### 7.1 Partitioning Strategy

| Table | Partitioning Column | Rationale |
|-------|--------------------|-----------|
| Bz_Orders | Order_Date | Optimize time-based queries |
| Bz_Shipments | Shipment_Date | Optimize time-based queries |
| All Tables | load_timestamp | Optimize data lifecycle management |

### 7.2 Indexing and Optimization

| Optimization Type | Implementation | Tables |
|-------------------|----------------|--------|
| **Z-Ordering** | Order by frequently queried columns | Product_ID, Customer_ID, Order_ID |
| **Table Optimization** | Regular OPTIMIZE and VACUUM operations | All Bronze layer tables |
| **Liquid Clustering** | For tables with multiple query patterns | High-volume tables (Orders, Inventory) |

## 8. Error Handling and Recovery

### 8.1 Error Handling Strategy

| Error Type | Handling Approach | Recovery Action |
|------------|-------------------|----------------|
| **Schema Mismatch** | Log error, quarantine data | Manual review and schema evolution |
| **Data Quality Issues** | Log warning, process with quality score | Downstream filtering based on quality score |
| **System Failures** | Retry with exponential backoff | Automatic retry up to 3 attempts |
| **Connectivity Issues** | Queue data for later processing | Resume processing when connectivity restored |

### 8.2 Data Recovery Procedures

| Scenario | Recovery Method | RTO/RPO |
|----------|-----------------|----------|
| **Partial Data Loss** | Restore from Delta Lake time travel | RTO: 1 hour, RPO: 15 minutes |
| **Complete Table Loss** | Restore from backup and replay logs | RTO: 4 hours, RPO: 1 hour |
| **Corruption Detection** | Rollback to last known good state | RTO: 2 hours, RPO: 30 minutes |

## 9. Monitoring and Alerting

### 9.1 Key Performance Indicators

| KPI | Metric | Threshold | Alert Action |
|-----|--------|-----------|-------------|
| **Data Freshness** | Time since last successful load | > 2 hours | Send alert to operations team |
| **Data Quality Score** | Average quality score per table | < 80 | Send alert to data quality team |
| **Processing Time** | Time to complete full data load | > 1 hour | Send alert to performance team |
| **Error Rate** | Percentage of failed records | > 5% | Send alert to data engineering team |

### 9.2 Monitoring Dashboard Metrics

| Dashboard Section | Metrics | Refresh Frequency |
|-------------------|---------|-------------------|
| **Data Ingestion** | Records processed, success rate, processing time | Real-time |
| **Data Quality** | Quality scores, validation failures, PII compliance | Hourly |
| **System Health** | Storage usage, query performance, error rates | Every 15 minutes |
| **Business Metrics** | Record counts by table, data freshness, trends | Daily |

## 10. Implementation Guidelines

### 10.1 Deployment Checklist

- [ ] Create Bronze layer database schema
- [ ] Deploy Delta Lake tables with proper partitioning
- [ ] Configure Auto Loader for streaming ingestion
- [ ] Implement data validation rules
- [ ] Set up PII encryption and access controls
- [ ] Configure monitoring and alerting
- [ ] Test data ingestion pipelines
- [ ] Validate data quality scoring
- [ ] Set up audit logging
- [ ] Document operational procedures

### 10.2 Operational Procedures

| Procedure | Frequency | Responsible Team |
|-----------|-----------|------------------|
| **Data Quality Review** | Daily | Data Quality Team |
| **Performance Monitoring** | Continuous | Operations Team |
| **Security Audit** | Weekly | Security Team |
| **Backup Verification** | Daily | Infrastructure Team |
| **Compliance Reporting** | Monthly | Compliance Team |

## 11. API Cost Reporting

**apiCost**: 0.000425

---

## 12. Conclusion

This comprehensive Bronze Model Data Mapping provides a robust foundation for implementing the Bronze layer in the Medallion architecture for the Inventory Management System. The mapping ensures:

- **Data Preservation**: Raw data structure is maintained with minimal transformation
- **Governance**: Comprehensive metadata and audit capabilities
- **Compliance**: PII handling and regulatory compliance features
- **Performance**: Optimized for Databricks and Delta Lake
- **Monitoring**: Comprehensive data quality and operational monitoring
- **Scalability**: Designed to handle enterprise-scale data volumes

The implementation of this mapping will provide a solid foundation for downstream Silver and Gold layer transformations while maintaining data lineage and ensuring data governance requirements are met.