_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Comprehensive PySpark pipeline for Bronze to Silver layer data processing with data quality validation and error handling
## *Version*: 1
## *Updated on*: 
_____________________________________________

# Databricks Silver Layer Data Engineering Pipeline
## Inventory Management System - Bronze to Silver Data Processing

## Overview

This PySpark pipeline processes data from the Bronze layer to the Silver layer in the Databricks Medallion Architecture. It implements comprehensive data validation, cleansing, error handling, and audit logging to ensure high-quality data in the Silver layer.

## Pipeline Features

- **Data Validation**: 26 comprehensive validation checks including completeness, validity, uniqueness, and referential integrity
- **Error Handling**: Robust error capture and logging with detailed failure reasons
- **Audit Logging**: Complete pipeline execution tracking and monitoring
- **Schema Evolution**: Support for schema changes and data type evolution
- **Performance Optimization**: Delta Lake optimization with partitioning and Z-ordering
- **PII Protection**: Data masking for personally identifiable information

## PySpark Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import logging
import time
import uuid

# Initialize Spark Session with Delta configurations
spark = SparkSession.builder \n    .appName("Bronze to Silver Data Processing - Inventory Management") \n    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n    .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n    .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n    .config("spark.sql.adaptive.enabled", "true") \n    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        # Get credentials for Bronze and Silver layers from Azure Key Vault
        self.source_db_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
        self.user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
        self.password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")
        
        # Define layer paths
        self.lakehouse_bronze = "workspace.inventory_bronze"
        self.lakehouse_silver = "workspace.inventory_silver"
        self.lakehouse_gold = "workspace.inventory_gold"
        
        # Define schema for error table
        self.error_schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("error_timestamp", TimestampType(), False),
            StructField("source_table", StringType(), False),
            StructField("error_type", StringType(), False),
            StructField("error_description", StringType(), True),
            StructField("error_column", StringType(), True),
            StructField("error_value", StringType(), True),
            StructField("record_identifier", StringType(), True),
            StructField("severity_level", StringType(), False),
            StructField("validation_rule", StringType(), True),
            StructField("error_count", IntegerType(), False),
            StructField("processed_by", StringType(), False),
            StructField("resolution_status", StringType(), False),
            StructField("resolution_timestamp", TimestampType(), True),
            StructField("load_date", TimestampType(), False),
            StructField("source_system", StringType(), False)
        ])
        
        # Define schema for audit table
        self.audit_schema = StructType([
            StructField('audit_id', StringType(), False),
            StructField('audit_timestamp', TimestampType(), False),
            StructField('pipeline_name', StringType(), False),
            StructField('pipeline_run_id', StringType(), False),
            StructField('source_table', StringType(), False),
            StructField('target_table', StringType(), False),
            StructField('records_read', IntegerType(), False),
            StructField('records_processed', IntegerType(), False),
            StructField('records_inserted', IntegerType(), False),
            StructField('records_updated', IntegerType(), False),
            StructField('records_deleted', IntegerType(), False),
            StructField('records_rejected', IntegerType(), False),
            StructField('processing_start_time', TimestampType(), False),
            StructField('processing_end_time', TimestampType(), False),
            StructField('processing_duration', IntegerType(), False),
            StructField('pipeline_status', StringType(), False),
            StructField('error_message', StringType(), True),
            StructField('data_quality_score', FloatType(), True),
            StructField('processed_by', StringType(), False),
            StructField('environment', StringType(), False),
            StructField('pipeline_version', StringType(), False),
            StructField('load_date', TimestampType(), False),
            StructField('source_system', StringType(), False)
        ])
        
        # Initialize tracking
        self.error_records = []
        self.audit_records = []
        self.pipeline_run_id = str(uuid.uuid4())
        
        # Get current user
        try:
            self.current_user = mssparkutils.env.getUserName()
        except:
            try:
                self.current_user = spark.sparkContext.sparkUser()
            except:
                self.current_user = 'silver_etl_pipeline'

    def validate_data(self, df, table_name, validation_rules):
        """Apply comprehensive validation rules and return valid and invalid records"""
        valid_records = df
        validation_results = []
        total_records = df.count()
        
        try:
            for rule in validation_rules:
                # Completeness Checks
                if rule['type'] == 'not_null':
                    null_records = valid_records.filter(col(rule['column']).isNull() | (col(rule['column']) == ""))
                    null_count = null_records.count()
                    if null_count > 0:
                        validation_results.append(f"Found {null_count} null/empty values in {rule['column']}")
                        for row in null_records.collect():
                            self.log_error(table_name, f"Null/empty value in {rule['column']}", 
                                         rule['column'], "NULL", row.asDict().get(rule.get('id_column', 'ID'), 'Unknown'),
                                         "CRITICAL", "COMPLETENESS_VIOLATION")
                        valid_records = valid_records.filter(col(rule['column']).isNotNull() & (col(rule['column']) != ""))
                        
                # Uniqueness Checks
                elif rule['type'] == 'unique':
                    duplicates = valid_records.groupBy(rule['column']).count().filter(col('count') > 1)
                    duplicate_count = duplicates.count()
                    if duplicate_count > 0:
                        validation_results.append(f"Found {duplicate_count} duplicate values in {rule['column']}")
                        duplicate_records = valid_records.join(duplicates, rule['column']).drop('count')
                        for row in duplicate_records.collect():
                            self.log_error(table_name, f"Duplicate value in {rule['column']}", 
                                         rule['column'], str(row[rule['column']]), 
                                         row.asDict().get(rule.get('id_column', 'ID'), 'Unknown'),
                                         "MEDIUM", "UNIQUENESS_VIOLATION")
                        valid_records = valid_records.dropDuplicates([rule['column']])
                
                # Referential Integrity Checks
                elif rule['type'] == 'foreign_key':
                    try:
                        referenced_df = spark.read.format("delta").table(f"{self.lakehouse_silver}.si_{rule['referenced_table']}")
                        invalid_fk = valid_records.join(
                            referenced_df.select(rule['referenced_column']),
                            valid_records[rule['column']] == referenced_df[rule['referenced_column']],
                            'left_anti'
                        )
                        invalid_count = invalid_fk.count()
                        if invalid_count > 0:
                            validation_results.append(f"Found {invalid_count} invalid foreign keys in {rule['column']}")
                            for row in invalid_fk.collect():
                                self.log_error(table_name, f"Invalid foreign key in {rule['column']}", 
                                             rule['column'], str(row[rule['column']]), 
                                             row.asDict().get(rule.get('id_column', 'ID'), 'Unknown'),
                                             "CRITICAL", "REFERENTIAL_INTEGRITY_VIOLATION")
                        valid_records = valid_records.join(
                            referenced_df.select(rule['referenced_column']),
                            valid_records[rule['column']] == referenced_df[rule['referenced_column']],
                            'inner'
                        )
                    except Exception as e:
                        msg = f"Foreign key validation failed for {rule['column']}: {str(e)}"
                        validation_results.append(msg)
                        self.log_error(table_name, msg, rule['column'], "N/A", "N/A", "HIGH", "VALIDATION_ERROR")
                
                # Data Type and Format Validation
                elif rule['type'] == 'positive_integer':
                    invalid_records = valid_records.filter((col(rule['column']) < 0) | col(rule['column']).isNull())
                    invalid_count = invalid_records.count()
                    if invalid_count > 0:
                        validation_results.append(f"Found {invalid_count} non-positive values in {rule['column']}")
                        for row in invalid_records.collect():
                            self.log_error(table_name, f"Non-positive value in {rule['column']}", 
                                         rule['column'], str(row[rule['column']]), 
                                         row.asDict().get(rule.get('id_column', 'ID'), 'Unknown'),
                                         "HIGH", "BUSINESS_RULE_VIOLATION")
                    valid_records = valid_records.filter(col(rule['column']) >= 0)
                
                elif rule['type'] == 'non_negative_integer':
                    invalid_records = valid_records.filter(col(rule['column']) < 0)
                    invalid_count = invalid_records.count()
                    if invalid_count > 0:
                        validation_results.append(f"Found {invalid_count} negative values in {rule['column']}")
                        for row in invalid_records.collect():
                            self.log_error(table_name, f"Negative value in {rule['column']}", 
                                         rule['column'], str(row[rule['column']]), 
                                         row.asDict().get(rule.get('id_column', 'ID'), 'Unknown'),
                                         "HIGH", "BUSINESS_RULE_VIOLATION")
                    valid_records = valid_records.filter(col(rule['column']) >= 0)
                
                elif rule['type'] == 'email_format':
                    email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                    invalid_emails = valid_records.filter(~col(rule['column']).rlike(email_pattern))
                    invalid_count = invalid_emails.count()
                    if invalid_count > 0:
                        validation_results.append(f"Found {invalid_count} invalid email formats in {rule['column']}")
                        for row in invalid_emails.collect():
                            self.log_error(table_name, f"Invalid email format in {rule['column']}", 
                                         rule['column'], str(row[rule['column']]), 
                                         row.asDict().get(rule.get('id_column', 'ID'), 'Unknown'),
                                         "MEDIUM", "FORMAT_VALIDATION_ERROR")
                    valid_records = valid_records.filter(col(rule['column']).rlike(email_pattern))
                
                elif rule['type'] == 'phone_format':
                    phone_pattern = r'^[0-9]{10,15}$'
                    invalid_phones = valid_records.filter(~regexp_replace(col(rule['column']), r'[^0-9]', '').rlike(phone_pattern))
                    invalid_count = invalid_phones.count()
                    if invalid_count > 0:
                        validation_results.append(f"Found {invalid_count} invalid phone formats in {rule['column']}")
                        for row in invalid_phones.collect():
                            self.log_error(table_name, f"Invalid phone format in {rule['column']}", 
                                         rule['column'], str(row[rule['column']]), 
                                         row.asDict().get(rule.get('id_column', 'ID'), 'Unknown'),
                                         "LOW", "FORMAT_VALIDATION_ERROR")
                    # Standardize phone format
                    valid_records = valid_records.withColumn(rule['column'], 
                                                           regexp_replace(col(rule['column']), r'[^0-9]', ''))
                    valid_records = valid_records.filter(col(rule['column']).rlike(phone_pattern))
                
                elif rule['type'] == 'date_logic':
                    if 'compare_column' in rule:
                        invalid_dates = valid_records.filter(col(rule['column']) < col(rule['compare_column']))
                        invalid_count = invalid_dates.count()
                        if invalid_count > 0:
                            validation_results.append(f"Found {invalid_count} illogical dates in {rule['column']}")
                            for row in invalid_dates.collect():
                                self.log_error(table_name, f"Illogical date in {rule['column']}", 
                                             rule['column'], str(row[rule['column']]), 
                                             row.asDict().get(rule.get('id_column', 'ID'), 'Unknown'),
                                             "HIGH", "BUSINESS_RULE_VIOLATION")
                        valid_records = valid_records.filter(col(rule['column']) >= col(rule['compare_column']))
                        
        except Exception as e:
            msg = f"Validation error: {str(e)}"
            validation_results.append(msg)
            self.log_error(table_name, msg, "N/A", "N/A", "N/A", "CRITICAL", "VALIDATION_ERROR")
            
        # Calculate data quality score
        valid_count = valid_records.count()
        data_quality_score = (valid_count / total_records * 100) if total_records > 0 else 0
        
        # Print validation results
        if validation_results:
            print(f"\nValidation Results for {table_name}:")
            for result in validation_results:
                print(f"- {result}")
            print(f"Data Quality Score: {data_quality_score:.2f}%")
        else:
            print(f"\nAll validations passed for {table_name}")
            print(f"Data Quality Score: {data_quality_score:.2f}%")
            
        return valid_records, data_quality_score

    def log_error(self, table_name, error_description, error_column=None, error_value=None, 
                  record_identifier=None, severity_level="MEDIUM", validation_rule="UNKNOWN"):
        """Log errors to error tracking system"""
        error_record = {
            'error_id': str(uuid.uuid4()),
            'error_timestamp': datetime.now(),
            'source_table': table_name,
            'error_type': validation_rule,
            'error_description': error_description,
            'error_column': error_column,
            'error_value': error_value,
            'record_identifier': record_identifier,
            'severity_level': severity_level,
            'validation_rule': validation_rule,
            'error_count': 1,
            'processed_by': self.current_user,
            'resolution_status': 'OPEN',
            'resolution_timestamp': None,
            'load_date': datetime.now(),
            'source_system': 'ERP_SYSTEM'
        }
        self.error_records.append(error_record)
        logger.error(f"Table: {table_name}, Error: {error_description}")

    def log_audit(self, pipeline_name, source_table, target_table, records_read, records_processed, 
                  records_inserted, records_updated, records_deleted, records_rejected, 
                  processing_start_time, processing_end_time, processing_duration, 
                  pipeline_status, error_message, data_quality_score):
        """Log audit information"""
        audit_record = {
            'audit_id': str(uuid.uuid4()),
            'audit_timestamp': datetime.now(),
            'pipeline_name': pipeline_name,
            'pipeline_run_id': self.pipeline_run_id,
            'source_table': source_table,
            'target_table': target_table,
            'records_read': records_read,
            'records_processed': records_processed,
            'records_inserted': records_inserted,
            'records_updated': records_updated,
            'records_deleted': records_deleted,
            'records_rejected': records_rejected,
            'processing_start_time': processing_start_time,
            'processing_end_time': processing_end_time,
            'processing_duration': processing_duration,
            'pipeline_status': pipeline_status,
            'error_message': error_message,
            'data_quality_score': data_quality_score,
            'processed_by': self.current_user,
            'environment': 'PRODUCTION',
            'pipeline_version': 'v1.0',
            'load_date': datetime.now(),
            'source_system': 'ERP_SYSTEM'
        }
        
        audit_df = spark.createDataFrame([audit_record], schema=self.audit_schema)
        audit_df.write.format('delta').mode('append').saveAsTable(f'{self.lakehouse_silver}.si_pipeline_audit')

    def apply_transformations(self, df, table_name):
        """Apply data transformations and standardizations"""
        transformed_df = df
        
        try:
            # Convert timestamp columns to date format
            if 'load_timestamp' in df.columns:
                transformed_df = transformed_df.withColumn('load_date', col('load_timestamp').cast('date'))
                transformed_df = transformed_df.drop('load_timestamp')
            
            if 'update_timestamp' in df.columns:
                transformed_df = transformed_df.withColumn('update_date', col('update_timestamp').cast('date'))
                transformed_df = transformed_df.drop('update_timestamp')
            
            # Table-specific transformations
            if table_name == 'products':
                # Standardize product names and categories
                transformed_df = transformed_df.withColumn('Product_Name', 
                    trim(initcap(col('Product_Name'))))
                transformed_df = transformed_df.withColumn('Category', 
                    upper(col('Category')))
            
            elif table_name == 'suppliers':
                # Standardize supplier names and phone numbers
                transformed_df = transformed_df.withColumn('Supplier_Name', 
                    trim(initcap(col('Supplier_Name'))))
                transformed_df = transformed_df.withColumn('Contact_Number', 
                    regexp_replace(col('Contact_Number'), r'[^0-9]', ''))
            
            elif table_name == 'customers':
                # Apply PII masking for customer data
                transformed_df = transformed_df.withColumn('Customer_Name',
                    when(length(col('Customer_Name')) > 4,
                         concat(substring(col('Customer_Name'), 1, 2),
                               lit('***'),
                               substring(col('Customer_Name'), -2, 2)))
                    .otherwise(col('Customer_Name')))
                
                transformed_df = transformed_df.withColumn('Email',
                    when(col('Email').rlike(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
                         concat(substring_index(col('Email'), '@', 1), lit('@*****.com')))
                    .otherwise(col('Email')))
            
            elif table_name == 'warehouses':
                # Standardize location format
                transformed_df = transformed_df.withColumn('Location', 
                    trim(initcap(col('Location'))))
            
            elif table_name == 'returns':
                # Standardize return reasons
                transformed_df = transformed_df.withColumn('Return_Reason', 
                    upper(trim(col('Return_Reason'))))
            
            print(f"Applied transformations for {table_name}")
            
        except Exception as e:
            print(f"Error applying transformations for {table_name}: {str(e)}")
            self.log_error(table_name, f"Transformation error: {str(e)}", "N/A", "N/A", "N/A", "HIGH", "TRANSFORMATION_ERROR")
        
        return transformed_df

    def process_table(self, table_name, validation_rules):
        """Process a single table from Bronze to Silver"""
        start_time = datetime.now()
        print(f"\nProcessing table: {table_name}")
        
        records_read = 0
        records_processed = 0
        records_inserted = 0
        records_updated = 0
        records_deleted = 0
        records_rejected = 0
        pipeline_status = "SUCCESS"
        error_message = None
        data_quality_score = 0.0
        
        try:
            # Read from Bronze
            bronze_table = f"{self.lakehouse_bronze}.bz_{table_name}"
            bronze_df = spark.read.format("delta").table(bronze_table)
            records_read = bronze_df.count()
            print(f"Total records read from bronze: {records_read}")
            
            # Apply validations
            valid_df, data_quality_score = self.validate_data(bronze_df, table_name, validation_rules)
            records_processed = valid_df.count()
            records_rejected = records_read - records_processed
            
            # Apply transformations
            transformed_df = self.apply_transformations(valid_df, table_name)
            
            # Write to Silver with schema evolution enabled
            silver_table = f"{self.lakehouse_silver}.si_{table_name}"
            
            # Use merge for incremental updates if ID column exists
            id_column = f"{table_name.title().replace('_', '')}_ID"
            if table_name == 'order_details':
                id_column = 'Order_Detail_ID'
            elif table_name == 'stock_levels':
                id_column = 'Stock_Level_ID'
            
            if id_column in transformed_df.columns:
                # Perform MERGE operation for incremental updates
                transformed_df.createOrReplaceTempView(f"temp_{table_name}")
                
                merge_sql = f"""
                MERGE INTO {silver_table} AS target
                USING temp_{table_name} AS source
                ON target.{id_column} = source.{id_column}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
                
                try:
                    spark.sql(merge_sql)
                    records_inserted = records_processed  # Simplified for this example
                    print(f"Successfully merged {records_processed} records into {silver_table}")
                except Exception as merge_error:
                    # Fallback to overwrite if merge fails
                    print(f"Merge failed, falling back to overwrite: {str(merge_error)}")
                    transformed_df.write \n                        .format("delta") \n                        .option("mergeSchema", "true") \n                        .option("overwriteSchema", "true") \n                        .mode("overwrite") \n                        .saveAsTable(silver_table)
                    records_inserted = records_processed
            else:
                # Direct write for tables without clear ID column
                transformed_df.write \n                    .format("delta") \n                    .option("mergeSchema", "true") \n                    .option("overwriteSchema", "true") \n                    .mode("overwrite") \n                    .saveAsTable(silver_table)
                records_inserted = records_processed
            
            print(f"Successfully processed {table_name}: {records_processed} valid records, {records_rejected} rejected records")
            
        except Exception as e:
            pipeline_status = "FAILED"
            error_message = str(e)
            print(f"Failed to process {table_name}: {str(e)}")
            self.log_error(table_name, f"Pipeline processing error: {str(e)}", "N/A", "N/A", "N/A", "CRITICAL", "PIPELINE_ERROR")
        
        # Log audit record
        end_time = datetime.now()
        processing_duration = int((end_time - start_time).total_seconds())
        
        self.log_audit(
            pipeline_name=f"bronze_to_silver_{table_name}",
            source_table=f"bz_{table_name}",
            target_table=f"si_{table_name}",
            records_read=records_read,
            records_processed=records_processed,
            records_inserted=records_inserted,
            records_updated=records_updated,
            records_deleted=records_deleted,
            records_rejected=records_rejected,
            processing_start_time=start_time,
            processing_end_time=end_time,
            processing_duration=processing_duration,
            pipeline_status=pipeline_status,
            error_message=error_message,
            data_quality_score=data_quality_score
        )

    def write_error_data(self):
        """Write error records to Silver and Gold layers"""
        try:
            if self.error_records:
                print(f"\nWriting {len(self.error_records)} error records to error table")
                error_df = spark.createDataFrame(self.error_records, self.error_schema)
                
                # Write to Silver layer
                error_df.write \n                    .format("delta") \n                    .option("mergeSchema", "true") \n                    .mode("append") \n                    .saveAsTable(f"{self.lakehouse_silver}.si_data_quality_errors")
                
                # Write to Gold layer for reporting
                error_df.write \n                    .format("delta") \n                    .option("mergeSchema", "true") \n                    .mode("append") \n                    .saveAsTable(f"{self.lakehouse_gold}.go_data_quality_errors")
                
                print("Successfully wrote error records to both Silver and Gold layers")
            else:
                print("\nNo error records to write")
                
        except Exception as e:
            print(f"Failed to write error records: {str(e)}")
            logger.error(f"Error writing error records: {str(e)}")

    def optimize_tables(self):
        """Optimize Silver layer tables for performance"""
        tables_to_optimize = [
            ('si_products', ['Category']),
            ('si_inventory', ['Warehouse_ID', 'Product_ID']),
            ('si_orders', ['Order_Date', 'Customer_ID']),
            ('si_shipments', ['Shipment_Date']),
            ('si_stock_levels', ['Warehouse_ID', 'Product_ID'])
        ]
        
        for table_name, zorder_columns in tables_to_optimize:
            try:
                print(f"Optimizing {table_name}...")
                spark.sql(f"OPTIMIZE {self.lakehouse_silver}.{table_name} ZORDER BY ({', '.join(zorder_columns)})")
                print(f"Successfully optim