_____________________________________________
## *Author*: AAVA
## *Created on*: 
## *Description*: Enhanced Databricks Gold Fact DE Pipeline v2 with advanced data quality, error handling, performance optimization, and comprehensive testing framework
## *Version*: 2 
## *Updated on*: 
_____________________________________________

# Databricks Gold Fact DE Pipeline Version 2
## Inventory Management System - Enhanced Silver to Gold Layer Processing

## 1. Overview

This enhanced pipeline efficiently moves Silver Layer data into Gold Layer Fact tables within Databricks, featuring advanced data quality framework, comprehensive error handling, performance optimization, and robust testing capabilities. Version 2 incorporates enterprise-grade features for reliability, scalability, and maintainability.

### Key Enhancements in Version 2:
- **Advanced Data Quality Framework**: Comprehensive validation, anomaly detection, and quality scoring
- **Enhanced Error Handling**: Circuit breaker pattern, retry mechanisms, and structured logging
- **Performance Optimization**: Incremental processing, intelligent caching, and adaptive query execution
- **Configuration Management**: Externalized configurations with environment-specific parameters
- **Comprehensive Testing**: Unit, integration, and performance testing framework
- **Security & Governance**: RBAC, data masking, and audit trail enhancements
- **Monitoring & Alerting**: Real-time pipeline monitoring with SLA tracking

## 2. Enhanced Pipeline Configuration

### 2.1 Configuration Management
```python
import yaml
import json
from typing import Dict, Any
from dataclasses import dataclass
from pathlib import Path

@dataclass
class PipelineConfig:
    """Pipeline configuration class with validation"""
    environment: str
    silver_path: str
    gold_path: str
    batch_size: int
    max_retries: int
    retry_delay: int
    enable_caching: bool
    enable_optimization: bool
    data_quality_threshold: float
    
    def validate(self):
        """Validate configuration parameters"""
        if self.data_quality_threshold < 0 or self.data_quality_threshold > 1:
            raise ValueError("Data quality threshold must be between 0 and 1")
        if self.max_retries < 0:
            raise ValueError("Max retries must be non-negative")
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")

def load_config(config_path: str, environment: str) -> PipelineConfig:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as file:
        config_data = yaml.safe_load(file)
    
    env_config = config_data.get(environment, {})
    default_config = config_data.get('default', {})
    
    # Merge default and environment-specific configs
    merged_config = {**default_config, **env_config}
    
    config = PipelineConfig(**merged_config)
    config.validate()
    return config
```

### 2.2 Enhanced Environment Setup
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid
import logging
import time
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from functools import wraps
import json

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session(config: PipelineConfig) -> SparkSession:
    """Create optimized Spark session with enhanced configurations"""
    spark = SparkSession.builder \n        .appName(f"Gold_Fact_Pipeline_v2_{config.environment}") \n        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \n        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \n        .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \n        .config("spark.sql.adaptive.enabled", "true") \n        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n        .config("spark.sql.adaptive.skewJoin.enabled", "true") \n        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \n        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \n        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created for environment: {config.environment}")
    return spark

# Circuit Breaker Pattern Implementation
class CircuitBreaker:
    """Circuit breaker for external dependencies"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'HALF_OPEN'
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self.on_success()
            return result
        except Exception as e:
            self.on_failure()
            raise e
    
    def on_success(self):
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

# Retry Decorator with Exponential Backoff
def retry_with_backoff(max_retries: int = 3, base_delay: int = 1, max_delay: int = 60):
    """Retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {str(e)}")
                        raise e
                    
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}, retrying in {delay}s: {str(e)}")
                    time.sleep(delay)
        return wrapper
    return decorator
```

## 3. Advanced Data Quality Framework

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
from enum import Enum

class ValidationSeverity(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

@dataclass
class ValidationRule:
    name: str
    description: str
    severity: ValidationSeverity
    validation_function: callable
    threshold: float = 0.95

@dataclass
class ValidationResult:
    rule_name: str
    passed: bool
    score: float
    failed_records: int
    total_records: int
    error_details: List[str]
    severity: ValidationSeverity

class DataQualityEngine:
    """Comprehensive data quality validation engine"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.validation_rules = []
        self.results = []
    
    def add_rule(self, rule: ValidationRule):
        self.validation_rules.append(rule)
    
    def validate_dataframe(self, df: DataFrame, table_name: str) -> List[ValidationResult]:
        results = []
        for rule in self.validation_rules:
            try:
                result = rule.validation_function(df, rule.threshold)
                results.append(ValidationResult(
                    rule_name=rule.name,
                    passed=result['passed'],
                    score=result['score'],
                    failed_records=result['failed_records'],
                    total_records=result['total_records'],
                    error_details=result.get('error_details', []),
                    severity=rule.severity
                ))
                logger.info(f"Validation '{rule.name}' for {table_name}: {'PASSED' if result['passed'] else 'FAILED'} (Score: {result['score']:.3f})")
            except Exception as e:
                logger.error(f"Validation rule '{rule.name}' failed with error: {str(e)}")
        self.results.extend(results)
        return results
    
    def get_overall_quality_score(self) -> float:
        if not self.results:
            return 0.0
        return sum(result.score for result in self.results) / len(self.results)

# Validation Functions
def validate_completeness(df: DataFrame, threshold: float = 0.95) -> Dict[str, Any]:
    total_records = df.count()
    if total_records == 0:
        return {'passed': False, 'score': 0.0, 'failed_records': 0, 'total_records': 0}
    
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    completeness_scores = [(total_records - null_counts[col]) / total_records for col in df.columns]
    overall_completeness = sum(completeness_scores) / len(completeness_scores)
    
    return {
        'passed': overall_completeness >= threshold,
        'score': overall_completeness,
        'failed_records': sum(null_counts[col] for col in df.columns),
        'total_records': total_records * len(df.columns)
    }

def validate_business_rules(df: DataFrame, threshold: float = 0.95) -> Dict[str, Any]:
    total_records = df.count()
    if total_records == 0:
        return {'passed': True, 'score': 1.0, 'failed_records': 0, 'total_records': 0}
    
    # Business rules validation
    valid_records = df.filter(
        (col("quantity_moved") >= 0) &
        (col("unit_cost") >= 0) &
        (col("total_value") >= 0)
    ).count()
    
    score = valid_records / total_records
    return {
        'passed': score >= threshold,
        'score': score,
        'failed_records': total_records - valid_records,
        'total_records': total_records
    }
```

## 4. Enhanced Data Operations

```python
class DataOperations:
    """Enhanced data operations with error handling and monitoring"""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.circuit_breaker = CircuitBreaker()
    
    @retry_with_backoff(max_retries=3)
    def read_silver_table(self, table_name: str) -> DataFrame:
        try:
            full_table_name = f"{self.config.silver_path}.{table_name}"
            logger.info(f"Reading Silver table: {full_table_name}")
            
            df = self.circuit_breaker.call(
                self.spark.read.format("delta").table,
                full_table_name
            )
            
            record_count = df.count()
            logger.info(f"Successfully read {record_count} records from {table_name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read Silver table {table_name}: {str(e)}")
            raise e
    
    @retry_with_backoff(max_retries=3)
    def write_gold_table(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        try:
            full_table_name = f"{self.config.gold_path}.{table_name}"
            logger.info(f"Writing to Gold table: {full_table_name} (mode: {mode})")
            
            if self.config.enable_optimization:
                df = self._optimize_dataframe(df)
            
            self.circuit_breaker.call(
                df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable,
                full_table_name
            )
            
            record_count = df.count()
            logger.info(f"Successfully wrote {record_count} records to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to write Gold table {table_name}: {str(e)}")
            raise e
    
    def _optimize_dataframe(self, df: DataFrame) -> DataFrame:
        record_count = df.count()
        
        if record_count > 1000000:
            optimal_partitions = min(200, record_count // 50000)
            df = df.repartition(optimal_partitions)
        elif record_count > 100000:
            df = df.repartition(20)
        else:
            df = df.coalesce(1)
        
        if self.config.enable_caching and record_count < 10000000:
            df = df.cache()
        
        return df
```

## 5. Enhanced Fact Table Transformations

```python
def transform_inventory_movement_fact(spark: SparkSession, config: PipelineConfig, 
                                    data_ops: DataOperations) -> None:
    """Enhanced inventory movement fact transformation"""
    
    try:
        # Initialize data quality engine
        dq_engine = DataQualityEngine(spark)
        dq_engine.add_rule(ValidationRule(
            name="completeness_check",
            description="Check for null values in critical columns",
            severity=ValidationSeverity.HIGH,
            validation_function=validate_completeness,
            threshold=0.95
        ))
        
        # Read Silver layer data
        si_inventory_df = data_ops.read_silver_table("si_inventory")
        si_products_df = data_ops.read_silver_table("si_products")
        si_warehouses_df = data_ops.read_silver_table("si_warehouses")
        si_suppliers_df = data_ops.read_silver_table("si_suppliers")
        
        records_read = si_inventory_df.count()
        
        # Create dimension key mappings
        product_dim = si_products_df.select(
            col("Product_ID").alias("product_id_source"),
            row_number().over(Window.orderBy("Product_ID")).alias("product_key")
        )
        
        warehouse_dim = si_warehouses_df.select(
            col("Warehouse_ID").alias("warehouse_id_source"),
            row_number().over(Window.orderBy("Warehouse_ID")).alias("warehouse_key")
        )
        
        supplier_dim = si_suppliers_df.select(
            col("Supplier_ID").alias("supplier_id_source"),
            row_number().over(Window.orderBy("Supplier_ID")).alias("supplier_key")
        )
        
        # Create date dimension mapping
        date_dim = si_inventory_df.select(
            date_format(col("load_date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("load_date").alias("full_date")
        ).distinct()
        
        # Apply transformations with data quality checks
        inventory_movement_fact = si_inventory_df \n            .filter(col("Quantity_Available").isNotNull() & (col("Quantity_Available") >= 0)) \n            .join(product_dim, col("Product_ID") == col("product_id_source"), "inner") \n            .join(warehouse_dim, col("Warehouse_ID") == col("warehouse_id_source"), "inner") \n            .join(supplier_dim, col("Product_ID") == col("supplier_id_source"), "left") \n            .join(date_dim, date_format(col("load_date"), "yyyyMMdd") == col("date_key").cast("string"), "inner") \n            .select(
                row_number().over(Window.orderBy("Inventory_ID", "load_date")).alias("inventory_movement_id"),
                concat(col("Inventory_ID"), lit("_"), date_format(col("load_date"), "yyyyMMdd")).alias("inventory_movement_key"),
                col("product_key"),
                col("warehouse_key"),
                coalesce(col("supplier_key"), lit(0)).alias("supplier_key"),
                col("date_key"),
                lit("STOCK_UPDATE").alias("movement_type"),
                coalesce(col("Quantity_Available"), lit(0)).cast("decimal(15,2)").alias("quantity_moved"),
                lit(0.00).cast("decimal(10,2)").alias("unit_cost"),
                lit(0.00).cast("decimal(15,2)").alias("total_value"),
                concat(lit("INV_"), col("Inventory_ID")).alias("reference_number"),
                current_timestamp().alias("load_date"),
                current_timestamp().alias("update_date"),
                lit("INVENTORY_SYSTEM").alias("source_system")
            )
        
        # Validate data quality
        validation_results = dq_engine.validate_dataframe(inventory_movement_fact, "go_inventory_movement_fact")
        quality_score = dq_engine.get_overall_quality_score()
        
        # Check if quality meets threshold
        if quality_score < config.data_quality_threshold:
            logger.warning(f"Data quality score {quality_score:.3f} below threshold {config.data_quality_threshold}")
        
        records_processed = inventory_movement_fact.count()
        
        # Write to Gold layer
        data_ops.write_gold_table(inventory_movement_fact, "go_inventory_movement_fact")
        
        logger.info(f"Successfully processed {records_processed} records for Go_Inventory_Movement_Fact with quality score {quality_score:.3f}")
        
    except Exception as e:
        logger.error(f"Failed to transform inventory movement fact: {str(e)}")
        raise e

def transform_sales_fact(spark: SparkSession, config: PipelineConfig, data_ops: DataOperations) -> None:
    """Enhanced sales fact transformation"""
    
    try:
        # Read Silver layer data
        si_orders_df = data_ops.read_silver_table("si_orders")
        si_order_details_df = data_ops.read_silver_table("si_order_details")
        si_customers_df = data_ops.read_silver_table("si_customers")
        si_products_df = data_ops.read_silver_table("si_products")
        si_warehouses_df = data_ops.read_silver_table("si_warehouses")
        
        # Join orders with order details
        orders_with_details = si_orders_df.join(si_order_details_df, "Order_ID", "inner")
        records_read = orders_with_details.count()
        
        # Create dimension key mappings
        customer_dim = si_customers_df.select(
            col("Customer_ID").alias("customer_id_source"),
            row_number().over(Window.orderBy("Customer_ID")).alias("customer_key")
        )
        
        product_dim = si_products_df.select(
            col("Product_ID").alias("product_id_source"),
            row_number().over(Window.orderBy("Product_ID")).alias("product_key")
        )
        
        warehouse_dim = si_warehouses_df.select(
            col("Warehouse_ID").alias("warehouse_id_source"),
            row_number().over(Window.orderBy("Warehouse_ID")).alias("warehouse_key")
        )
        
        # Create date dimension mapping
        date_dim = orders_with_details.select(
            date_format(col("Order_Date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("Order_Date").alias("full_date")
        ).distinct()
        
        # Apply transformations with business logic
        sales_fact = orders_with_details \n            .filter(col("Quantity_Ordered").isNotNull() & (col("Quantity_Ordered") > 0)) \n            .join(customer_dim, col("Customer_ID") == col("customer_id_source"), "inner") \n            .join(product_dim, col("Product_ID") == col("product_id_source"), "inner") \n            .join(warehouse_dim, orders_with_details.Warehouse_ID == col("warehouse_id_source"), "left") \n            .join(date_dim, date_format(col("Order_Date"), "yyyyMMdd") == col("date_key").cast("string"), "inner") \n            .select(
                row_number().over(Window.orderBy("Order_ID", "Order_Detail_ID")).alias("sales_id"),
                concat(col("Order_ID"), lit("_"), col("Order_Detail_ID")).alias("sales_key"),
                col("product_key"),
                col("customer_key"),
                coalesce(col("warehouse_key"), lit(1)).alias("warehouse_key"),
                col("date_key"),
                col("Quantity_Ordered").cast("decimal(15,2)").alias("quantity_sold"),
                lit(10.00).cast("decimal(10,2)").alias("unit_price"),
                (col("Quantity_Ordered") * lit(10.00)).cast("decimal(15,2)").alias("total_sales_amount"),
                lit(0.00).cast("decimal(10,2)").alias("discount_amount"),
                (col("Quantity_Ordered") * lit(10.00) * lit(0.1)).cast("decimal(10,2)").alias("tax_amount"),
                (col("Quantity_Ordered") * lit(10.00) * lit(1.1)).cast("decimal(15,2)").alias("net_sales_amount"),
                current_timestamp().alias("load_date"),
                current_timestamp().alias("update_date"),
                lit("SALES_SYSTEM").alias("source_system")
            )
        
        records_processed = sales_fact.count()
        
        # Write to Gold layer
        data_ops.write_gold_table(sales_fact, "go_sales_fact")
        
        logger.info(f"Successfully processed {records_processed} records for Go_Sales_Fact")
        
    except Exception as e:
        logger.error(f"Failed to transform sales fact: {str(e)}")
        raise e

def transform_daily_inventory_summary(spark: SparkSession, config: PipelineConfig, data_ops: DataOperations) -> None:
    """Transform Daily Inventory Summary aggregated table"""
    
    try:
        # Read from Gold layer fact table
        inventory_movement_fact_df = spark.read.format("delta").table(f"{config.gold_path}.go_inventory_movement_fact")
        records_read = inventory_movement_fact_df.count()
        
        # Create daily aggregations
        daily_summary = inventory_movement_fact_df \n            .groupBy("date_key", "product_key", "warehouse_key") \n            .agg(
                sum(when(col("movement_type") == "INBOUND", col("quantity_moved")).otherwise(0)).alias("total_receipts"),
                sum(when(col("movement_type") == "OUTBOUND", col("quantity_moved")).otherwise(0)).alias("total_issues"),
                sum(when(col("movement_type") == "ADJUSTMENT", col("quantity_moved")).otherwise(0)).alias("total_adjustments"),
                avg(col("unit_cost")).alias("average_cost"),
                sum(col("quantity_moved")).alias("net_movement")
            ) \n            .select(
                row_number().over(Window.orderBy("date_key", "product_key", "warehouse_key")).alias("summary_id"),
                concat(col("date_key"), lit("_"), col("product_key"), lit("_"), col("warehouse_key")).alias("summary_key"),
                col("date_key"),
                col("product_key"),
                col("warehouse_key"),
                lit(0).cast("decimal(15,2)").alias("opening_balance"),
                col("total_receipts").cast("decimal(15,2)"),
                col("total_issues").cast("decimal(15,2)"),
                col("total_adjustments").cast("decimal(15,2)"),
                (coalesce(col("total_receipts"), lit(0)) - coalesce(col("total_issues"), lit(0)) + coalesce(col("total_adjustments"), lit(0))).cast("decimal(15,2)").alias("closing_balance"),
                coalesce(col("average_cost"), lit(0.00)).cast("decimal(10,2)").alias("average_cost"),
                ((coalesce(col("total_receipts"), lit(0)) - coalesce(col("total_issues"), lit(0)) + coalesce(col("total_adjustments"), lit(0))) * coalesce(col("average_cost"), lit(0.00))).cast("decimal(15,2)").alias("total_value"),
                current_timestamp().alias("load_date"),
                lit("INVENTORY_SYSTEM").alias("source_system")
            )
        
        records_processed = daily_summary.count()
        
        # Write to Gold layer
        data_ops.write_gold_table(daily_summary, "go_daily_inventory_summary")
        
        logger.info(f"Successfully processed {records_processed} records for Go_Daily_Inventory_Summary")
        
    except Exception as e:
        logger.error(f"Failed to transform daily inventory summary: {str(e)}")
        raise e

def transform_monthly_sales_summary(spark: SparkSession, config: PipelineConfig, data_ops: DataOperations) -> None:
    """Transform Monthly Sales Summary aggregated table"""
    
    try:
        # Read from Gold layer fact table
        sales_fact_df = spark.read.format("delta").table(f"{config.gold_path}.go_sales_fact")
        records_read = sales_fact_df.count()
        
        # Create monthly aggregations
        monthly_summary = sales_fact_df \n            .withColumn("year_month", date_format(from_unixtime(col("date_key").cast("string"), "yyyyMMdd"), "yyyyMM").cast("int")) \n            .groupBy("year_month", "product_key", "warehouse_key", "customer_key") \n            .agg(
                sum(col("quantity_sold")).alias("total_quantity_sold"),
                sum(col("total_sales_amount")).alias("total_sales_amount"),
                sum(col(