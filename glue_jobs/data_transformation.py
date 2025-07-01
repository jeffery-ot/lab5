import sys
import boto3
import logging
import json
from datetime import datetime
from urllib.parse import urlparse
from awsglue.job import Job
import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    lit,
    current_timestamp,
    date_format,
    row_number,
    regexp_replace,
    trim,
)
from typing import Dict, Optional, Tuple, List
from collections import defaultdict


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Glue Context Setup
# SparkSession with Delta + S3 support
spark = (
    SparkSession.builder
    .appName("GlueDeltaJob")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

# Glue Context
glueContext = GlueContext(spark.sparkContext)

# Get args
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BATCH_SIZE', 'MAX_BATCHES'])
    BATCH_SIZE = int(args.get('BATCH_SIZE', 5))  # Process 5 partitions per batch by default
    MAX_BATCHES = int(args.get('MAX_BATCHES', 10))  # Maximum number of batches to process
except Exception:
    args = {'JOB_NAME': 'local_test'}
    BATCH_SIZE = 5
    MAX_BATCHES = 10

# Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 paths
INPUT_PATH = "s3://lab5-processed/curated/"
OUTPUT_PATH = "s3://lab5-lakehouse-dwh/"
ARCHIVE_PATH = "s3://lab5-processed/archived-curated/"
REJECTED_PATH = "s3://lab5-lakehouse-dwh/rejected_records/"
MANIFEST_KEY = "manifest/transformation_processed.json"
BATCH_STATE_KEY = "manifest/batch_processing_state.json"

# Table schema definitions (unchanged)
TABLE_SCHEMAS = {
    "product_data": {
        "primary_key": "product_id",
        "not_null_columns": ["product_id", "category_id", "product_name"],
        "partitions": ["category_id"]
    },
    "category": {
        "primary_key": "category_id", 
        "not_null_columns": ["category_id", "category"],
        "partitions": []
    },
    "users": {
        "primary_key": "user_id",
        "not_null_columns": ["user_id"],
        "partitions": []
    },
    "orders": {
        "primary_key": "order_id",
        "not_null_columns": ["order_id", "user_id", "order_timestamp"],
        "partitions": ["DATE(order_timestamp)"],
        "foreign_keys": {"user_id": "users.user_id"}
    },
    "order_items": {
        "primary_key": "order_items_id",
        "not_null_columns": ["order_items_id", "order_id", "product_id"],
        "partitions": [],
        "foreign_keys": {"order_id": "orders.order_id", "product_id": "product_data.product_id"}
    }
}

# ========== BATCH PROCESSING UTILITIES ==========

def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip("/")

def ensure_bucket_exists(bucket: str, region: str = None):
    """Ensure S3 bucket exists; create it if not."""
    s3 = boto3.client("s3", region_name=region)
    existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    
    if bucket not in existing_buckets:
        logger.info(f"Creating bucket: {bucket}")
        if region and region != "us-east-1":
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        else:
            s3.create_bucket(Bucket=bucket)

def list_all_partitions(bucket: str, prefix: str) -> Dict[str, List[str]]:
    """Find all partitions for each data type, organized by table"""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    
    table_partitions = defaultdict(list)
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for prefix_info in page.get("CommonPrefixes", []):
            table_path = prefix_info["Prefix"]
            table_name = table_path.split("/")[-2]
            
            # Find all dt partitions for this table
            table_paginator = s3.get_paginator("list_objects_v2")
            for table_page in table_paginator.paginate(Bucket=bucket, Prefix=table_path, Delimiter="/"):
                for dt_prefix in table_page.get("CommonPrefixes", []):
                    dt_path = dt_prefix["Prefix"]
                    if "dt=" in dt_path:
                        dt_value = dt_path.split("dt=")[1].rstrip("/")
                        partition_path = f"s3://{bucket}/{dt_path}"
                        table_partitions[table_name].append({
                            "path": partition_path,
                            "dt": dt_value,
                            "table": table_name
                        })
    
    # Sort partitions by date
    for table_name in table_partitions:
        table_partitions[table_name].sort(key=lambda x: x["dt"])
    
    return dict(table_partitions)

def get_processed_partitions(bucket: str) -> Dict[str, List[str]]:
    """Get list of already processed partitions from state file"""
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=BATCH_STATE_KEY)
        content = obj["Body"].read().decode("utf-8")
        state = json.loads(content)
        return state.get("processed_partitions", {})
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        logger.warning(f"Failed to read batch state: {e}")
        return {}

def update_processed_state(bucket: str, processed_partitions: Dict[str, List[str]]):
    """Update the batch processing state file"""
    s3 = boto3.client("s3")
    
    # Read existing state
    existing_state = get_processed_partitions(bucket)
    
    # Merge with new processed partitions
    for table_name, partitions in processed_partitions.items():
        if table_name not in existing_state:
            existing_state[table_name] = []
        existing_state[table_name].extend(partitions)
        # Remove duplicates and sort
        existing_state[table_name] = sorted(list(set(existing_state[table_name])))
    
    state = {
        "processed_partitions": existing_state,
        "last_updated": datetime.now().isoformat() + "Z"
    }
    
    local_path = "/tmp/batch_state.json"
    with open(local_path, "w") as f:
        json.dump(state, f, indent=2)
    s3.upload_file(local_path, bucket, BATCH_STATE_KEY)
    logger.info("Updated batch processing state")

def create_batches(all_partitions: Dict[str, List[str]], processed_partitions: Dict[str, List[str]], batch_size: int) -> List[Dict[str, List[str]]]:
    """Create batches of unprocessed partitions"""
    batches = []
    
    # Find unprocessed partitions for each table
    unprocessed = {}
    for table_name, partitions in all_partitions.items():
        processed_paths = processed_partitions.get(table_name, [])
        unprocessed_partitions = [
            p for p in partitions 
            if p["path"] not in processed_paths
        ]
        if unprocessed_partitions:
            unprocessed[table_name] = unprocessed_partitions
    
    if not unprocessed:
        return []
    
    # Create batches by grouping partitions across tables
    max_partitions = max(len(partitions) for partitions in unprocessed.values())
    
    for batch_idx in range(0, max_partitions, batch_size):
        batch = {}
        has_data = False
        
        for table_name, partitions in unprocessed.items():
            batch_partitions = partitions[batch_idx:batch_idx + batch_size]
            if batch_partitions:
                batch[table_name] = batch_partitions
                has_data = True
        
        if has_data:
            batches.append(batch)
    
    return batches

def archive_batch_partitions(batch_partitions: Dict[str, List[str]], archive_base: str):
    """Archive all partitions in a batch"""
    s3 = boto3.client("s3")
    
    for table_name, partitions in batch_partitions.items():
        for partition_info in partitions:
            source_path = partition_info["path"]
            dt_value = partition_info["dt"]
            
            bucket, key_prefix = parse_s3_path(source_path)
            archive_bucket, archive_prefix = parse_s3_path(archive_base)
            
            archive_key = f"{archive_prefix}{table_name}/dt={dt_value}/"
            
            try:
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
                    for obj in page.get("Contents", []):
                        source_key = obj["Key"]
                        dest_key = source_key.replace(key_prefix.rstrip("/"), archive_key.rstrip("/"))
                        s3.copy_object(
                            Bucket=archive_bucket, 
                            CopySource={"Bucket": bucket, "Key": source_key}, 
                            Key=dest_key
                        )
                
                logger.info(f"Archived {table_name} partition dt={dt_value}")
            except Exception as e:
                logger.error(f"Failed to archive {table_name} partition dt={dt_value}: {e}")

def write_rejected_records(df: DataFrame, table_name: str, rejection_reason: str):
    """Log rejected records with reasons"""
    if df.count() > 0:
        rejected_df = df.withColumn("rejection_reason", lit(rejection_reason)) \
                       .withColumn("rejected_at", current_timestamp())
        
        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        rejected_path = f"{REJECTED_PATH}{table_name}/dt={timestamp}/"
        
        rejected_df.write.mode("append").parquet(rejected_path)
        logger.warning(f"Rejected {df.count()} records from {table_name}: {rejection_reason}")

def write_delta_table(df: DataFrame, table_name: str, output_base: str, mode: str = "merge") -> str:
    """Write DataFrame as partitioned Delta table with merge logic"""
    output_path = f"{output_base}{table_name}/"
    schema_config = TABLE_SCHEMAS[table_name]
    
    # Add partitioning if specified
    writer = df.write.format("delta").option("overwriteSchema", "true")
    
    if schema_config["partitions"]:
        # Add partition columns
        for partition_expr in schema_config["partitions"]:
            if "DATE(" in partition_expr:
                col_name = partition_expr.replace("DATE(", "").replace(")", "")
                df = df.withColumn(f"order_date", date_format(col(col_name), "yyyy-MM-dd"))
        
        if "order_date" in df.columns:
            writer = writer.partitionBy("order_date")
    
    if mode == "overwrite":
        writer.mode("overwrite").save(output_path)
    else:
        # Check if table exists for merge
        try:
            existing_df = spark.read.format("delta").load(output_path)
            
            # Perform merge (upsert)
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(spark, output_path)
            
            pk_col = schema_config["primary_key"]
            merge_condition = f"existing.{pk_col} = updates.{pk_col}"
            
            delta_table.alias("existing") \
                .merge(df.alias("updates"), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
                
        except Exception:
            # Table doesn't exist, create it
            writer.mode("overwrite").save(output_path)
    
    logger.info(f"Wrote Delta table {table_name} to: {output_path}")
    return output_path

# ========== DATA QUALITY & VALIDATION ==========

def validate_schema_and_quality(df: DataFrame, table_name: str) -> Tuple[DataFrame, DataFrame]:
    """Validate data quality and return clean/rejected DataFrames"""
    schema_config = TABLE_SCHEMAS[table_name]
    clean_df = df
    rejected_dfs = []
    
    # 1. Check primary key not null
    pk_col = schema_config["primary_key"]
    pk_null_df = clean_df.filter(col(pk_col).isNull())
    if pk_null_df.count() > 0:
        write_rejected_records(pk_null_df, table_name, f"Primary key {pk_col} is null")
        rejected_dfs.append(pk_null_df)
        clean_df = clean_df.filter(col(pk_col).isNotNull())
    
    # 2. Check not null constraints
    for not_null_col in schema_config["not_null_columns"]:
        if not_null_col in clean_df.columns:
            null_df = clean_df.filter(col(not_null_col).isNull())
            if null_df.count() > 0:
                write_rejected_records(null_df, table_name, f"Required column {not_null_col} is null")
                rejected_dfs.append(null_df)
                clean_df = clean_df.filter(col(not_null_col).isNotNull())
    
    # 3. Validate timestamps
    timestamp_cols = [c for c in clean_df.columns if "timestamp" in c.lower()]
    for ts_col in timestamp_cols:
        invalid_ts_df = clean_df.filter(
            col(ts_col).isNull() | 
            (col(ts_col) < lit("1970-01-01")) |
            (col(ts_col) > current_timestamp())
        )
        if invalid_ts_df.count() > 0:
            write_rejected_records(invalid_ts_df, table_name, f"Invalid timestamp in {ts_col}")
            rejected_dfs.append(invalid_ts_df)
            clean_df = clean_df.filter(
                col(ts_col).isNotNull() & 
                (col(ts_col) >= lit("1970-01-01")) &
                (col(ts_col) <= current_timestamp())
            )
    
    # 4. Deduplicate by primary key (keep latest)
    original_count = clean_df.count()
    if pk_col in clean_df.columns:
        # Add row number for deduplication
        window_spec = Window.partitionBy(pk_col).orderBy(col("order_timestamp").desc() if "order_timestamp" in clean_df.columns else monotonically_increasing_id().desc())
        clean_df = clean_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter(col("row_num") == 1) \
                          .drop("row_num")
        
        deduped_count = clean_df.count()
        if original_count > deduped_count:
            logger.info(f"Deduplicated {table_name}: {original_count} -> {deduped_count} records")
    
    # Combine all rejected records
    rejected_df = None
    if rejected_dfs:
        rejected_df = rejected_dfs[0]
        for rdf in rejected_dfs[1:]:
            rejected_df = rejected_df.union(rdf)
    
    return clean_df, rejected_df

def check_referential_integrity(spark, table_name: str, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Check foreign key constraints"""
    schema_config = TABLE_SCHEMAS[table_name]
    
    if "foreign_keys" not in schema_config:
        return df, None
    
    clean_df = df
    rejected_dfs = []
    
    for fk_col, ref_table_col in schema_config["foreign_keys"].items():
        ref_table, ref_col = ref_table_col.split(".")
        ref_path = f"{OUTPUT_PATH}{ref_table}/"
        
        try:
            # Read reference table
            ref_df = spark.read.format("delta").load(ref_path)
            valid_keys = ref_df.select(ref_col).distinct()
            
            # Find invalid foreign keys
            invalid_fk_df = clean_df.join(valid_keys, clean_df[fk_col] == valid_keys[ref_col], "left_anti")
            
            if invalid_fk_df.count() > 0:
                write_rejected_records(invalid_fk_df, table_name, f"Invalid foreign key {fk_col} referencing {ref_table_col}")
                rejected_dfs.append(invalid_fk_df)
                
                # Keep only valid foreign keys
                clean_df = clean_df.join(valid_keys, clean_df[fk_col] == valid_keys[ref_col], "inner")
            
        except Exception as e:
            logger.warning(f"Could not validate foreign key {fk_col} for {table_name}: {e}")
    
    rejected_df = None
    if rejected_dfs:
        rejected_df = rejected_dfs[0]
        for rdf in rejected_dfs[1:]:
            rejected_df = rejected_df.union(rdf)
    
    return clean_df, rejected_df

# ========== MANIFEST MANAGEMENT ==========

def read_manifest_json(bucket: str) -> List[Dict[str, str]]:
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)
        content = obj["Body"].read().decode("utf-8")
        return json.loads(content)
    except s3.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.warning(f"Failed to read manifest: {e}")
        return []

def update_manifest_json(bucket: str, processed_batch: Dict[str, List[str]]):
    s3 = boto3.client("s3")
    manifest = read_manifest_json(bucket)
    
    for table_name, partitions in processed_batch.items():
        for partition_info in partitions:
            new_record = {
                "table_name": table_name,
                "partition_path": partition_info["path"],
                "dt": partition_info["dt"],
                "processed_at": datetime.now().isoformat() + "Z"
            }
            manifest.append(new_record)
    
    local_path = "/tmp/transformation_manifest.json"
    with open(local_path, "w") as f:
        json.dump(manifest, f, indent=2)
    s3.upload_file(local_path, bucket, MANIFEST_KEY)
    logger.info("Updated transformation manifest")

# ========== SPARK SQL TRANSFORMATIONS ==========

def create_temp_views_for_batch(spark, batch_partitions: Dict[str, List[str]]):
    """Create temporary views for batch processing by combining multiple partitions"""
    for table_name, partitions in batch_partitions.items():
        partition_paths = [p["path"] for p in partitions]
        
        # Read and union all partitions for this table
        dfs = []
        for path in partition_paths:
            try:
                df = spark.read.parquet(path)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read partition {path}: {e}")
        
        if dfs:
            # Union all DataFrames
            combined_df = dfs[0]
            for df in dfs[1:]:
                combined_df = combined_df.union(df)
            
            combined_df.createOrReplaceTempView(f"raw_{table_name}")
            logger.info(f"Created temp view: raw_{table_name} with {combined_df.count()} records from {len(dfs)} partitions")

def transform_product_data(spark) -> DataFrame:
    """Transform product_data using Spark SQL"""
    sql = """
    SELECT 
        CAST(product_id AS INTEGER) as product_id,
        CAST(department_id AS INTEGER) as category_id,
        TRIM(REGEXP_REPLACE(product_name, 'Product_\\\\d+_', '')) as product_name
    FROM raw_product_data 
    WHERE product_id IS NOT NULL 
    AND department_id IS NOT NULL
    AND TRIM(product_name) != ''
    """
    return spark.sql(sql)

def create_category_table(spark) -> DataFrame:
    """Create category table using Spark SQL"""
    sql = """
    SELECT DISTINCT
        CAST(department_id AS INTEGER) as category_id,
        TRIM(department) as category
    FROM raw_product_data 
    WHERE department_id IS NOT NULL 
    AND TRIM(department) IS NOT NULL
    AND TRIM(department) != ''
    ORDER BY category_id
    """
    return spark.sql(sql)

def create_users_table(spark) -> DataFrame:
    """Create users table using Spark SQL"""
    sql = """
    SELECT DISTINCT
        CAST(user_id AS INTEGER) as user_id
    FROM raw_orders 
    WHERE user_id IS NOT NULL
    ORDER BY user_id
    """
    return spark.sql(sql)

def transform_orders(spark) -> DataFrame:
    """Transform orders using Spark SQL"""
    sql = """
    SELECT 
        CAST(order_num AS INTEGER) as order_num,
        CAST(order_id AS INTEGER) as order_id,
        CAST(user_id AS INTEGER) as user_id,
        CAST(order_timestamp AS TIMESTAMP) as order_timestamp,
        DATE_FORMAT(CAST(order_timestamp AS TIMESTAMP), 'yyyy-MM-dd') AS order_date, -- Add this line
        CAST(total_amount AS DECIMAL(10,2)) as total_amount
    FROM raw_orders
    WHERE order_id IS NOT NULL
    AND user_id IS NOT NULL
    AND order_timestamp IS NOT NULL
    AND total_amount >= 0
    """
    return spark.sql(sql)

def transform_order_items(spark) -> DataFrame:
    """Transform order_items using Spark SQL"""
    sql = """
    SELECT 
        CAST(id AS INTEGER) as order_items_id,
        CAST(order_id AS INTEGER) as order_id,
        CAST(days_since_prior_order AS INTEGER) as days_since_prior_order,
        CAST(product_id AS INTEGER) as product_id,
        CAST(add_to_cart_order AS INTEGER) as add_to_cart_order,
        CAST(reordered AS BOOLEAN) as reordered
    FROM raw_order_items 
    WHERE id IS NOT NULL
    AND order_id IS NOT NULL
    AND product_id IS NOT NULL
    """
    return spark.sql(sql)

# ========== BATCH PROCESSING ==========

def process_batch_normalization(spark, batch_partitions: Dict[str, List[str]], output_base: str) -> Dict[str, str]:
    """Process a batch of partitions with data quality"""
    results = {}
    
    # Create temporary views for the batch
    create_temp_views_for_batch(spark, batch_partitions)
    
    # Process in dependency order: category -> users -> product_data -> orders -> order_items
    
    # 1. Category table (no dependencies)
    if "product_data" in batch_partitions:
        logger.info("Processing category table for batch")
        category_raw = create_category_table(spark)
        category_clean, category_rejected = validate_schema_and_quality(category_raw, "category")
        results["category"] = write_delta_table(category_clean, "category", output_base, "merge")
    
    # 2. Users table (no dependencies)  
    if "orders" in batch_partitions:
        logger.info("Processing users table for batch")
        users_raw = create_users_table(spark)
        users_clean, users_rejected = validate_schema_and_quality(users_raw, "users")
        results["users"] = write_delta_table(users_clean, "users", output_base, "merge")
    
    # 3. Product data (depends on category)
    if "product_data" in batch_partitions:
        logger.info("Processing product_data table for batch")
        product_raw = transform_product_data(spark)
        product_clean, product_rejected = validate_schema_and_quality(product_raw, "product_data")
        product_final, product_fk_rejected = check_referential_integrity(spark, "product_data", product_clean)
        results["product_data"] = write_delta_table(product_final, "product_data", output_base, "merge")
    
    # 4. Orders (depends on users)
    if "orders" in batch_partitions:
        logger.info("Processing orders table for batch")
        orders_raw = transform_orders(spark)
        orders_clean, orders_rejected = validate_schema_and_quality(orders_raw, "orders")
        orders_final, orders_fk_rejected = check_referential_integrity(spark, "orders", orders_clean)
        results["orders"] = write_delta_table(orders_final, "orders", output_base, "merge")
    
    # 5. Order items (depends on orders and products)
    if "order_items" in batch_partitions:
        logger.info("Processing order_items table for batch")
        order_items_raw = transform_order_items(spark)
        order_items_clean, order_items_rejected = validate_schema_and_quality(order_items_raw, "order_items")
        order_items_final, order_items_fk_rejected = check_referential_integrity(spark, "order_items", order_items_clean)
        results["order_items"] = write_delta_table(order_items_final, "order_items", output_base, "merge")
    
    return results

def validate_transformations(spark, results: Dict[str, str]) -> Dict[str, Dict]:
    """Validate transformed data quality"""
    validation_results = {}
    
    for table_name, output_path in results.items():
        try:
            df = spark.read.format("delta").load(output_path)
            df.createOrReplaceTempView(f"validate_{table_name}")
            
            validation_sql = f"""
            SELECT 
                '{table_name}' as table_name,
                COUNT(*) as row_count,
                COUNT(DISTINCT *) as distinct_rows
            FROM validate_{table_name}
            """
            
            validation_df = spark.sql(validation_sql)
            validation_row = validation_df.collect()[0]
            
            validation_results[table_name] = {
                "row_count": validation_row["row_count"],
                "distinct_rows": validation_row["distinct_rows"],
                "columns": df.columns,
                "duplicate_percentage": round(
                    (1 - validation_row["distinct_rows"] / validation_row["row_count"]) * 100, 2
                ) if validation_row["row_count"] > 0 else 0
            }
            
        except Exception as e:
            validation_results[table_name] = {"error": str(e)}
    
    return validation_results

# ========== JOB ENTRY POINT ==========

def main():
    input_bucket, input_prefix = parse_s3_path(INPUT_PATH)
    
    # Optional: override region manually, or pull from environment
    region = boto3.session.Session().region_name or "us-east-1"
    ensure_bucket_exists(input_bucket, region=region)
    
    logger.info("Discovering all available partitions...")
    all_partitions = list_all_partitions(input_bucket, input_prefix)
    if not all_partitions:
        logger.warning("No partitions found to process.")
        return
    
    total_partitions = sum(len(partitions) for partitions in all_partitions.values())
    logger.info(f"Found {total_partitions} total partitions across {len(all_partitions)} tables")
    
    logger.info("Checking processed partitions state...")
    processed_partitions = get_processed_partitions(input_bucket)
    
    logger.info(f"Creating batches with batch size: {BATCH_SIZE}")
    batches = create_batches(all_partitions, processed_partitions, BATCH_SIZE)
    
    if not batches:
        logger.info("No new partitions to process.")
        return
    
    logger.info(f"Created {len(batches)} batches to process")
    
    # Limit number of batches processed in one job run
    batches_to_process = batches[:MAX_BATCHES]
    if len(batches) > MAX_BATCHES:
        logger.info(f"Processing first {MAX_BATCHES} batches out of {len(batches)} total batches")
    
    batch_results = []
    processed_batch_paths = []
    
    for batch_idx, batch_partitions in enumerate(batches_to_process, 1):
        logger.info(f"Processing batch {batch_idx}/{len(batches_to_process)}")
        
        # Log batch details
        batch_summary = {}
        for table_name, partitions in batch_partitions.items():
            batch_summary[table_name] = len(partitions)
        logger.info(f"Batch {batch_idx} contains: {batch_summary}")
        
        try:
            # Process the batch
            processed_paths = process_batch_normalization(spark, batch_partitions, OUTPUT_PATH)
            
            # Validate transformations
            validation_results = validate_transformations(spark, processed_paths)
            
            batch_result = {
                "batch_number": batch_idx,
                "processed_paths": processed_paths,
                "validation_results": validation_results,
                "partitions_processed": batch_partitions
            }
            batch_results.append(batch_result)
            
            # Archive processed partitions
            logger.info(f"Archiving batch {batch_idx} partitions...")
            archive_batch_partitions(batch_partitions, ARCHIVE_PATH)
            
            # Update manifest
            logger.info(f"Updating manifest for batch {batch_idx}...")
            update_manifest_json(input_bucket, batch_partitions)
            
            # Track processed partitions for state update
            processed_batch_paths.append(batch_partitions)
            
            logger.info(f"Successfully processed batch {batch_idx}")
            
        except Exception as e:
            logger.error(f"Failed to process batch {batch_idx}: {e}")
            # Continue with next batch instead of failing completely
            continue
    
    # Update batch processing state
    if processed_batch_paths:
        logger.info("Updating batch processing state...")
        all_processed = {}
        for batch in processed_batch_paths:
            for table_name, partitions in batch.items():
                if table_name not in all_processed:
                    all_processed[table_name] = []
                all_processed[table_name].extend([p["path"] for p in partitions])
        
        update_processed_state(input_bucket, all_processed)
    
    # Summary report
    successful_batches = len([r for r in batch_results if "processed_paths" in r])
    total_records_processed = 0
    
    logger.info("=== BATCH PROCESSING SUMMARY ===")
    logger.info(f"Total batches processed: {successful_batches}/{len(batches_to_process)}")
    
    for result in batch_results:
        if "validation_results" in result:
            batch_records = sum(
                v.get("row_count", 0) for v in result["validation_results"].values()
                if isinstance(v, dict) and "row_count" in v
            )
            total_records_processed += batch_records
            logger.info(f"Batch {result['batch_number']}: {batch_records} records processed")
    
    logger.info(f"Total records processed across all batches: {total_records_processed}")
    
    # Check if there are remaining batches
    remaining_batches = len(batches) - len(batches_to_process)
    if remaining_batches > 0:
        logger.info(f"Note: {remaining_batches} batches remaining for next job run")
    
    logger.info("Batch processing job completed successfully.")

if __name__ == "__main__":
    main()
    job.commit()