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
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
except Exception:
    args = {'JOB_NAME': 'local_test'}

# Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 paths
INPUT_PATH = "s3://lab5-processed/curated/"
OUTPUT_PATH = "s3://lab5-lakehouse-dwh/"
ARCHIVE_PATH = "s3://lab5-processed/archived-curated/"
REJECTED_PATH = "s3://lab5-lakehouse-dwh/rejected_records/"
MANIFEST_KEY = "manifest/transformation_processed.json"

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


# Utility, validation, transformation, and manifest functions here...
# Copy all the utility and transformation functions from the previous script.
# (For brevity, you can place all the previously shown functions without change.)

def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip("/")

def ensure_bucket_exists(bucket: str):
    s3 = boto3.client("s3")
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket not in buckets:
        logger.info(f"Creating bucket: {bucket}")
        s3.create_bucket(Bucket=bucket)

def list_latest_partitions(bucket: str, prefix: str) -> Dict[str, str]:
    """Find latest partition for each data type"""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    
    partitions = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for prefix_info in page.get("CommonPrefixes", []):
            table_path = prefix_info["Prefix"]
            table_name = table_path.split("/")[-2]
            
            # Find latest dt partition
            table_paginator = s3.get_paginator("list_objects_v2")
            latest_dt = ""
            for table_page in table_paginator.paginate(Bucket=bucket, Prefix=table_path, Delimiter="/"):
                for dt_prefix in table_page.get("CommonPrefixes", []):
                    dt_path = dt_prefix["Prefix"]
                    if "dt=" in dt_path:
                        dt_value = dt_path.split("dt=")[1].rstrip("/")
                        if dt_value > latest_dt:
                            latest_dt = dt_value
            
            if latest_dt:
                partitions[table_name] = f"s3://{bucket}/{table_path}dt={latest_dt}/"
    
    return partitions

def archive_partition(source_path: str, archive_base: str, table_name: str):
    """Archive processed partition"""
    s3 = boto3.client("s3")
    bucket, key_prefix = parse_s3_path(source_path)
    archive_bucket, archive_prefix = parse_s3_path(archive_base)
    
    dt_value = source_path.split("dt=")[1].rstrip("/")
    archive_key = f"{archive_prefix}{table_name}/dt={dt_value}/"
    
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            source_key = obj["Key"]
            dest_key = source_key.replace(key_prefix, archive_key)
            s3.copy_object(
                Bucket=archive_bucket, 
                CopySource={"Bucket": bucket, "Key": source_key}, 
                Key=dest_key
            )
    
    logger.info(f"Archived {table_name} partition to {archive_base}{table_name}/dt={dt_value}/")

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
                df = df.withColumn(f"partition_date", date_format(col(col_name), "yyyy-MM-dd"))
        
        if "partition_date" in df.columns:
            writer = writer.partitionBy("partition_date")
    
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

def update_manifest_json(bucket: str, processed_partitions: Dict[str, str]):
    s3 = boto3.client("s3")
    manifest = read_manifest_json(bucket)
    
    for table_name, partition_path in processed_partitions.items():
        new_record = {
            "table_name": table_name,
            "partition_path": partition_path,
            "processed_at": datetime.now().isoformat() + "Z"
        }
        manifest.append(new_record)
    
    local_path = "/tmp/transformation_manifest.json"
    with open(local_path, "w") as f:
        json.dump(manifest, f, indent=2)
    s3.upload_file(local_path, bucket, MANIFEST_KEY)
    logger.info("Updated transformation manifest")

# ========== SPARK SQL TRANSFORMATIONS ==========

def create_temp_views(spark, input_partitions: Dict[str, str]):
    """Create temporary views for Spark SQL"""
    for table_name, partition_path in input_partitions.items():
        df = spark.read.parquet(partition_path)
        df.createOrReplaceTempView(f"raw_{table_name}")
        logger.info(f"Created temp view: raw_{table_name}")

def transform_product_data(spark) -> DataFrame:
    """Transform product_data using Spark SQL"""
    sql = """
    SELECT 
        CAST(product_id AS INTEGER) as product_id,
        CAST(department_id AS INTEGER) as category_id,
        TRIM(REGEXP_REPLACE(product_name, 'product_\\\\d+_name\\\\s*=\\\\s*', '')) as product_name
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
        order_num,
        CAST(order_id AS INTEGER) as order_id,
        CAST(user_id AS INTEGER) as user_id,
        CAST(order_timestamp AS TIMESTAMP) as order_timestamp,
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

# ========== MAIN PROCESSING ==========

def process_normalization(spark, input_partitions: Dict[str, str], output_base: str) -> Dict[str, str]:
    """Process all table transformations with data quality"""
    results = {}
    
    # Create temporary views
    create_temp_views(spark, input_partitions)
    
    # Process in dependency order: category -> users -> product_data -> orders -> order_items
    
    # 1. Category table (no dependencies)
    if "product_data" in input_partitions:
        logger.info("Processing category table")
        category_raw = create_category_table(spark)
        category_clean, category_rejected = validate_schema_and_quality(category_raw, "category")
        results["category"] = write_delta_table(category_clean, "category", output_base, "overwrite")
    
    # 2. Users table (no dependencies)  
    if "orders" in input_partitions:
        logger.info("Processing users table")
        users_raw = create_users_table(spark)
        users_clean, users_rejected = validate_schema_and_quality(users_raw, "users")
        results["users"] = write_delta_table(users_clean, "users", output_base, "merge")
    
    # 3. Product data (depends on category)
    if "product_data" in input_partitions:
        logger.info("Processing product_data table")
        product_raw = transform_product_data(spark)
        product_clean, product_rejected = validate_schema_and_quality(product_raw, "product_data")
        product_final, product_fk_rejected = check_referential_integrity(spark, "product_data", product_clean)
        results["product_data"] = write_delta_table(product_final, "product_data", output_base, "merge")
    
    # 4. Orders (depends on users)
    if "orders" in input_partitions:
        logger.info("Processing orders table")
        orders_raw = transform_orders(spark)
        orders_clean, orders_rejected = validate_schema_and_quality(orders_raw, "orders")
        orders_final, orders_fk_rejected = check_referential_integrity(spark, "orders", orders_clean)
        results["orders"] = write_delta_table(orders_final, "orders", output_base, "merge")
    
    # 5. Order items (depends on orders and products)
    if "order_items" in input_partitions:
        logger.info("Processing order_items table")
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
    ensure_bucket_exists(input_bucket)

    logger.info("Listing latest partitions for curated data...")
    input_partitions = list_latest_partitions(input_bucket, input_prefix)
    if not input_partitions:
        logger.warning("No new partitions found to process.")
        return

    logger.info(f"Found partitions: {json.dumps(input_partitions, indent=2)}")

    logger.info("Starting transformation process...")
    processed_paths = process_normalization(spark, input_partitions, OUTPUT_PATH)

    logger.info("Validating transformations...")
    validation_results = validate_transformations(spark, processed_paths)
    logger.info(json.dumps(validation_results, indent=2))

    logger.info("Archiving processed data...")
    for table_name, partition_path in input_partitions.items():
        archive_partition(partition_path, ARCHIVE_PATH, table_name)

    logger.info("Updating manifest...")
    update_manifest_json(input_bucket, input_partitions)

    logger.info("Job completed successfully.")

if __name__ == "__main__":
    main()
    job.commit()
