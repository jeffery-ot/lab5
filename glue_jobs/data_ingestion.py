import sys
import boto3
import pandas as pd
import logging
import json
from datetime import datetime
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import Dict, Optional, Tuple, List
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Constants for schema validation
SCHEMAS = {
    "order_items": {
        "required_columns": ["id", "order_id", "user_id", "days_since_prior_order", 
                             "product_id", "add_to_cart_order", "reordered", "order_timestamp"],
        "optional_columns": ["date"]
    },
    "orders": {
        "required_columns": ["order_num", "order_id", "user_id", "order_timestamp", "total_amount"],
        "optional_columns": ["date"]
    },
    "product_data": {
        "required_columns": ["product_id", "department_id", "department", "product_name"],
        "optional_columns": []
    }
}

SUPPORTED_EXTENSIONS = ['.csv', '.xlsx', '.xls']
MANIFEST_KEY = "manifest/processed_files.json"

# Batch processing configuration
BATCH_SIZE = 10  # Number of files per batch
MAX_WORKERS = 4  # Maximum parallel workers
SMALL_FILE_THRESHOLD_MB = 50  # Files smaller than this are prioritized for batching

# Hardcoded S3 paths
INPUT_PATH = "s3://lab5-raw/inputs/"
OUTPUT_PATH = "s3://lab5-processed/curated/"
ARCHIVE_PATH = "s3://lab5-raw/archived/"

# ========== UTILITIES ==========

def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip("/")

def detect_file_type(file_path: str) -> str:
    file_path = file_path.lower()
    if file_path.endswith(".csv"):
        return "csv"
    elif file_path.endswith((".xlsx", ".xls")):
        return "xlsx"
    raise ValueError(f"Unsupported file type: {file_path}")

def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB from S3"""
    try:
        bucket, key = parse_s3_path(file_path)
        s3 = boto3.client("s3")
        response = s3.head_object(Bucket=bucket, Key=key)
        size_bytes = response['ContentLength']
        return size_bytes / (1024 * 1024)  # Convert to MB
    except Exception as e:
        logger.warning(f"Could not get size for {file_path}: {e}")
        return 0

def ensure_bucket_exists(bucket: str):
    s3 = boto3.client("s3")
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket not in buckets:
        logger.info(f"Creating bucket: {bucket}")
        s3.create_bucket(Bucket=bucket)

def check_bucket_exists(bucket: str):
    s3 = boto3.client("s3")
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket not in buckets:
        raise ValueError(f"Input bucket '{bucket}' does not exist")

def list_s3_files(bucket: str, prefix: str) -> List[Dict[str, any]]:
    """List S3 files with metadata for batch optimization"""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if any(obj["Key"].endswith(ext) for ext in SUPPORTED_EXTENSIONS):
                file_path = f"s3://{bucket}/{obj['Key']}"
                files.append({
                    'path': file_path,
                    'size_mb': obj['Size'] / (1024 * 1024),
                    'last_modified': obj['LastModified']
                })
    return files

def read_file(spark, file_path: str) -> DataFrame:
    file_type = detect_file_type(file_path)
    if file_type == "csv":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    else:
        bucket, key = parse_s3_path(file_path)
        local_path = f"/tmp/{key.split('/')[-1]}"
        boto3.client("s3").download_file(bucket, key, local_path)
        df = pd.read_excel(local_path)
        return spark.createDataFrame(df)

def identify_data_type(df: DataFrame) -> Optional[str]:
    cols = set(df.columns)
    for dtype, schema in SCHEMAS.items():
        if set(schema["required_columns"]).issubset(cols):
            return dtype
    return None

def validate_data_quality(df: DataFrame, data_type: str) -> Dict[str, any]:
    schema = SCHEMAS[data_type]
    issues = []

    for col_name in schema["required_columns"]:
        if col_name in df.columns:
            nulls = df.filter(col(col_name).isNull() | (col(col_name) == "")).count()
            if nulls > 0:
                issues.append(f"Column '{col_name}' has {nulls} null/empty values")

    return {
        "data_type": data_type,
        "total_rows": df.count(),
        "issues": issues
    }

def write_batch_output(dfs_with_metadata: List[Tuple[DataFrame, str, List[str]]], 
                      data_type: str, output_base: str) -> str:
    """Write multiple DataFrames as a single batch output"""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = f"{output_base}/{data_type}/dt={timestamp}/"
    
    if len(dfs_with_metadata) == 1:
        # Single file - write directly
        df, _, _ = dfs_with_metadata[0]
        df.write.mode("overwrite").parquet(output_path)
    else:
        # Multiple files - union them with source tracking
        combined_dfs = []
        for df, source_file, _ in dfs_with_metadata:
            # Add source file column for traceability
            df_with_source = df.withColumn("source_file", lit(source_file))
            combined_dfs.append(df_with_source)
        
        # Union all DataFrames
        combined_df = combined_dfs[0]
        for df in combined_dfs[1:]:
            combined_df = combined_df.union(df)
        
        combined_df.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Wrote batch of {len(dfs_with_metadata)} files to: {output_path}")
    return output_path

def archive_files(file_paths: List[str], archive_prefix: str):
    """Archive multiple files"""
    s3 = boto3.client("s3")
    archived_files = []
    
    for source_path in file_paths:
        try:
            bucket, key = parse_s3_path(source_path)
            filename = key.split("/")[-1]
            archive_key = f"{archive_prefix}/{filename}"
            s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=archive_key)
            s3.delete_object(Bucket=bucket, Key=key)
            archived_files.append(source_path)
        except Exception as e:
            logger.error(f"Failed to archive {source_path}: {e}")
    
    logger.info(f"Archived {len(archived_files)} files")
    return archived_files

# ========== MANIFEST ==========

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

def update_manifest_batch(bucket: str, file_paths: List[str], data_type: str):
    """Update manifest with multiple files at once"""
    s3 = boto3.client("s3")
    manifest = read_manifest_json(bucket)
    
    for file_path in file_paths:
        new_record = {
            "file_path": file_path,
            "data_type": data_type,
            "processed_at": datetime.now().isoformat() + "Z"
        }
        manifest.append(new_record)
    
    local_path = "/tmp/manifest.json"
    with open(local_path, "w") as f:
        json.dump(manifest, f, indent=2)
    s3.upload_file(local_path, bucket, MANIFEST_KEY)
    logger.info(f"Updated manifest with {len(file_paths)} files")

# ========== BATCH PROCESSING ==========

def group_files_for_batching(files: List[Dict[str, any]]) -> Dict[str, List[List[str]]]:
    """Group files by data type and create batches"""
    # First, we need to peek at each file to determine its data type
    # This is a simplified approach - in production, you might want to cache this info
    file_groups = defaultdict(list)
    
    for file_info in files:
        file_groups['unknown'].append(file_info['path'])  # We'll determine type during processing
    
    # Create batches based on file size and count
    batches = defaultdict(list)
    current_batch = []
    current_batch_size = 0
    
    # Sort files by size (smaller files first for better batching)
    sorted_files = sorted(files, key=lambda x: x['size_mb'])
    
    for file_info in sorted_files:
        file_path = file_info['path']
        file_size = file_info['size_mb']
        
        # Start new batch if current batch is full or would be too large
        if (len(current_batch) >= BATCH_SIZE or 
            (current_batch_size + file_size > 500)):  # 500MB max batch size
            if current_batch:
                batches['mixed'].append(current_batch)
                current_batch = []
                current_batch_size = 0
        
        current_batch.append(file_path)
        current_batch_size += file_size
    
    # Add the last batch
    if current_batch:
        batches['mixed'].append(current_batch)
    
    return dict(batches)

def process_file_batch(spark, file_paths: List[str], output_base: str, 
                      archive_prefix: str, manifest_bucket: str) -> Dict[str, any]:
    """Process a batch of files"""
    logger.info(f"Processing batch of {len(file_paths)} files")
    
    try:
        # Group files by data type after reading
        data_type_groups = defaultdict(list)
        failed_files = []
        
        for file_path in file_paths:
            try:
                df = read_file(spark, file_path)
                data_type = identify_data_type(df)
                
                if not data_type:
                    failed_files.append((file_path, "Schema not recognized"))
                    continue
                
                quality = validate_data_quality(df, data_type)
                data_type_groups[data_type].append((df, file_path, quality))
                
            except Exception as e:
                failed_files.append((file_path, str(e)))
                logger.error(f"Failed to process {file_path}: {e}")
        
        # Process each data type group
        processed_files = []
        results = []
        
        for data_type, dfs_with_metadata in data_type_groups.items():
            try:
                # Write batch output
                output_path = write_batch_output(dfs_with_metadata, data_type, output_base)
                
                # Collect file paths for archiving and manifest update
                batch_file_paths = [file_path for _, file_path, _ in dfs_with_metadata]
                processed_files.extend(batch_file_paths)
                
                # Archive files
                archived_files = archive_files(batch_file_paths, archive_prefix)
                
                # Update manifest
                update_manifest_batch(manifest_bucket, archived_files, data_type)
                
                # Collect quality reports
                quality_reports = [quality for _, _, quality in dfs_with_metadata]
                
                results.append({
                    "status": "success",
                    "data_type": data_type,
                    "files": batch_file_paths,
                    "output_path": output_path,
                    "quality_reports": quality_reports
                })
                
            except Exception as e:
                logger.error(f"Failed to process batch for data type {data_type}: {e}")
                batch_file_paths = [file_path for _, file_path, _ in dfs_with_metadata]
                results.append({
                    "status": "error",
                    "data_type": data_type,
                    "files": batch_file_paths,
                    "error": str(e)
                })
        
        # Add failed files to results
        for file_path, error in failed_files:
            results.append({
                "status": "error",
                "files": [file_path],
                "error": error
            })
        
        return {
            "batch_status": "completed",
            "total_files": len(file_paths),
            "successful_files": len(processed_files),
            "failed_files": len(failed_files),
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        return {
            "batch_status": "failed",
            "total_files": len(file_paths),
            "error": str(e),
            "files": file_paths
        }

def process_batches_parallel(spark, batches: List[List[str]], output_base: str,
                           archive_prefix: str, manifest_bucket: str) -> List[Dict[str, any]]:
    """Process multiple batches in parallel using ThreadPoolExecutor"""
    results = []
    
    # For small number of batches, process sequentially to avoid overhead
    if len(batches) <= 2:
        for batch in batches:
            result = process_file_batch(spark, batch, output_base, archive_prefix, manifest_bucket)
            results.append(result)
        return results
    
    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batches))) as executor:
        future_to_batch = {
            executor.submit(process_file_batch, spark, batch, output_base, 
                          archive_prefix, manifest_bucket): batch 
            for batch in batches
        }
        
        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Batch failed with exception: {e}")
                results.append({
                    "batch_status": "failed",
                    "files": batch,
                    "error": str(e)
                })
    
    return results

# ========== MAIN ==========

def main():
    # args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    args = {"JOB_NAME": "batch-processing-job"}

    sc = SparkContext()
    
    # Set S3 access configs via Spark Hadoop configuration
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")  # override if using VPC/custom/localstack
    


    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    input_bucket, input_prefix = parse_s3_path(INPUT_PATH)
    output_bucket, output_prefix = parse_s3_path(OUTPUT_PATH)
    archive_bucket, archive_prefix = parse_s3_path(ARCHIVE_PATH)

    check_bucket_exists(input_bucket)
    ensure_bucket_exists(output_bucket)
    ensure_bucket_exists(archive_bucket)

    manifest_bucket = output_bucket
    processed_manifest = read_manifest_json(manifest_bucket)
    already_processed = {entry["file_path"] for entry in processed_manifest}

    # Get files with metadata for intelligent batching
    files_info = list_s3_files(input_bucket, input_prefix)
    logger.info(f"Found {len(files_info)} file(s)")

    # Filter out already processed files
    to_process = [f for f in files_info if f['path'] not in already_processed]
    logger.info(f"Processing {len(to_process)} new files")

    if not to_process:
        logger.info("No new files to process")
        job.commit()
        return

    # Group files into batches
    file_batches = group_files_for_batching(to_process)
    all_batches = []
    for data_type, batches in file_batches.items():
        all_batches.extend(batches)
    
    logger.info(f"Created {len(all_batches)} batches for processing")

    # Process batches
    batch_results = process_batches_parallel(
        spark, all_batches, OUTPUT_PATH, ARCHIVE_PATH, manifest_bucket
    )

    import builtins
    # Log summary
    total_files = builtins.sum(result.get('total_files', 0) for result in batch_results)
    successful_files = builtins.sum(result.get('successful_files', 0) for result in batch_results)
    failed_files = builtins.sum(result.get('failed_files', 0) for result in batch_results)
    
    logger.info(f"Batch processing completed:")
    logger.info(f"  Total files: {total_files}")
    logger.info(f"  Successful: {successful_files}")
    logger.info(f"  Failed: {failed_files}")
    logger.info(f"  Batches processed: {len(batch_results)}")

    # Log detailed results
    for i, result in enumerate(batch_results):
        logger.info(f"Batch {i+1}: {result}")

    job.commit()

if __name__ == "__main__":
    main()