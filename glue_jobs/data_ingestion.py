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
from pyspark.sql.functions import col, lit, trim, lower, regexp_replace # Added regexp_replace
from pyspark.sql.window import Window
from typing import Dict, Optional, Tuple, List
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.types import StructType, StructField, StringType, IntegerType # Import Spark SQL types

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
        "required_columns": ["product_id", "department", "product_name"],
        "optional_columns": []
    }
}

SUPPORTED_EXTENSIONS = ['.csv', '.xlsx', '.xls']
MANIFEST_KEY = "manifest/processed_files.json"
DEPARTMENT_MAPPING_KEY = "manifest/department_mappings.json"

# Batch processing configuration
BATCH_SIZE = 10  # Number of files per batch
MAX_WORKERS = 4  # Maximum parallel workers
SMALL_FILE_THRESHOLD_MB = 50  # Files smaller than this are prioritized for batching

# Hardcoded S3 paths
INPUT_PATH = "s3://lab5-raw/inputs/"
OUTPUT_PATH = "s3://lab5-processed/curated/"
ARCHIVE_PATH = "s3://lab5-raw/archived/"

# ========== UTILITIES ==========

def validate_configuration():
    """Validate configuration before processing"""
    logger.info("Validating configuration...")
    
    # Check SCHEMAS constant
    for dtype, schema in SCHEMAS.items():
        if not isinstance(schema.get("required_columns"), list):
            raise ValueError(f"Invalid schema for {dtype}: required_columns must be a list")
        if not isinstance(schema.get("optional_columns"), list):
            raise ValueError(f"Invalid schema for {dtype}: optional_columns must be a list")
    
    logger.info("Configuration validation passed")

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
        # For Excel, download locally and then read with pandas, then convert to Spark DataFrame
        bucket, key = parse_s3_path(file_path)
        local_path = f"/tmp/{key.split('/')[-1]}"
        logger.info(f"Downloading {file_path} to {local_path}")
        boto3.client("s3").download_file(bucket, key, local_path)
        
        # Read with pandas and ensure column names are cleaned
        df_pd = pd.read_excel(local_path)
        df_pd.columns = [str(col_name).strip() for col_name in df_pd.columns] # Clean column names

        # Define a schema for product_data if applicable, for more robust conversion
        # This part assumes we are primarily dealing with product data files here.
        # If read_file is generic, this specific schema might need to be passed in or inferred dynamically.
        # For this context, assuming product_data schema.
        if "product_id" in df_pd.columns and "department" in df_pd.columns and "product_name" in df_pd.columns:
            excel_schema = StructType([
                StructField("product_id", IntegerType(), True),
                StructField("department", StringType(), True),
                StructField("product_name", StringType(), True)
                # Add other expected columns if necessary, with nullable=True for safety
            ])
            try:
                spark_df = spark.createDataFrame(df_pd, schema=excel_schema)
                logger.info(f"Spark DataFrame created from {file_path} with explicit schema.")
            except Exception as e:
                logger.warning(f"Failed to create Spark DataFrame with explicit schema for {file_path}: {e}. Falling back to inferred schema.", exc_info=True)
                spark_df = spark.createDataFrame(df_pd) # Fallback to inferred
        else:
            spark_df = spark.createDataFrame(df_pd) # Infer schema if it doesn't match product_data
            
        logger.info(f"Spark DataFrame created from {file_path}. Columns: {spark_df.columns}")
        spark_df.printSchema() # Debug: print schema after conversion
        return spark_df

def read_department_mappings(bucket: str) -> Dict[str, int]:
    """Read existing department mappings from S3"""
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=DEPARTMENT_MAPPING_KEY)
        content = obj["Body"].read().decode("utf-8")
        mappings = json.loads(content)
        # Ensure keys are lowercase for consistency with 'department_cleaned'
        cleaned_mappings = {k.lower(): v for k, v in mappings.items()}
        logger.info(f"Read {len(cleaned_mappings)} existing department mappings from S3 (keys lowercased).")
        return cleaned_mappings
    except s3.exceptions.NoSuchKey:
        logger.info("No existing department mappings found, starting fresh.")
        return {}
    except Exception as e:
        logger.warning(f"Failed to read department mappings: {e}. Returning empty mappings.", exc_info=True)
        return {}

def save_department_mappings(bucket: str, mappings: Dict[str, int]):
    """Save department mappings to S3"""
    s3 = boto3.client("s3")
    local_path = "/tmp/department_mappings.json"
    
    # Ensure keys are saved as they are (already lowercased by read_department_mappings)
    # or you might want to save them in their original casing if that's preferred for the JSON itself.
    # For now, saving as lowercased keys for consistency with internal logic.
    with open(local_path, "w") as f:
        json.dump(mappings, f, indent=2)
    s3.upload_file(local_path, bucket, DEPARTMENT_MAPPING_KEY)
    logger.info(f"Saved department mappings with {len(mappings)} entries to S3.")

# ================================================================
# MODIFIED clean_product_data FUNCTION - Add more robust cleaning and logging
# ================================================================
def clean_product_data(spark, df: DataFrame, manifest_bucket: str) -> DataFrame:
    """Clean product data by generating consistent department IDs across all batches"""
    try:
        logger.info("Starting clean_product_data - generating consistent department IDs.")
        
        # Initial DataFrame check
        logger.info(f"Initial DataFrame columns: {df.columns}")
        df.printSchema() # Debug: print initial schema

        # 1. Validate 'department' column presence
        if "department" not in df.columns:
            logger.error("DataFrame does not have 'department' column. Cannot clean product data.")
            if "department_id" not in df.columns: # Ensure department_id exists if missing
                df = df.withColumn("department_id", lit(None).cast("int"))
            return df
        
        # 2. Drop existing 'department_id' if present, to ensure a fresh calculation
        if "department_id" in df.columns:
            logger.info("Dropping existing 'department_id' column to recalculate.")
            df = df.drop("department_id")
            logger.info(f"Columns after dropping old department_id: {df.columns}")
        
        # 3. Standardize the 'department' column:
        #    - Remove non-alphanumeric characters (keep spaces)
        #    - Trim leading/trailing spaces
        #    - Convert to lowercase
        df = df.withColumn(
            "department_cleaned", 
            trim(lower(regexp_replace(col("department"), "[^a-zA-Z0-9\\s]", ""))) # Remove special chars
        )
        logger.info(f"Added 'department_cleaned' column. Columns: {df.columns}")
        logger.info("Sample of 'department' and 'department_cleaned' columns before join:")
        # Show more rows to catch varied data
        df.select("product_id", "department", "department_cleaned").show(20, truncate=False)
        df.printSchema() # Debug: print schema after adding cleaned column

        # 4. Read existing department mappings from S3
        existing_mappings = read_department_mappings(manifest_bucket)
        logger.info(f"Loaded existing department mappings (keys are already lowercased): {existing_mappings}")
        
        # 5. Get distinct cleaned department names from the current batch
        distinct_cleaned_departments = [
            row.department_cleaned for row in df.select("department_cleaned").distinct().collect()
            if row.department_cleaned is not None and row.department_cleaned != ""
        ]
        logger.info(f"Distinct cleaned departments found in current batch for mapping: {distinct_cleaned_departments}")
        
        if not distinct_cleaned_departments:
            logger.warning("No valid (non-null/non-empty) departments found in data after cleaning. department_id will be NULL.")
            return df.drop("department_cleaned").withColumn("department_id", lit(None).cast("int"))
        
        # 6. Update mappings with new departments from the current batch
        updated_mappings = existing_mappings.copy()
        new_departments_added = []
        
        next_id = 1
        if updated_mappings: 
            next_id = max(updated_mappings.values()) + 1
        
        for dept_name in sorted(distinct_cleaned_departments): # Sort for consistent ID assignment
            if dept_name not in updated_mappings:
                updated_mappings[dept_name] = next_id
                new_departments_added.append(dept_name)
                logger.info(f"NEW MAPPING: Department '{dept_name}' assigned ID {next_id}.")
                next_id += 1
            else:
                logger.debug(f"EXISTING MAPPING: Department '{dept_name}' already mapped to ID {updated_mappings[dept_name]}.")
        
        # 7. Save updated mappings back to S3 if there were any changes
        if new_departments_added:
            save_department_mappings(manifest_bucket, updated_mappings)
            logger.info(f"Saved {len(new_departments_added)} new department mappings to S3. Total mappings: {len(updated_mappings)}")
        else:
            logger.info("No new departments in this batch; department mappings in S3 are up-to-date.")

        logger.info(f"Final combined mappings for join (dict form): {updated_mappings}")

        # 8. Create a Spark DataFrame from the *full set* of updated mappings
        all_mappings_list = [(dept, dept_id) for dept, dept_id in updated_mappings.items()]
        
        if not all_mappings_list:
            logger.warning("No department mappings available (even after processing current batch). department_id column will be NULL.")
            return df.drop("department_cleaned").withColumn("department_id", lit(None).cast("int"))

        mapping_schema = StructType([
            StructField("department_cleaned", StringType(), True), 
            StructField("department_id", IntegerType(), True)
        ])
        
        department_mapping_df = spark.createDataFrame(all_mappings_list, schema=mapping_schema)
        logger.info("Department mapping DataFrame created for join:")
        department_mapping_df.show(truncate=False) # Show the mapping DF content
        department_mapping_df.printSchema() # Debug: print schema of mapping DF

        # 9. Perform the Left Join
        logger.info(f"Attempting left join on 'department_cleaned'.")
        cleaned_df = df.join(department_mapping_df, on="department_cleaned", how="left")
        
        logger.info(f"DataFrame columns after left join: {cleaned_df.columns}")
        logger.info("Sample of joined DataFrame (department, department_cleaned, department_id - check for NULLs):")
        # Show ample rows and check for the specific columns
        cleaned_df.select("product_id", "department", "department_cleaned", "department_id").show(20, truncate=False)
        cleaned_df.printSchema() # Debug: print schema of result DF

        # 10. Drop the temporary 'department_cleaned' column
        cleaned_df = cleaned_df.drop("department_cleaned")
        logger.info(f"DataFrame columns after dropping 'department_cleaned': {cleaned_df.columns}")

        # 11. Final column reordering and safety check for 'department_id'
        final_columns = [col_name for col_name in cleaned_df.columns if col_name != "department_id"]
        
        if "department" in final_columns:
            dept_index = final_columns.index("department")
            final_columns.insert(dept_index + 1, "department_id")
        else:
            final_columns.append("department_id")
        
        cleaned_df = cleaned_df.select(*final_columns)
        logger.info(f"Final DataFrame columns after reordering: {cleaned_df.columns}")
        
        logger.info("Finished clean_product_data - successfully assigned department_ids.")
        return cleaned_df
        
    except Exception as e:
        logger.error(f"Critical error in clean_product_data: {e}", exc_info=True)
        # Ensure 'department_id' column exists with nulls as a fallback
        if "department_id" not in df.columns:
            df = df.withColumn("department_id", lit(None).cast("int"))
        if "department_cleaned" in df.columns:
            df = df.drop("department_cleaned")
        logger.error("Returning DataFrame with potentially NULL department_ids due to error.")
        return df
    
def identify_data_type(df: DataFrame) -> Optional[str]:
    """Identifies the data type of a DataFrame based on column presence."""
    try:
        cols = set(df.columns)
        logger.debug(f"DataFrame has columns: {list(cols)}") # Changed to debug for less verbosity
        
        for dtype, schema in SCHEMAS.items():
            required_columns = schema["required_columns"]
            
            if not isinstance(required_columns, list):
                logger.error(f"Invalid schema for {dtype}: required_columns is {type(required_columns)}")
                continue
                
            required_cols_set = set(required_columns)
            
            if required_cols_set.issubset(cols):
                logger.info(f"Identified data type: {dtype}")
                return dtype
        
        logger.warning(f"No matching data type found for columns: {list(cols)}")
        return None
        
    except Exception as e:
        logger.error(f"Error in identify_data_type: {e}", exc_info=True) # Added exc_info
        return None

def validate_data_quality(df: DataFrame, data_type: str) -> Dict[str, any]:
    """Validates data quality for a given DataFrame and data type."""
    try:
        schema = SCHEMAS[data_type]
        issues = []

        required_columns = schema["required_columns"]
        if not isinstance(required_columns, list):
            logger.error(f"Schema for {data_type} has invalid required_columns: {type(required_columns)}")
            return {
                "data_type": data_type,
                "total_rows": 0,
                "issues": ["Invalid schema configuration"]
            }

        for col_name in required_columns:
            if col_name in df.columns:
                try:
                    col_name_str = str(col_name)
                    nulls = df.filter(col(col_name_str).isNull() | (trim(col(col_name_str)) == "")).count() # Added trim for empty string check
                    if nulls > 0:
                        issues.append(f"Column '{col_name_str}' has {nulls} null/empty values")
                except Exception as e:
                    logger.error(f"Error checking column {col_name}: {e}", exc_info=True) # Added exc_info
                    issues.append(f"Error validating column '{col_name}': {str(e)}")
            else: # If a required column is completely missing
                issues.append(f"Required column '{col_name}' is missing from DataFrame.")

        return {
            "data_type": data_type,
            "total_rows": df.count(),
            "issues": issues
        }
    except Exception as e:
        logger.error(f"Error in validate_data_quality: {e}", exc_info=True) # Added exc_info
        return {
            "data_type": data_type,
            "total_rows": 0,
            "issues": [f"Validation error: {str(e)}"]
        }

def write_batch_output(dfs_with_metadata: List[Tuple[DataFrame, str, List[str]]], 
                      data_type: str, output_base: str, spark, manifest_bucket: str) -> str:
    """Write multiple DataFrames as a single batch output"""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = f"{output_base}/{data_type}/dt={timestamp}/"
    
    if len(dfs_with_metadata) == 1:
        df, _, _ = dfs_with_metadata[0]
        if data_type == "product_data":
            logger.info(f"Applying clean_product_data for single file batch of type {data_type}.")
            df = clean_product_data(spark, df, manifest_bucket)
        df.write.mode("overwrite").parquet(output_path)
    else:
        combined_dfs = []
        for df, source_file, _ in dfs_with_metadata:
            if data_type == "product_data":
                logger.info(f"Applying clean_product_data for file {source_file} in multi-file batch.")
                df = clean_product_data(spark, df, manifest_bucket)
            
            df_with_source = df.withColumn("source_file", lit(source_file))
            combined_dfs.append(df_with_source)
        
        combined_df = combined_dfs[0]
        for i, df_to_union in enumerate(combined_dfs[1:]):
            try:
                # Use unionByName for safer unions across different schemas (e.g., if columns are missing in some)
                combined_df = combined_df.unionByName(df_to_union, allowMissingColumns=True)
                logger.info(f"Successfully unioned DataFrame {i+1} to combined_df.")
            except Exception as e:
                logger.error(f"Error unioning DataFrame {i+1} in batch: {e}", exc_info=True)
                raise # Re-raise to fail the batch if unionByName fails
        
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
            archive_key = f"{archive_prefix}{filename}" # Changed to directly append filename
            logger.info(f"Archiving {key} to {archive_key}")
            s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=archive_key)
            s3.delete_object(Bucket=bucket, Key=key)
            archived_files.append(source_path)
        except Exception as e:
            logger.error(f"Failed to archive {source_path}: {e}", exc_info=True)
    
    logger.info(f"Archived {len(archived_files)} files")
    return archived_files

# ========== MANIFEST ==========

def read_manifest_json(bucket: str) -> List[Dict[str, str]]:
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)
        content = obj["Body"].read().decode("utf-8")
        manifest = json.loads(content)
        logger.info(f"Read {len(manifest)} entries from manifest file.")
        return manifest
    except s3.exceptions.NoSuchKey:
        logger.info("Manifest file not found, starting with empty manifest.")
        return []
    except Exception as e:
        logger.warning(f"Failed to read manifest: {e}. Returning empty manifest.", exc_info=True)
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
    logger.info(f"Updated manifest with {len(file_paths)} new entries.")

# ========== BATCH PROCESSING ==========

def group_files_for_batching(files: List[Dict[str, any]]) -> Dict[str, List[List[str]]]:
    """Group files by data type and create batches"""
    batches = defaultdict(list)
    current_batch = []
    current_batch_size = 0
    
    # Sort files by size (smaller files first for better packing into batches)
    sorted_files = sorted(files, key=lambda x: x['size_mb'])
    
    for file_info in sorted_files:
        file_path = file_info['path']
        file_size = file_info['size_mb']
        
        if len(current_batch) >= BATCH_SIZE or (current_batch_size + file_size > 500 and current_batch_size > 0):
            if current_batch:
                batches['mixed'].append(current_batch)
                current_batch = []
                current_batch_size = 0
        
        current_batch.append(file_path)
        current_batch_size += file_size
    
    if current_batch:
        batches['mixed'].append(current_batch)
    
    logger.info(f"Grouped files into {len(batches['mixed'])} 'mixed' batches.")
    return dict(batches)

def process_file_batch(spark, file_paths: List[str], output_base: str, 
                            archive_prefix: str, manifest_bucket: str) -> Dict[str, any]:
    """Process a batch of files with improved error handling"""
    logger.info(f"Starting to process batch of {len(file_paths)} files.")
    
    try:
        validate_configuration() 
        
        data_type_groups = defaultdict(list)
        failed_files = []
        
        for file_path in file_paths:
            try:
                logger.info(f"Attempting to read file: {file_path}")
                df = read_file(spark, file_path)
                
                if df.isEmpty():
                    logger.warning(f"File {file_path} is empty. Skipping.")
                    failed_files.append((file_path, "File is empty"))
                    continue

                logger.info(f"Successfully read file {file_path}. Columns: {df.columns}")
                
                data_type = identify_data_type(df)
                
                if not data_type:
                    failed_files.append((file_path, "Schema not recognized"))
                    logger.warning(f"File {file_path}: Schema not recognized. Skipping.")
                    continue
                
                logger.info(f"File {file_path} identified as data type: {data_type}")
                
                quality = validate_data_quality(df, data_type)
                logger.info(f"Quality report for {file_path}: {quality}")

                data_type_groups[data_type].append((df, file_path, quality))
                
            except Exception as e:
                failed_files.append((file_path, str(e)))
                logger.error(f"Failed to process file {file_path} within batch: {e}", exc_info=True)
        
        processed_files_count = 0
        results = []
        
        for data_type, dfs_with_metadata in data_type_groups.items():
            try:
                logger.info(f"Writing aggregated batch output for data type: {data_type} (contains {len(dfs_with_metadata)} DFs).")
                
                output_loc = write_batch_output(dfs_with_metadata, data_type, output_base, spark, manifest_bucket)
                
                batch_file_paths = [file_path for _, file_path, _ in dfs_with_metadata]
                processed_files_count += len(batch_file_paths)
                
                archived = archive_files(batch_file_paths, archive_prefix)
                update_manifest_batch(manifest_bucket, archived, data_type)
                
                quality_reports = [quality for _, _, quality in dfs_with_metadata]
                
                results.append({
                    "status": "success",
                    "data_type": data_type,
                    "files_processed": batch_file_paths,
                    "output_path": output_loc,
                    "quality_reports": quality_reports
                })
                
            except Exception as e:
                logger.error(f"Failed to write output or archive batch for data type {data_type}: {e}", exc_info=True)
                batch_file_paths = [file_path for _, file_path, _ in dfs_with_metadata]
                results.append({
                    "status": "error",
                    "data_type": data_type,
                    "files_attempted": batch_file_paths,
                    "error": str(e)
                })
        
        for file_path, error_msg in failed_files:
            results.append({
                "status": "error",
                "files_attempted": [file_path],
                "error": error_msg
            })
        
        return {
            "batch_status": "completed",
            "total_files_in_batch": len(file_paths),
            "successful_files_processed_in_batch": processed_files_count,
            "failed_files_in_batch": len(failed_files),
            "results_per_data_type": results
        }
        
    except Exception as e:
        logger.error(f"Overall batch processing failed: {e}", exc_info=True)
        return {
            "batch_status": "failed",
            "total_files_in_batch": len(file_paths),
            "error": str(e),
            "files_attempted": file_paths
        }

def process_batches_parallel(spark, batches: List[List[str]], output_base: str,
                           archive_prefix: str, manifest_bucket: str) -> List[Dict[str, any]]:
    """Process multiple batches in parallel using ThreadPoolExecutor"""
    results = []
    logger.info(f"Starting parallel processing of {len(batches)} batches with max_workers={MAX_WORKERS}.")
    
    if not batches:
        logger.info("No batches to process.")
        return []

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
                logger.info(f"Batch {batch[0].split('/')[-1]}... completed with status: {result.get('batch_status')}")
            except Exception as e:
                logger.error(f"Batch {batch[0].split('/')[-1]}... failed with unhandled exception: {e}", exc_info=True)
                results.append({
                    "batch_status": "failed",
                    "files_attempted": batch,
                    "error": f"Unhandled exception: {str(e)}"
                })
    
    logger.info("All parallel batches completed.")
    return results

# ========== MAIN ==========

def main():
    # args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    # Mock args for local testing or if not running in Glue
    args = {"JOB_NAME": "batch-processing-job"} 

    sc = SparkContext()
    
    # Set S3 access configs via Spark Hadoop configuration
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    # For localstack, uncomment and adjust endpoint
    # hadoop_conf.set("fs.s3a.endpoint", "http://localhost:4566")
    


    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    validate_configuration()

    input_bucket, input_prefix = parse_s3_path(INPUT_PATH)
    output_bucket, output_prefix = parse_s3_path(OUTPUT_PATH)
    archive_bucket, archive_prefix = parse_s3_path(ARCHIVE_PATH)

    check_bucket_exists(input_bucket)
    ensure_bucket_exists(output_bucket)
    ensure_bucket_exists(archive_bucket)

    manifest_bucket = output_bucket
    processed_manifest = read_manifest_json(manifest_bucket)
    already_processed = {entry["file_path"] for entry in processed_manifest}
    logger.info(f"Found {len(already_processed)} files already processed in manifest.")

    # Get files with metadata for intelligent batching
    files_info = list_s3_files(input_bucket, input_prefix)
    logger.info(f"Found {len(files_info)} file(s) in input path.")

    # Filter out already processed files
    to_process = [f for f in files_info if f['path'] not in already_processed]
    logger.info(f"Processing {len(to_process)} new files.")

    if not to_process:
        logger.info("No new files to process. Job finished.")
        job.commit()
        return

    # Group files into batches
    file_batches = group_files_for_batching(to_process)
    all_batches = []
    for data_type, batches in file_batches.items(): # data_type here will likely be 'mixed'
        all_batches.extend(batches)
    
    logger.info(f"Prepared {len(all_batches)} processing batches.")

    # Process batches
    batch_results = process_batches_parallel(
        spark, all_batches, OUTPUT_PATH, ARCHIVE_PATH, manifest_bucket
    )

   #import builtins
    # Log summary
    total_files_overall = sum(result.get('total_files_in_batch', 0) for result in batch_results)
    successful_files_overall = sum(result.get('successful_files_processed_in_batch', 0) for result in batch_results)
    failed_files_overall = sum(result.get('failed_files_in_batch', 0) for result in batch_results)
    
    logger.info(f"----- Batch Processing Summary -----")
    logger.info(f"  Total files considered: {total_files_overall}")
    logger.info(f"  Successfully processed files: {successful_files_overall}")
    logger.info(f"  Files that failed processing: {failed_files_overall}")
    logger.info(f"  Total batches initiated: {len(all_batches)}")
    logger.info(f"  Batches completed (either success or error): {len(batch_results)}")

    # Log detailed results of each batch
    logger.info("\n----- Detailed Batch Results -----")
    for i, result in enumerate(batch_results):
        logger.info(f"Batch {i+1} Status: {result.get('batch_status')}")
        logger.info(f"  Files in batch: {result.get('total_files_in_batch')}")
        logger.info(f"  Processed files: {result.get('successful_files_processed_in_batch')}")
        logger.info(f"  Failed files: {result.get('failed_files_in_batch')}")
        if result.get('error'):
            logger.error(f"  Batch Error: {result.get('error')}")
        if result.get('results_per_data_type'):
            for dtype_result in result['results_per_data_type']:
                logger.info(f"    Data Type '{dtype_result.get('data_type')}': Status={dtype_result.get('status')}, Files={len(dtype_result.get('files_processed', dtype_result.get('files_attempted', [])))}")
                if dtype_result.get('error'):
                    logger.error(f"      Sub-batch Error: {dtype_result.get('error')}")

    job.commit()

if __name__ == "__main__":
    main()