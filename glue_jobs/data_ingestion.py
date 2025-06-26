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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
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

def list_s3_files(bucket: str, prefix: str) -> List[str]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if any(obj["Key"].endswith(ext) for ext in SUPPORTED_EXTENSIONS):
                files.append(f"s3://{bucket}/{obj['Key']}")
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

def write_output(df: DataFrame, data_type: str, output_base: str) -> str:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = f"{output_base}/{data_type}/dt={timestamp}/"
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Wrote {data_type} data to: {output_path}")
    return output_path

def archive_file(source_path: str, archive_prefix: str):
    s3 = boto3.client("s3")
    bucket, key = parse_s3_path(source_path)
    filename = key.split("/")[-1]
    archive_key = f"{archive_prefix}/{filename}"
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=archive_key)
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info(f"Archived file {source_path} to s3://{bucket}/{archive_key}")

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

def update_manifest_json(bucket: str, file_path: str, data_type: str):
    s3 = boto3.client("s3")
    manifest = read_manifest_json(bucket)
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
    logger.info(f"Updated manifest with: {file_path}")

# ========== PROCESSING ==========

def process_file(spark, file_path: str, output_base: str, archive_prefix: str,
                 manifest_bucket: str) -> Dict[str, any]:
    logger.info(f"Processing: {file_path}")
    try:
        df = read_file(spark, file_path)
        data_type = identify_data_type(df)
        if not data_type:
            raise ValueError("Schema not recognized")

        quality = validate_data_quality(df, data_type)
        write_output(df, data_type, output_base)
        archive_file(file_path, archive_prefix)
        update_manifest_json(manifest_bucket, file_path, data_type)

        return {
            "status": "success",
            "file": file_path,
            "data_type": data_type,
            "quality_report": quality
        }
    except Exception as e:
        logger.error(f"Failed to process {file_path}: {str(e)}")
        return {
            "status": "error",
            "file": file_path,
            "error": str(e)
        }

# ========== MAIN ==========

def main():
    # Replaced this:
    # args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    args = {"JOB_NAME": "local-job"}


    sc = SparkContext()
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

    files = list_s3_files(input_bucket, input_prefix)
    logger.info(f"Found {len(files)} file(s). Skipping already processed ones.")

    to_process = [f for f in files if f not in already_processed]

    for file_path in to_process:
        result = process_file(
            spark,
            file_path,
            OUTPUT_PATH,
            ARCHIVE_PATH,
            manifest_bucket
        )
        logger.info(result)

    job.commit()

if __name__ == "__main__":
    main()
