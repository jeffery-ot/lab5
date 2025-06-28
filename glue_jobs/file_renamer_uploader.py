import sys
import boto3
import os
import random
import string
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import os

def get_data_directory():
    """Dynamically find the data directory"""
    # Try different possible locations
    possible_paths = [
        "/home/glue_user/workspace/data/",  # Absolute path for Glue environment
        "data/",  # Relative to current directory
        "./data/",  # Explicit relative path
        os.path.join(os.getcwd(), "data/"),  # Current working directory + data
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"Found data directory at: {path}")
            return path
    
    # If none found, return the default and let the script handle it
    return "data/"

# Configuration
LOCAL_DATA_DIR = get_data_directory()
S3_TARGET_BUCKET = "lab5-raw"
S3_TARGET_PREFIX = "inputs/"
SUPPORTED_EXTENSIONS = ['.csv', '.xlsx', '.xls'] # Adjust if your data directory path is different
S3_TARGET_BUCKET = "lab5-raw"
S3_TARGET_PREFIX = "inputs/"
SUPPORTED_EXTENSIONS = ['.csv', '.xlsx', '.xls']

class FileRenamerUploader:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        
    def generate_random_suffix(self, length: int = 8) -> str:
        """Generate a random string of letters and numbers"""
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    
    def generate_random_timestamp(self) -> str:
        """Generate a timestamp-based suffix"""
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def create_random_filename(self, original_path: str) -> str:
        """Create a new random filename while preserving extension"""
        path_obj = Path(original_path)
        base_name = path_obj.stem
        extension = path_obj.suffix
        
        # Choose random naming strategy
        strategies = [
            f"{base_name}_{self.generate_random_suffix()}",
            f"{base_name}_{self.generate_random_timestamp()}",
            f"{base_name}_v{random.randint(1, 999)}",
            f"{base_name}_{random.choice(['updated', 'revised', 'new', 'latest', 'modified'])}_{self.generate_random_suffix(4)}",
            f"dataset_{self.generate_random_suffix(6)}_{base_name}",
        ]
        
        new_name = random.choice(strategies)
        return f"{new_name}{extension}"
    
    def find_data_files(self, directory: str) -> List[str]:
        """Find all CSV and XLSX files in the specified directory"""
        files = []
        logger.info(f"Looking for files in directory: {directory}")
        logger.info(f"Directory exists: {os.path.exists(directory)}")
        
        if os.path.exists(directory):
            all_files = os.listdir(directory)
            logger.info(f"All files in directory: {all_files}")
            
            for file in all_files:
                logger.info(f"Checking file: {file}")
                if any(file.lower().endswith(ext) for ext in SUPPORTED_EXTENSIONS):
                    full_path = os.path.join(directory, file)
                    files.append(full_path)
                    logger.info(f"Added file: {full_path}")
        else:
            logger.error(f"Directory does not exist: {directory}")
            
        return files
    
    def ensure_bucket_exists(self, bucket: str):
        """Ensure the target S3 bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=bucket)
            logger.info(f"Bucket {bucket} exists")
        except self.s3_client.exceptions.NoSuchBucket:
            logger.info(f"Creating bucket: {bucket}")
            self.s3_client.create_bucket(Bucket=bucket)
        except Exception as e:
            logger.error(f"Error checking/creating bucket {bucket}: {e}")
            raise
    
    def upload_file_with_new_name(self, local_file_path: str, bucket: str, prefix: str) -> Tuple[str, str]:
        """Upload file to S3 with a randomly generated name"""
        original_filename = os.path.basename(local_file_path)
        new_filename = self.create_random_filename(original_filename)
        s3_key = f"{prefix}{new_filename}"
        
        try:
            self.s3_client.upload_file(local_file_path, bucket, s3_key)
            s3_path = f"s3://{bucket}/{s3_key}"
            logger.info(f"Uploaded {original_filename} as {new_filename} to {s3_path}")
            return original_filename, s3_path
        except Exception as e:
            logger.error(f"Failed to upload {local_file_path}: {e}")
            raise
    
    def list_existing_s3_files(self, bucket: str, prefix: str) -> List[str]:
        """List existing files in S3 to show what's already there"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            files = []
            for obj in response.get('Contents', []):
                if any(obj['Key'].endswith(ext) for ext in SUPPORTED_EXTENSIONS):
                    files.append(f"s3://{bucket}/{obj['Key']}")
            return files
        except Exception as e:
            logger.warning(f"Could not list existing S3 files: {e}")
            return []
    
    def process_all_files(self) -> dict:
        """Main processing function"""
        results = {
            "uploaded_files": [],
            "errors": [],
            "summary": {}
        }
        
        # Ensure bucket exists
        self.ensure_bucket_exists(S3_TARGET_BUCKET)
        
        # List existing files in S3
        existing_files = self.list_existing_s3_files(S3_TARGET_BUCKET, S3_TARGET_PREFIX)
        logger.info(f"Found {len(existing_files)} existing files in S3:")
        for file in existing_files:
            logger.info(f"  - {file}")
        
        # Find local data files
        local_files = self.find_data_files(LOCAL_DATA_DIR)
        logger.info(f"Found {len(local_files)} local data files to upload:")
        for file in local_files:
            logger.info(f"  - {file}")
        
        if not local_files:
            logger.warning(f"No data files found in {LOCAL_DATA_DIR}")
            results["summary"]["warning"] = f"No data files found in {LOCAL_DATA_DIR}"
            return results
        
        # Upload each file with random name
        for local_file in local_files:
            try:
                original_name, s3_path = self.upload_file_with_new_name(
                    local_file, S3_TARGET_BUCKET, S3_TARGET_PREFIX
                )
                results["uploaded_files"].append({
                    "original_name": original_name,
                    "s3_path": s3_path,
                    "upload_time": datetime.now().isoformat()
                })
            except Exception as e:
                results["errors"].append({
                    "file": local_file,
                    "error": str(e)
                })
        
        # Summary
        results["summary"] = {
            "total_files_found": len(local_files),
            "successful_uploads": len(results["uploaded_files"]),
            "failed_uploads": len(results["errors"]),
            "target_location": f"s3://{S3_TARGET_BUCKET}/{S3_TARGET_PREFIX}"
        }
        
        return results

def main():
    """Main function for Glue job"""
    # For local testing, comment out the getResolvedOptions line
    # args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    args = {"JOB_NAME": "file-renamer-uploader"}
    
    # Initialize Glue context (even though we're not using Spark DataFrames)
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    
    try:
        # Create renamer/uploader instance
        uploader = FileRenamerUploader()
        
        # Process all files
        results = uploader.process_all_files()
        
        # Log results
        logger.info("=" * 50)
        logger.info("UPLOAD RESULTS SUMMARY")
        logger.info("=" * 50)
        
        summary = results["summary"]
        logger.info(f"Files found: {summary.get('total_files_found', 0)}")
        logger.info(f"Successful uploads: {summary.get('successful_uploads', 0)}")
        logger.info(f"Failed uploads: {summary.get('failed_uploads', 0)}")
        logger.info(f"Target location: {summary.get('target_location', 'N/A')}")
        
        if results["uploaded_files"]:
            logger.info("\nSuccessfully uploaded files:")
            for upload in results["uploaded_files"]:
                logger.info(f"  {upload['original_name']} -> {upload['s3_path']}")
        
        if results["errors"]:
            logger.error("\nFailed uploads:")
            for error in results["errors"]:
                logger.error(f"  {error['file']}: {error['error']}")
        
        # For testing: print results as JSON
        import json
        print("\n" + "="*50)
        print("DETAILED RESULTS (JSON):")
        print("="*50)
        print(json.dumps(results, indent=2))
        
    except Exception as e:
        logger.error(f"Job failed with error: {e}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()