import boto3
import json
import pandas as pd
from datetime import datetime
from urllib.parse import unquote_plus

s3 = boto3.client('s3')
stepfunctions = boto3.client('stepfunctions', region_name='us-east-1')

# Constants
STEP_FUNCTION_ARN = "arn:aws:states:us-east-1:123456789012:stateMachine:batch-processing-pipeline"
BAD_RECORDS_PREFIX = "badrecords"
LOG_BUCKET = "lab5-gluejobs"
LOG_PREFIX = "general-logs"
TARGET_BUCKET = "lab5-raw"

SCHEMAS = {
    "order_items": {
        "required_columns": ["id", "order_id", "user_id", "days_since_prior_order", 
                             "product_id", "add_to_cart_order", "reordered", "order_timestamp"]
    },
    "orders": {
        "required_columns": ["order_num", "order_id", "user_id", "order_timestamp", "total_amount"]
    },
    "product_data": {
        "required_columns": ["product_id", "department_id", "department", "product_name"]
    }
}

def lambda_handler(event, context):
    log_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "lambda_request_id": context.aws_request_id,
        "valid_files": [],
        "invalid_files": [],
        "validation_issues": {},
        "stepfunction_triggered": False,
        "stepfunction_arn": None,
        "errors": []
    }

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        print(f"Processing file: s3://{bucket}/{key}")

        try:
            local_path = f"/tmp/{key.split('/')[-1]}"
            s3.download_file(bucket, key, local_path)

            if key.lower().endswith(".csv"):
                df = pd.read_csv(local_path)
            elif key.lower().endswith((".xlsx", ".xls")):
                df = pd.read_excel(local_path)
            else:
                raise ValueError("Unsupported file extension")

            data_type = match_schema(df)
            if data_type:
                issues = validate_required_columns(df, data_type)
                if issues:
                    raise ValueError("Validation issues found")
                log_data["valid_files"].append({"key": key, "data_type": data_type})
            else:
                raise ValueError("Schema not recognized")

        except Exception as e:
            error_msg = f"Invalid file: {key}, Error: {str(e)}"
            print(error_msg)
            move_to_badrecords(bucket, key)
            log_data["invalid_files"].append(key)

            # Capture issues if available
            issues = validate_required_columns(df, data_type) if 'df' in locals() and data_type else [str(e)]
            log_data["validation_issues"][key] = issues
            log_data["errors"].append(error_msg)

    # Trigger Step Function only if valid files exist
    if log_data["valid_files"]:
        payload = {
            "bucket": bucket,
            "file_keys": [f["key"] for f in log_data["valid_files"]],
            "file_count": len(log_data["valid_files"]),
            "trigger_time": log_data["timestamp"],
            "lambda_request_id": log_data["lambda_request_id"]
        }

        try:
            response = stepfunctions.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                name=f"batch-processing-{int(datetime.utcnow().timestamp())}",
                input=json.dumps(payload)
            )
            log_data["stepfunction_triggered"] = True
            log_data["stepfunction_arn"] = response['executionArn']
            print(f"Step Function started: {response['executionArn']}")
        except Exception as e:
            error_msg = f"Step Function failed to start: {e}"
            log_data["errors"].append(error_msg)
            print(error_msg)

    # Write log to S3
    write_log_to_s3(log_data)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Lambda execution complete",
            "log_summary": {
                "valid_files": len(log_data["valid_files"]),
                "invalid_files": len(log_data["invalid_files"]),
                "step_function_triggered": log_data["stepfunction_triggered"]
            }
        })
    }

def match_schema(df: pd.DataFrame) -> str:
    cols = set(df.columns)
    for dtype, schema in SCHEMAS.items():
        if set(schema["required_columns"]).issubset(cols):
            return dtype
    return None

def validate_required_columns(df: pd.DataFrame, data_type: str) -> list:
    issues = []
    required = SCHEMAS[data_type]["required_columns"]

    # Check for missing columns
    missing = [col for col in required if col not in df.columns]
    if missing:
        issues.append(f"Missing required columns: {missing}")
        return issues  # no point checking further if columns are missing

    # Check for null or empty values
    for col in required:
        null_count = df[col].isnull().sum()
        empty_count = df[col].astype(str).eq('').sum()
        if null_count > 0 or empty_count > 0:
            issues.append(
                f"Column '{col}' has {null_count} nulls and {empty_count} empty strings"
            )
    return issues

def move_to_badrecords(bucket: str, key: str):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    bad_key = f"{BAD_RECORDS_PREFIX}/{timestamp}/{key.split('/')[-1]}"
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": key},
        Key=bad_key
    )
    s3.delete_object(Bucket=bucket, Key=key)
    print(f"Moved bad file to: s3://{bucket}/{bad_key}")

def write_log_to_s3(log_data: dict):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    log_key = f"{LOG_PREFIX}/lambda-log-{timestamp}.json"

    try:
        log_body = json.dumps(log_data, indent=2)
        s3.put_object(Bucket=LOG_BUCKET, Key=log_key, Body=log_body.encode("utf-8"))
        print(f"Log written to s3://{LOG_BUCKET}/{log_key}")
    except Exception as e:
        print(f"Failed to write log to S3: {e}")
