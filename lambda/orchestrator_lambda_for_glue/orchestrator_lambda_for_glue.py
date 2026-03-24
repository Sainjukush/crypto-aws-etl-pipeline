import os, boto3
from datetime import datetime, timedelta, timezone

glue = boto3.client("glue")
s3 = boto3.client("s3")

def lambda_handler(event, context):
    job_name = os.environ["GLUE_JOB_NAME"]
    raw_path = os.environ["RAW_PATH"]
    silver_path = os.environ["SILVER_PATH"]

    prev = datetime.now(timezone.utc) - timedelta(hours=1)
    ingest_date = prev.strftime("%Y-%m-%d")
    ingest_hour = prev.strftime("%H")

    bucket = "coin-project-newone"
    prefix = f"raw/ingest_date={ingest_date}/ingest_hour={ingest_hour}/"

    check = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if "Contents" not in check:
        return {
            "status": "skipped",
            "reason": "raw_partition_missing",
            "prefix": f"s3://{bucket}/{prefix}"
        }

    resp = glue.start_job_run(
        JobName=job_name,
        Arguments={
            "--RAW_PATH": raw_path,
            "--SILVER_PATH": silver_path,
            "--INGEST_DATE": ingest_date,
            "--INGEST_HOUR": ingest_hour
        }
    )

    return {
        "status": "started",
        "job_run_id": resp["JobRunId"],
        "ingest_date": ingest_date,
        "ingest_hour": ingest_hour
    }