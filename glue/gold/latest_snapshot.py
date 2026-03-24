import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://coin-project-newone/transformed/"
TARGET_PATH = "s3://coin-project-newone/gold/latest_snapshot/"

df = spark.read.parquet(SOURCE_PATH)

required_cols = ["id", "ingest_ts_utc"]
missing_cols = [c for c in required_cols if c not in df.columns]

if missing_cols:
    raise Exception(f"Missing required columns in Silver data: {missing_cols}")

df = df.withColumn(
    "event_ts",
    F.coalesce(
        F.col("ingest_ts_utc").cast("timestamp"),
        F.to_timestamp("ingest_ts_utc"),
        F.to_timestamp(F.regexp_replace(F.regexp_replace(F.col("ingest_ts_utc"), "T", " "), "Z", ""))
    )
)

if "ingest_date" in df.columns and "ingest_hour" in df.columns:
    df = df.withColumn(
        "event_ts",
        F.coalesce(
            F.col("event_ts"),
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.col("ingest_date").cast("string"),
                    F.lpad(F.col("ingest_hour").cast("string"), 2, "0")
                ),
                "yyyy-MM-dd HH"
            )
        )
    )

df = df.filter(F.col("event_ts").isNotNull())

window_spec = Window.partitionBy("id").orderBy(F.col("event_ts").desc())

latest_snapshot_df = (
    df.withColumn("row_num", F.row_number().over(window_spec))
      .filter(F.col("row_num") == 1)
      .drop("row_num", "event_ts")
      .orderBy("id")
)

latest_snapshot_df.coalesce(1).write.mode("overwrite").parquet(TARGET_PATH)

job.commit()