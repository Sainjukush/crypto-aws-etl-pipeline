import sys 
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://coin-project-newone/transformed/"
TARGET_PATH = "s3://coin-project-newone/gold/daily_aggregates/"

df = spark.read.parquet(SOURCE_PATH)

required_cols = [
    "id",
    "symbol",
    "ingest_date",
    "current_price",
    "total_volume",
    "market_cap"
]

missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    raise Exception(f"Missing required columns in Silver data: {missing_cols}")

clean_df = (
    df.select(
        F.col("id").cast("string").alias("id"),
        F.col("symbol").cast("string").alias("symbol"),
        F.col("ingest_date").cast("string").alias("ingest_date"),
        F.col("current_price").cast("double").alias("current_price"),
        F.col("total_volume").cast("double").alias("total_volume"),
        F.col("market_cap").cast("double").alias("market_cap ")
    )
    .filter(F.col("id").isNotNull())
    .filter(F.col("symbol").isNotNull())
    .filter(F.col("ingest_date").isNotNull())
)

daily_agg_df = (
    clean_df.groupBy("id", "symbol", "ingest_date")
    .agg(
        F.avg("current_price").alias("avg_price"),
        F.max("current_price").alias("max_price"),
        F.min("current_price").alias("min_price"),
        F.avg("total_volume").alias("avg_total_volume"),
        F.avg("market_cap").alias("avg_market_cap"),
        F.count("*").alias("record_count")
    )
)

(
    daily_agg_df.write
    .mode("overwrite")
    .partitionBy("ingest_date")
    .parquet(TARGET_PATH)
)

print("Daily aggregates full refresh completed successfully.")
print(f"Source: {SOURCE_PATH}")
print(f"Target: {TARGET_PATH}")

job.commit()