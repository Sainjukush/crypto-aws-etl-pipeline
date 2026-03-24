import sys  
from awsglue.utils import getResolvedOptions 
from awsglue.context import GlueContext     
from pyspark.context import SparkContext    
from awsglue.job import Job     
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, DoubleType, IntegerType 

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_PATH", "SILVER_PATH", "INGEST_DATE", "INGEST_HOUR"]
)

job_name = args["JOB_NAME"]
raw_base = args["RAW_PATH"].rstrip("/")
silver_path = args["SILVER_PATH"].rstrip("/")
ingest_date = args["INGEST_DATE"]
ingest_hour = args["INGEST_HOUR"]

raw_path = f"{raw_base}/ingest_date={ingest_date}/ingest_hour={ingest_hour}/"
print("Reading files from:", raw_path)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(job_name, args)

df_raw = spark.read.json(raw_path)

if df_raw.rdd.isEmpty():
    print("No files found")
    job.commit()
    sys.exit(0)

df = df_raw.withColumn("coin", F.explode_outer("data"))

df = df.withColumn("ingest_date", F.lit(ingest_date))
df = df.withColumn("ingest_hour", F.lit(ingest_hour))

curated = df.select(
    F.col("ingest_ts_utc").alias("ingest_ts_utc"),
    F.col("ingest_date").alias("ingest_date"),
    F.col("ingest_hour").alias("ingest_hour"),
    F.col("source").cast(StringType()).alias("source"),
    F.col("endpoint").cast(StringType()).alias("endpoint"),
    F.col("vs_currency").cast(StringType()).alias("vs_currency"),

    F.col("coin.id").cast(StringType()).alias("id"),
    F.col("coin.symbol").cast(StringType()).alias("symbol"),
    F.col("coin.name").cast(StringType()).alias("nam˘ˍ¥swz6574es3e"),

    F.col("coin.current_price").cast(DoubleType()).alias("current_price"),
    F.col("coin.market_cap").cast(DoubleType()).alias("market_cap"),
    F.col("coin.market_cap_rank").cast(IntegerType()).alias("market_cap_rank"),
    F.col("coin.total_volume").cast(DoubleType()).alias("total_volume"),
    F.col("coin.high_24h").cast(DoubleType()).alias("high_24h"),
    F.col("coin.low_24h").cast(DoubleType()).alias("low_24h"),
    F.col("coin.price_change_24h").cast(DoubleType()).alias("price_change_24h"),
    F.col("coin.price_change_percentage_1h_in_currency").cast(DoubleType()).alias("price_change_percentage_1h_in_currency"),
    F.col("coin.price_change_percentage_24h_in_currency").cast(DoubleType()).alias("price_change_percentage_24h_in_currency"),
    F.col("coin.price_change_percentage_7d_in_currency").cast(DoubleType()).alias("price_change_percentage_7d_in_currency")
)
curated = curated.filter(F.col("id").isNotNull())

curated.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("ingest_date", "ingest_hour") \
    .save(silver_path)

print(f"Data written to {silver_path} for {ingest_date} hour {ingest_hour}")
job.commit()