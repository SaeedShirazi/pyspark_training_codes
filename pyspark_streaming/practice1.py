from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, current_timestamp, rand

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySparkStreamingPractice") \
    .getOrCreate()

# Define schema for input data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("signup_date", StringType(), True),
    StructField("email", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("is_active", StringType(), True),
])

# Read streaming data
stream_df = spark.readStream.format("csv").option("header", True).schema(
    "id INT, name STRING, age INT, city STRING, salary FLOAT, signup_date STRING, email STRING, job_title STRING, is_active STRING"
).load("/opt/pyspark/pyspark_training_codes/pyspark_streaming/data/")

# Generate random eventTime by adding a random delta to the current timestamp
stream_df = stream_df.withColumn(
    "eventTime", 
    current_timestamp() + (rand() * 1000).cast("int").cast("interval second")  # Random time delta in seconds
)


# Add a watermark and create a windowed DataFrame
windowed_stream = stream_df \
    .withWatermark("eventTime", "2 seconds") \
    .groupBy(
        F.window("eventTime", "1 seconds").alias("window"),
        "job_title"
    ) \
    .agg(
        F.count("id").alias("count")
    )

# Write windowed stream to Parquet
windowed_stream_query = windowed_stream.writeStream \
    .format("parquet") \
    .option("path", "/opt/pyspark/pyspark_training_codes/pyspark_streaming/output") \
    .option("checkpointLocation", "/opt/pyspark/pyspark_training_codes/pyspark_streaming/checkpoint_windowed") \
    .outputMode("append") \
    .start()

# Wait for the windowed stream to process data
windowed_stream_query.awaitTermination(30)  # Adjust timeout if needed

# Load the materialized results from Parquet
static_windowed_df = spark.read.parquet("/opt/pyspark/pyspark_training_codes/pyspark_streaming/output")

# Check schema of static data to confirm window column
static_windowed_df.printSchema()

# If the 'window' column exists, extract start and end time
if 'window' in static_windowed_df.columns:
    static_windowed_df = static_windowed_df \
        .withColumn("window_start", static_windowed_df["window"].getField("start")) \
        .withColumn("window_end", static_windowed_df["window"].getField("end"))
else:
    raise Exception("Column `window` not found in static_windowed_df. Check the schema and data.")

# Join static results with another streaming DataFrame
joined_stream = stream_df \
    .join(
        static_windowed_df,
        (stream_df["job_title"] == static_windowed_df["job_title"]) &
        (stream_df["eventTime"].between(
            static_windowed_df["window_start"],
            static_windowed_df["window_end"]
        )),
        "left"
    ) \
    .withColumn(
        "is_outlier",
        F.when(stream_df["salary"] > 1.5 * static_windowed_df["count"], True).otherwise(False)
    )

# Write the joined stream to console for debugging
query = joined_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

# Await termination of the query
query.awaitTermination(30)  # Adjust timeout if needed

