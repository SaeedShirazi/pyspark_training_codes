from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, rand

spark = SparkSession.builder.appName("FileStreamExample").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Monitor a directory for new CSV files
stream_df = spark.readStream.format("csv").option("header", True).schema(
    "id INT, name STRING, age INT, city STRING, salary FLOAT, signup_date STRING, email STRING, job_title STRING, is_active STRING"
).load("/opt/pyspark/pyspark_training_codes/pyspark_streaming/data/")

# Generate random eventTime by adding a random delta to the current timestamp
stream_df = stream_df.withColumn(
    "eventTime", 
    current_timestamp() + (rand() * 1000).cast("int").cast("interval second")  # Random time delta in seconds
)

# Apply watermark and aggregation
watermarked_stream = stream_df.withWatermark("eventTime", "1 seconds") \
    .groupBy("job_title", "eventTime") \
    .count()

# Writing the stream to both console and a Parquet directory
query = watermarked_stream.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/opt/pyspark/pyspark_training_codes/pyspark_streaming/output") \
    .option("checkpointLocation", "/opt/pyspark/pyspark_training_codes/pyspark_streaming/checkpoint") \
    .start()

# Also outputting to the console for testing
console_query = watermarked_stream.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "/opt/pyspark/pyspark_training_codes/pyspark_streaming/checkpoint_console") \
    .start()

# Await termination to keep the query running
query.awaitTermination()

