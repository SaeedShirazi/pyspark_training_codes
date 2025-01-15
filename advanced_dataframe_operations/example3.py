from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load the dataset
df = spark.read.csv("/opt/pyspark/pyspark_training_codes/sample_data.csv", header=True, inferSchema=True)

df1 = df.select("id", "name", "job_title")
df2 = df.select("id", "salary")

# Inner join
inner_join_df = df1.join(df2, on="id", how="inner")
inner_join_df.show()

# Left join
left_join_df = df1.join(df2, on="id", how="left")
left_join_df.show()

#Avoiding Shuffles in Joins

#Broadcast Joins: Use when one DataFrame is small.
from pyspark.sql.functions import broadcast
broadcast_join_df = df1.join(broadcast(df2), on="id", how="inner")
