from pyspark.sql.window import Window
from pyspark.sql.functions import col

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Practice").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load the dataset
df = spark.read.csv("/opt/pyspark/pyspark_training_codes/sample_data.csv", header=True, inferSchema=True)

# Task 2: User-Defined Functions (UDFs)
# Define the UDF

def bonus_percentage(salary):
    return salary * 0.10

# Apply the UDF
df_with_bonus = df.withColumn("bonus_amount", bonus_percentage(col("salary")))
print("Task 2: DataFrame with Bonus Amount")
df_with_bonus.show()

# Task 3: Joining DataFrames
# Split the DataFrame into two smaller DataFrames
job_title = df.select("id", "name", "job_title")
salarys = df.select("id", "salary")

# Perform an Inner Join
joined_df = job_title.join(salarys, on="id", how="inner")
print("Task 3: Joined DataFrame")
joined_df.show()
