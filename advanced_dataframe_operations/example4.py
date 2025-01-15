from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, sum


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load the dataset
df = spark.read.csv("/opt/pyspark/pyspark_training_codes/sample_data.csv", header=True, inferSchema=True)

# Define a window
window_spec = Window.partitionBy("job_title").orderBy(col("salary").desc())

# Rank employees by salary within their job_title
ranked_df = df.withColumn("Rank", rank().over(window_spec))
ranked_df.show()

# Cumulative sum of salary
cumulative_salary_df = df.withColumn("cumulative_salary", sum("salary").over(window_spec))
cumulative_salary_df.show()
