from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load the dataset
df = spark.read.csv("/opt/pyspark/pyspark_training_codes/sample_data.csv", header=True, inferSchema=True)

# Define a UDF to categorize salary
def categorize_salary(salary):
    if salary > 900:
        return "High"
    elif salary > 700:
        return "Medium"
    else:
        return "Low"

# Register the UDF
categorize_salary_udf = udf(categorize_salary, StringType())

# Apply the UDF
df_with_category = df.withColumn("salary_category", categorize_salary_udf(col("salary")))
df_with_category.show()
