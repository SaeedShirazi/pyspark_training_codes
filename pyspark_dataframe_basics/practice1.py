from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, year, current_date, datediff

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark DataFrame") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load the dataset
df = spark.read.csv("/opt/pyspark/pyspark_training_codes/sample_data.csv", header=True, inferSchema=True)

# Task 1: Filter and Clean Data
df_cleaned = df.na.drop(subset=["salary"])
df_cleaned = df_cleaned.withColumn("signup_date", col("signup_date").cast("date"))

# Task 2: job title Analysis
avg_salary_df = df_cleaned.groupBy("job_title").agg(avg("salary").alias("average_salary"))
highest_avg_salary = avg_salary_df.orderBy(col("average_salary").desc()).first()

print("job title with highest average salary:")
print(highest_avg_salary)

# Task 3: Joining Trends
joining_trends = df_cleaned.withColumn("signup_year", year(col("signup_date")))
yearly_count = joining_trends.groupBy("signup_year").count()
year_with_most_joinings = yearly_count.orderBy(col("count").desc()).first()

print("Year with the highest number of joinings:")
print(year_with_most_joinings)

# Task 4: Employee Insights
df_with_experience = df_cleaned.withColumn(
    "experience_years", (datediff(current_date(), col("signup_date")) / 365).cast("int")
)
experienced_employees = df_with_experience.filter(col("experience_years") >= 5)


# Task 5: Write the Result
experienced_employees.write.parquet("/opt/pyspark/pyspark_dataframe_basics/pyspark_training_codes/output", mode="overwrite")

# Show Final Result
experienced_employees.show()
