from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load the dataset
df = spark.read.csv("/opt/pyspark/pyspark_training_codes/sample_data.csv", header=True, inferSchema=True)

#Registering DataFrames as Temporary SQL Tables

# Registering a DataFrame as a temporary SQL table
df.createOrReplaceTempView("employee_table")

#Executing SQL Queries

# Querying the table
high_salary_employees = spark.sql("SELECT * FROM employee_table WHERE salary > 900")
high_salary_employees.show()
