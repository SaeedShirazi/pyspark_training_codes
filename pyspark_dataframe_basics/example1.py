from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Example").getOrCreate()

# Access SparkContext from SparkSession
sc = spark.sparkContext
spark.sparkContext.setLogLevel("WARN")


#From Lists:
data = [(1, "Alice", 29), (2, "Bob", 35), (3, "Cathy", 25)]
columns = ["ID", "Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()

#From Dictionaries:
data = [{"ID": 1, "Name": "Alice", "Age": 29},
        {"ID": 2, "Name": "Bob", "Age": 35},
        {"ID": 3, "Name": "Cathy", "Age": 25}]
        
df = spark.read.json(sc.parallelize(data))
df.show()


#CSV:
df = spark.read.csv("/opt/pyspark/pyspark_training_codes/sample_data.csv", header=True, inferSchema=True)
df.show()

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CSV to Parquet and JSON") \
    .getOrCreate()


parquet_output_path = "/opt/pyspark/pyspark_training_codes/"
df.write.mode("overwrite").parquet(parquet_output_path)


json_output_path = "/opt/pyspark/pyspark_training_codes/"
df.write.mode("overwrite").json(json_output_path)

# Read the Parquet file back into a DataFrame
df_parquet = spark.read.parquet(parquet_output_path)

# Read the JSON file back into a DataFrame
df_json = spark.read.json(json_output_path)

# Show data from Parquet DataFrame
print("Data from Parquet:")
df_parquet.show()

# Show data from JSON DataFrame
print("Data from JSON:")
df_json.show()

# Stop the SparkSession
spark.stop()
