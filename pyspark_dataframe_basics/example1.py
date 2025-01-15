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

#JSON:
#df = spark.read.json("data.json")
#df.show()

#Parquet:
#df = spark.read.parquet("data.parquet")
#df.show()
