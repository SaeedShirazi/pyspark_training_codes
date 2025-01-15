from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Example").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("/opt/pyspark/pyspark_training_codes/sample_data.csv", header=True, inferSchema=True)

#Select and Filter
#Select Columns:
df.select("name", "age").show()

#Filter Rows:
df.filter(df.age > 30).show()

#GroupBy and Aggregate
#GroupBy:
df.groupBy("age").count().show()

#Aggregate:
df.groupBy("age").agg({"id": "count"}).show()

#Handling Missing or Corrupt Data
#Drop Rows with Null Values:
df.na.drop().show()

#Fill Missing Values:
df.na.fill({"age": 0}).show()

#Add a New Column:

df = df.withColumn("NewColumn", df.age * 2)
df.show()

#Rename Columns:

df = df.withColumnRenamed("age", "years")
df.show()


#Inspecting Schemas:
df.printSchema()

#Explaining Logical and Physical Plans:
df.select("name", "age").explain()
