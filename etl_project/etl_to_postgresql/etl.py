from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .config("spark.sql.broadcastTimeout", "1200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


#Task1

# Load datasets
primary_df = spark.read.csv("/home/saeed/PycharmProjects/pyspark_training_codes/etl_project/generate_data/primary_data.csv", header=True, inferSchema=True)
secondary_df = spark.read.csv("/home/saeed/PycharmProjects/pyspark_training_codes/etl_project/generate_data/secondary_data.csv", header=True, inferSchema=True)

# Show schemas
primary_df.printSchema()
secondary_df.printSchema()
