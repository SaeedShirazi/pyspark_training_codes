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

#Task2

# Calculate 70% threshold
total_count = primary_df.count()
threshold = int(total_count * 0.7)

# Filter data based on `id`
filtered_df = primary_df.filter(primary_df.id <= threshold)

# Verify the filtered data
filtered_df.show(5)

#Task3

from pyspark.sql.functions import broadcast

# Perform broadcast join
joined_df = filtered_df.join(broadcast(secondary_df), on="region", how="inner")

# Verify the join result
joined_df.show(2)


