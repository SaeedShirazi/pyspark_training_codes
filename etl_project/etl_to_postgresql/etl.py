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


#Task4

from pyspark.sql.functions import col, year

# Add transformations
transformed_df = joined_df.withColumn(
    "spending_per_year", col("total_spent") / (2025 - year(col("join_date")))
).filter(col("is_active") == True)

# Verify transformations
transformed_df.show(5)

#Task5

# PostgreSQL connection properties
pg_url = "jdbc:postgresql://localhost:5432/pyspark_training"
pg_properties = {
    "user": "pyspark_user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Write to PostgreSQL
transformed_df.write \
    .jdbc(url=pg_url, table="etl_transformed_data", mode="overwrite", properties=pg_properties)


#Task 6
#sudo adduser pyspark_user
#sudo su - pyspark_user
#psql -U pyspark_user -d pyspark_training
#SELECT * FROM etl_transformed_data LIMIT 10;

# SELECT count(*) FROM etl_transformed_data LIMIT 10;
#  count
# -------
#  34931
# (1 row)
#
# pyspark_training=# SELECT max(id) FROM etl_transformed_data LIMIT 10;
#   max
# -------
#  70000
# (1 row)
