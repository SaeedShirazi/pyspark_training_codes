from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Pipeline").getOrCreate()
df = spark.read.csv("/opt/pyspark/pyspark_training_codes/sample_data.csv", header=True, inferSchema=True)
processed_df = df.filter(df["age"] > 30)
processed_df.write.mode("overwrite").option("header", "true").csv("/opt/pyspark/pyspark_training_codes/general_concepts/output_data")
