from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "Practice")

# Load the CSV file
rdd = sc.textFile("/opt/pyspark/pyspark_training_codes/sample_data.csv")

# Transformations
filtered_rdd = rdd.filter(lambda line: "Marketing" in line)
mapped_rdd = filtered_rdd.map(lambda line: line.split(",")[1])  # Extract second column
distinct_rdd = mapped_rdd.distinct()

# Actions
print("Filtered Lines:", filtered_rdd.take(5))
print("Distinct Names:", distinct_rdd.collect())
print("Total Rows:", distinct_rdd.count())
