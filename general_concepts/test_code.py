import sys

print("Python executable:", sys.executable)


from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "IntroApp")

# Create an RDD
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Transformation and action
result = rdd.map(lambda x: x * 2).collect()
print(result)

sc.stop()
