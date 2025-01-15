from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "Example")


pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 2)])
result = pairs.reduceByKey(lambda x, y: x + y)  # [("a", 3), ("b", 2)]
print(result.collect())


grouped = pairs.groupByKey()
print([(k, list(v)) for k, v in grouped.collect()])  # [("a", [1, 2]), ("b", [2])]
