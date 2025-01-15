from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "Example")

rdd = sc.parallelize([1,2,3,4,5], numSlices=2)
print(rdd.getNumPartitions()) #Output: 2


pairs = sc.parallelize([("a",1), ("b", 2), ("a", 2)])
partiotioned = pairs.partitionBy(2)
print(partiotioned.getNumPartitions()) #Output: 2
