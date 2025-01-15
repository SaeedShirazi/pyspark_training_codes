from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "Example")

#Transformations

#map
#Applies a function to each element.
rdd = sc.parallelize([1, 2, 3])
mapped_rdd = rdd.map(lambda x: x * 2)  # [2, 4, 6]
print(mapped_rdd)
#filter
#Filters elements based on a condition.
filtered_rdd = rdd.filter(lambda x: x > 2)  # [3]
print(filtered_rdd)
#flatMap
#Flattens results into a single list
flat_mapped_rdd = rdd.flatMap(lambda x: [x, x*2])  # [1, 2, 2, 4, 3, 6]

#reduceByKey
#Aggregates values for each key.
pairs = sc.parallelize([("a", 1), ("a", 2)])
reduced = pairs.reduceByKey(lambda x, y: x + y)  # [("a", 3)]
print(reduced)
#distinct
#Removes duplicates.
distinct_rdd = rdd.distinct()  # [1, 2, 3]

#union
#Combines two RDDs.
union_rdd = rdd.union(sc.parallelize([4, 5]))  # [1, 2, 3, 4, 5]

#intersection
#Returns elements common to both RDDs.
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([2, 3, 4])
intersection_rdd = rdd1.intersection(rdd2)  # [2, 3]
print(intersection_rdd.collect())

#cartesian
#Returns all pairs of elements between two RDDs.
rdd1 = sc.parallelize([1, 2])
rdd2 = sc.parallelize(["a", "b"])
cartesian_rdd = rdd1.cartesian(rdd2)  # [(1, 'a'), (1, 'b'), (2, 'a'), (2, 'b')]
print(cartesian_rdd.collect())

#groupBy
#Groups elements based on a key function.
rdd = sc.parallelize([1, 2, 3, 4])
grouped_rdd = rdd.groupBy(lambda x: x % 2)  # Group by even/odd
print({k: list(v) for k, v in grouped_rdd.collect()})  # {1: [1, 3], 0: [2, 4]}


#Actions

#collect
#Retrieves all elements from the RDD.
print(rdd.collect())  # [1, 2, 3]

#count
#Returns the number of elements.
print(rdd.count())  # 3

#take
#Retrieves the first n elements.
print(rdd.take(2))  # [1, 2]

#reduce
#Aggregates elements using a function.
result = rdd.reduce(lambda x, y: x + y)  # 6

#first
#Returns the first element.
print(rdd.first())  # 1


