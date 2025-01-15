from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "Example")

lookup_table = {"a": 1, "b": 2, "c": 3}
broadcast_var = sc.broadcast(lookup_table)

rdd = sc.parallelize(["a", "b", "c", "d"])
result = rdd.map(lambda x: broadcast_var.value.get(x, 0))
print(result.collect())  # Output: [1, 2, 3, 0]


accum = sc.accumulator(0)

def add_to_accum(x):
    global accum
    accum += x

rdd = sc.parallelize([1, 2, 3, 4])
rdd.foreach(add_to_accum)
print(accum.value)  # Output: 10
