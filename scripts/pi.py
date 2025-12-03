import random
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("KautilyaPi").getOrCreate()

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

num_samples = 100000
count = spark.sparkContext.parallelize(range(0, num_samples)).filter(inside).count()

print("Pi is roughly %f" % (4.0 * count / num_samples))

spark.stop()
sys.exit(0)