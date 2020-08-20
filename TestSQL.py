# coding:utf-8
import sys, re
from operator import add
from pyspark.sql import SparkSession
  
spark = SparkSession\
  .builder\
  .appName("PythonWordCount")\
  .getOrCreate()

# Access the file  
sc = spark.sparkContext
a = sc.parallelize([1, 2, 3])
b = a.flatMap(lambda x: (x,x ** 2))
print(a.collect())
print(b.collect())
# counts = lines.flatMap(lambda x: x.split(',')) \
#   .map(lambda x: (x, 1)) \
#   .reduceByKey(add) \
#   .sortBy(lambda x: x[1], False)
# output = counts.collect()
# for (word, count) in output:
#   print("%s: %i" % (word, count))

spark.stop()