# coding:utf-8
import sys, re
from operator import add
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
  
conf=SparkConf().setAppName("miniProject").setMaster("spark://localhost:7077")
spark = SparkSession\
  .builder\
  .appName("PythonWordCount")\
  .config(conf = conf)\
  .getOrCreate()

# Access the file  
sc = spark.sparkContext
a = sc.parallelize([1, 2, 3])
print(a.collect())
b = a.flatMap(lambda x: (x,x ** 2))
print(b.collect())
# counts = lines.flatMap(lambda x: x.split(',')) \
#   .map(lambda x: (x, 1)) \
#   .reduceByKey(add) \
#   .sortBy(lambda x: x[1], False)
# output = counts.collect()
# for (word, count) in output:
#   print("%s: %i" % (word, count))

spark.stop()