# coding:utf-8
import sys, re
from operator import add
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
  
if __name__ == "__main__":
    spark = SparkSession\
            .builder\
            .config(conf = SparkConf())\
            .getOrCreate()
    sc = spark.sparkContext
    a = sc.parallelize([1, 2, 3])
    b = a.flatMap(lambda x: (x,x ** 2))
    print(a.collect())
    print(b.collect())

    spark.stop()