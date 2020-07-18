from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, Row

string_test = 'pyspark_test'
conf = SparkConf().setAppName(string_test).setMaster('hdfs://instance-2:8020')
sc=SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# 加载文本文件并转换成Row.
lines = sc.textFile("/tmp/examples/test.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# 将DataFrame注册为table.
schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.registerTempTable("people")

# 执行sql查询，查下条件年龄在13岁到19岁之间
teenagers = sqlContext.sql("SELECT name,age FROM people WHERE age >= 60 AND age <= 70")

# 将查询结果保存至hdfs中
teenagers.write.save("/tmp/examples/teenagers")