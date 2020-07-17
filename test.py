from pyspark import SparkContext

logFile = "readme.md"
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
print(logData)
# numAs = logData.filter(lambda s: 'a' in s).count()
# numBs = logData.filter(lambda s: 'b' in s).count()
# print("Line with a:%i,lines with b :%i" % (numAs, numBs))