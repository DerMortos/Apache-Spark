# 46 - Introduction to Spark Resilient Distributed Dataset (RDD) APIs
import sys
from collections import namedtuple

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from lib.logger import Log4J


SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])
if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("HelloRDD")
# sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()

sc = spark.sparkContext
logger = Log4J(spark)

# Pass the file location as a command-line argument to application
        # Run>Debug Configure
        # Parameters: data\sample.csv
if len(sys.argv) !=2:
    logger.error("Usage: HelloSpark <filename>")
    sys.exit(-1)


linesRDD = sc.textFile(sys.argv[1])
partitionedRDD = linesRDD.repartition(2)

# remove quotations from values
colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
filteredRDD = selectRDD. filter(lambda r: r.Age < 40)
kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

colsList = countRDD.collect()
for x in colsList:
    logger.info(x)