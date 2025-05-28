########################################################
#
# Create Date: 2024-03-11
# Design Name: DataSink
# Project: Apache Spark DataFrame Sink with Partitioning
# Description: Demonstrates reading Parquet files, writing DataFrames in Avro and JSON formats,
#              and managing partitions using PySpark. Includes partition count logging and
#              custom logger integration.
#
# Additional Comments: Requires PySpark and sample data files in the data directory. Includes custom logging via lib.logger.Log4j.
# Source: Master Apache Spark Programming in Python (PySpark) Using Free Databricks Community - Udemy Course
#           ## 55 DataSink writing dataframe & partitions
#
########################################################


# from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    flightTimeParquetDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save()

    logger.info("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDF.repartition(5)

    logger.info("Num PFartitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    # partitionedDF.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()