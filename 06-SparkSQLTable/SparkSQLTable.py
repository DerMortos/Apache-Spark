########################################################
#
# Create Date: 2024-03-11
# Design Name: SparkSQLTable
# Project: Apache Spark SQL Table Management
# Description: Demonstrates reading Parquet files, creating a Hive database, and writing DataFrames as bucketed and
#              sorted tables using PySpark. Includes custom logging via lib.logger.Log4j.
#
# Additional Comments: Requires PySpark and sample data files in the data directory. Includes custom logging via lib.logger.Log4j.
# Source: Master Apache Spark Programming in Python (PySpark) Using Free Databricks Community - Udemy Course
#              57 Working with Spark SQL Tables
#
########################################################


# from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTable") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    # Create a database if it doesn't exist
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))