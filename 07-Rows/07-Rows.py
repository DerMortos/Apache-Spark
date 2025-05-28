##################################################################
##                                                              ##
## @file    07-Rows.py                                          ##
## @author  Norb Lara                                           ##
## @date    03/11/2025                                          ##
## @brief   60 DataFrame Rows and Unit Testing                  ##
##                                                              ##
##                                                              ##
##################################################################


from lib.logger import Log4j
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_date, col
from pyspark.sql import Row

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(fld, fmt))

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("Rows") \
        .getOrCreate()

    logger = Log4j(spark)

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())])

my_rows = [Row("123", "2023-10-01"), Row("124", "2023-10-01"), Row("125", "2023-10-01"), Row("126", "2023-10-01")]
my_rdd = spark.sparkContext.parallelize(my_rows, 2)
my_df = spark.createDataFrame(my_rdd, my_schema)

my_df.printSchema()
my_df.show()
new_df = to_date_df(my_df, "MM/dd/yyyy", "EventDate")
new_df.printSchema()
new_df.show()