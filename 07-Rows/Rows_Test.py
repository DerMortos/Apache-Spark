##################################################################
##                                                              ##
## @file    Rows_Test.py                                        ##
## @author  Norb Lara                                           ##
## @date    03/11/2025                                          ##
## @brief   60 DataFrame Rows and Unit Testing                  ##
##                                                              ##
##                                                              ##
##################################################################


import warnings
from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_date, col
from pyspark.sql import Row
from datetime import date

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))

class TestRows(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession \
            .builder \
            .master("local[3]") \
            .appName("Row_Test") \
            .getOrCreate()

        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())])

        my_rows = [Row("123", "2023-10-01"), Row("124", "2023-10-01"), Row("125", "2023-10-01"), Row("126", "2023-10-01")]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    ## test 1: Validate DataType
    ## cant assert the dataframe must bring it to the driver
    # def test_data_type(self):
    #     rows = to_date_df(self.my_df, "yyyy-MM-dd", "EventDate").collect()
    #     for row in rows:
    #         self.assertIsInstance(row["EventDate"], date)

    def test_data_type(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            rows = to_date_df(self.my_df, "yyyy-MM-dd", "EventDate").collect()
            for row in rows:
                self.assertIsInstance(row["EventDate"], date)

    ## test 2: Validate Data
    # def test_date_value(self):
    #     rows = to_date_df(self.my_df, "yyyy-MM-dd", "EventDate").collect()
    #     for row in rows:
    #         self.assertEqual(row["EventDate"], date(2023,10,1))

    def test_date_value(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            rows = to_date_df(self.my_df, "yyyy-MM-dd", "EventDate").collect()
            for row in rows:
                self.assertEqual(row["EventDate"], date(2023, 10, 1))