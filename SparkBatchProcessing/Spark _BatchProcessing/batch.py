import pandas as pd
from pyspark.sql import SparkSession
import lit
from datetime import date
import re


spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

# Load data from a CSV
file_location = "/home/phani/Desktop/Spark _BatchProcessing/Stocks.csv"
df = spark.read.format("CSV").option("inferSchema", True).option("header", True).load(file_location)
print(df.take(5))
data= spark.createDataFrame(df)
print(data.take(5))

# ##  Getting third year of data
# year3_data = df.filter(sql_functions.col('date').between('2015-03-08','2016-02-08').select('open','close','date').show())
# print(year3_data.take(5))
#
#
#
# ## Getting fourth year of data
# year4_data = df.filter(sql_functions.col('date').between('2015-03-08','2017-02-08').select('open','close','date').show())
# print(year4_data.take(5))


