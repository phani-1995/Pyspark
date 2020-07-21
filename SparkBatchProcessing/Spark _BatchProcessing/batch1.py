from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import avg, max, min
from pyspark.sql.types import DateType
import lit
import pyspark.sql.functions as sql_functions
import os
os.environ["PYSPARK_PYTHON"]='/usr/bin/python3'
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

# Load data from a CSV
file_location = "/home/phani/Desktop/Spark _BatchProcessing/Stocks.csv"
df = spark.read.format("CSV").option("inferSchema", True).option("header", True).load(file_location)
#print(df.show())

## Getting First part of data and applying transformation on it
try:
    year1_data = df.filter(sql_functions.col('date') <= '2015-02-08').select('date', df.open.cast('double'), df.close.cast('double'))
    year1_data.show()
    max_value=year1_data.select(max('open'))
    max_value.show()
    min_value=year1_data.select(min('open'))
    min_value.show()
    average_open_price = year1_data.select(avg('open'))
    average_open_price.show()
    #Saving data
    year1_data.write.save("year1_data")
except:
    print("syntax Error")

## Getting second part of data and applying transformations on it

year2_data = df.filter(sql_functions.col('date') >= '2015-03-08').select('date', df.open.cast('double'), df.close.cast('double'))
year2_data.show()
print("maximum close price")
year2_data.agg({'close': 'max'}).show()
print("minmum close price")
year2_data.agg({'close': 'min'}).show()
print("Average close price")
year2_data.select(avg('close')).show()
#Saving file
year2_data.write.save("year2_data")










